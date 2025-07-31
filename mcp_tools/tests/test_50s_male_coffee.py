#!/usr/bin/env python3
"""
50대 남성, 커피음료 매대 기준 진열대 분석 테스트
"""
import os
import sys
from dotenv import load_dotenv

# 상위 디렉토리의 모듈 import를 위한 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 환경변수 로드
load_dotenv()

def _create_clickhouse_client(database="plusinsight"):
    """ClickHouse 클라이언트 생성"""
    import clickhouse_connect
    
    # 환경변수 로드
    CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
    CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
    SSH_HOST = os.getenv("SSH_HOST")
    SSH_PORT = int(os.getenv("SSH_PORT", "22"))
    SSH_USERNAME = os.getenv("SSH_USERNAME")
    SSH_PASSWORD = os.getenv("SSH_PASSWORD")
    
    # SSH 터널링이 필요한 경우
    try:
        from sshtunnel import SSHTunnelForwarder
        SSH_AVAILABLE = True
    except ImportError:
        SSH_AVAILABLE = False
        
    if SSH_AVAILABLE and SSH_HOST:
        try:
            ssh_tunnel = SSHTunnelForwarder(
                (SSH_HOST, SSH_PORT),
                ssh_username=SSH_USERNAME,
                ssh_password=SSH_PASSWORD,
                remote_bind_address=(CLICKHOUSE_HOST, int(CLICKHOUSE_PORT)),
                local_bind_address=("localhost", 0),
            )
            ssh_tunnel.start()
            print(f"SSH 터널 생성: localhost:{ssh_tunnel.local_bind_port}")
            
            host = "localhost"
            port = ssh_tunnel.local_bind_port
            
        except Exception as e:
            print(f"SSH 터널 생성 실패: {e}, 직접 연결 시도")
            host = CLICKHOUSE_HOST
            port = int(CLICKHOUSE_PORT)
    else:
        # 직접 연결
        host = CLICKHOUSE_HOST
        port = int(CLICKHOUSE_PORT)
    
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=database,
        )
        print(f"ClickHouse 연결 성공: {host}:{port}, db={database}")
        return client
    except Exception as e:
        print(f"ClickHouse 연결 실패: {e}")
        return None

def test_50s_male_coffee_analysis():
    """50대 남성, 커피음료 매대 기준 분석"""
    print("=== 50대 남성, 커피음료 매대 픽업 전후 진열대 분석 ===")
    
    client = _create_clickhouse_client()
    if not client:
        return {"error": "ClickHouse 연결 실패"}
    
    # mcp_shelf.py의 쿼리를 50대 남성, 커피음료 매대 기준으로 수정
    query = """
    WITH pickup_visit_counts AS (
        SELECT
            cbe.person_seq AS person_seq,
            cba.attention_target_zone_id AS attention_target_zone_id,
            z.name AS zone_name,
            z.coords AS coords,
            MIN(cbe.`timestamp`) AS first_event_date,
            cbe.age AS age,
            cbe.gender AS gender,
            COUNT(*) AS visit_count
        FROM customer_behavior_event cbe
        LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
        LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
        WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
            AND cbe.date != '2025-06-22'
            AND cbe.event_type = 1  -- 픽업
            AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
            AND z.name IS NOT NULL
        GROUP BY
            cbe.person_seq,
            cbe.age,
            cbe.gender,
            cba.attention_target_zone_id,
            z.name,
            z.coords
    ),
    gaze_visit_counts AS (
        SELECT
            cbe.person_seq AS person_seq,
            cba.attention_target_zone_id AS attention_target_zone_id,
            z.name AS zone_name,
            z.coords AS coords,
            MIN(cbe.`timestamp`) AS first_event_date,
            cbe.age AS age,
            cbe.gender AS gender,
            COUNT(*) AS visit_count
        FROM customer_behavior_event cbe
        LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
        LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
        WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
            AND cbe.date != '2025-06-22'
            AND cbe.event_type = 0  -- 응시
            AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
            AND z.name IS NOT NULL
        GROUP BY
            cbe.person_seq,
            cbe.age,
            cbe.gender,
            cba.attention_target_zone_id,
            z.name,
            z.coords
    ),
    pickup_df AS (
        SELECT
            person_seq,
            attention_target_zone_id,
            zone_name,
            coords,
            first_event_date,
            age,
            gender,
            visit_count,
            ROW_NUMBER() OVER (
                PARTITION BY person_seq
                ORDER BY first_event_date
            ) AS pickup_order
        FROM pickup_visit_counts
    ),
    gaze_df AS (
        SELECT
            person_seq,
            attention_target_zone_id,
            zone_name,
            coords,
            first_event_date,
            age,
            gender,
            visit_count,
            ROW_NUMBER() OVER (
                PARTITION BY person_seq
                ORDER BY first_event_date
            ) AS gaze_order
        FROM gaze_visit_counts
        WHERE visit_count >= 3
    ),
    combined_events AS (
        -- 픽업 이벤트 (모든 방문)
        SELECT 
            person_seq,
            first_event_date,
            zone_name,
            coords,
            age,
            gender,
            'P' as event_type_label  -- P for Pickup
        FROM pickup_df
        UNION ALL
        -- 응시 이벤트 (3회 이상 방문한 존만)
        SELECT 
            person_seq,
            first_event_date,
            zone_name,
            coords,
            age,
            gender,
            'G' as event_type_label  -- G for Gaze
        FROM gaze_df
    ),
    integrated_routes AS (
        SELECT
            person_seq,
            multiIf(
                age >= 60, '60대 이상',
                age >= 50, '50대',
                age >= 40, '40대',
                age >= 30, '30대',
                age >= 20, '20대',
                age >= 10, '10대',
                age IS NULL, '미상',
                '10세 미만'
            ) AS age_group,
            multiIf(
                gender = 0, '남자',
                gender = 1, '여자',
                '미상'
            ) AS gender_label,
            -- 첫 번째 픽업한 매대 이름
            arrayElement(
                arrayMap(x -> x.2,
                    arrayFilter(y -> y.4 = 'P', arraySort(groupArray((first_event_date, zone_name, coords, event_type_label))))
                ), 1
            ) AS first_pickup_zone,
            -- 픽업 직전 1번째 응시 매대 (가장 마지막)
            arrayElement(
                arrayReverse(
                    arrayMap(x -> x.2,
                        arraySlice(
                            arrayFilter(y -> y.4 = 'G', arraySort(groupArray((first_event_date, zone_name, coords, event_type_label)))),
                            1,
                            arrayFirstIndex(x -> x = 'P', 
                                arrayMap(x -> x.4, arraySort(groupArray((first_event_date, zone_name, coords, event_type_label))))
                            ) - 1
                        )
                    )
                ), 1
            ) AS before_pickup_gaze_1st,
            -- 픽업 후 1번째 응시 매대
            arrayElement(
                arrayMap(x -> x.2,
                    arraySlice(
                        arrayFilter(y -> y.4 = 'G', arraySort(groupArray((first_event_date, zone_name, coords, event_type_label)))),
                        arrayCount(x -> x = 'G', 
                            arraySlice(
                                arrayMap(z -> z.4, arraySort(groupArray((first_event_date, zone_name, coords, event_type_label)))),
                                1,
                                arrayFirstIndex(x -> x = 'P', 
                                    arrayMap(z -> z.4, arraySort(groupArray((first_event_date, zone_name, coords, event_type_label))))
                                ) - 1
                            )
                        ) + 1,
                        length(arrayFilter(y -> y.4 = 'G', arraySort(groupArray((first_event_date, zone_name, coords, event_type_label)))))
                    )
                ), 1
            ) AS after_pickup_gaze_1st
        FROM combined_events
        GROUP BY person_seq, age, gender
    ),
    pivot AS (
        SELECT
            person_seq,
            age_group,
            gender_label,
            first_pickup_zone,
            before_pickup_gaze_1st,
            after_pickup_gaze_1st
        FROM integrated_routes
        WHERE age_group = '50대' 
            AND gender_label = '남자'
            AND first_pickup_zone = '커피음료'
        ORDER BY person_seq
    ),
    shelf_analysis AS (
        -- 픽업 직전 마지막 응시매대 (1st만, 계산대 제외)
        SELECT 
            'before' as period,
            COALESCE(NULLIF(before_pickup_gaze_1st, ''), '진열대없음') as shelf_name
        FROM pivot
        WHERE COALESCE(NULLIF(before_pickup_gaze_1st, ''), '진열대없음') != '계산대'
        
        UNION ALL
        
        -- 픽업 후 첫 번째 응시매대 (1st만, 계산대 제외)
        SELECT 
            'after' as period,
            COALESCE(NULLIF(after_pickup_gaze_1st, ''), '진열대없음') as shelf_name
        FROM pivot
        WHERE COALESCE(NULLIF(after_pickup_gaze_1st, ''), '진열대없음') != '계산대'
    ),
    
    -- 진열대별 집계 및 비율 계산 (정수 반올림)
    aggregated AS (
        SELECT 
            period,
            shelf_name,
            COUNT(*) as visit_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY period)) as percentage
        FROM shelf_analysis
        GROUP BY period, shelf_name
    ),
    
    -- 기간별 Top 5 순위 매기기
    ranked AS (
        SELECT 
            period,
            shelf_name,
            percentage,
            ROW_NUMBER() OVER (PARTITION BY period ORDER BY percentage DESC) as rank
        FROM aggregated
        WHERE percentage > 0  -- 0% 제외
    ),
    
    -- Top 5만 필터링
    top5 AS (
        SELECT *
        FROM ranked
        WHERE rank <= 5
    )
    
    -- 최종 결과 (픽업 전/후 나란히 배치)
    SELECT 
        COALESCE(b.rank, a.rank) as no,
        b.shelf_name as before_shelf,
        CONCAT(CAST(b.percentage as String), '%') as before_pct,
        a.shelf_name as after_shelf,
        CONCAT(CAST(a.percentage as String), '%') as after_pct
    FROM 
        (SELECT * FROM top5 WHERE period = 'after') a
    FULL OUTER JOIN 
        (SELECT * FROM top5 WHERE period = 'before') b
    ON a.rank = b.rank
    ORDER BY COALESCE(b.rank, a.rank)
    """
    
    try:
        result = client.query(query)
        print(f"✅ 50대 남성, 커피음료 매대 분석 완료: {len(result.result_rows):,}행")
        
        if len(result.result_rows) == 0:
            print("❌ 결과가 없습니다. 커피음료 매대 데이터가 없거나 조건에 맞는 고객이 없을 수 있습니다.")
            return []
        
        print("\n📊 분석 결과:")
        print("=" * 80)
        print(f"{'No':>3} | {'픽업직전 마지막':^15} | {'비율':^8} | {'픽업후 첫번째':^15} | {'비율':^8}")
        print("=" * 80)
        
        for row in result.result_rows:
            no = row[0] if row[0] else "-"
            before_shelf = row[1] if row[1] else "-"
            before_pct = row[2] if row[2] else "-"
            after_shelf = row[3] if row[3] else "-"
            after_pct = row[4] if row[4] else "-"
            
            print(f"{no:>3} | {before_shelf:^15} | {before_pct:^8} | {after_shelf:^15} | {after_pct:^8}")
            
        return result.result_rows
        
    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {e}")
        return {"error": str(e)}

def check_coffee_zone_exists():
    """커피음료 매대가 존재하는지 확인"""
    print("\n=== 커피음료 매대 존재 여부 확인 ===")
    
    client = _create_clickhouse_client()
    if not client:
        return
    
    # 존재하는 매대 이름들 확인
    query = """
    SELECT DISTINCT z.name as zone_name, COUNT(*) as event_count
    FROM customer_behavior_event cbe
    LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
    LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
    WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
        AND cbe.date != '2025-06-22'
        AND z.name IS NOT NULL
        AND z.name LIKE '%커피%' OR z.name LIKE '%음료%' OR z.name LIKE '%coffee%'
    GROUP BY z.name
    ORDER BY event_count DESC
    """
    
    try:
        result = client.query(query)
        print(f"✅ 커피/음료 관련 매대 검색 완료: {len(result.result_rows):,}개")
        
        if len(result.result_rows) > 0:
            print("\n📍 발견된 커피/음료 관련 매대:")
            for row in result.result_rows:
                print(f"  - {row[0]}: {row[1]:,}회 이벤트")
        else:
            print("❌ 커피/음료 관련 매대를 찾을 수 없습니다.")
            
            # 전체 매대 이름 샘플 확인
            sample_query = """
            SELECT DISTINCT z.name as zone_name, COUNT(*) as event_count
            FROM customer_behavior_event cbe
            LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
            LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
            WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
                AND cbe.date != '2025-06-22'
                AND z.name IS NOT NULL
            GROUP BY z.name
            ORDER BY event_count DESC
            LIMIT 10
            """
            
            sample_result = client.query(sample_query)
            print(f"\n📋 전체 매대 이름 샘플 (Top 10):")
            for row in sample_result.result_rows:
                print(f"  - {row[0]}: {row[1]:,}회 이벤트")
                
    except Exception as e:
        print(f"❌ 매대 확인 실패: {e}")

if __name__ == "__main__":
    print("🧪 50대 남성, 커피음료 매대 분석 테스트 시작")
    print("=" * 60)
    
    # 먼저 커피음료 매대가 존재하는지 확인
    check_coffee_zone_exists()
    
    # 메인 분석 실행
    test_50s_male_coffee_analysis()
    
    print("\n✅ 테스트 완료")