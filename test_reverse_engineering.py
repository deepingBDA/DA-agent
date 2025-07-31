#!/usr/bin/env python3
"""
리버스 엔지니어링 테스트 - 이미지 결과와 동일하게 만들기
원본 쿼리 방식을 따라서 pivot 테이블 생성 후 필터링
"""

import os
import sys
from typing import Dict, Any, List, Union, Optional
from dotenv import load_dotenv

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
                allow_agent=False,
                host_pkey_directories=[]
            )
            ssh_tunnel.start()
            local_port = ssh_tunnel.local_bind_port
            print(f"SSH 터널 생성: localhost:{local_port}")
            
            client = clickhouse_connect.get_client(
                host="localhost",
                port=local_port,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=database
            )
            print(f"ClickHouse 연결 성공: localhost:{local_port}, db={database}")
            return client
            
        except Exception as e:
            print(f"SSH 터널 연결 실패: {e}")
            return None
    else:
        # 직접 연결
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=int(CLICKHOUSE_PORT),
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=database
            )
            print(f"ClickHouse 직접 연결 성공: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}, db={database}")
            return client
        except Exception as e:
            print(f"ClickHouse 직접 연결 실패: {e}")
            return None


def reverse_engineering_test():
    """리버스 엔지니어링 테스트 - 원본 방식으로 pivot 생성 후 필터링"""
    
    print("🔍 리버스 엔지니어링 테스트 시작")
    print("=" * 60)
    
    client = _create_clickhouse_client()
    if not client:
        return {"error": "ClickHouse 연결 실패"}
    
    # 가설 1: 원본 쿼리 방식 - 모든 데이터로 pivot 생성 후 필터링
    print("📋 가설 1: 모든 데이터로 pivot 생성 후 조건 필터링")
    
    pivot_query = """
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
        SELECT 
            person_seq,
            first_event_date,
            zone_name,
            coords,
            age,
            gender,
            'P' as event_type_label
        FROM pickup_df
        UNION ALL
        SELECT 
            person_seq,
            first_event_date,
            zone_name,
            coords,
            age,
            gender,
            'G' as event_type_label
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
            -- 픽업 직전 1번째 응시 매대
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
        WHERE first_pickup_zone IS NOT NULL  -- 픽업이 있는 고객만
    ),
    filtered_pivot AS (
        SELECT *
        FROM pivot
        WHERE first_pickup_zone = '빵'
            AND age_group = '10대'
            AND gender_label = '여자'
    ),
    shelf_analysis AS (
        -- 픽업 직전 응시매대 (계산대 제외)
        SELECT 
            'before' as period,
            COALESCE(NULLIF(before_pickup_gaze_1st, ''), '진열대없음') as shelf_name
        FROM filtered_pivot
        WHERE COALESCE(NULLIF(before_pickup_gaze_1st, ''), '진열대없음') != '계산대'
        
        UNION ALL
        
        -- 픽업 후 응시매대 (계산대 제외)
        SELECT 
            'after' as period,
            COALESCE(NULLIF(after_pickup_gaze_1st, ''), '진열대없음') as shelf_name
        FROM filtered_pivot
        WHERE COALESCE(NULLIF(after_pickup_gaze_1st, ''), '진열대없음') != '계산대'
    ),
    aggregated AS (
        SELECT 
            period,
            shelf_name,
            COUNT(*) as visit_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY period)) as percentage
        FROM shelf_analysis
        GROUP BY period, shelf_name
    ),
    ranked AS (
        SELECT 
            period,
            shelf_name,
            percentage,
            ROW_NUMBER() OVER (PARTITION BY period ORDER BY percentage DESC) as rank
        FROM aggregated
        WHERE percentage > 0
    ),
    top5 AS (
        SELECT *
        FROM ranked
        WHERE rank <= 5
    )
    
    SELECT 
        period,
        rank,
        shelf_name,
        percentage
    FROM top5
    ORDER BY period DESC, rank
    """
    
    try:
        print("🔄 리버스 엔지니어링 쿼리 실행 중...")
        
        result = client.query(pivot_query)
        analysis_rows = result.result_rows
        print(f"✅ 분석 완료: {len(analysis_rows):,}행")
        
        print("\n📊 리버스 엔지니어링 결과:")
        print("-" * 50)
        
        # BEFORE 결과 출력
        before_results = [r for r in analysis_rows if r[0] == 'before']
        if before_results:
            print("🔍 픽업 전 (BEFORE) Top 5:")
            for period, rank, shelf_name, percentage in before_results:
                print(f"  {rank:2d}위: {shelf_name:15s} ({percentage:4.0f}%)")
            print()
        
        # AFTER 결과 출력
        after_results = [r for r in analysis_rows if r[0] == 'after']
        if after_results:
            print("🔍 픽업 후 (AFTER) Top 5:")
            for period, rank, shelf_name, percentage in after_results:
                print(f"  {rank:2d}위: {shelf_name:15s} ({percentage:4.0f}%)")
            print()
        
        # 이미지와 비교
        print("🔍 이미지 결과와 비교:")
        print("-" * 30)
        print("이미지 (BEFORE): 진열대없음(46%) → 빵(34%) → 전자렌지(3%)")
        print("이미지 (AFTER):  진열대없음(33%) → 빵(11%) → 도시락,김밥(8%)")
        
        return analysis_rows
        
    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {e}")
        return {"error": str(e)}


def test_additional_hypotheses():
    """추가 가설들 테스트"""
    
    print("\n🔍 추가 가설 테스트")
    print("=" * 60)
    
    client = _create_clickhouse_client()
    if not client:
        return
    
    # 가설 2: 응시 조건 없이 테스트 (visit_count >= 3 제거)
    print("📋 가설 2: 응시 조건(visit_count >= 3) 제거")
    
    no_visit_count_query = """
    WITH pickup_visit_counts AS (
        SELECT
            cbe.person_seq AS person_seq,
            cba.attention_target_zone_id AS attention_target_zone_id,
            z.name AS zone_name,
            MIN(cbe.`timestamp`) AS first_event_date,
            cbe.age AS age,
            cbe.gender AS gender,
            COUNT(*) AS visit_count
        FROM customer_behavior_event cbe
        LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
        LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
        WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
            AND cbe.date != '2025-06-22'
            AND cbe.event_type = 1
            AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
            AND z.name IS NOT NULL
        GROUP BY cbe.person_seq, cbe.age, cbe.gender, cba.attention_target_zone_id, z.name
    ),
    gaze_visit_counts AS (
        SELECT
            cbe.person_seq AS person_seq,
            cba.attention_target_zone_id AS attention_target_zone_id,
            z.name AS zone_name,
            MIN(cbe.`timestamp`) AS first_event_date,
            cbe.age AS age,
            cbe.gender AS gender,
            COUNT(*) AS visit_count
        FROM customer_behavior_event cbe
        LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
        LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
        WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
            AND cbe.date != '2025-06-22'
            AND cbe.event_type = 0
            AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
            AND z.name IS NOT NULL
        GROUP BY cbe.person_seq, cbe.age, cbe.gender, cba.attention_target_zone_id, z.name
        -- visit_count >= 3 조건 제거
    )
    
    SELECT 
        COUNT(DISTINCT person_seq) as total_customers,
        COUNT(*) as total_events
    FROM pickup_visit_counts p
    WHERE EXISTS (
        SELECT 1 FROM gaze_visit_counts g 
        WHERE g.person_seq = p.person_seq
    )
    """
    
    try:
        result = client.query(no_visit_count_query)
        rows = result.result_rows
        if rows:
            total_customers, total_events = rows[0]
            print(f"  📈 응시 조건 없이: 고객 {total_customers}명, 이벤트 {total_events}개")
            
    except Exception as e:
        print(f"  ❌ 가설 2 테스트 실패: {e}")


if __name__ == "__main__":
    print("🚀 리버스 엔지니어링 테스트 프로그램 시작")
    print()
    
    # 메인 리버스 엔지니어링 테스트
    reverse_engineering_test()
    
    # 추가 가설 테스트
    test_additional_hypotheses()