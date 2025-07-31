#!/usr/bin/env python3
"""
진열대 분석 완전한 테스트 코드
사용자 지정 파라미터로 전체 분석 실행

파라미터:
- 분석 기간: 2025년 6월 12일 ~ 2025년 7월 12일
- 제외 날짜: 2025년 6월 22일
- 첫 픽업 진열대: 빵
- 연령대: 10대
- 성별: 여성
- 제외 진열대: 진열대없음, 전자렌지
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


def complete_shelf_analysis(
    start_date: str = "2025-06-12",
    end_date: str = "2025-07-12",
    exclude_dates: List[str] = None,
    target_shelves: List[str] = None,
    exclude_shelves: List[str] = None,
    age_groups: List[str] = None,
    gender_labels: List[str] = None,
    top_n: int = 5,
    exclude_from_top: List[str] = None,
    period: str = "both"
):
    """
    완전한 진열대 분석 함수
    """
    print(f"🔍 [DEBUG] complete_shelf_analysis 호출됨")
    print(f"  start_date: {start_date}")
    print(f"  end_date: {end_date}")
    print(f"  target_shelves: {target_shelves}")
    print(f"  age_groups: {age_groups}")
    print(f"  gender_labels: {gender_labels}")
    print(f"  exclude_dates: {exclude_dates}")
    print(f"  top_n: {top_n}")
    
    client = _create_clickhouse_client()
    if not client:
        return {"error": "ClickHouse 연결 실패"}
    
    # 안전장치: 너무 넓은 범위 쿼리 방지
    if not target_shelves and not age_groups and not gender_labels:
        return {
            "error": "분석 범위가 너무 넓습니다. target_shelves, age_groups, gender_labels 중 최소 하나는 지정해야 합니다.",
            "suggestion": "예: target_shelves=['빵'], age_groups=['20대'], gender_labels=['여자']"
        }
    
    # 파라미터 처리
    exclude_dates = exclude_dates or ['2025-06-22']
    exclude_shelves = exclude_shelves or ['계산대']
    exclude_from_top = exclude_from_top or []
    
    # 날짜 조건
    exclude_dates_str = "', '".join(exclude_dates)
    date_condition = f"AND cbe.date NOT IN ('{exclude_dates_str}')"
    
    # 연령대 조건
    age_condition = ""
    if age_groups:
        age_conditions = []
        for age_group in age_groups:
            if age_group == '10대':
                age_conditions.append("(cbe.age >= 10 AND cbe.age < 20)")
            elif age_group == '20대':
                age_conditions.append("(cbe.age >= 20 AND cbe.age < 30)")
            elif age_group == '30대':
                age_conditions.append("(cbe.age >= 30 AND cbe.age < 40)")
            elif age_group == '40대':
                age_conditions.append("(cbe.age >= 40 AND cbe.age < 50)")
            elif age_group == '50대':
                age_conditions.append("(cbe.age >= 50 AND cbe.age < 60)")
            elif age_group == '60대 이상':
                age_conditions.append("(cbe.age >= 60)")
            elif age_group == '미상':
                age_conditions.append("(cbe.age IS NULL)")
        if age_conditions:
            age_condition = f"AND ({' OR '.join(age_conditions)})"
    
    # 성별 조건
    gender_condition = ""
    if gender_labels:
        gender_conditions = []
        for gender_label in gender_labels:
            if gender_label == '남자':
                gender_conditions.append("cbe.gender = 0")
            elif gender_label == '여자':
                gender_conditions.append("cbe.gender = 1")
            elif gender_label == '미상':
                gender_conditions.append("cbe.gender IS NULL")
        if gender_conditions:
            gender_condition = f"AND ({' OR '.join(gender_conditions)})"
    
    # 진열대 필터 조건
    target_shelf_condition = ""
    if target_shelves:
        target_shelves_str = "', '".join(target_shelves)
        target_shelf_condition = f"AND z.name IN ('{target_shelves_str}')"
    
    exclude_shelf_condition = ""
    if exclude_shelves:
        exclude_shelves_str = "', '".join(exclude_shelves)
        exclude_shelf_condition = f"AND COALESCE(NULLIF(shelf_name, ''), '진열대없음') NOT IN ('{exclude_shelves_str}')"
    
    exclude_from_top_condition = ""
    if exclude_from_top:
        exclude_from_top_str = "', '".join(exclude_from_top)
        exclude_from_top_condition = f"AND shelf_name NOT IN ('{exclude_from_top_str}')"
    
    print("🔍 [DEBUG] 쿼리 조건들:")
    print(f"  date_condition: {date_condition}")
    print(f"  age_condition: {age_condition}")
    print(f"  gender_condition: {gender_condition}")
    print(f"  target_shelf_condition: {target_shelf_condition}")
    print(f"  exclude_shelf_condition: {exclude_shelf_condition}")
    
    # 완전한 분석 쿼리
    analysis_query = f"""
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
        WHERE cbe.date BETWEEN '{start_date}' AND '{end_date}'
            {date_condition}
            AND cbe.event_type = 1  -- 픽업
            AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
            AND z.name IS NOT NULL
            {age_condition}
            {gender_condition}
            {target_shelf_condition}
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
        WHERE cbe.date BETWEEN '{start_date}' AND '{end_date}'
            {date_condition}
            AND cbe.event_type = 0  -- 응시
            AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
            AND z.name IS NOT NULL
            {age_condition}
            {gender_condition}
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
        -- 픽업 이벤트
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
        -- 응시 이벤트 (3회 이상 방문한 존만)
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
            -- 픽업 직전 2번째 응시 매대
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
                ), 2
            ) AS before_pickup_gaze_2nd,
            -- 픽업 직전 3번째 응시 매대
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
                ), 3
            ) AS before_pickup_gaze_3rd,
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
            ) AS after_pickup_gaze_1st,
            -- 픽업 후 2번째 응시 매대
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
                ), 2
            ) AS after_pickup_gaze_2nd,
            -- 픽업 후 3번째 응시 매대
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
                ), 3
            ) AS after_pickup_gaze_3rd
        FROM combined_events
        GROUP BY person_seq
        HAVING arrayFirstIndex(x -> x = 'P', 
            arrayMap(x -> x.4, arraySort(groupArray((first_event_date, zone_name, coords, event_type_label))))
        ) > 0  -- 픽업 이벤트가 있는 고객만
    ),
    
    -- 픽업 전/후 진열대 분석 데이터 생성
    shelf_analysis AS (
        SELECT 'before' as period, COALESCE(NULLIF(before_pickup_gaze_1st, ''), '진열대없음') as shelf_name FROM integrated_routes WHERE before_pickup_gaze_1st IS NOT NULL AND before_pickup_gaze_1st != ''
        UNION ALL
        SELECT 'before' as period, COALESCE(NULLIF(before_pickup_gaze_2nd, ''), '진열대없음') as shelf_name FROM integrated_routes WHERE before_pickup_gaze_2nd IS NOT NULL AND before_pickup_gaze_2nd != ''
        UNION ALL
        SELECT 'before' as period, COALESCE(NULLIF(before_pickup_gaze_3rd, ''), '진열대없음') as shelf_name FROM integrated_routes WHERE before_pickup_gaze_3rd IS NOT NULL AND before_pickup_gaze_3rd != ''
        UNION ALL
        SELECT 'after' as period, COALESCE(NULLIF(after_pickup_gaze_1st, ''), '진열대없음') as shelf_name FROM integrated_routes WHERE after_pickup_gaze_1st IS NOT NULL AND after_pickup_gaze_1st != ''
        UNION ALL
        SELECT 'after' as period, COALESCE(NULLIF(after_pickup_gaze_2nd, ''), '진열대없음') as shelf_name FROM integrated_routes WHERE after_pickup_gaze_2nd IS NOT NULL AND after_pickup_gaze_2nd != ''
        UNION ALL
        SELECT 'after' as period, COALESCE(NULLIF(after_pickup_gaze_3rd, ''), '진열대없음') as shelf_name FROM integrated_routes WHERE after_pickup_gaze_3rd IS NOT NULL AND after_pickup_gaze_3rd != ''
    ),
    
    -- 진열대별 방문 집계 및 비율 계산
    aggregated AS (
        SELECT 
            period,
            shelf_name,
            COUNT(*) as visit_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY period), 2) as percentage
        FROM shelf_analysis
        WHERE 1=1 {exclude_shelf_condition}
        GROUP BY period, shelf_name
    ),
    
    -- 기간별 Top N 순위 매기기
    ranked AS (
        SELECT 
            period,
            shelf_name,
            percentage,
            ROW_NUMBER() OVER (PARTITION BY period ORDER BY percentage DESC) as rank
        FROM aggregated
        WHERE percentage > 0
        {exclude_from_top_condition}
    ),
    
    -- Top N만 필터링
    topN AS (
        SELECT *
        FROM ranked
        WHERE rank <= {top_n}
    ),
    
    -- 픽업 전 TopN 결과
    before_results AS (
        SELECT 
            'BEFORE' as analysis_type,
            rank as no,
            shelf_name,
            percentage
        FROM topN 
        WHERE period = 'before'
    ),
    
    -- 픽업 후 TopN 결과
    after_results AS (
        SELECT 
            'AFTER' as analysis_type,
            rank as no,
            shelf_name,
            percentage
        FROM topN 
        WHERE period = 'after'
    )
    
    -- 최종 결과
    SELECT * FROM before_results
    UNION ALL
    SELECT * FROM after_results
    ORDER BY analysis_type, no
    """
    
    try:
        print(f"🔍 [DEBUG] 완전한 분석 쿼리 실행 시작")
        
        result = client.query(analysis_query)
        analysis_rows = result.result_rows
        print(f"✅ 진열대 분석 완료: {len(analysis_rows):,}행")
        
        # 결과를 튜플 형태로 변환
        formatted_results = []
        for row in analysis_rows:
            period, rank, shelf_name, percentage = row
            formatted_results.append((period, rank, shelf_name, percentage))
        
        return formatted_results
        
    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {e}")
        return {"error": str(e)}


def test_complete_shelf_analysis():
    """완전한 진열대 분석 테스트"""
    
    print("=" * 60)
    print("🧪 완전한 진열대 분석 테스트 시작")
    print("=" * 60)
    
    # 테스트 파라미터 설정
    test_params = {
        "start_date": "2025-06-12",
        "end_date": "2025-07-12", 
        "exclude_dates": ["2025-06-22"],
        "target_shelves": ["빵"],
        "age_groups": ["10대"],
        "gender_labels": ["여자"],
        "exclude_shelves": ["진열대없음", "전자렌지"],
        "top_n": 5,
        "period": "both"
    }
    
    print("📋 테스트 파라미터:")
    for key, value in test_params.items():
        print(f"  - {key}: {value}")
    print()
    
    try:
        print("🔄 complete_shelf_analysis 함수 호출 중...")
        print("-" * 40)
        
        # 함수 호출
        result = complete_shelf_analysis(**test_params)
        
        print("✅ 함수 호출 완료!")
        print("-" * 40)
        
        # 결과 출력
        print("📊 분석 결과:")
        print("-" * 40)
        
        if isinstance(result, dict) and "error" in result:
            print(f"❌ 오류 발생: {result['error']}")
        elif isinstance(result, list):
            print(f"📈 총 {len(result)}개의 결과:")
            print()
            
            # BEFORE 결과 출력
            before_results = [r for r in result if r[0] == 'BEFORE']
            if before_results:
                print("🔍 픽업 전 (BEFORE) Top 진열대:")
                for item in before_results:
                    period, rank, shelf_name, percentage = item
                    print(f"  {rank:2d}위: {shelf_name:15s} ({percentage:6.2f}%)")
                print()
            
            # AFTER 결과 출력
            after_results = [r for r in result if r[0] == 'AFTER']
            if after_results:
                print("🔍 픽업 후 (AFTER) Top 진열대:")
                for item in after_results:
                    period, rank, shelf_name, percentage = item
                    print(f"  {rank:2d}위: {shelf_name:15s} ({percentage:6.2f}%)")
                print()
                
            # 전체 결과 요약
            print("📋 전체 결과 요약:")
            for i, item in enumerate(result, 1):
                period, rank, shelf_name, percentage = item
                print(f"  {i:2d}. [{period:6s}] {rank}위: {shelf_name:15s} ({percentage:6.2f}%)")
                
        else:
            print(f"🔍 결과 타입: {type(result)}")
            print(f"📄 결과 내용: {result}")
            
    except Exception as e:
        print(f"❌ 테스트 실행 중 오류 발생:")
        print(f"   오류 타입: {type(e).__name__}")
        print(f"   오류 메시지: {str(e)}")
        
        # 상세 오류 정보
        import traceback
        print("\n🔍 상세 오류 정보:")
        print("-" * 40)
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("🧪 테스트 완료")
    print("=" * 60)


if __name__ == "__main__":
    print("🚀 완전한 진열대 분석 테스트 프로그램 시작")
    print()
    
    # 완전한 분석 테스트
    test_complete_shelf_analysis()