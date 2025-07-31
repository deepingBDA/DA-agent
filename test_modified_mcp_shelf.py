#!/usr/bin/env python3
"""
수정된 mcp_shelf.py 테스트
리버스 엔지니어링 결과를 적용한 함수 테스트
"""

import os
import sys
from typing import Dict, Any, List, Union, Optional
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# mcp_tools 경로를 추가하고 직접 모듈을 import
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mcp_tools'))

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


def test_modified_function():
    """수정된 mcp_shelf.py의 함수를 직접 로직으로 테스트"""
    
    print("=" * 60)
    print("🧪 수정된 mcp_shelf.py 함수 테스트")
    print("=" * 60)
    
    # 테스트 파라미터
    start_date = "2025-06-12"
    end_date = "2025-07-12"
    exclude_dates = ["2025-06-22"]
    target_shelves = ["빵"]
    age_groups = ["10대"]
    gender_labels = ["여자"]
    exclude_shelves = []  # 아무것도 제외하지 않음 (계산대는 하드코딩으로 제외됨)
    top_n = 5
    
    print("📋 테스트 파라미터:")
    print(f"  - start_date: {start_date}")
    print(f"  - end_date: {end_date}")
    print(f"  - exclude_dates: {exclude_dates}")
    print(f"  - target_shelves: {target_shelves}")
    print(f"  - age_groups: {age_groups}")
    print(f"  - gender_labels: {gender_labels}")
    print(f"  - exclude_shelves: {exclude_shelves}")
    print(f"  - top_n: {top_n}")
    print()
    
    client = _create_clickhouse_client()
    if not client:
        print("❌ ClickHouse 연결 실패")
        return
    
    # 수정된 로직 구현
    exclude_dates_str = "', '".join(exclude_dates)
    date_condition = f"AND cbe.date NOT IN ('{exclude_dates_str}')"
    
    # pivot 테이블 필터링 조건들
    target_shelf_filter = "1=1"
    if target_shelves:
        target_shelves_str = "', '".join(target_shelves)
        target_shelf_filter = f"first_pickup_zone IN ('{target_shelves_str}')"
    
    age_filter = "1=1"
    if age_groups:
        age_filters = []
        for age_group in age_groups:
            age_filters.append(f"age_group = '{age_group}'")
        if age_filters:
            age_filter = f"({' OR '.join(age_filters)})"
    
    gender_filter = "1=1"
    if gender_labels:
        gender_filters = []
        for gender_label in gender_labels:
            gender_filters.append(f"gender_label = '{gender_label}'")
        if gender_filters:
            gender_filter = f"({' OR '.join(gender_filters)})"
    
    exclude_shelf_condition = ""
    if exclude_shelves:
        exclude_shelves_str = "', '".join(exclude_shelves)
        exclude_shelf_condition = f"AND COALESCE(NULLIF(shelf_name, ''), '진열대없음') NOT IN ('{exclude_shelves_str}')"
    
    print("🔍 생성된 필터 조건들:")
    print(f"  - target_shelf_filter: {target_shelf_filter}")
    print(f"  - age_filter: {age_filter}")
    print(f"  - gender_filter: {gender_filter}")
    print(f"  - exclude_shelf_condition: {exclude_shelf_condition}")
    print()
    
    # 수정된 분석 쿼리
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
        ORDER BY person_seq
    ),
    filtered_pivot AS (
        -- pivot 테이블에서 조건 필터링
        SELECT *
        FROM pivot
        WHERE first_pickup_zone IS NOT NULL
            AND ({target_shelf_filter})
            AND ({age_filter})
            AND ({gender_filter})
    ),
    shelf_analysis AS (
        -- 픽업 직전 응시매대 (계산대 제외)
        SELECT 
            'before' as period,
            COALESCE(NULLIF(before_pickup_gaze_1st, ''), '진열대없음') as shelf_name
        FROM filtered_pivot
        WHERE COALESCE(NULLIF(before_pickup_gaze_1st, ''), '진열대없음') != '계산대'
            {exclude_shelf_condition}
        
        UNION ALL
        
        -- 픽업 후 응시매대 (계산대 제외)
        SELECT 
            'after' as period,
            COALESCE(NULLIF(after_pickup_gaze_1st, ''), '진열대없음') as shelf_name
        FROM filtered_pivot
        WHERE COALESCE(NULLIF(after_pickup_gaze_1st, ''), '진열대없음') != '계산대'
            {exclude_shelf_condition}
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
        WHERE rank <= {top_n}
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
        print("🔄 수정된 분석 쿼리 실행 중...")
        
        result = client.query(analysis_query)
        analysis_rows = result.result_rows
        print(f"✅ 분석 완료: {len(analysis_rows):,}행")
        
        print("\n📊 수정된 함수 결과:")
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


if __name__ == "__main__":
    print("🚀 수정된 mcp_shelf.py 테스트 프로그램 시작")
    print()
    
    # 수정된 함수 테스트
    test_modified_function()