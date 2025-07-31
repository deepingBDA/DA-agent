#!/usr/bin/env python3
"""
진열대 분석 수정된 테스트 코드
원본 서버 로직과 정확히 동일하게 구현

파라미터:
- 분석 기간: 2025년 6월 12일 ~ 2025년 7월 12일
- 제외 날짜: 2025년 6월 22일
- 첫 픽업 진열대: 빵
- 연령대: 10대
- 성별: 여성
- 제외 진열대: 진열대없음, 전자렌지 (+ 계산대는 무조건 제외)
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


def fixed_shelf_analysis(
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
    원본 서버 로직과 정확히 동일한 진열대 분석 함수
    """
    print(f"🔍 [DEBUG] fixed_shelf_analysis 호출됨")
    print(f"  start_date: {start_date}")
    print(f"  end_date: {end_date}")
    print(f"  target_shelves: {target_shelves}")
    print(f"  age_groups: {age_groups}")
    print(f"  gender_labels: {gender_labels}")
    print(f"  exclude_dates: {exclude_dates}")
    print(f"  exclude_shelves: {exclude_shelves}")
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
    
    # 파라미터 처리 (원본과 동일)
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
    
    # exclude_shelves는 shelf_analysis에서 추가 필터링용
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
    
    # 원본과 정확히 동일한 분석 쿼리
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
            -- 시간순으로 정렬된 통합 경로
            arrayStringConcat(
                arrayMap(x -> concat(x.2, '(', x.4, ')'), arraySort(
                    groupArray((first_event_date, zone_name, coords, event_type_label))
                )), ' → '
            ) AS integrated_route,
            -- 시간순 존 이름들
            arrayMap(x -> x.2,
                arraySort(groupArray((first_event_date, zone_name, coords, event_type_label)))
            ) AS zone_names,
            -- 시간순 좌표들
            arrayMap(x -> x.3,
                arraySort(groupArray((first_event_date, zone_name, coords, event_type_label)))
            ) AS zone_coords,
            -- 시간순 이벤트 발생시간들
            arrayMap(x -> x.1,
                arraySort(groupArray((first_event_date, zone_name, coords, event_type_label)))
            ) AS event_timestamps,
            -- 시간순 이벤트 타입들
            arrayMap(x -> x.4,
                arraySort(groupArray((first_event_date, zone_name, coords, event_type_label)))
            ) AS event_types,
            -- 첫 번째 픽업 전 매대 방문 수 (응시 이벤트만)
            multiIf(
                arrayFirstIndex(x -> x = 'P', 
                    arrayMap(x -> x.4, arraySort(groupArray((first_event_date, zone_name, coords, event_type_label))))
                ) = 0, 0,
                arrayCount(x -> x = 'G', 
                    arraySlice(
                        arrayMap(x -> x.4, arraySort(groupArray((first_event_date, zone_name, coords, event_type_label)))),
                        1,
                        arrayFirstIndex(x -> x = 'P', 
                            arrayMap(x -> x.4, arraySort(groupArray((first_event_date, zone_name, coords, event_type_label))))
                        ) - 1
                    )
                )
            ) AS gaze_count_before_first_pickup,
            -- 첫 번째 픽업 직후 매대 방문 수 (응시 이벤트만)
            multiIf(
                arrayFirstIndex(x -> x = 'P', 
                    arrayMap(x -> x.4, arraySort(groupArray((first_event_date, zone_name, coords, event_type_label))))
                ) = 0, 0,
                arrayCount(x -> x = 'G',
                    arraySlice(
                        arrayMap(x -> x.4, arraySort(groupArray((first_event_date, zone_name, coords, event_type_label)))),
                        arrayFirstIndex(x -> x = 'P', 
                            arrayMap(x -> x.4, arraySort(groupArray((first_event_date, zone_name, coords, event_type_label))))
                        ) + 1,
                        length(arrayMap(x -> x.4, arraySort(groupArray((first_event_date, zone_name, coords, event_type_label)))))
                    )
                )
            ) AS gaze_count_after_first_pickup,
            -- 첫 번째 픽업한 매대 이름
            arrayElement(
                arrayMap(x -> x.2,
                    arrayFilter(y -> y.4 = 'P', arraySort(groupArray((first_event_date, zone_name, coords, event_type_label))))
                ), 1
            ) AS first_pickup_zone,
            -- 첫 번째 픽업 시간
            arrayElement(
                arrayMap(x -> x.1,
                    arrayFilter(y -> y.4 = 'P', arraySort(groupArray((first_event_date, zone_name, coords, event_type_label))))
                ), 1
            ) AS first_pickup_time,
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
    pivot as (
        SELECT
            person_seq,
            age_group,
            gender_label,
            integrated_route,
            zone_names,
            zone_coords,
            event_timestamps,
            event_types,
            gaze_count_before_first_pickup,
            gaze_count_after_first_pickup,
            first_pickup_zone,
            first_pickup_time,
            before_pickup_gaze_1st,
            after_pickup_gaze_1st
        FROM integrated_routes
        ORDER BY person_seq
    ),
    shelf_analysis AS (
        -- 픽업 직전 마지막 응시매대 (1st만, 계산대 제외)
        SELECT 
            'before' as period,
            COALESCE(NULLIF(before_pickup_gaze_1st, ''), '진열대없음') as shelf_name
        FROM pivot
        WHERE COALESCE(NULLIF(before_pickup_gaze_1st, ''), '진열대없음') != '계산대'
            {exclude_shelf_condition}
        
        UNION ALL
        
        -- 픽업 후 첫 번째 응시매대 (1st만, 계산대 제외)
        SELECT 
            'after' as period,
            COALESCE(NULLIF(after_pickup_gaze_1st, ''), '진열대없음') as shelf_name
        FROM pivot
        WHERE COALESCE(NULLIF(after_pickup_gaze_1st, ''), '진열대없음') != '계산대'
            {exclude_shelf_condition}
    ),
    
    -- 진열대별 집계 및 비율 계산
    aggregated AS (
        SELECT 
            period,
            shelf_name,
            COUNT(*) as visit_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY period)) as percentage
        FROM shelf_analysis
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
            CONCAT(CAST(percentage as String), '%') as pct
        FROM topN 
        WHERE period = 'before'
    ),
    
    -- 픽업 후 TopN 결과
    after_results AS (
        SELECT 
            'AFTER' as analysis_type,
            rank as no,
            shelf_name,
            CONCAT(CAST(percentage as String), '%') as pct
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
        print(f"🔍 [DEBUG] 원본과 동일한 분석 쿼리 실행 시작")
        
        result = client.query(analysis_query)
        analysis_rows = result.result_rows
        print(f"✅ 진열대 분석 완료: {len(analysis_rows):,}행")
        
        # 결과를 튜플 형태로 변환 (원본 함수와 동일한 형태)
        formatted_results = []
        for row in analysis_rows:
            period, rank, shelf_name, pct_str = row
            # 퍼센티지 문자열에서 숫자만 추출
            percentage = float(pct_str.replace('%', ''))
            formatted_results.append((period, rank, shelf_name, percentage))
        
        return formatted_results
        
    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {e}")
        return {"error": str(e)}


def test_fixed_shelf_analysis():
    """수정된 진열대 분석 테스트"""
    
    print("=" * 60)
    print("🧪 수정된 진열대 분석 테스트 시작")
    print("=" * 60)
    
    # 테스트 파라미터 설정
    test_params = {
        "start_date": "2025-06-12",
        "end_date": "2025-07-12", 
        "exclude_dates": ["2025-06-22"],
        "target_shelves": ["빵"],
        "age_groups": ["10대"],
        "gender_labels": ["여자"],
        "exclude_shelves": ["진열대없음", "전자렌지"],  # 계산대는 쿼리에서 하드코딩 제외
        "top_n": 5,
        "period": "both"
    }
    
    print("📋 테스트 파라미터:")
    for key, value in test_params.items():
        print(f"  - {key}: {value}")
    print("  - 계산대: 무조건 제외 (하드코딩)")
    print()
    
    try:
        print("🔄 fixed_shelf_analysis 함수 호출 중...")
        print("-" * 40)
        
        # 함수 호출
        result = fixed_shelf_analysis(**test_params)
        
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
                    print(f"  {rank:2d}위: {shelf_name:15s} ({percentage:4.0f}%)")
                print()
            
            # AFTER 결과 출력
            after_results = [r for r in result if r[0] == 'AFTER']
            if after_results:
                print("🔍 픽업 후 (AFTER) Top 진열대:")
                for item in after_results:
                    period, rank, shelf_name, percentage = item
                    print(f"  {rank:2d}위: {shelf_name:15s} ({percentage:4.0f}%)")
                print()
                
            # 서버 결과와 비교
            print("🔍 서버 결과와 비교:")
            print("-" * 30)
            print("서버 결과 (BEFORE):")
            print("1. 빵 - 9%")
            print("2. 커피음료 - 8%") 
            print("3. 캔디 - 7%")
            print("4. 냉장즉석식1 - 6%")
            print("5. 도시락, 김밥 - 6%")
            print()
            print("서버 결과 (AFTER):")
            print("1. 냉장즉석식1 - 7%")
            print("2. 캔디 - 7%")
            print("3. 도시락, 김밥 - 6%")
            print("4. 빵 - 6%")
            print("5. 차음료 - 5%")
                
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
    print("🚀 수정된 진열대 분석 테스트 프로그램 시작")
    print()
    
    # 수정된 분석 테스트
    test_fixed_shelf_analysis()