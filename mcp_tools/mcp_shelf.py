"""
진열 최적화 분석을 위한 MCP 도구들

기능:
- 픽업존 전후 탐색 진열대 분석
- 매대별 고객 동선 패턴 분석
"""

import os
import sys
from typing import Dict, Any, List, Union, Optional
from fastmcp import FastMCP
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# FastMCP 인스턴스
mcp = FastMCP("shelf_optimization")


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


@mcp.tool()
def get_shelf_analysis_flexible(
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
    🛒 고객별 첫 픽업 전후 진열대 방문 패턴 분석 MCP 툴
    
    ## 📊 분석 개요
    각 고객의 **첫 번째 픽업 이벤트**를 기준점으로 하여:
    - 픽업 **직전 마지막** 응시한 진열대 (before_pickup_gaze_1st)
    - 픽업 **직후 첫 번째** 응시한 진열대 (after_pickup_gaze_1st)
    를 분석하여 고객 행동 패턴과 진열대 간 연관성을 파악합니다.
    
    ## 🎯 분석 기준
    - **기준점**: 각 고객의 첫 번째 픽업 이벤트 (시간순 정렬)
    - **응시 조건**: 3회 이상 방문한 매대만 포함 (노이즈 제거)
    - **자동 제외**: '진열대없음', '계산대' (실제 진열대만 분석)
    - **비율 계산**: 정수 반올림 (소수점 제거)
    
    ## 📋 매개변수 상세 설명
    
    ### 📅 날짜 설정
    - start_date (str): 분석 시작 날짜 (기본: "2025-06-12")
      형식: "YYYY-MM-DD"
    - end_date (str): 분석 종료 날짜 (기본: "2025-07-12")
      형식: "YYYY-MM-DD"  
    - exclude_dates (List[str]): 제외할 특정 날짜들 (기본: ['2025-06-22'])
      예시: ['2025-06-22', '2025-06-30'] (휴일, 이벤트일 등)
    
    ### 🏪 진열대 필터링
    - target_shelves (List[str]): **첫 픽업한 진열대** 조건 (기본: None=모든 진열대)
      ⚠️ 중요: 이 진열대를 **첫 번째로 픽업한 고객만** 분석 대상
      예시: ['빵'] → 빵을 첫 픽업한 고객들의 픽업 전후 행동 분석
      예시: ['커피음료', '탄산음료'] → 커피음료 또는 탄산음료를 첫 픽업한 고객
    
    - exclude_shelves (List[str]): 분석에서 제외할 진열대 (기본: ['진열대없음', '계산대'])
      자동으로 '진열대없음', '계산대' 제외됨
    
    ### 👥 고객 세분화
    - age_groups (List[str]): 분석할 연령대 (기본: None=모든 연령대)
      가능한 값: ['10대', '20대', '30대', '40대', '50대', '60대 이상', '미상']
      예시: ['20대', '30대'] → 20-30대만 분석
    
    - gender_labels (List[str]): 분석할 성별 (기본: None=모든 성별)
      가능한 값: ['남자', '여자', '미상']
      예시: ['여자'] → 여성만 분석
    
    ### 📊 결과 제어
    - top_n (int): 상위 몇 개 진열대 표시 (기본: 5)
      예시: top_n=3 → Top 3 진열대만 결과에 포함
    
    - exclude_from_top (List[str]): 상위 결과에서 제외할 진열대 (기본: None)
      예시: ['진열대없음'] → '진열대없음'을 제외하고 순위 매김
    
    - period (str): 분석 기간 선택 (기본: "both")
      - "before": 픽업 전만 분석
      - "after": 픽업 후만 분석  
      - "both": 픽업 전후 모두 분석 (권장)
    
    ## 📈 반환값 형식
    List[Tuple]: 각 행은 (순위, 픽업전_진열대, 픽업전_비율, 픽업후_진열대, 픽업후_비율)
    
    예시 결과:
    [(1, '진열대없음', '48%', '진열대없음', '29%'),
     (2, '빵', '34%', '빵', '12%'),
     (3, '전자렌지', '4%', '도시락,김밥', '9%')]
    
    ## 💡 사용 예시
    
    ### 예시 1: 10대 여성, 빵 매대 첫 픽업 고객 분석
    ```python
    result = get_shelf_analysis_flexible(
        target_shelves=['빵'],        # 빵을 첫 픽업한 고객만
        age_groups=['10대'],          # 10대만
        gender_labels=['여자'],       # 여성만
        top_n=5
    )
    ```
    
    ### 예시 2: 50대 남성, 커피음료 매대 첫 픽업 고객 분석
    ```python
    result = get_shelf_analysis_flexible(
        target_shelves=['커피음료'],   # 커피음료를 첫 픽업한 고객만
        age_groups=['50대'],          # 50대만
        gender_labels=['남자'],       # 남성만
        top_n=5
    )
    ```
    
    ### 예시 3: 전체 고객, 특정 기간, Top 3만
    ```python
    result = get_shelf_analysis_flexible(
        start_date='2025-07-01',
        end_date='2025-07-31',
        top_n=3
    )
    ```
    
    ## ⚠️ 주의사항
    - target_shelves는 **첫 픽업한 진열대** 조건입니다 (단순 방문이 아님)
    - 결과가 비어있다면 조건에 맞는 고객이 없거나 해당 진열대가 존재하지 않을 수 있습니다
    - 응시 이벤트는 3회 이상 방문한 매대만 포함됩니다 (노이즈 제거)
    """
    client = _create_clickhouse_client()
    if not client:
        return {"error": "ClickHouse 연결 실패"}
    
    # 파라미터 처리
    exclude_dates = exclude_dates or ['2025-06-22']
    exclude_shelves = exclude_shelves or ['진열대없음', '계산대']
    exclude_from_top = exclude_from_top or []
    
    # 날짜 조건
    exclude_dates_str = "', '".join(exclude_dates)
    date_condition = f"AND cbe.date NOT IN ('{exclude_dates_str}')"
    
    # 연령대 조건
    age_condition = ""
    if age_groups:
        age_groups_str = "', '".join(age_groups)
        age_condition = f"AND age_group IN ('{age_groups_str}')"
    
    # 성별 조건
    gender_condition = ""
    if gender_labels:
        gender_labels_str = "', '".join(gender_labels)
        gender_condition = f"AND gender_label IN ('{gender_labels_str}')"
    
    # 진열대 필터 조건
    target_shelf_condition = ""
    if target_shelves:
        target_shelves_str = "', '".join(target_shelves)
        target_shelf_condition = f"AND shelf_name IN ('{target_shelves_str}')"
    
    exclude_shelf_condition = ""
    if exclude_shelves:
        exclude_shelves_str = "', '".join(exclude_shelves)
        exclude_shelf_condition = f"AND shelf_name NOT IN ('{exclude_shelves_str}')"
    
    exclude_from_top_condition = ""
    if exclude_from_top:
        exclude_from_top_str = "', '".join(exclude_from_top)
        exclude_from_top_condition = f"AND shelf_name NOT IN ('{exclude_from_top_str}')"
    
    # 기간 조건
    period_condition = ""
    if period == "before":
        period_condition = "AND period = 'before'"
    elif period == "after":
        period_condition = "AND period = 'after'"
    
    # 복잡한 분석 쿼리
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
            -- 시간순으로 정렬된 통합 경로 (이벤트 타입 포함)
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
            -- 첫 번째 픽업 전 매대 방문 수 (응시 이벤트만) - 픽업이 없으면 0
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
            -- 첫 번째 픽업 직후 매대 방문 수 (응시 이벤트만) - 픽업이 없으면 0
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
            -- 픽업 전 응시 매대 경로 (시간순)
            arrayStringConcat(
                arrayMap(x -> x.2,
                    arraySlice(
                        arrayFilter(y -> y.4 = 'G', arraySort(groupArray((first_event_date, zone_name, coords, event_type_label)))),
                        1,
                        arrayFirstIndex(x -> x = 'P', 
                            arrayMap(x -> x.4, arraySort(groupArray((first_event_date, zone_name, coords, event_type_label))))
                        ) - 1
                    )
                ), ' → '
            ) AS gaze_route_before_first_pickup,
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
            -- 픽업 직전 4번째 응시 매대
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
                ), 4
            ) AS before_pickup_gaze_4th,
            -- 픽업 직전 5번째 응시 매대
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
                ), 5
            ) AS before_pickup_gaze_5th,
            -- 픽업 후 응시 매대 경로 (시간순)
            arrayStringConcat(
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
                ), ' → '
            ) AS gaze_route_after_first_pickup,
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
            ) AS after_pickup_gaze_3rd,
            -- 픽업 후 4번째 응시 매대
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
                ), 4
            ) AS after_pickup_gaze_4th,
            -- 픽업 후 5번째 응시 매대
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
                ), 5
            ) AS after_pickup_gaze_5th
        FROM combined_events
        GROUP BY person_seq, age, gender
    )
    , pivot as (
    SELECT
        person_seq,
        age_group,
        gender_label,
        integrated_route,      -- 픽업과 응시가 시간순으로 통합된 경로
        zone_names,            -- 시간순 존 이름 배열
        zone_coords,           -- 시간순 좌표 배열
        event_timestamps,      -- 시간순 이벤트 발생시간 배열
        event_types,           -- 시간순 이벤트 타입 배열 (P: 픽업, G: 응시)
        gaze_count_before_first_pickup,  -- 첫 번째 픽업 전 응시 매대 수
        gaze_count_after_first_pickup,   -- 첫 번째 픽업 후 응시 매대 수
        first_pickup_zone,               -- 첫 번째 픽업한 매대 이름
        first_pickup_time,               -- 첫 번째 픽업 시간
        gaze_route_before_first_pickup,  -- 픽업 전 응시 매대 경로
        gaze_route_after_first_pickup,   -- 픽업 후 응시 매대 경로
        before_pickup_gaze_1st,          -- 픽업 직전 1번째 응시 매대 (가장 마지막)
        before_pickup_gaze_2nd,          -- 픽업 직전 2번째 응시 매대
        before_pickup_gaze_3rd,          -- 픽업 직전 3번째 응시 매대
        before_pickup_gaze_4th,          -- 픽업 직전 4번째 응시 매대
        before_pickup_gaze_5th,          -- 픽업 직전 5번째 응시 매대
        after_pickup_gaze_1st,           -- 픽업 후 1번째 응시 매대
        after_pickup_gaze_2nd,           -- 픽업 후 2번째 응시 매대
        after_pickup_gaze_3rd,           -- 픽업 후 3번째 응시 매대
        after_pickup_gaze_4th,           -- 픽업 후 4번째 응시 매대
        after_pickup_gaze_5th            -- 픽업 후 5번째 응시 매대
    FROM integrated_routes
    ORDER BY person_seq
    )
    , shelf_analysis AS (
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
    
    -- 기간별 Top 5 순위 매기기
    ranked AS (
        SELECT 
            period,
            shelf_name,
            percentage,
            ROW_NUMBER() OVER (PARTITION BY period ORDER BY percentage DESC) as rank
        FROM aggregated
        WHERE percentage > 0  -- 0% 제외z 
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
        result = client.query(analysis_query)
        print(f"✅ 진열대 분석 완료: {len(result.result_rows):,}행")
        return result.result_rows
    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {e}")
        return {"error": str(e)}

if __name__ == "__main__":
    mcp.run()
