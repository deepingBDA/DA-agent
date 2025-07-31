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
    고객별 첫 픽업 전후 진열대 방문 패턴 분석 도구
    
    각 고객의 첫 번째 픽업 이벤트를 기준으로 픽업 직전/직후 응시한 진열대를 분석합니다.
    
    주요 파라미터:
    - target_shelves: 첫 픽업한 진열대 조건 (예: ['빵'])
    - age_groups: 연령대 필터 (예: ['10대'])  
    - gender_labels: 성별 필터 (예: ['여자'])
    - exclude_shelves: 제외할 진열대 (예: ['진열대없음', '전자렌지'])
    - start_date, end_date: 분석 기간 (YYYY-MM-DD 형식)
    
    반환값: [('BEFORE'/'AFTER', 순위, 진열대명, 비율%)] 형식의 리스트
    
    사용 예시:
    get_shelf_analysis_flexible(
        target_shelves=['빵'], 
        age_groups=['10대'], 
        gender_labels=['여자'],
        exclude_shelves=['진열대없음', '전자렌지']
    )
    """
    # 🔍 디버깅: 실제 전달받은 파라미터 로깅
    print(f"🔍 [MCP_TOOL] get_shelf_analysis_flexible 실제 호출됨!")
    print(f"🔍 [MCP_TOOL] 받은 파라미터:")
    print(f"  start_date: {repr(start_date)}")
    print(f"  end_date: {repr(end_date)}")
    print(f"  target_shelves: {repr(target_shelves)}")
    print(f"  age_groups: {repr(age_groups)}")
    print(f"  gender_labels: {repr(gender_labels)}")
    print(f"  exclude_dates: {repr(exclude_dates)}")
    print(f"  exclude_shelves: {repr(exclude_shelves)}")
    print(f"  top_n: {repr(top_n)}")
    print(f"🔍 [MCP_TOOL] 파라미터 타입 체크:")
    print(f"  target_shelves type: {type(target_shelves)}")
    print(f"  age_groups type: {type(age_groups)}")
    print(f"  gender_labels type: {type(gender_labels)}")
    
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
    ),
    
    -- 픽업 전 Top5 결과
    before_results AS (
        SELECT 
            'BEFORE' as analysis_type,
            rank as no,
            shelf_name,
            CONCAT(CAST(percentage as String), '%') as pct
        FROM top5 
        WHERE period = 'before'
    ),
    
    -- 픽업 후 Top5 결과
    after_results AS (
        SELECT 
            'AFTER' as analysis_type,
            rank as no,
            shelf_name,
            CONCAT(CAST(percentage as String), '%') as pct
        FROM top5 
        WHERE period = 'after'
    )
    
    -- 최종 결과 (픽업 전/후 별도 테이블)
    SELECT * FROM before_results
    UNION ALL
    SELECT * FROM after_results
    ORDER BY analysis_type, no
    """
    
    try:
        print(f"🔍 [DEBUG] 쿼리 실행 시작 - 예상 조건:")
        print(f"  날짜 범위: {start_date} ~ {end_date}")
        print(f"  타겟 진열대: {target_shelves}")
        print(f"  연령대: {age_groups}")
        print(f"  성별: {gender_labels}")
        
        result = client.query(analysis_query)
        print(f"✅ 진열대 분석 완료: {len(result.result_rows):,}행")
        return result.result_rows
    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {e}")
        print(f"🔍 [DEBUG] 쿼리 길이: {len(analysis_query)} 문자")
        
        # 구문 오류 위치 정보 추출
        error_str = str(e)
        if "position" in error_str:
            import re
            pos_match = re.search(r'position (\d+)', error_str)
            if pos_match:
                position = int(pos_match.group(1))
                print(f"🔍 [DEBUG] 오류 위치: {position}번째 문자")
                
                # 오류 위치 주변 텍스트 표시
                start = max(0, position - 100)
                end = min(len(analysis_query), position + 100)
                context = analysis_query[start:end]
                print(f"🔍 [DEBUG] 오류 위치 주변:")
                print(f"'{context}'")
        
        return {"error": str(e)}

if __name__ == "__main__":
    mcp.run()
