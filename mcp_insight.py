from fastmcp import FastMCP
import clickhouse_connect
import os
from dotenv import load_dotenv
import tiktoken
import logging
import sys
import time
from pathlib import Path

# SSH 터널링 관련 import
try:
    from sshtunnel import SSHTunnelForwarder
    SSH_AVAILABLE = True
except ImportError:
    SSH_AVAILABLE = False
    logging.warning("sshtunnel 패키지가 설치되어 있지 않습니다.")

from utils import create_transition_data
from map_config import item2zone

load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

# SSH 설정
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT", "22"))
SSH_USERNAME = os.getenv("SSH_USERNAME")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")

# 전역 SSH 터널 관리
_ssh_tunnel = None


def get_clickhouse_client(database="plusinsight"):
    """ClickHouse 클라이언트를 가져옵니다. SSH 터널링 지원."""
    global _ssh_tunnel
    
    # SSH 터널링이 필요한 경우
    if SSH_AVAILABLE and SSH_HOST:
        try:
            # 기존 터널이 없거나 비활성 상태면 새로 생성
            if not _ssh_tunnel or not _ssh_tunnel.is_active:
                if _ssh_tunnel:
                    _ssh_tunnel.stop()
                
                _ssh_tunnel = SSHTunnelForwarder(
                    (SSH_HOST, SSH_PORT),
                    ssh_username=SSH_USERNAME,
                    ssh_password=SSH_PASSWORD,
                    remote_bind_address=(CLICKHOUSE_HOST, int(CLICKHOUSE_PORT)),
                    local_bind_address=("localhost", 0),
                )
                _ssh_tunnel.start()
                print(f"SSH 터널 생성: localhost:{_ssh_tunnel.local_bind_port}")
            
            # SSH 터널을 통해 연결
            host = "localhost"
            port = _ssh_tunnel.local_bind_port
            
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

model_name = "gpt-4o"
model_max_tokens = {
    "gpt-4o": 128000,
}

def num_tokens_from_string(string: str, model: str) -> int:
    encoding = tiktoken.encoding_for_model(model)
    num_tokens = len(encoding.encode(string))
    return num_tokens

def is_token_limit_exceeded(text: str, model: str, reserved_tokens: int = 1000) -> bool:
    token_count = num_tokens_from_string(text, model)
    max_tokens = model_max_tokens.get(model, 4096)  # 기본값 4096
    return token_count > (max_tokens - reserved_tokens)

mcp = FastMCP("insight")

@mcp.tool()
def pickup_transition(database: str, start_date: str, end_date: str) -> int:
    """픽업 구역 전환 데이터 조회"""
    try:
        query = f"""WITH transitions AS 
    (
        SELECT
            arrayZip(
                arrayPopBack(visited_zones_no_consecutive),
                arrayPopFront(visited_zones_no_consecutive)
            ) AS transitions
        FROM
        (
            SELECT
                arrayFilter(
                    (zone, i) -> (i = 1 OR zone != visited_zones_in_order[i-1]),
                    visited_zones_in_order,
                    arrayEnumerate(visited_zones_in_order)
                ) AS visited_zones_no_consecutive
            FROM
            (
                SELECT
                    e.person_seq AS person_seq,
                    arrayMap(x -> x.2,
                        arraySort(x -> x.1,
                            groupArray((e.timestamp, z.name))
                        )
                    ) AS visited_zones_in_order
                FROM customer_behavior_event e
                INNER JOIN customer_behavior_area a
                    ON e.customer_behavior_area_id = a.id
                INNER JOIN zone z
                    ON a.attention_target_zone_id = z.id
                WHERE
                    e.is_staff = false
                    AND e.event_type = 1
                    AND z.name NOT LIKE '%시식대%'
                    AND e.timestamp BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY
                    e.person_seq
            )
            WHERE length(visited_zones_no_consecutive) >= 2
        )
    )
    SELECT
        t.1 AS from_zone,
        t.2 AS to_zone,
        count(*) AS transition_count
    FROM transitions
    ARRAY JOIN transitions AS t
    GROUP BY
        from_zone,
        to_zone
    ORDER BY
        transition_count DESC"""

        client = get_clickhouse_client(database=database)

        result = client.query(query)
        
        answer = "픽업 발생 구역간 전환 데이터"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"\n{row[0]} -> {row[1]} : {row[2]}"
        else:
            answer = "데이터가 없습니다."

        if is_token_limit_exceeded(answer, model_name):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        return answer
    except Exception as e:
        return f"오류가 발생했습니다: {e}"

@mcp.tool()
def sales_funnel(database: str, start_date: str, end_date: str) -> str:
    """sales_funnel: 방문, 노출, 픽업의 전환율 조회"""
    try:
        query = f"""SELECT
        shelf_name
        , sum(visit) AS visit_count
        , sum(gaze1) AS exposed_count
        , sum(pickup) AS pickup_count
        , floor(sum(sales_funnel.pickup )/sum(gaze1), 2) AS pickup_rate
    FROM sales_funnel
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY shelf_name
    ORDER BY pickup_rate DESC"""

        # 클라이언트 생성
        db = get_clickhouse_client(database=database)

        result = db.query(query.strip())

        answer = f"{result.column_names}\n"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"{row}\n"
        else:
            answer = "데이터가 없습니다."

        if is_token_limit_exceeded(answer, model_name):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        return answer
    except Exception as e:
        return f"오류가 발생했습니다: {e}"

@mcp.tool()
def representative_movement(database: str, start_date: str, end_date: str, limit: int = 20) -> str:
    """대표적인 이동 경로 리스트 조회"""
    try:
        query = f"""
SELECT
    CASE 
        WHEN gender = 0 THEN 'male'
        WHEN gender = 1 THEN 'female'
        ELSE 'unknown'
    END AS gender,
    age_group,
    z1.name AS zone1_name,
    z2.name AS zone2_name,
    z3.name AS zone3_name,
    SUM(tsf.num_people) AS total_people,
    toString(round(SUM(tsf.num_people) / (SELECT SUM(num_people) FROM two_step_flow WHERE date BETWEEN '{start_date}' AND '{end_date}') * 100, 2)) || '%' AS percentage
FROM
    two_step_flow tsf
INNER JOIN zone z1 ON tsf.zone1_id = z1.id
INNER JOIN zone z2 ON tsf.zone2_id = z2.id
INNER JOIN zone z3 ON tsf.zone3_id = z3.id
WHERE date BETWEEN '{start_date}' AND '{end_date}'
GROUP BY
    gender,
    age_group,
    z1.name,
    z2.name,
    z3.name
ORDER BY
    total_people DESC
LIMIT {limit}"""

        client = get_clickhouse_client(database=database)

        result = client.query(query)

        answer = f"대표 이동 동선 리스트:\n{result.column_names}\n"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"{row}\n"

            query = f"""WITH tsf_zones AS (SELECT DISTINCT z.name AS zone_name
FROM 
(
    SELECT zone1_id AS zone_id FROM two_step_flow
    UNION ALL
    SELECT zone2_id AS zone_id FROM two_step_flow
    UNION ALL
    SELECT zone3_id AS zone_id FROM two_step_flow
) AS zones
LEFT JOIN zone z ON zones.zone_id = z.id
WHERE z.name IS NOT NULL
ORDER BY z.name),

zone_centers AS (
    SELECT 
        id,
        name,
        arrayReduce('avg', arrayMap(x -> tupleElement(x, 1), coords)) AS center_x,
        arrayReduce('avg', arrayMap(x -> tupleElement(x, 2), coords)) AS center_y
    FROM zone
),

zone_distances AS (
    SELECT 
        z1.name AS from_zone,
        z2.name AS to_zone,
        sqrt(pow(z1.center_x - z2.center_x, 2) + pow(z1.center_y - z2.center_y, 2)) AS distance
    FROM zone_centers z1
    CROSS JOIN zone_centers z2
    WHERE z1.name != z2.name
),

top_five_zones AS (
    SELECT
        from_zone,
        to_zone,
        distance,
        ROW_NUMBER() OVER (PARTITION BY from_zone ORDER BY distance ASC) as rank
    FROM zone_distances
    WHERE from_zone IN (SELECT zone_name FROM tsf_zones)
),

closest_zones AS (
    SELECT
        from_zone,
        groupArray(to_zone) AS closest_5_zones
    FROM top_five_zones
    WHERE rank <= 5
    GROUP BY from_zone
)

SELECT 
    tz.zone_name,
    cz.closest_5_zones
FROM tsf_zones tz
LEFT JOIN closest_zones cz ON tz.zone_name = cz.from_zone
"""
            result = client.query(query)

            answer += f"구역과 가장 가까운 진열대 목록:\n{result.column_names}\n"
            if len(result.result_rows) > 0:
                for row in result.result_rows:
                    answer += f"{row}\n"
            else:
                answer = "데이터가 없습니다."
        else:
            answer = "데이터가 없습니다."

        if is_token_limit_exceeded(answer, model_name):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        return answer
    except Exception as e:
        return f"오류가 발생했습니다: {e}"

@mcp.tool()
def inflow_by_entrance_line(database: str, start_date: str, end_date: str) -> str:
    """유입률 분석"""
    try:
        query = f"""
SELECT
    -- UTC 시(hour)와 타임존 오프셋을 문자열로 결합
    -- formatDateTime(timestamp, '%H')
    -- || 'GMT'
    -- || toString(floor(timeZoneOffset(timestamp) / 3600)) AS hour,
    gender,
    -- date,
    -- setting_general에 따라 그룹화 (이것은 예시일 뿐!)
    CASE
        WHEN age >= 1 AND age < 10 THEN '1-9'
        WHEN age >= 10 AND age < 20 THEN '10-19'
        WHEN age >= 20 AND age < 30 THEN '20-29'
        WHEN age >= 30 AND age < 40 THEN '30-39'
        WHEN age >= 40 AND age < 50 THEN '40-49'
        WHEN age >= 50 AND age < 60 THEN '50-59'
        WHEN age >= 60             THEN '60+'
        ELSE 'Unknown'
    END AS age_group,
    -- 입장 기준 유니크 방문자 수
    countDistinctIf(person_seq, entrance = true) AS visitor_count,
    -- 외부 유입 기준 유니크 방문자 수
    countDistinctIf(person_seq, has(keywords, 'external')) AS traffic_count
FROM line_in_out_individual AS lioi
INNER JOIN line AS l
    ON l.id = lioi.triggered_line_id
WHERE
    -- 기간
    date BETWEEN '{start_date}' AND '{end_date}'
    -- setting_general에 따라
    AND age >= 1
    -- 스태프 제외
    AND is_staff = false
    -- 입장 이벤트만 (IN)
    AND in_out = 'IN'
GROUP BY
    -- hour,
    -- date,
    gender,
    age_group
HAVING
    -- 방문자 수가 유입 수 이하인 그룹만 ⭐⭐
    visitor_count <= traffic_count
    AND traffic_count > 0
ORDER BY
    visitor_count DESC,
    traffic_count DESC"""

        client = get_clickhouse_client(database=database)

        result = client.query(query)
        
        # 결과 형식화
        answer = f"{start_date} ~ {end_date} 방문자 수와 유동인구 수 비교:\n"
        answer += "성별, 연령대, 방문자 수, 유동인구 수, 유입률(방문자 수 / 유동인구 수)\n"
        
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                gender, age, visitor_count, traffic_count = row
                
                # 성별 변환
                gender_str = "남성" if gender == 0 else "여성"
                
                answer += f"{gender_str}, {age}, {visitor_count}, {traffic_count}, {visitor_count / traffic_count * 100}%\n"
        else:
            answer = "데이터가 없습니다."

        if is_token_limit_exceeded(answer, model_name):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        return answer
    except Exception as e:
        return f"오류가 발생했습니다: {e}"
            
if __name__ == "__main__":
    mcp.run()