from fastmcp import FastMCP
import clickhouse_connect
import os
from dotenv import load_dotenv
import tiktoken
import logging
import sys
import time
from pathlib import Path

from utils import create_transition_data
from map_config import item2zone

# SSH 터널링 관련 import
try:
    from sshtunnel import SSHTunnelForwarder
    SSH_AVAILABLE = True
except ImportError:
    SSH_AVAILABLE = False
    logging.warning("sshtunnel 패키지가 설치되어 있지 않습니다.")

# 로그 디렉토리 생성
log_dir = Path("/app/logs")
log_dir.mkdir(parents=True, exist_ok=True)

# 파일 핸들러 직접 설정
log_file = log_dir / "mcp_diagnose.log"
file_handler = logging.FileHandler(log_file, mode='a')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 루트 로거 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout), file_handler]
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# 시작 로그 기록
logger.info("MCP 진단 서버 시작")
logger.info(f"=== 새 세션 시작: {time.strftime('%Y-%m-%d %H:%M:%S')} ===")

# 환경 변수 설정
os.environ["FASTMCP_DEBUG"] = "1"
os.environ["FASTMCP_LOG_LEVEL"] = "DEBUG"

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
                logger.info(f"SSH 터널 생성: localhost:{_ssh_tunnel.local_bind_port}")
            
            # SSH 터널을 통해 연결
            host = "localhost"
            port = _ssh_tunnel.local_bind_port
            
        except Exception as e:
            logger.error(f"SSH 터널 생성 실패: {e}, 직접 연결 시도")
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
        logger.info(f"ClickHouse 연결 성공: {host}:{port}, db={database}")
        return client
    except Exception as e:
        logger.error(f"ClickHouse 연결 실패: {e}")
        return None

model_name = "gpt-4o"
model_max_tokens = {
    "gpt-4o": 128000,
}

db_list = {
    # '갈매점': 'plusinsight_bgf_galmae',
    # '강북센터피스점':'plusinsight_bgf_gangbuk_centerpiece',
    # '강동센트럴점': 'plusinsight_bgf_gangdong_central',
    # '금천프라임점': 'plusinsight_bgf_geumcheon_prime',
    # '마천점': 'plusinsight_bgf_macheon',
    # '마천힐스테이트점': 'plusinsight_bgf_manchon_hillstate',
    # 'BGF사옥점': 'plusinsight_bgf_saok',
    # '신촌르메이르점': 'plusinsight_bgf_sinchon_lemeilleur',
    # '수성캐슬점': 'plusinsight_bgf_suseong_castle',
    # '타워팰리스점': 'plusinsight_bgf_tower_palace',
    # '역삼점': 'plusinsight_bgf_yeoksam',
    '망우혜원점': 'plusinsight'
}

def num_tokens_from_string(string: str, model: str) -> int:
    encoding = tiktoken.encoding_for_model(model)
    num_tokens = len(encoding.encode(string))
    return num_tokens

def is_token_limit_exceeded(text: str, model: str, reserved_tokens: int = 1000) -> bool:
    token_count = num_tokens_from_string(text, model)
    max_tokens = model_max_tokens.get(model, 4096)  # 기본값 4096
    return token_count > (max_tokens - reserved_tokens)

mcp = FastMCP("diagnose")

@mcp.tool()
def get_db_name() -> str:
    """편의점 이름과 데이터베이스 매핑 조회"""
    answer = "편의점 이름과 데이터베이스 매핑"
    for store, db in db_list.items():
        answer += f"\n{store}: {db}"
    return answer

@mcp.tool()
def diagnose_avg_in(start_date: str, end_date: str) -> str:
    """일평균 방문객 수 진단"""
    # 파라미터 기록
    param_log = f"diagnose_avg_in 호출됨: start_date={start_date}, end_date={end_date}"
    logger.info(param_log)
    
    query = f"""WITH df AS 
(
    SELECT 
        li.date, 
        li.triggered_line_id,
        l.name,
        -- 연령대 그룹화
        CASE 
            WHEN dt.age BETWEEN 0 AND 9 THEN '0-9s'
            WHEN dt.age BETWEEN 10 AND 19 THEN '10-19s'
            WHEN dt.age BETWEEN 20 AND 29 THEN '20-29s'
            WHEN dt.age BETWEEN 30 AND 39 THEN '30-39s'
            WHEN dt.age BETWEEN 40 AND 49 THEN '40-49s'
            WHEN dt.age BETWEEN 50 AND 59 THEN '50-59s'
            WHEN dt.age >= 60 THEN '60+s'
            ELSE 'Unknown'
        END AS age_group,
        -- 성별 그룹화
        CASE 
            WHEN dt.gender = '0' THEN '남성'
            WHEN dt.gender = '1' THEN '여성'
            ELSE 'Unknown'
        END AS gender,
        li.in_out,
        li.person_seq
    FROM 
        line_in_out_individual li 
    LEFT JOIN 
        detected_time dt ON li.person_seq = dt.person_seq 
    LEFT JOIN 
        line l on li.triggered_line_id = l.id
    WHERE
        li.date BETWEEN '{start_date}' AND '{end_date}'
        AND li.is_staff = false
        AND l.entrance = 1
--        AND l.id ='bdaa9cda-fbe7-4e10-9b5f-0ed90cf8fc02' --홍대 입구보정
    GROUP BY 
        li.date, 
        li.triggered_line_id,
        li.in_out,
        l.name,
        age_group, 
        gender,
        li.person_seq
),
daily_counts_overall AS (
    -- 전체 방문객의 일별 집계
    SELECT 
        li.date,
        COUNT(DISTINCT CASE WHEN li.in_out = 'IN' THEN li.person_seq END) AS unique_in_count
    FROM df
    GROUP BY li.date
),
daily_counts_gender AS (
    -- 성별만 고려한 일별 방문객 집계
    SELECT 
        li.date,
        gender,
        COUNT(DISTINCT CASE WHEN li.in_out = 'IN' THEN li.person_seq END) AS unique_in_count
    FROM df
    GROUP BY li.date, gender
),
daily_counts_age AS (
    -- 연령대별 일별 방문객 집계 (중복 제거)
    SELECT 
        li.date,
        age_group,
        COUNT(DISTINCT CASE WHEN li.in_out = 'IN' THEN li.person_seq END) AS unique_in_count
    FROM df
    GROUP BY li.date, age_group
),
avg_counts AS (
    -- 전체 방문객의 일평균
    SELECT 
        CONCAT(toString(if(isNaN(AVG(unique_in_count)) OR isInfinite(AVG(unique_in_count)), 0, toInt32(AVG(unique_in_count)))), '명') AS avg_daily_in_count
    FROM daily_counts_overall
),
gender_avg_counts AS (
    -- 성별별 일평균 방문객 수
    SELECT 
        gender,
        CONCAT(toString(if(isNaN(AVG(unique_in_count)) OR isInfinite(AVG(unique_in_count)), 0, toInt32(AVG(unique_in_count)))), '명') AS avg_daily_in_count_by_gender
    FROM daily_counts_gender
    GROUP BY gender
),
age_avg_counts AS (
    -- 연령대별 일평균 방문객 수 (중복 방지, 오류 해결)
    SELECT 
        age_group,
        CONCAT(toString(if(isNaN(SUM(unique_in_count) / COUNT(DISTINCT li.date)) OR isInfinite(SUM(unique_in_count) / COUNT(DISTINCT li.date)), 0, toInt32(SUM(unique_in_count) / COUNT(DISTINCT li.date)))), '명') AS avg_daily_in_count_by_age
    FROM daily_counts_age
    GROUP BY age_group
)
, final as (
SELECT 
    '일 평균' AS category,
    avg_daily_in_count AS avg_in
FROM avg_counts
UNION ALL
SELECT 
    gender AS category,
    avg_daily_in_count_by_gender AS avg_in
FROM gender_avg_counts
UNION ALL
SELECT 
    age_group AS category,
    avg_daily_in_count_by_age AS avg_in
FROM age_avg_counts)
select *
from  final
order by category desc
"""

    answer = ""
    for store, db in db_list.items():
        try:
            client = get_clickhouse_client(database=db)
            result = client.query(query)

            if len(result.result_rows) > 0:
                store_answer = f"{store} - "
                for row in result.result_rows:
                    store_answer += f"{row[0]}: {row[1]}, "

                answer += f"\n{store_answer[:-2]}"
            else:
                answer += f"\n{store} 데이터가 없습니다."
            
        except Exception as e:
            answer += f"\n{store} 데이터 조회 오류: {e}"

    # log answer
    logger.info(f"diagnose_avg_in 답변: {answer}")

    return answer

@mcp.tool()
def diagnose_avg_sales(start_date: str, end_date: str) -> str:
    """일평균 판매 건수 진단"""
    # 파라미터 기록
    param_log = f"diagnose_avg_sales 호출됨: start_date={start_date}, end_date={end_date}"
    logger.info(param_log)
    
    query = f"""
    WITH daily_sales AS (
        SELECT 
            store_nm,
            tran_ymd,
            COUNT(DISTINCT (pos_no, tran_no)) as daily_receipt_count
        FROM cu_revenue_total
        WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY store_nm, tran_ymd
    ),
    avg_sales AS (
        SELECT 
            store_nm,
            CONCAT(toString(toInt32(AVG(daily_receipt_count))), '건') as avg_daily_sales
        FROM daily_sales
        GROUP BY store_nm
    )
    SELECT *
    FROM avg_sales
    ORDER BY store_nm
    """

    try:
        client = get_clickhouse_client(database='cu_base')
        result = client.query(query)

        if len(result.result_rows) > 0:
            answer = "(지점, 판매 건수)"
            for row in result.result_rows:
                answer += f"\n{row}"

        else:
            answer = "데이터가 없습니다."
        
    except Exception as e:
        answer = f"데이터 조회 오류: {e}"

    # log answer
    logger.info(f"diagnose_avg_sales 답변: {answer}")

    return answer

@mcp.tool()
def check_zero_visits(start_date: str, end_date: str, database: str) -> str:
    """방문객수 데이터 이상 조회"""
    query = f"""WITH
start_date AS (SELECT toDate('{start_date}') AS value),
end_date AS (SELECT toDate('{end_date}') AS value),
date_range AS (
    SELECT addDays((SELECT value FROM start_date), number) AS date
    FROM numbers(
        assumeNotNull(toUInt64(
            dateDiff('day', (SELECT value FROM start_date), (SELECT value FROM end_date)) + 1
        ))
    )
),
daily_visits AS (
    SELECT
        li.date,
        COUNT(DISTINCT li.person_seq) AS visitor_count
    FROM line_in_out_individual li
    LEFT JOIN line l ON li.triggered_line_id = l.id
    WHERE li.date BETWEEN (SELECT value FROM start_date) AND (SELECT value FROM end_date)
      AND li.is_staff = false
      AND l.entrance = 1
      AND li.in_out = 'IN'
    GROUP BY li.date
),
hourly_visits AS (
    SELECT
        li.date,
        intDiv(toHour(li.timestamp), 3) * 3 AS hour,
        COUNT(DISTINCT li.person_seq) AS visitor_count
    FROM line_in_out_individual li
    LEFT JOIN line l ON li.triggered_line_id = l.id
    WHERE li.date BETWEEN (SELECT value FROM start_date) AND (SELECT value FROM end_date)
      AND li.is_staff = false
      AND l.entrance = 1
      AND li.in_out = 'IN'
    GROUP BY li.date, intDiv(toHour(li.timestamp), 3)
),
date_hour_grid AS (
    SELECT d.date, h.number * 3 AS hour
    FROM date_range d
    CROSS JOIN numbers(8) h -- 0~7 * 3 → 0,3,6,9,12,15,18,21
    WHERE h.number * 3 BETWEEN 9 AND 21 -- 원하는 시간대 제한
),
has_zero_hour AS (
    SELECT
        dh.date,
        COUNT(*) AS zero_hour_count
    FROM date_hour_grid dh
    LEFT JOIN hourly_visits hv ON dh.date = hv.date AND dh.hour = hv.hour
    WHERE hv.visitor_count IS NULL OR hv.visitor_count = 0
    GROUP BY dh.date
),
zero_daily AS (
    SELECT dr.date, '일별 방문자 없음' AS reason
    FROM date_range dr
    LEFT JOIN daily_visits dv ON dr.date = dv.date
    WHERE dv.visitor_count IS NULL OR dv.visitor_count = 0
),
zero_hourly AS (
    SELECT zh.date, '특정 시간대 0명 존재' AS reason
    FROM has_zero_hour zh
    LEFT JOIN zero_daily zd ON zh.date = zd.date
    WHERE zd.date IS NULL
),
final_zero_dates AS (
    SELECT * FROM zero_daily
    UNION ALL
    SELECT * FROM zero_hourly
)
SELECT *
FROM final_zero_dates
ORDER BY date;"""

    try:
        client = get_clickhouse_client(database=database)
        result = client.query(query)

        if len(result.result_rows) > 0:
            answer = "방문객수 데이터 이상한 날"
            for row in result.result_rows:
                answer += f"\n{row}"
        else:
            answer = "이상 없습니다."
        
    except Exception as e:
        answer = f"데이터 조회 오류: {e}"

    # log answer
    logger.info(f"check_zero_visits 답변: {answer}")

    return answer

@mcp.tool()
def diagnose_purchase_conversion_rate(start_date: str, end_date: str) -> str:
    """구매전환율 진단"""
    # 파라미터 기록
    param_log = f"get_purchase_conversion_rate 호출됨: start_date={start_date}, end_date={end_date}"
    logger.info(param_log)
    
    # 방문객 수와 판매 건수 조회
    avg_in_result = diagnose_avg_in(start_date, end_date)
    avg_sales_result = diagnose_avg_sales(start_date, end_date)

    # 구매전환율 계산
    answer = f"구매전환율 = (판매 건수 / 방문객 수) * 100 % 라는 공식이야. 일평균 방문객 수를 조회하고, 일평균 판매 건수를 조회해서, 구매전환율을 추정해줘. 구매전환율이 100%를 넘으면 방문객 수가 잘못 측정된거야. 참고해."
    answer += f"\n일평균 방문객 수: {avg_in_result}"
    answer += f"\n일평균 판매 건수: {avg_sales_result}"
    
    return answer

@mcp.tool()
def diagnose_exploratory_tendency(start_date: str, end_date: str) -> str:
    """탐색 경향성 진단"""

    query = f"""WITH sales_funnel AS (
    SELECT
        shelf_name
        , sum(visit) AS visit_count
        , sum(gaze1) AS exposed_count
        , sum(pickup) AS pickup_count
    FROM sales_funnel
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY shelf_name
    ),
    visitor_count AS (
        SELECT 
            COUNT(*) AS total_unique_visitors
        FROM 
        (
            SELECT 
                li.person_seq
            FROM 
                line_in_out_individual li 
            LEFT JOIN 
                line l ON li.triggered_line_id = l.id
            WHERE
                li.date BETWEEN '{start_date}' AND '{end_date}'
                AND li.is_staff = false
                AND l.entrance = 1
                AND li.in_out = 'IN'
            GROUP BY 
                li.person_seq
        )
    ),
    total_sales AS (
        SELECT
            sum(visit_count) AS total_visit_count
            , sum(exposed_count) AS total_exposed_count
            , sum(pickup_count) AS total_pickup_count
        FROM sales_funnel
    )
    SELECT
        ROUND(ts.total_visit_count / vc.total_unique_visitors, 2) AS ratio_visit_count,
        ROUND(ts.total_exposed_count / vc.total_unique_visitors, 2) AS ratio_exposed_count,
        ROUND(ts.total_pickup_count / vc.total_unique_visitors, 2) AS ratio_pickup_count
    FROM total_sales ts
    CROSS JOIN visitor_count vc
    """

    total_funnel = ""
    for store, db in db_list.items():
        store_answer = f"{store} - "
        try:
            client = get_clickhouse_client(database=db)
            result = client.query(query.strip())
            
            if len(result.result_rows) > 0:
                for row in result.result_rows:
                    store_answer += f"1인당 진열대 방문: {row[0]}, 1인당 진열대 노출: {row[1]}, 1인당 진열대 픽업: {row[2]}"

                total_funnel += f"{store_answer}\n"
            else:
                store_answer += f"데이터가 없습니다."
                total_funnel += f"{store_answer}\n"
        except Exception as e:
            store_answer += f"데이터 조회 오류: {e}"
            total_funnel += f"{store_answer}\n"

    answer = f"주어진 기간의 지점별 sales funnel 데이터: {total_funnel}"
    return answer

@mcp.tool()
def diagnose_shelf(start_date: str, end_date: str) -> str:
    """진열대 진단"""
    query = f"""WITH base AS (
SELECT
    shelf_name,
    sum(visit) AS visit_count,
    sum(gaze1) AS exposed_count,
    sum(pickup) AS pickup_count,
    floor(sum(sales_funnel.gaze1)/sum(visit), 2) AS gaze_rate,
    floor(sum(sales_funnel.pickup)/sum(gaze1), 2) AS pickup_rate
FROM sales_funnel
WHERE date BETWEEN '{start_date}' AND '{end_date}'
AND shelf_name NOT LIKE '%시식대%'
GROUP BY shelf_name
)

SELECT * FROM (
SELECT 'visit_count_hot' AS metric, shelf_name, visit_count, exposed_count, pickup_count, gaze_rate, pickup_rate
FROM base
ORDER BY visit_count DESC
LIMIT 3

UNION ALL

SELECT 'visit_count_cold' AS metric, shelf_name, visit_count, exposed_count, pickup_count, gaze_rate, pickup_rate
FROM base
ORDER BY visit_count ASC
LIMIT 3

UNION ALL

SELECT 'exposed_count_hot' AS metric, shelf_name, visit_count, exposed_count, pickup_count, gaze_rate, pickup_rate
FROM base
ORDER BY exposed_count DESC
LIMIT 3

UNION ALL

SELECT 'exposed_count_cold' AS metric, shelf_name, visit_count, exposed_count, pickup_count, gaze_rate, pickup_rate
FROM base
ORDER BY exposed_count ASC
LIMIT 3

UNION ALL

SELECT 'pickup_count_hot' AS metric, shelf_name, visit_count, exposed_count, pickup_count, gaze_rate, pickup_rate
FROM base
ORDER BY pickup_count DESC
LIMIT 3

UNION ALL

SELECT 'pickup_count_cold' AS metric, shelf_name, visit_count, exposed_count, pickup_count, gaze_rate, pickup_rate
FROM base
ORDER BY pickup_count ASC
LIMIT 3

UNION ALL

SELECT 'gaze_rate_hot' AS metric, shelf_name, visit_count, exposed_count, pickup_count, gaze_rate, pickup_rate
FROM base
ORDER BY gaze_rate DESC
LIMIT 3

UNION ALL

SELECT 'gaze_rate_cold' AS metric, shelf_name, visit_count, exposed_count, pickup_count, gaze_rate, pickup_rate
FROM base
ORDER BY gaze_rate ASC
LIMIT 3

UNION ALL

SELECT 'pickup_rate_hot' AS metric, shelf_name, visit_count, exposed_count, pickup_count, gaze_rate, pickup_rate
FROM base
ORDER BY pickup_rate DESC
LIMIT 3

UNION ALL

SELECT 'pickup_rate_cold' AS metric, shelf_name, visit_count, exposed_count, pickup_count, gaze_rate, pickup_rate
FROM base
ORDER BY pickup_rate ASC
LIMIT 3
) AS results
ORDER BY 
CASE metric
    WHEN 'visit_count_hot' THEN 1
    WHEN 'visit_count_cold' THEN 2
    WHEN 'exposed_count_hot' THEN 3
    WHEN 'exposed_count_cold' THEN 4
    WHEN 'pickup_count_hot' THEN 5
    WHEN 'pickup_count_cold' THEN 6
    WHEN 'gaze_rate_hot' THEN 7
    WHEN 'gaze_rate_cold' THEN 8
    WHEN 'pickup_rate_hot' THEN 9
    WHEN 'pickup_rate_cold' THEN 10
END"""

    answer = "진열대 진단 결과. hot은 관심이 많고, cold은 관심이 적은 진열대를 의미함."
    for store, db in db_list.items():
        store_answer = f"{store}:"

        try:
            client = get_clickhouse_client(database=db)

            result = client.query(query)

            if len(result.result_rows) > 0:
                for row in result.result_rows:
                    store_answer += f"\n{row}"
            else:
                store_answer += "데이터가 없습니다."

        except Exception as e:
            store_answer += f"데이터 조회 오류: {e}"

        answer += f"\n{store_answer}"

    # log answer
    logger.info(f"diagnose_shelf 답변: {answer}")

    return answer
        

@mcp.tool()
def diagnose_table_occupancy(start_date: str, end_date: str) -> str:
    """시식대 혼잡도 진단"""

    query = f"""
WITH minute_data AS (
    SELECT
        zone_id,
        zone.name as zone_name,
        AVG(occupancy_count) AS avg_occupancy,
        MAX(occupancy_count) AS max_occupancy
    FROM zone_occupancy_minute
    INNER JOIN zone ON zone.id = zone_id
    WHERE
        zone.name LIKE '%시식대%'
        AND occupancy_count > 0
        AND date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY zone_id, zone_name
),
session_data AS (
    SELECT
        zone_id,
        zone.name as zone_name,
        count(*) AS number_of_sessions,
        avg(dateDiff('minute', start_time, end_time)) AS avg_session_duration,
        max(dateDiff('minute', start_time, end_time)) AS max_session_duration,
        min(dateDiff('minute', start_time, end_time)) AS min_session_duration
    FROM zone_occupancy_session
    INNER JOIN zone ON zone.id = zone_id
    WHERE zone.name LIKE '%시식대%'
    GROUP BY zone_id, zone_name
)
SELECT 
    COALESCE(m.zone_name, s.zone_name) AS zone_name,
    CONCAT(toString(ROUND(m.avg_occupancy, 2)), '명') AS avg_occupancy,
    CONCAT(toString(m.max_occupancy), '명') AS max_occupancy,
    CONCAT(toString(s.number_of_sessions), '회') AS number_of_sessions,
    CONCAT(toString(ROUND(s.avg_session_duration, 2)), '분') AS avg_duration,
    CONCAT(toString(s.max_session_duration), '분') AS max_duration,
    CONCAT(toString(s.min_session_duration), '분') AS min_duration
FROM minute_data m
ALL LEFT JOIN session_data s ON m.zone_id = s.zone_id
ORDER BY zone_name ASC
"""

    answer = ""
    for store, db in db_list.items():
        store_answer = f"{store}:"

        try:
            client = get_clickhouse_client(database=db)

            result = client.query(query)

            if len(result.result_rows) > 0:
                for row in result.result_rows:
                    store_answer += f"\n{row}"
            else:
                store_answer += "데이터가 없습니다."

        except Exception as e:
            store_answer += f"데이터 조회 오류: {e}"

        answer += f"\n{store_answer}"

    # log answer
    logger.info(f"diagnose_table_occupancy 답변: {answer}")

    return answer

if __name__ == "__main__":
    print("FastMCP 서버 시작 - diagnose", file=sys.stderr)
    try:
        mcp.run()
    except Exception as e:
        print(f"서버 오류 발생: {e}", file=sys.stderr)