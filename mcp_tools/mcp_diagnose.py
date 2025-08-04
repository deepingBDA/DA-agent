from fastmcp import FastMCP
import clickhouse_connect
import os
from dotenv import load_dotenv
import logging
import sys
import time
from pathlib import Path

from utils import create_transition_data
from map_config import item2zone
from database_manager import get_site_client, get_site_connection_info
from mcp_utils import is_token_limit_exceeded, DEFAULT_MODEL
from typing import Optional

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

# 토큰 관련 함수들은 mcp_utils로 이동됨

mcp = FastMCP("diagnose")

@mcp.tool()
def get_db_name(site: str) -> str:
    """특정 매장의 데이터베이스명 조회"""
    try:
        connection_info = get_site_connection_info(site)
        if not connection_info:
            return f"❌ {site} 매장 정보를 찾을 수 없습니다."
        
        db_name = connection_info.get('db_name', 'plusinsight')
        return f"📋 **{site} 매장 정보:**\n데이터베이스명: {db_name}"
    except Exception as e:
        return f"❌ {site} 매장 DB명 조회 실패: {e}"

@mcp.tool()
def diagnose_avg_in(start_date: str, end_date: str, site: str) -> str:
    """[VISITOR_DIAGNOSE]
    Diagnose **average daily visitors** and related trends (gender, age,
    time-slot) for a given period.

    Args:
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)  
        site: 매장명 (필수)

    Trigger words (case-insensitive):
        - "방문객 진단", "유입 진단", "visitor diagnose", "average visitors"
        - Any request like "방문객 분석해줘", "일평균 방문객수" etc.

    Use this tool when the user wants a textual diagnostic (not necessarily
    Excel) about store visitors within a date range.
    """
    # 파라미터 기록
    param_log = f"diagnose_avg_in 호출됨: start_date={start_date}, end_date={end_date}, site={site}"
    logger.info(param_log)
    
    query = f"""
        WITH
df AS (
    SELECT
        li.date AS visit_date,
        li.timestamp,
        li.person_seq AS visitor_id,
        if(toDayOfWeek(li.date) IN (1,2,3,4,5), 'weekday', 'weekend')                     AS day_type,
        multiIf(
            toHour(li.timestamp) IN (22,23,0,1), '22-01',
            toHour(li.timestamp) BETWEEN 2  AND 5 , '02-05',
            toHour(li.timestamp) BETWEEN 6  AND 9 , '06-09',
            toHour(li.timestamp) BETWEEN 10 AND 13, '10-13',
            toHour(li.timestamp) BETWEEN 14 AND 17, '14-17',
            '18-21'
        ) AS time_range,
        multiIf(
            dt.age BETWEEN 0  AND  9 , '10대 미만',
            dt.age BETWEEN 10 AND 19, '10대',
            dt.age BETWEEN 20 AND 29, '20대',
            dt.age BETWEEN 30 AND 39, '30대',
            dt.age BETWEEN 40 AND 49, '40대',
            dt.age BETWEEN 50 AND 59, '50대',
            dt.age >= 60           , '60대 이상',
            'Unknown'
        ) AS age_group,
        if(dt.gender = '0', '남성', if(dt.gender='1','여성','Unknown'))                   AS gender
    FROM line_in_out_individual li
    LEFT JOIN detected_time dt ON li.person_seq = dt.person_seq
    LEFT JOIN line          l  ON li.triggered_line_id = l.id
    WHERE li.date BETWEEN '{start_date}' AND '{end_date}'
      AND li.is_staff = 0
      AND li.in_out   = 'IN'
      AND l.entrance  = 1
),
daily_all     AS (SELECT visit_date, uniqExact(visitor_id) AS ucnt FROM df GROUP BY visit_date),
avg_all       AS (SELECT toUInt64(round(avg(ucnt))) AS avg_cnt FROM daily_all),
daily_dayType AS (SELECT visit_date, day_type, uniqExact(visitor_id) AS ucnt FROM df GROUP BY visit_date, day_type),
avg_dayType   AS (SELECT day_type, toUInt64(round(avg(ucnt))) AS avg_cnt FROM daily_dayType GROUP BY day_type),
daily_gender  AS (SELECT visit_date, gender, uniqExact(visitor_id) AS ucnt FROM df GROUP BY visit_date, gender),
avg_gender    AS (SELECT gender, toUInt64(round(avg(ucnt))) AS avg_cnt FROM daily_gender GROUP BY gender),
daily_age     AS (SELECT visit_date, age_group, uniqExact(visitor_id) AS ucnt FROM df GROUP BY visit_date, age_group),
avg_age       AS (SELECT age_group, toUInt64(round(avg(ucnt))) AS avg_cnt FROM daily_age GROUP BY age_group),
rank_age      AS (SELECT *, row_number() OVER (ORDER BY avg_cnt DESC) AS rk FROM avg_age),
overall_cnt   AS (SELECT avg_cnt AS cnt FROM avg_all),
range_cnt AS (
    SELECT day_type, time_range, uniqExact(visitor_id) AS visit_cnt
    FROM df GROUP BY day_type, time_range
),
tot_cnt   AS (SELECT day_type, sum(visit_cnt) AS total_cnt FROM range_cnt GROUP BY day_type),
range_pct AS (
    SELECT r.day_type, r.time_range, r.visit_cnt,
           toUInt64(round(
               CASE 
                   WHEN t.total_cnt > 0 THEN r.visit_cnt / t.total_cnt * 100
                   ELSE 0
               END
           )) AS pct
    FROM range_cnt r JOIN tot_cnt t USING(day_type)
),
rank_slot AS (
    SELECT *, row_number() OVER (PARTITION BY day_type ORDER BY pct DESC) AS rk
    FROM range_pct
),
final AS (
    SELECT '일평균' AS section, '전체' AS label,
           avg_cnt AS value_cnt, CAST(NULL AS Nullable(UInt64)) AS value_pct, 0 AS ord
    FROM avg_all
    UNION ALL
    SELECT '일평균', '평일', avg_cnt, CAST(NULL AS Nullable(UInt64)), 1
    FROM avg_dayType WHERE day_type='weekday'
    UNION ALL
    SELECT '일평균', '주말', avg_cnt, CAST(NULL AS Nullable(UInt64)), 2
    FROM avg_dayType WHERE day_type='weekend'
    UNION ALL
    SELECT '성별경향', gender, avg_cnt,
           toUInt64(round(
               CASE 
                   WHEN (SELECT cnt FROM overall_cnt) > 0 THEN avg_cnt / (SELECT cnt FROM overall_cnt) * 100
                   ELSE 0
               END
           )) AS value_pct,
           10 + if(gender='남성',0,1) AS ord
    FROM avg_gender WHERE gender IN ('남성','여성')
    UNION ALL
    SELECT '연령대경향',
           concat(toString(rk),'위_',age_group)                         AS label,
           avg_cnt,
           toUInt64(round(
               CASE 
                   WHEN (SELECT cnt FROM overall_cnt) > 0 THEN avg_cnt / (SELECT cnt FROM overall_cnt) * 100
                   ELSE 0
               END
           )) AS value_pct,
           20 + rk AS ord
    FROM rank_age WHERE rk<=3
    UNION ALL
    SELECT '시간대경향',
           concat('평일_',toString(rk),'_',time_range)                 AS label,
           visit_cnt, pct, 30 + rk
    FROM rank_slot WHERE day_type='weekday' AND rk<=3
    UNION ALL
    SELECT '시간대경향',
           concat('주말_',toString(rk),'_',time_range),
           visit_cnt, pct, 40 + rk
    FROM rank_slot WHERE day_type='weekend' AND rk<=3
)
SELECT section, label, value_cnt, value_pct
FROM final
ORDER BY ord
"""

    try:
        client = get_site_client(site)
        if not client:
            return f"❌ {site}: 연결 실패"
            
        result = client.query(query)

        if len(result.result_rows) > 0:
            # 섹션별로 데이터 분류
            sections = {
                '일평균': [],
                '성별경향': [],
                '연령대경향': [],
                '시간대경향': []
            }
            
            for row in result.result_rows:
                section, label, value_cnt, value_pct = row
                sections[section].append((label, value_cnt, value_pct))
            
            # 표 형태로 포맷팅
            answer = f"=== {site} ===\n"
            
            # 1. 일평균 방문객수
            answer += "일평균 방문객수\n"
            for label, cnt, _ in sections['일평균']:
                answer += f"  {label}: {cnt}명\n"
            
            # 2. 성별경향
            answer += "\n성별경향\n"
            for label, cnt, pct in sections['성별경향']:
                gender_display = 'M' if label == '남성' else 'F'
                answer += f"  {gender_display}: {pct}%\n"
            
            # 3. 연령대별 순위 (상위 3개)
            answer += "\n연령대별 순위\n"
            for label, cnt, pct in sections['연령대경향']:
                rank = label.split('위_')[0]
                age_group = label.split('위_')[1]
                answer += f"  {rank}: {age_group} - {pct}%\n"
            
            # 4. 주요 방문시간대
            answer += "\n주요 방문시간대\n"
            
            # 시간대 명칭 매핑
            time_names = {
                '22-01': '심야',
                '02-05': '새벽',
                '06-09': '아침',
                '10-13': '낮',
                '14-17': '오후',
                '18-21': '저녁'
            }
                
            # 평일 시간대 분리
            weekday_slots = [item for item in sections['시간대경향'] if '평일_' in item[0]]
            weekend_slots = [item for item in sections['시간대경향'] if '주말_' in item[0]]
            
            answer += "  평일:\n"
            for label, cnt, pct in weekday_slots:
                rank = label.split('_')[1]
                time_range = label.split('_')[2]
                time_name = time_names.get(time_range, time_range)
                answer += f"    {rank}: {time_name}({time_range}) - {pct}%\n"
            
            answer += "  주말:\n"
            for label, cnt, pct in weekend_slots:
                rank = label.split('_')[1]
                time_range = label.split('_')[2]
                time_name = time_names.get(time_range, time_range)
                answer += f"    {rank}: {time_name}({time_range}) - {pct}%\n"
        else:
            answer = f"⚠️ {site}: 데이터가 없습니다."
        
    except Exception as e:
        answer = f"❌ {site}: 데이터 조회 오류 - {e}"

    # log answer
    logger.info(f"diagnose_avg_in 답변: {answer}")

    return answer

@mcp.tool()
def diagnose_avg_sales(start_date: str, end_date: str, site: str) -> str:
    """일평균 판매 건수 진단
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)  
        site: 매장명 (필수)
    """
    # 파라미터 기록
    param_log = f"diagnose_avg_sales 호출됨: start_date={start_date}, end_date={end_date}, site={site}"
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
        client = get_site_client(site, database='cu_base')
        if not client:
            return f"❌ {site}: 연결 실패"
            
        result = client.query(query)
        client.close()

        if len(result.result_rows) > 0:
            answer = f"📊 **{site}** 일평균 판매 건수:"
            for row in result.result_rows:
                answer += f"\n  - {row[0]}: {row[1]}"
        else:
            answer = f"⚠️ {site}: 데이터가 없습니다."
                
    except Exception as e:
        answer = f"❌ {site}: 오류 - {e}"

    # log answer
    logger.info(f"diagnose_avg_sales 답변: {answer}")

    return answer

@mcp.tool()
def check_zero_visits(start_date: str, end_date: str, site: str) -> str:
    """방문객수 데이터 이상 조회
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)  
        site: 매장명 (필수)
    """
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
ORDER BY date"""

    try:
        client = get_site_client(site)
        if not client:
            return f"❌ {site}: 연결 실패"
            
        result = client.query(query)
        client.close()

        if len(result.result_rows) > 0:
            answer = f"🚨 **{site}** 방문객수 데이터 이상한 날:"
            for row in result.result_rows:
                answer += f"\n  - {row[0]}: {row[1]}"
        else:
            answer = f"✅ {site}: 이상 없습니다."
            
    except Exception as e:
        answer = f"❌ {site}: 오류 - {e}"

    # log answer
    logger.info(f"check_zero_visits 답변: {answer}")
    logger.info(f"answer type : {type(answer)}")

    return answer

@mcp.tool()
def diagnose_purchase_conversion_rate(start_date: str, end_date: str, site: str) -> str:
    """구매전환율 진단
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)  
        site: 매장명 (필수)
    """
    # 파라미터 기록
    param_log = f"get_purchase_conversion_rate 호출됨: start_date={start_date}, end_date={end_date}, site={site}"
    logger.info(param_log)
    
    # 방문객 수와 판매 건수 조회
    avg_in_result = diagnose_avg_in(start_date, end_date, site)
    avg_sales_result = diagnose_avg_sales(start_date, end_date, site)

    # 구매전환율 계산
    answer = f"구매전환율 = (판매 건수 / 방문객 수) * 100 % 라는 공식이야. 일평균 방문객 수를 조회하고, 일평균 판매 건수를 조회해서, 구매전환율을 추정해줘. 구매전환율이 100%를 넘으면 방문객 수가 잘못 측정된거야. 참고해."
    answer += f"\n일평균 방문객 수: {avg_in_result}"
    answer += f"\n일평균 판매 건수: {avg_sales_result}"

    logger.info(f"answer type : {type(answer)}")
    
    return answer

@mcp.tool()
def diagnose_exploratory_tendency(start_date: str, end_date: str, site: str) -> str:
    """탐색 경향성 진단
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)  
        site: 매장명 (필수)
    """

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

    try:
        client = get_site_client(site)
        if not client:
            return f"❌ {site}: 연결 실패"
            
        result = client.query(query.strip())
        client.close()

        if len(result.result_rows) > 0:
            answer = f"📊 **{site}** 탐색 경향성:"
            for row in result.result_rows:
                answer += f"\n  - 1인당 진열대 방문: {row[0]}, 1인당 진열대 노출: {row[1]}, 1인당 진열대 픽업: {row[2]}"
        else:
            answer = f"⚠️ {site}: 데이터가 없습니다."
            
    except Exception as e:
        answer = f"❌ {site}: 오류 - {e}"

    return answer

@mcp.tool()
def diagnose_shelf(start_date: str, end_date: str, site: str) -> str:
    """진열대 진단
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)  
        site: 매장명 (필수)
    """
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

    try:
        client = get_site_client(site)
        if not client:
            return f"❌ {site}: 연결 실패"
            
        result = client.query(query)
        client.close()

        if len(result.result_rows) > 0:
            answer = f"🛍️ **{site}** 진열대 진단 (hot=관심많음, cold=관심적음):"
            for row in result.result_rows:
                answer += f"\n  - {row[0]}: {row[1]} ({row[2]}방문, {row[3]}노출, {row[4]}픽업)"
        else:
            answer = f"⚠️ {site}: 데이터가 없습니다."
            
    except Exception as e:
        answer = f"❌ {site}: 오류 - {e}"

    # log answer
    logger.info(f"diagnose_shelf 답변: {answer}")

    return answer
        

@mcp.tool()
def diagnose_table_occupancy(start_date: str, end_date: str, site: str) -> str:
    """시식대 혼잡도 진단
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)  
        site: 매장명 (필수)
    """

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

    try:
        client = get_site_client(site)
        if not client:
            return f"❌ {site}: 연결 실패"
            
        result = client.query(query)
        client.close()

        if len(result.result_rows) > 0:
            answer = f"🍽️ **{site}** 시식대 혼잡도:"
            for row in result.result_rows:
                answer += f"\n  - {row[0]}: 평균{row[1]}, 최대{row[2]}, 세션{row[3]}, 평균시간{row[4]}"
        else:
            answer = f"⚠️ {site}: 데이터가 없습니다."
            
    except Exception as e:
        answer = f"❌ {site}: 오류 - {e}"

    # log answer
    logger.info(f"diagnose_table_occupancy 답변: {answer}")

    return answer

# 3. 새로운 함수를 생성하는건 데이터베이스 생성하는것뿐이다 (위의 get_db_name 함수로 대체됨)

# get_available_sites 기능은 mcp_agent_helper.py로 분리됨

if __name__ == "__main__":
    print("FastMCP 서버 시작 - diagnose", file=sys.stderr)
    try:
        mcp.run()
    except Exception as e:
        print(f"서버 오류 발생: {e}", file=sys.stderr)