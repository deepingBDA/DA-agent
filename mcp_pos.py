from fastmcp import FastMCP
import clickhouse_connect
import os
from dotenv import load_dotenv
import tiktoken
import logging
import sys
import time
from pathlib import Path
from clickhouse_manager import get_clickhouse_client

from utils import create_transition_data
from map_config import item2zone

# 로그 디렉토리 생성
log_dir = Path("/app/logs")
log_dir.mkdir(parents=True, exist_ok=True)

# 파일 핸들러 직접 설정
log_file = log_dir / "mcp_pos.log"
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
logger.info("MCP POS 서버 시작")
logger.info(f"=== 새 세션 시작: {time.strftime('%Y-%m-%d %H:%M:%S')} ===")

load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

model_name = "gpt-4o"
model_max_tokens = {
    "gpt-4o": 128000,
}

db_list = {
    '갈매점': 'plusinsight_bgf_galmae',
    '강북센터피스점':'plusinsight_bgf_gangbuk_centerpiece',
    '강동센트럴점': 'plusinsight_bgf_gangdong_central',
    '금천프라임점': 'plusinsight_bgf_geumcheon_prime',
    '마천점': 'plusinsight_bgf_macheon',
    '마천힐스테이트점': 'plusinsight_bgf_manchon_hillstate',
    'BGF사옥점': 'plusinsight_bgf_saok',
    '신촌르메이르점': 'plusinsight_bgf_sinchon_lemeilleur',
    '수성캐슬점': 'plusinsight_bgf_suseong_castle',
    '타워팰리스점': 'plusinsight_bgf_tower_palace',
    '역삼점': 'plusinsight_bgf_yeoksam',
}

def num_tokens_from_string(string: str, model: str) -> str:
    encoding = tiktoken.encoding_for_model(model)
    num_tokens = len(encoding.encode(string))
    return num_tokens

def is_token_limit_exceeded(text: str, model: str, reserved_tokens: int = 1000) -> bool:
    token_count = num_tokens_from_string(text, model)
    max_tokens = model_max_tokens.get(model, 4096)  # 기본값 4096
    return token_count > (max_tokens - reserved_tokens)

mcp = FastMCP("pos")

@mcp.tool()
def sales_statistics(start_date: str, end_date: str) -> str:
    """POS 데이터 기반 매출 통계 요약"""
    # 파라미터 기록
    param_log = f"sales_statistics 호출됨: start_date={start_date}, end_date={end_date}"
    logger.info(param_log)
    
    try:
        client = get_clickhouse_client(database='cu_base')

        query = f"""
WITH receipt_stats AS (
    SELECT 
        store_nm,
        tran_ymd,
        pos_no,
        tran_no,
        COUNT(DISTINCT item_cd) as sku_count,
        SUM(sale_qty) as total_items,
        SUM(sale_amt) as receipt_total
    FROM cu_revenue_total
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY store_nm, tran_ymd, pos_no, tran_no
)
SELECT 
    store_nm,
    CONCAT(toString(COUNT(*)), '건') as receipt_count,
    CONCAT(toString(toInt32(SUM(receipt_total) / COUNT(DISTINCT tran_ymd) / 10000)), '만원') as daily_avg_sales,
    CONCAT(toString(toInt32(AVG(receipt_total))), '원') as avg_receipt_amount,
    FLOOR(AVG(sku_count), 1) as avg_sku_per_receipt,
    FLOOR(AVG(total_items), 1) as avg_items_per_receipt
FROM receipt_stats
GROUP BY store_nm
ORDER BY store_nm
"""

        result = client.query(query)
        
        answer = "(지점, 총 영수증 건수, 일 평균 매출, 객단가, 건당 SKU 수, 건당 상품 수)"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"\n{row}"
        else:
            answer = "데이터가 없습니다."

        if is_token_limit_exceeded(answer, model_name):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        # 로그 기록
        logger.info(f"sales_statistics 답변: {answer}")
        
        return answer
    except Exception as e:
        error_msg = f"오류가 발생했습니다: {e}"
        logger.error(error_msg)
        return error_msg

@mcp.tool()
def receipt_ranking(start_date: str, end_date: str) -> str:
    """POS 데이터 기반 영수증 건수 비중 Top 5 조회"""
    # 파라미터 기록
    param_log = f"receipt_ranking 호출됨: start_date={start_date}, end_date={end_date}"
    logger.info(param_log)
    
    try:
        client = get_clickhouse_client(database='cu_base')

        query = f"""
WITH receipt_total AS (
    SELECT 
        store_nm,
        COUNT(DISTINCT (tran_ymd, pos_no, tran_no)) as total_receipts
    FROM cu_revenue_total
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY store_nm
),
small_category_receipts AS (
    SELECT 
        store_nm,
        small_nm,
        COUNT(DISTINCT (tran_ymd, pos_no, tran_no)) as receipt_count,
        ROUND(COUNT(DISTINCT (tran_ymd, pos_no, tran_no)) * 100.0 / rt.total_receipts, 2) as receipt_ratio
    FROM cu_revenue_total
    JOIN receipt_total rt USING(store_nm)
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY store_nm, small_nm, rt.total_receipts
),
ranked_categories AS (
    SELECT 
        store_nm,
        small_nm,
        receipt_count,
        receipt_ratio,
        ROW_NUMBER() OVER (PARTITION BY store_nm ORDER BY receipt_ratio DESC) as rank
    FROM small_category_receipts
)
SELECT 
    store_nm,
    MAX(IF(rank = 1, CONCAT(small_nm, ' (', toString(receipt_count), ', ', toString(receipt_ratio), '%)'), '')) as top1_small_nm,
    MAX(IF(rank = 2, CONCAT(small_nm, ' (', toString(receipt_count), ', ', toString(receipt_ratio), '%)'), '')) as top2_small_nm,
    MAX(IF(rank = 3, CONCAT(small_nm, ' (', toString(receipt_count), ', ', toString(receipt_ratio), '%)'), '')) as top3_small_nm,
    MAX(IF(rank = 4, CONCAT(small_nm, ' (', toString(receipt_count), ', ', toString(receipt_ratio), '%)'), '')) as top4_small_nm,
    MAX(IF(rank = 5, CONCAT(small_nm, ' (', toString(receipt_count), ', ', toString(receipt_ratio), '%)'), '')) as top5_small_nm
FROM ranked_categories
GROUP BY store_nm
ORDER BY store_nm
"""

        result = client.query(query)
        
        answer = "(지점, 1위, 2위, 3위, 4위, 5위)"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"\n{row}"
        else:
            answer = "데이터가 없습니다."

        if is_token_limit_exceeded(answer, model_name):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        # 로그 기록
        logger.info(f"receipt_ranking 답변: {answer}")
            
        return answer
    except Exception as e:
        error_msg = f"오류가 발생했습니다: {e}"
        logger.error(error_msg)
        return error_msg

@mcp.tool()
def sales_ranking(start_date: str, end_date: str) -> str:
    """POS 데이터 기반 총 매출 비중 Top 5 조회"""
    # 파라미터 기록
    param_log = f"sales_ranking 호출됨: start_date={start_date}, end_date={end_date}"
    logger.info(param_log)
    
    try:
        client = get_clickhouse_client(database='cu_base')

        query = f"""
WITH store_total AS (
    SELECT 
        store_nm,
        SUM(sale_amt) as total_sales
    FROM cu_revenue_total
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY store_nm
),
small_category_sales AS (
    SELECT 
        store_nm,
        small_nm,
        SUM(sale_amt) as category_sales,
        ROUND(SUM(sale_amt) * 100.0 / st.total_sales, 2) as sales_ratio
    FROM cu_revenue_total
    JOIN store_total st USING(store_nm)
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY store_nm, small_nm, st.total_sales
),
ranked_categories AS (
    SELECT 
        store_nm,
        small_nm,
        category_sales,
        sales_ratio,
        ROW_NUMBER() OVER (PARTITION BY store_nm ORDER BY sales_ratio DESC) as rank
    FROM small_category_sales
)
SELECT 
    store_nm,
    MAX(IF(rank = 1, CONCAT(small_nm, ' (', toString(ROUND(category_sales/10000, 0)), '만원, ', toString(sales_ratio), '%)'), '')) as top1_small_nm,
    MAX(IF(rank = 2, CONCAT(small_nm, ' (', toString(ROUND(category_sales/10000, 0)), '만원, ', toString(sales_ratio), '%)'), '')) as top2_small_nm,
    MAX(IF(rank = 3, CONCAT(small_nm, ' (', toString(ROUND(category_sales/10000, 0)), '만원, ', toString(sales_ratio), '%)'), '')) as top3_small_nm,
    MAX(IF(rank = 4, CONCAT(small_nm, ' (', toString(ROUND(category_sales/10000, 0)), '만원, ', toString(sales_ratio), '%)'), '')) as top4_small_nm,
    MAX(IF(rank = 5, CONCAT(small_nm, ' (', toString(ROUND(category_sales/10000, 0)), '만원, ', toString(sales_ratio), '%)'), '')) as top5_small_nm
FROM ranked_categories
GROUP BY store_nm
ORDER BY store_nm
"""

        result = client.query(query)
        
        answer = "(지점, 1위, 2위, 3위, 4위, 5위)"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"\n{row}"
        else:
            answer = "데이터가 없습니다."

        if is_token_limit_exceeded(answer, model_name):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        # 로그 기록
        logger.info(f"sales_ranking 답변: {answer}")
            
        return answer
    except Exception as e:
        error_msg = f"오류가 발생했습니다: {e}"
        logger.error(error_msg)
        return error_msg

@mcp.tool()
def volume_ranking(start_date: str, end_date: str) -> str:
    """POS 데이터 기반 총 판매량 비중 Top 5 조회"""
    # 파라미터 기록
    param_log = f"volume_ranking 호출됨: start_date={start_date}, end_date={end_date}"
    logger.info(param_log)
    
    try:
        client = get_clickhouse_client()

        query = f"""
WITH store_total AS (
    SELECT 
        store_nm,
        SUM(sale_qty) as total_qty
    FROM cu_revenue_total
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY store_nm
),
small_category_qty AS (
    SELECT 
        store_nm,
        small_nm,
        SUM(sale_qty) as category_qty,
        ROUND(SUM(sale_qty) * 100.0 / st.total_qty, 2) as qty_ratio
    FROM cu_revenue_total
    JOIN store_total st USING(store_nm)
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY store_nm, small_nm, st.total_qty
),
ranked_categories AS (
    SELECT 
        store_nm,
        small_nm,
        category_qty,
        qty_ratio,
        ROW_NUMBER() OVER (PARTITION BY store_nm ORDER BY qty_ratio DESC) as rank
    FROM small_category_qty
)
SELECT 
    store_nm,
    MAX(IF(rank = 1, CONCAT(small_nm, ' (', toString(category_qty), '개, ', toString(qty_ratio), '%)'), '')) as top1_small_nm,
    MAX(IF(rank = 2, CONCAT(small_nm, ' (', toString(category_qty), '개, ', toString(qty_ratio), '%)'), '')) as top2_small_nm,
    MAX(IF(rank = 3, CONCAT(small_nm, ' (', toString(category_qty), '개, ', toString(qty_ratio), '%)'), '')) as top3_small_nm,
    MAX(IF(rank = 4, CONCAT(small_nm, ' (', toString(category_qty), '개, ', toString(qty_ratio), '%)'), '')) as top4_small_nm,
    MAX(IF(rank = 5, CONCAT(small_nm, ' (', toString(category_qty), '개, ', toString(qty_ratio), '%)'), '')) as top5_small_nm
FROM ranked_categories
GROUP BY store_nm
ORDER BY store_nm
"""

        result = client.query(query)
        
        answer = "(지점, 1위, 2위, 3위, 4위, 5위)"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"\n{row}"
        else:
            answer = "데이터가 없습니다."

        if is_token_limit_exceeded(answer, model_name):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        # 로그 기록
        logger.info(f"volume_ranking 답변: {answer}")
            
        return answer
    except Exception as e:
        error_msg = f"오류가 발생했습니다: {e}"
        logger.error(error_msg)
        return error_msg

@mcp.tool()
def event_product_analysis(start_date: str, end_date: str) -> str:
    """POS 데이터 기반 행사 상품 분석 (매출 비중, SKU 비중)"""
    # 파라미터 기록
    param_log = f"event_product_analysis 호출됨: start_date={start_date}, end_date={end_date}"
    logger.info(param_log)
    
    try:
        client = get_clickhouse_client(database='cu_base')

        query = f"""
WITH store_metrics AS (
    SELECT 
        store_nm,
        SUM(sale_amt) as total_sales,
        COUNT(DISTINCT item_cd) as total_sku_count
    FROM cu_revenue_total
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY store_nm
),
event_metrics AS (
    SELECT 
        store_nm,
        SUM(sale_amt) as event_sales,
        COUNT(DISTINCT item_cd) as event_sku_count
    FROM cu_revenue_total
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
    AND evt_nm != ''
    GROUP BY store_nm
)
SELECT 
    sm.store_nm,
    ROUND(em.event_sales * 100.0 / sm.total_sales, 2) as sales_ratio,
    ROUND(em.event_sku_count * 100.0 / sm.total_sku_count, 2) as sku_ratio
FROM store_metrics sm
JOIN event_metrics em USING(store_nm)
ORDER BY sm.store_nm
"""

        result = client.query(query)
        
        answer = "(지점, 매출 비중(%), SKU 비중(%))"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"\n{row}"
        else:
            answer = "데이터가 없습니다."

        if is_token_limit_exceeded(answer, model_name):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        # 로그 기록
        logger.info(f"event_product_analysis 답변: {answer}")
            
        return answer
    except Exception as e:
        error_msg = f"오류가 발생했습니다: {e}"
        logger.error(error_msg)
        return error_msg


@mcp.tool()
def ranking_event_product() -> str:
    """지점별 행사 상품 분석 (매장명, 행사명, 총 판매수량, 거래 횟수, 총 판매금액, 순위)"""
    # 함수 호출 기록
    logger.info("ranking_event_product 호출됨")
    
    try:
        query = """
WITH event_popularity AS (
    SELECT 
        store_nm,
        evt_nm,
        SUM(sale_qty) AS total_qty,
        COUNT(DISTINCT (pos_no, tran_no)) AS transaction_count,
        SUM(sale_amt) AS total_sales
    FROM cu_revenue_total
    WHERE evt_nm != ''
    GROUP BY store_nm, evt_nm
),
ranked_events AS (
    SELECT
        store_nm,
        evt_nm,
        total_qty,
        transaction_count,
        total_sales,
        ROW_NUMBER() OVER (PARTITION BY store_nm ORDER BY total_qty DESC) AS rank
    FROM event_popularity
)
SELECT
    store_nm,
    evt_nm,
    toString(total_qty) AS total_qty_str,
    toString(transaction_count) AS transaction_count_str,
    CONCAT(toString(total_sales), '원') AS total_sales_str,
    toString(rank) AS rank_str
FROM ranked_events
WHERE rank <= 5
ORDER BY store_nm, rank
"""
        client = get_clickhouse_client(database='cu_base')

        result = client.query(query)

        answer = "매장명, 행사명, 총 판매수량, 거래 횟수, 총 판매금액, 순위"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"\n{row}"
        else:
            answer = "데이터가 없습니다."

        if is_token_limit_exceeded(answer, model_name):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        # 로그 기록
        logger.info(f"ranking_event_product 답변: {answer}")
            
        return answer
    except Exception as e:
        error_msg = f"오류가 발생했습니다: {e}"
        logger.error(error_msg)
        return error_msg

@mcp.tool()
def co_purchase_trend(start_date: str, end_date: str) -> str:
    """지점별 / 시간대별 연관 구매 경향성"""
    # 파라미터 기록
    param_log = f"co_purchase_trend 호출됨: start_date={start_date}, end_date={end_date}"
    logger.info(param_log)

    query = """
    WITH receipt_items AS (
        -- 각 영수증에 포함된 상품 추출
        SELECT
            store_nm,
            tran_ymd,
            pos_no,
            tran_no,
            toHour(tran_timestamp) AS hour_of_day,
            CASE
                WHEN toHour(tran_timestamp) >= 6 AND toHour(tran_timestamp) < 11 THEN '아침(06-11)'
                WHEN toHour(tran_timestamp) >= 11 AND toHour(tran_timestamp) < 14 THEN '점심(11-14)'
                WHEN toHour(tran_timestamp) >= 14 AND toHour(tran_timestamp) < 18 THEN '오후(14-18)'
                WHEN toHour(tran_timestamp) >= 18 AND toHour(tran_timestamp) < 22 THEN '저녁(18-22)'
                ELSE '심야(22-06)'
            END AS time_period,
            item_cd,
            item_nm,
            large_nm,
            mid_nm
        FROM cu_revenue_total
        WHERE tran_timestamp IS NOT NULL
        AND store_nm = '{target_store}'  -- 특정 지점만 필터링
        AND tran_ymd BETWEEN '{start_date}' AND '{end_date}'  -- 날짜 범위 필터링
    ),
    item_pairs AS (
        -- 같은 영수증 내에서 함께 구매된 상품 쌍 생성
        SELECT
            a.store_nm,
            a.time_period,
            a.item_cd AS item1_cd,
            a.item_nm AS item1_nm,
            a.large_nm AS item1_category,
            b.item_cd AS item2_cd,
            b.item_nm AS item2_nm,
            b.large_nm AS item2_category,
            COUNT(*) AS pair_count
        FROM receipt_items a
        JOIN receipt_items b ON
            a.store_nm = b.store_nm AND
            a.tran_ymd = b.tran_ymd AND
            a.pos_no = b.pos_no AND
            a.tran_no = b.tran_no AND
            a.time_period = b.time_period
        WHERE a.item_cd < b.item_cd  -- 중복 쌍 방지
        GROUP BY
            a.store_nm,
            a.time_period,
            a.item_cd,
            a.item_nm,
            a.large_nm,
            b.item_cd,
            b.item_nm,
            b.large_nm
    ),
    aggregated_pairs AS (
        -- 시간대별로 상품 쌍 통합
        SELECT
            store_nm,
            time_period,
            item1_nm,
            item1_category,
            item2_nm,
            item2_category,
            SUM(pair_count) AS total_pair_count,
            ROUND(SUM(pair_count) / SUM(SUM(pair_count)) OVER (PARTITION BY store_nm, time_period) * 100, 2) AS percentage
        FROM item_pairs
        GROUP BY
            store_nm,
            time_period,
            item1_nm,
            item1_category,
            item2_nm,
            item2_category
    ),
    ranked_pairs AS (
        -- 시간대별 순위 부여
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY time_period ORDER BY total_pair_count DESC) AS rank
        FROM aggregated_pairs
    )
    -- 시간대별 상위 5개만 선택
    SELECT
        store_nm,
        time_period,
        item1_nm,
        item1_category,
        item2_nm,
        item2_category,
        total_pair_count AS pair_count,
        CONCAT(toString(percentage), '%') AS percentage
    FROM ranked_pairs
    WHERE rank <= 5
    ORDER BY
        CASE 
            WHEN time_period = '아침(06-11)' THEN 1
            WHEN time_period = '점심(11-14)' THEN 2
            WHEN time_period = '오후(14-18)' THEN 3
            WHEN time_period = '저녁(18-22)' THEN 4
            WHEN time_period = '심야(22-06)' THEN 5
        END,
        rank
    """

    answer = ""
    for target_store in db_list.keys():
        store_answer = f"{target_store}"
        try:
                    client = get_clickhouse_client(database='cu_base')
        
        logger.info(f"co_purchase_trend 호출됨: {target_store}, {start_date}, {end_date}")

            result = client.query(query.format(target_store=target_store, start_date=start_date, end_date=end_date))

            
            if len(result.result_rows) > 0:
                for row in result.result_rows:
                    store_answer += f"\n{row}"
            else:
                store_answer += "데이터가 없습니다."

            # 로그 기록
            logger.info(f"co_purchase_trend 답변: {answer}")
            
        except Exception as e:
            store_answer += f"오류가 발생했습니다: {e}"
            logger.error(f"오류가 발생했습니다: {e}")
            continue

        answer += f"{store_answer}\n"
        
    return answer

if __name__ == "__main__":
    print("FastMCP 서버 시작 - pos", file=sys.stderr)
    try:
        mcp.run()
    except Exception as e:
        logger.error(f"서버 오류 발생: {e}")
        print(f"서버 오류 발생: {e}", file=sys.stderr)