from fastmcp import FastMCP
import logging
import sys
import time
from pathlib import Path
from typing import List

# 데이터베이스 매니저 및 공통 유틸리티 import
from database_manager import get_site_client
from mcp_utils import is_token_limit_exceeded, DEFAULT_MODEL

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



# 토큰 관련 함수들은 mcp_utils로 이동됨

mcp = FastMCP("pos")

@mcp.tool()
def sales_statistics(start_date: str, end_date: str, site: str) -> str:
    """
    POS 데이터 기반 매출 통계 요약
    매장명이 지정되지 않으면 모든 매장의 통계를 조회합니다.
    
    Args:
start_date: 시작 날짜
        end_date: 종료 날짜
        site: 매장명 (필수)
    """
    # 파라미터 기록
    param_log = f"sales_statistics 호출됨: start_date={start_date}, end_date={end_date}, site={site}"
    logger.info(param_log)
    
    try:
        # TODO: 실제 로직 구현 필요
        return f"sales_statistics 호출됨: {site} 매장, {start_date}~{end_date}"
        
    except Exception as e:
        error_msg = f"오류가 발생했습니다: {e}"
        logger.error(error_msg)
        return error_msg

@mcp.tool()
def receipt_ranking(start_date: str, end_date: str, site: str) -> str:
    """
    특정 매장의 POS 데이터 기반 영수증 건수 비중 Top 5 조회
    
    Args:
start_date: 시작 날짜
        end_date: 종료 날짜
        site: 매장명 (필수)
    """
    # 파라미터 기록
    param_log = f"receipt_ranking 호출됨: start_date={start_date}, end_date={end_date}, site={site}"
    logger.info(param_log)
    
    try:
        client = get_site_client(site, 'cu_base')
        if not client:
            return f"❌ {site} 매장 연결 실패"

        query = f"""
WITH receipt_total AS (
    SELECT 
        store_nm,
        COUNT(DISTINCT (tran_ymd, pos_no, tran_no)) as total_receipts
    FROM cu_revenue_total
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
      AND store_nm = '{site}'
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
      AND store_nm = '{site}'
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
        
        answer = f"🏪 **{site} 매장 영수증 랭킹 ({start_date} ~ {end_date}):**\n\n"
        answer += "(지점, 1위, 2위, 3위, 4위, 5위)"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"\n{row}"
        else:
            answer += "\n데이터가 없습니다."

        if is_token_limit_exceeded(answer, DEFAULT_MODEL):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        # 로그 기록
        logger.info(f"receipt_ranking 답변: {answer}")
            
        return answer
    except Exception as e:
        error_msg = f"❌ {site} 매장 오류: {e}"
        logger.error(error_msg)
        return error_msg

@mcp.tool()
def sales_ranking(start_date: str, end_date: str, site: str) -> str:
    """POS 데이터 기반 총 매출 비중 Top 5 조회"""
    # 파라미터 기록
    param_log = f"sales_ranking 호출됨: start_date={start_date}, end_date={end_date}, site={site}"
    logger.info(param_log)
    
    try:
        client = get_site_client(site, 'cu_base')
        if not client:
            return f"❌ {site} 매장 연결 실패"

        query = f"""
WITH store_total AS (
    SELECT 
        store_nm,
        SUM(sale_amt) as total_sales
    FROM cu_revenue_total
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
      AND store_nm = '{site}'
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
      AND store_nm = '{site}'
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

        if is_token_limit_exceeded(answer, DEFAULT_MODEL):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        # 로그 기록
        logger.info(f"sales_ranking 답변: {answer}")
            
        return answer
    except Exception as e:
        error_msg = f"오류가 발생했습니다: {e}"
        logger.error(error_msg)
        return error_msg

@mcp.tool()
def volume_ranking(start_date: str, end_date: str, site: str) -> str:
    """POS 데이터 기반 총 판매량 비중 Top 5 조회"""
    # 파라미터 기록
    param_log = f"volume_ranking 호출됨: start_date={start_date}, end_date={end_date}"
    logger.info(param_log)
    
    try:
        client = get_site_client(site, 'cu_base')

        query = f"""
WITH store_total AS (
    SELECT 
        store_nm,
        SUM(sale_qty) as total_qty
    FROM cu_revenue_total
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
      AND store_nm = '{site}'
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
      AND store_nm = '{site}'
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

        if is_token_limit_exceeded(answer, DEFAULT_MODEL):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        # 로그 기록
        logger.info(f"volume_ranking 답변: {answer}")
            
        return answer
    except Exception as e:
        error_msg = f"오류가 발생했습니다: {e}"
        logger.error(error_msg)
        return error_msg

@mcp.tool()
def event_product_analysis(start_date: str, end_date: str, site: str) -> str:
    """POS 데이터 기반 행사 상품 분석 (매출 비중, SKU 비중)"""
    # 파라미터 기록
    param_log = f"event_product_analysis 호출됨: start_date={start_date}, end_date={end_date}, site={site}"
    logger.info(param_log)
    
    try:
        client = get_site_client(site, 'cu_base')
        if not client:
            return f"❌ {site} 매장 연결 실패"

        query = f"""
WITH store_metrics AS (
    SELECT 
        store_nm,
        SUM(sale_amt) as total_sales,
        COUNT(DISTINCT item_cd) as total_sku_count
    FROM cu_revenue_total
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
      AND store_nm = '{site}'
    GROUP BY store_nm
),
event_metrics AS (
    SELECT 
        store_nm,
        SUM(sale_amt) as event_sales,
        COUNT(DISTINCT item_cd) as event_sku_count
    FROM cu_revenue_total
    WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
      AND store_nm = '{site}'
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

        if is_token_limit_exceeded(answer, DEFAULT_MODEL):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        # 로그 기록
        logger.info(f"event_product_analysis 답변: {answer}")
            
        return answer
    except Exception as e:
        error_msg = f"오류가 발생했습니다: {e}"
        logger.error(error_msg)
        return error_msg


@mcp.tool()
def ranking_event_product(site: str) -> str:
    """지점별 행사 상품 분석 (매장명, 행사명, 총 판매수량, 거래 횟수, 총 판매금액, 순위)"""
    # 함수 호출 기록
    logger.info("ranking_event_product 호출됨")
    
    try:
        query = f"""
WITH event_popularity AS (
    SELECT 
        store_nm,
        evt_nm,
        SUM(sale_qty) AS total_qty,
        COUNT(DISTINCT (pos_no, tran_no)) AS transaction_count,
        SUM(sale_amt) AS total_sales
    FROM cu_revenue_total
    WHERE evt_nm != ''
      AND store_nm = '{site}'
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
        client = get_site_client(site, 'cu_base')
        if not client:
            return f"❌ {site} 매장 연결 실패"

        result = client.query(query)

        answer = "매장명, 행사명, 총 판매수량, 거래 횟수, 총 판매금액, 순위"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"\n{row}"
        else:
            answer = "데이터가 없습니다."

        if is_token_limit_exceeded(answer, DEFAULT_MODEL):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        # 로그 기록
        logger.info(f"ranking_event_product 답변: {answer}")
            
        return answer
    except Exception as e:
        error_msg = f"오류가 발생했습니다: {e}"
        logger.error(error_msg)
        return error_msg

@mcp.tool()
def co_purchase_trend(start_date: str, end_date: str, site: str) -> str:
    """지점별 / 시간대별 연관 구매 경향성"""
    # 파라미터 기록
    param_log = f"co_purchase_trend 호출됨: start_date={start_date}, end_date={end_date}"
    logger.info(param_log)

    query = f"""
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
        AND store_nm = '{site}'
        AND tran_ymd BETWEEN '{start_date}' AND '{end_date}'
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

    try:
        client = get_site_client(site, 'cu_base')
        if not client:
            return f"❌ {site} 매장 연결 실패"
        
        logger.info(f"co_purchase_trend 호출됨: {site}, {start_date}, {end_date}")

        result = client.query(query)

        if len(result.result_rows) > 0:
            answer = f"🛒 **{site}** 연관 구매 경향성:"
            for row in result.result_rows:
                answer += f"\n{row}"
        else:
            answer = f"⚠️ {site}: 데이터가 없습니다."

        # 로그 기록
        logger.info(f"co_purchase_trend 답변: {answer}")
        return answer
        
    except Exception as e:
        error_msg = f"❌ {site} 매장 오류: {e}"
        logger.error(f"co_purchase_trend 오류: {e}")
        return error_msg

# get_available_sites 기능은 mcp_agent_helper.py로 분리됨

@mcp.tool()
def pos_daily_sales_stats(start_date: str, end_date: str, site: str) -> str:
    """POS 데이터 기반 일평균 판매 건수 통계
    
    🏪 POS DATABASE TOOL (cu_base only)
    - 데이터베이스: cu_base (중앙 단일 DB) 
    - 테이블: cu_revenue_total (모든 매장 POS 데이터)
    - 필터링: store_nm = '{site}' 로 특정 매장만 조회
    
    ⚠️ IMPORTANT: cu_revenue_total은 중앙 cu_base DB에만 존재합니다.
    plusinsight DB에서는 접근할 수 없습니다.
    
    Args:
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)  
        site: 매장명 (필수) - store_nm 컬럼과 매칭
        
    Returns:
        일평균 판매 건수 통계 결과
    """
    # 파라미터 기록
    param_log = f"pos_daily_sales_stats 호출됨: start_date={start_date}, end_date={end_date}, site={site}"
    logger.info(param_log)
    
    query = f"""
    WITH daily_sales AS (
        SELECT 
            store_nm,
            tran_ymd,
            COUNT(DISTINCT (pos_no, tran_no)) as daily_receipt_count
        FROM cu_revenue_total
        WHERE tran_ymd BETWEEN '{start_date}' AND '{end_date}'
          AND store_nm = '{site}'
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
    logger.info(f"pos_daily_sales_stats 답변: {answer}")
    
    return answer


if __name__ == "__main__":
    print("FastMCP 서버 시작 - pos", file=sys.stderr)
    try:
        mcp.run()
    except Exception as e:
        logger.error(f"서버 오류 발생: {e}")
        print(f"서버 오류 발생: {e}", file=sys.stderr)