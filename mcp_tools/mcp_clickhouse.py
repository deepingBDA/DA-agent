from fastmcp import FastMCP
from typing import List

# 데이터베이스 매니저 및 공통 유틸리티 import
from database_manager import get_site_client
from mcp_utils import is_token_limit_exceeded, DEFAULT_MODEL

mcp = FastMCP("clickhouse")

@mcp.tool()
async def show_databases(site: str) -> str:
    """
    특정 매장의 데이터베이스 목록을 조회합니다.
    
    Args:
        site: 매장명 (필수)
    """
    try:
        db = get_site_client(site, "plusinsight")
        if not db:
            return f"❌ {site} 매장 연결 실패"
            
        query = "SHOW DATABASES"
        result = db.query(query.strip())
        answer = f"🏪 **{site} 매장의 데이터베이스 목록:**\n\n"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"{row}\n"
        else:
            answer += "데이터베이스가 없습니다."
        
        if is_token_limit_exceeded(answer, DEFAULT_MODEL):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."
        
        return answer
    except Exception as e:
        return f"❌ {site} 매장 오류: {e}"

@mcp.tool()
async def show_tables(database: str, site: str) -> str:
    """
    특정 매장의 테이블 목록을 조회합니다.
    
    Args:
        database: 데이터베이스 이름
        site: 매장명 (필수)
    """
    try:
        db = get_site_client(site, database)
        if not db:
            return f"❌ {site} 매장 연결 실패"
            
        query = "SHOW TABLES"
        result = db.query(query.strip())
        answer = f"🏪 **{site} 매장 ({database}) 테이블 목록:**\n\n"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"{row}\n"
        else:
            answer += "테이블이 없습니다."
        
        if is_token_limit_exceeded(answer, DEFAULT_MODEL):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."
        
        return answer
    except Exception as e:
        return f"❌ {site} 매장 오류: {e}"

@mcp.tool()
def execute_query(database: str, query: str, site: str) -> str:
    """
    특정 매장에서 쿼리를 실행합니다.
    
    Args:
        database: 데이터베이스 이름
        query: 실행할 쿼리
        site: 매장명 (필수)
    """
    try:
        db = get_site_client(site, database)
        if not db:
            return f"❌ {site} 매장 연결 실패"

        result = db.query(query.strip())
        answer = f"🏪 **{site} 매장 ({database}) 쿼리 결과:**\n\n"
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"{row}\n"
        else:
            answer += "데이터가 없습니다."
        
        if is_token_limit_exceeded(answer, DEFAULT_MODEL):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."
        
        return answer
    except Exception as e:
        return f"❌ {site} 매장 오류: {e}"

# get_available_sites 기능은 mcp_agent_helper.py로 분리됨

@mcp.tool()
def create_database(database_name: str, site: str) -> str:
    """
    특정 매장에 새로운 데이터베이스를 생성합니다.
    
    Args:
        database_name: 생성할 데이터베이스 이름
        site: 매장명 (필수)
    """
    try:
        db = get_site_client(site, "plusinsight")
        if not db:
            return f"❌ {site} 매장 연결 실패"

        query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
        db.query(query)
        return f"✅ {site} 매장에서 데이터베이스 '{database_name}' 생성 완료"
    except Exception as e:
        return f"❌ {site} 매장 데이터베이스 생성 실패: {e}"


if __name__ == "__main__":
    mcp.run()