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

load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

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

mcp = FastMCP("clickhouse")

@mcp.tool()
async def show_databases() -> str:
    try:
        """데이터베이스 목록을 조회한다"""
        # 클라이언트 생성
        db = get_clickhouse_client()
        query = "SHOW DATABASES"
        result = db.query(query.strip())
        answer = ""
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"{row}\n"
        else:
            answer = "데이터베이스가 없습니다."
        
        if is_token_limit_exceeded(answer, model_name):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        return answer
    except Exception as e:
        return f"오류가 발생했습니다: {e}"

@mcp.tool()
async def show_tables(database: str) -> str:
    try:
        """테이블 목록을 조회한다"""
        # 클라이언트 생성
        db = get_clickhouse_client(database=database)
        query = "SHOW TABLES"
        result = db.query(query.strip())
        answer = ""
        if len(result.result_rows) > 0:
            for row in result.result_rows:
                answer += f"{row}\n"
        else:
            answer = "테이블이 없습니다."

        if is_token_limit_exceeded(answer, model_name):
            return "토큰 제한을 초과했습니다. 쿼리를 줄여서 다시 시도해주세요."

        return answer
    except Exception as e:
        return f"오류가 발생했습니다: {e}"

@mcp.tool()
def execute_query(database: str, query: str) -> str:
    try:
        """데이터베이스에서 쿼리를 실행한다"""
        
        # 클라이언트 생성
        db = get_clickhouse_client(database=database)

        result = db.query(query.strip())

        answer = ""
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


if __name__ == "__main__":
    mcp.run()