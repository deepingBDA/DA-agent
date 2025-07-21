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


def get_clickhouse_client(database=None):
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