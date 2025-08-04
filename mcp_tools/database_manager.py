"""
Database Manager
================

site_db_connection_config 테이블에서 가져온 연결 정보를 통해
모든 매장의 데이터베이스에 접속할 수 있는 관리자입니다.
"""

import os
import sys
import logging
import clickhouse_connect
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def debug_print(message: str):
    """디버깅 메시지를 즉시 출력"""
    print(message, file=sys.stderr)  # stderr로 출력
    sys.stderr.flush()
    logger.error(message)  # ERROR 레벨로 강제 출력

def _create_config_client() -> Optional[Any]:
    """설정 데이터베이스 클라이언트 생성 (SSH 터널링 지원)"""
    debug_print(f"🔧 [DEBUG] 설정 DB 연결 시도:")
    try:
        # SSH 터널링이 필요한 경우
        ssh_host = os.getenv("SSH_HOST")
        if ssh_host:
            try:
                from sshtunnel import SSHTunnelForwarder
                
                ssh_tunnel = SSHTunnelForwarder(
                    (ssh_host, int(os.getenv("SSH_PORT", "22"))),
                    ssh_username=os.getenv("SSH_USERNAME"),
                    ssh_password=os.getenv("SSH_PASSWORD"),
                    remote_bind_address=(os.getenv("CONFIG_DB_HOST", "localhost"), int(os.getenv("CONFIG_DB_PORT", "8123"))),
                    local_bind_address=("localhost", 0),
                )
                ssh_tunnel.start()
                print(f"설정 DB SSH 터널 생성: localhost:{ssh_tunnel.local_bind_port}")
                
                host = "localhost"
                port = ssh_tunnel.local_bind_port
                
            except Exception as e:
                print(f"설정 DB SSH 터널 생성 실패: {e}, 직접 연결 시도")
                host = os.getenv("CONFIG_DB_HOST", "localhost")
                port = int(os.getenv("CONFIG_DB_PORT", "8123"))
        else:
            # 직접 연결
            host = os.getenv("CONFIG_DB_HOST", "localhost")
            port = int(os.getenv("CONFIG_DB_PORT", "8123"))
        
        print(f"🔌 [DEBUG] 설정 DB ClickHouse 연결:")
        print(f"  - Host: {host}")
        print(f"  - Port: {port}")
        print(f"  - Username: {os.getenv('CLICKHOUSE_USER', 'None')}")
        print(f"  - Password: {'***' if os.getenv('CLICKHOUSE_PASSWORD') else 'None'}")
        print(f"  - Database: cu_base")
        
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            database="cu_base"
        )
        print(f"✅ [SUCCESS] 설정 DB 연결 성공: {host}:{port}")
        return client
    except Exception as e:
        print(f"❌ [ERROR] 설정 데이터베이스 연결 실패: {e}")
        print(f"🔍 [DEBUG] 설정 DB 연결 실패 상세: {type(e).__name__}: {str(e)}")
        return None

def get_site_connection_info(site: str) -> Optional[Dict[str, Any]]:
    """site_db_connection_config 테이블에서 매장 연결 정보 조회"""
    try:
        # 설정 DB에 연결
        config_client = _create_config_client()
        if not config_client:
            return None
        
        query = f"""
        SELECT ssh_host, ssh_port, db_host, db_port, db_name
        FROM site_db_connection_config
        WHERE site = '{site}'
        """
        
        result = config_client.query(query)
        config_client.close()
        
        if result.result_rows:
            row = result.result_rows[0]
            return {
                "ssh_host": row[0],
                "ssh_port": row[1] or 22,
                "db_host": row[2],
                "db_port": row[3],
                "db_name": row[4] or "plusinsight"
            }
        return None
    except Exception as e:
        print(f"매장 '{site}' 연결 정보 조회 실패: {e}")
        return None

def get_site_client(site: str, database: str = 'plusinsight') -> Optional[Any]:
    """특정 매장의 ClickHouse 클라이언트 생성"""
    debug_print(f"🔍 [DEBUG] 매장 '{site}' 연결 시도 시작")
    
    conn_info = get_site_connection_info(site)
    if not conn_info:
        print(f"❌ [ERROR] 매장 '{site}'의 연결 정보를 찾을 수 없습니다.")
        return None
    
    print(f"📋 [DEBUG] 매장 '{site}' 연결 정보:")
    print(f"  - SSH Host: {conn_info.get('ssh_host', 'None')}")
    print(f"  - SSH Port: {conn_info.get('ssh_port', 'None')}")
    print(f"  - DB Host: {conn_info.get('db_host', 'None')}")
    print(f"  - DB Port: {conn_info.get('db_port', 'None')}")
    print(f"  - DB Name: {conn_info.get('db_name', 'None')}")
    
    # SSH 터널링 처리
    if conn_info["ssh_host"]:
        print(f"🚇 [DEBUG] SSH 터널링 시도 중...")
        print(f"  - SSH 서버: {conn_info['ssh_host']}:{conn_info['ssh_port']}")
        print(f"  - SSH 사용자: {os.getenv('SSH_USERNAME', 'None')}")
        print(f"  - 원격 DB: {conn_info['db_host']}:{conn_info['db_port']}")
        
        try:
            from sshtunnel import SSHTunnelForwarder
            
            ssh_tunnel = SSHTunnelForwarder(
                (conn_info["ssh_host"], conn_info["ssh_port"]),
                ssh_username=os.getenv("SSH_USERNAME"),
                ssh_password=os.getenv("SSH_PASSWORD"),
                remote_bind_address=(conn_info["db_host"], conn_info["db_port"]),
                local_bind_address=("localhost", 0),
            )
            ssh_tunnel.start()
            print(f"✅ [SUCCESS] SSH 터널 생성: {site} -> localhost:{ssh_tunnel.local_bind_port}")
            
            host = "localhost"
            port = ssh_tunnel.local_bind_port
            
        except Exception as e:
            print(f"❌ [ERROR] SSH 터널 생성 실패: {e}")
            print(f"🔄 [INFO] 직접 연결로 전환")
            host = conn_info["db_host"]
            port = conn_info["db_port"]
    else:
        print(f"🔗 [DEBUG] 직접 연결 모드")
        host = conn_info["db_host"]
        port = conn_info["db_port"]
    
    print(f"🔌 [DEBUG] ClickHouse 연결 시도:")
    print(f"  - Host: {host}")
    print(f"  - Port: {port}")
    print(f"  - Username: {os.getenv('CLICKHOUSE_USER', 'None')}")
    print(f"  - Password: {'***' if os.getenv('CLICKHOUSE_PASSWORD') else 'None'}")
    print(f"  - Database: plusinsight")
    
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            database='plusinsight'
        )
        print(f"✅ [SUCCESS] 매장 '{site}' 연결 성공: {host}:{port}")
        return client
    except Exception as e:
        print(f"❌ [ERROR] 매장 '{site}' 연결 실패: {e}")
        print(f"🔍 [DEBUG] 연결 실패 상세 정보: {type(e).__name__}: {str(e)}")
        return None

def get_all_sites() -> List[str]:
    """모든 매장 목록 조회"""
    try:
        config_client = _create_config_client()
        if not config_client:
            return []
        
        result = config_client.query("SELECT DISTINCT site FROM site_db_connection_config ORDER BY site")
        sites = [row[0] for row in result.result_rows]
        config_client.close()
        
        print(f"사용 가능한 매장: {sites}")
        return sites
    except Exception as e:
        print(f"매장 목록 조회 실패: {e}")
        return []

def test_connection(site: str = None) -> str:
    """연결 테스트"""
    if site:
        client = get_site_client(site)
        if client:
            try:
                result = client.query("SELECT 1")
                client.close()
                return f"매장 '{site}' 연결 테스트 성공"
            except Exception as e:
                return f"매장 '{site}' 연결 테스트 실패: {e}"
        else:
            return f"매장 '{site}' 클라이언트 생성 실패"
    else:
        sites = get_all_sites()
        results = []
        for s in sites[:3]:  # 처음 3개만 테스트
            result = test_connection(s)
            results.append(result)
        return "\n".join(results)

if __name__ == "__main__":
    # 테스트 실행
    print("=== Database Manager 테스트 ===")
    print("\n1. 매장 목록 조회:")
    sites = get_all_sites()
    for i, site in enumerate(sites, 1):
        print(f"  {i}. {site}")
    
    print("\n2. 연결 테스트:")
    print(test_connection())