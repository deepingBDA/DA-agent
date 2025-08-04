"""
Database Manager
================

site_db_connection_config 테이블에서 가져온 연결 정보를 통해
모든 매장의 데이터베이스에 접속할 수 있는 관리자입니다.
"""

import os
import clickhouse_connect
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

def _create_config_client() -> Optional[Any]:
    """설정 데이터베이스 클라이언트 생성 (SSH 터널링 지원)"""
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
        
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database="cu_base"
        )
        print(f"설정 DB 연결 성공: {host}:{port}")
        return client
    except Exception as e:
        print(f"설정 데이터베이스 연결 실패: {e}")
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

def get_site_client(site: str, database: str = None) -> Optional[Any]:
    """특정 매장의 ClickHouse 클라이언트 생성"""
    conn_info = get_site_connection_info(site)
    if not conn_info:
        print(f"매장 '{site}'의 연결 정보를 찾을 수 없습니다.")
        return None
    
    # SSH 터널링 처리
    if conn_info["ssh_host"]:
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
            print(f"SSH 터널 생성: {site} -> localhost:{ssh_tunnel.local_bind_port}")
            
            host = "localhost"
            port = ssh_tunnel.local_bind_port
            
        except Exception as e:
            print(f"SSH 터널 생성 실패: {e}, 직접 연결 시도")
            host = conn_info["db_host"]
            port = conn_info["db_port"]
    else:
        # 직접 연결
        host = conn_info["db_host"]
        port = conn_info["db_port"]
    
    db_name = database or conn_info["db_name"]
    
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=db_name
        )
        print(f"매장 '{site}' 연결 성공: {host}:{port}, db={db_name}")
        return client
    except Exception as e:
        print(f"매장 '{site}' 연결 실패: {e}")
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