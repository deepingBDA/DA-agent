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
from pathlib import Path
from datetime import datetime

load_dotenv()

# 컨테이너 환경에서만 로그 파일 생성
log_dir = None
connection_log_file = None

# 컨테이너 환경 체크 (/app 디렉토리 존재 여부)
if Path("/app").exists():
    try:
        log_dir = Path("/app/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        connection_log_file = log_dir / "database_connections.log"
        print(f"📋 로그 파일: {connection_log_file}")
    except Exception as e:
        print(f"⚠️ 로그 파일 생성 실패: {e}")
        connection_log_file = None

# 로깅 설정 (콘솔만 또는 콘솔+파일)
handlers = [logging.StreamHandler(sys.stdout)]
if connection_log_file:
    handlers.append(logging.FileHandler(connection_log_file, encoding='utf-8'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=handlers
)
logger = logging.getLogger(__name__)

def log_connection_attempt(action: str, site: str = None, details: Dict[str, Any] = None):
    """데이터베이스 연결 시도를 로그에 기록 (컨테이너에서만 파일 기록)"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {action}"
    
    if site:
        log_entry += f" - 매장: {site}"
    
    if details:
        for key, value in details.items():
            if 'password' in key.lower():
                value = '***' if value else 'None'
            log_entry += f" | {key}: {value}"
    
    logger.info(log_entry)
    
    # 컨테이너 환경에서만 파일에 추가 기록
    if connection_log_file:
        try:
            with open(connection_log_file, 'a', encoding='utf-8') as f:
                f.write(log_entry + '\n')
                f.flush()
        except Exception as e:
            print(f"⚠️ 로그 파일 쓰기 실패: {e}")

def debug_print(message: str):
    """디버깅 메시지를 즉시 출력"""
    print(message, file=sys.stderr)  # stderr로 출력
    sys.stderr.flush()
    logger.error(message)  # ERROR 레벨로 강제 출력

def _create_config_client() -> Optional[Any]:
    """설정 데이터베이스 클라이언트 생성 (SSH 터널링 지원)"""
    debug_print(f"🔧 [DEBUG] 설정 DB 연결 시도:")
    
    # 연결 시도 로그
    log_connection_attempt("CONFIG_DB_CONNECTION_START", details={
        "ssh_host": os.getenv("SSH_HOST"),
        "config_db_host": os.getenv("CONFIG_DB_HOST", "localhost"),
        "config_db_port": os.getenv("CONFIG_DB_PORT", "8123"),
        "username": os.getenv("CLICKHOUSE_USER")
    })
    
    try:
        # SSH 터널링이 필요한 경우
        ssh_host = os.getenv("SSH_HOST") 
        if ssh_host:
            try:
                # Paramiko 환경 디버깅 및 호환성 처리
                import paramiko
                print(f"🔍 Paramiko 버전: {paramiko.__version__}")
                print(f"🔍 Paramiko 경로: {paramiko.__file__}")
                print(f"🔍 DSSKey 존재: {hasattr(paramiko, 'DSSKey')}")
                
                # 사용 가능한 키 타입들 출력
                key_types = [attr for attr in dir(paramiko) if 'Key' in attr and not attr.startswith('_')]
                print(f"🔍 사용 가능한 키 타입들: {key_types}")
                
                # DSSKey 속성이 없으면 더미 클래스 추가
                if not hasattr(paramiko, 'DSSKey'):
                    print("⚠️ Paramiko DSSKey 호환성 문제 감지, 더미 클래스 생성")
                    # 더미 DSSKey 클래스 생성
                    class DummyDSSKey:
                        def __init__(self, *args, **kwargs):
                            raise NotImplementedError("DSS keys are not supported in this paramiko version")
                        
                        @classmethod
                        def from_private_key_file(cls, *args, **kwargs):
                            raise NotImplementedError("DSS keys are not supported in this paramiko version")
                    
                    paramiko.DSSKey = DummyDSSKey
                    print("✅ DSSKey 더미 클래스 생성 완료")
                
                from sshtunnel import SSHTunnelForwarder
                
                # SSH 터널 설정 - 호환성 강화
                ssh_tunnel = SSHTunnelForwarder(
                    (ssh_host, int(os.getenv("SSH_PORT", "22"))),
                    ssh_username=os.getenv("SSH_USERNAME"),
                    ssh_password=os.getenv("SSH_PASSWORD"),
                    remote_bind_address=(os.getenv("CONFIG_DB_HOST", "localhost"), int(os.getenv("CONFIG_DB_PORT", "8123"))),
                    local_bind_address=("localhost", 0),
                    # 호환성을 위한 추가 옵션
                    ssh_config_file=None,
                    allow_agent=False,
                    host_pkey_directories=None,
                    # 키 타입 제한
                    ssh_pkey=None
                )
                ssh_tunnel.start()
                print(f"설정 DB SSH 터널 생성: localhost:{ssh_tunnel.local_bind_port}")
                
                # SSH 터널 성공 로그
                log_connection_attempt("CONFIG_DB_SSH_TUNNEL_SUCCESS", details={
                    "local_port": ssh_tunnel.local_bind_port,
                    "remote_host": os.getenv("CONFIG_DB_HOST", "localhost"),
                    "remote_port": os.getenv("CONFIG_DB_PORT", "8123")
                })
                
                host = "localhost"
                port = ssh_tunnel.local_bind_port
                
            except Exception as e:
                print(f"설정 DB SSH 터널 생성 실패: {e}, 직접 연결 시도")
                log_connection_attempt("CONFIG_DB_SSH_TUNNEL_FAILED", details={
                    "error": str(e),
                    "error_type": type(e).__name__
                })
                # SSH 실패 시 원격 호스트로 직접 연결 시도
                host = ssh_host
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
        
        # ClickHouse 연결 시도 (재시도 로직 포함)
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            database="cu_base",
            # 연결 타임아웃 설정
            connect_timeout=10,
            send_receive_timeout=30
        )
        
        # 연결 테스트
        client.query("SELECT 1")
        print(f"✅ [SUCCESS] 설정 DB 연결 성공: {host}:{port}")
        
        # 연결 성공 로그
        log_connection_attempt("CONFIG_DB_CONNECTION_SUCCESS", details={
            "final_host": host,
            "final_port": port,
            "database": "cu_base"
        })
        
        return client
    except Exception as e:
        print(f"❌ [ERROR] 설정 데이터베이스 연결 실패: {e}")
        print(f"🔍 [DEBUG] 설정 DB 연결 실패 상세: {type(e).__name__}: {str(e)}")
        
        # 연결 실패 로그
        log_connection_attempt("CONFIG_DB_CONNECTION_FAILED", details={
            "error_type": type(e).__name__,
            "error_message": str(e)
        })
        
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
    debug_print(f"🔍 [DEBUG] 매장 '{site}' 연결 시도 시작 (database: {database})")
    
    # 매장 연결 시도 로그
    log_connection_attempt("SITE_CONNECTION_START", site=site, details={
        "requested_database": database
    })
    
    # cu_base 데이터베이스 요청 시 중앙 DB 사용
    if database == 'cu_base':
        debug_print(f"🏪 [DEBUG] cu_base 요청됨 - 중앙 DB 연결 사용")
        return _create_config_client()
    
    conn_info = get_site_connection_info(site)
    if not conn_info:
        print(f"❌ [ERROR] 매장 '{site}'의 연결 정보를 찾을 수 없습니다.")
        log_connection_attempt("SITE_CONNECTION_INFO_NOT_FOUND", site=site)
        return None
    
    print(f"📋 [DEBUG] 매장 '{site}' 연결 정보:")
    print(f"  - SSH Host: {conn_info.get('ssh_host', 'None')}")
    print(f"  - SSH Port: {conn_info.get('ssh_port', 'None')}")
    print(f"  - DB Host: {conn_info.get('db_host', 'None')}")
    print(f"  - DB Port: {conn_info.get('db_port', 'None')}")
    print(f"  - DB Name: {conn_info.get('db_name', 'None')}")
    
    # 연결 정보 로그
    log_connection_attempt("SITE_CONNECTION_INFO_FOUND", site=site, details={
        "ssh_host": conn_info.get('ssh_host'),
        "ssh_port": conn_info.get('ssh_port'),
        "db_host": conn_info.get('db_host'),
        "db_port": conn_info.get('db_port'),
        "db_name": conn_info.get('db_name')
    })
    
    # SSH 터널링 처리
    if conn_info["ssh_host"]:
        print(f"🚇 [DEBUG] SSH 터널링 시도 중...")
        print(f"  - SSH 서버: {conn_info['ssh_host']}:{conn_info['ssh_port']}")
        print(f"  - SSH 사용자: {os.getenv('SSH_USERNAME', 'None')}")
        print(f"  - 원격 DB: {conn_info['db_host']}:{conn_info['db_port']}")
        
        try:
            # Paramiko 호환성 문제 해결
            import paramiko
            if not hasattr(paramiko, 'DSSKey'):
                print("⚠️ Paramiko DSSKey 호환성 문제 감지, RSA 키만 사용")
            
            from sshtunnel import SSHTunnelForwarder
            
            ssh_tunnel = SSHTunnelForwarder(
                (conn_info["ssh_host"], int(conn_info["ssh_port"])),
                ssh_username=os.getenv("SSH_USERNAME"),
                ssh_password=os.getenv("SSH_PASSWORD"),
                remote_bind_address=(conn_info["db_host"], int(conn_info["db_port"])),
                local_bind_address=("localhost", 0),
                # 호환성을 위한 추가 옵션
                ssh_config_file=None,
                allow_agent=False,
                host_pkey_directories=None
            )
            ssh_tunnel.start()
            print(f"✅ [SUCCESS] SSH 터널 생성: {site} -> localhost:{ssh_tunnel.local_bind_port}")
            
            # SSH 터널 성공 로그
            log_connection_attempt("SITE_SSH_TUNNEL_SUCCESS", site=site, details={
                "local_port": ssh_tunnel.local_bind_port,
                "remote_host": conn_info["db_host"],
                "remote_port": conn_info["db_port"]
            })
            
            host = "localhost"
            port = ssh_tunnel.local_bind_port
            
        except Exception as e:
            print(f"❌ [ERROR] SSH 터널 생성 실패: {e}")
            print(f"🔄 [INFO] 직접 연결로 전환")
            log_connection_attempt("SITE_SSH_TUNNEL_FAILED", site=site, details={
                "error": str(e),
                "error_type": type(e).__name__
            })
            # SSH 실패 시 원격 호스트로 직접 연결 시도
            host = conn_info["ssh_host"] if conn_info["ssh_host"] else conn_info["db_host"]
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
    print(f"  - Database: {database}")
    
    try:
        # ClickHouse 연결 시도 (타임아웃 설정)
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            database=database,
            # 연결 타임아웃 설정
            connect_timeout=10,
            send_receive_timeout=30
        )
        
        # 연결 테스트
        client.query("SELECT 1")
        print(f"✅ [SUCCESS] 매장 '{site}' 연결 성공: {host}:{port}")
        
        # 연결 성공 로그
        log_connection_attempt("SITE_CONNECTION_SUCCESS", site=site, details={
            "final_host": host,
            "final_port": port,
            "database": database
        })
        
        return client
    except Exception as e:
        print(f"❌ [ERROR] 매장 '{site}' 연결 실패: {e}")
        print(f"🔍 [DEBUG] 연결 실패 상세 정보: {type(e).__name__}: {str(e)}")
        
        # 연결 실패 로그
        log_connection_attempt("SITE_CONNECTION_FAILED", site=site, details={
            "error_type": type(e).__name__,
            "error_message": str(e),
            "attempted_host": host,
            "attempted_port": port
        })
        
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