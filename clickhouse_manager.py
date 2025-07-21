"""ClickHouse 연결 관리자
SSH 터널링과 직접 연결을 지원하며, 중앙에서 관리합니다."""

import os
import logging
from typing import Optional, Dict, Any

import clickhouse_connect
from dotenv import load_dotenv

try:
    from sshtunnel import SSHTunnelForwarder
    SSH_AVAILABLE = True
except ImportError:
    SSH_AVAILABLE = False
    logging.warning("sshtunnel 패키지가 설치되어 있지 않습니다. 'pip install sshtunnel paramiko' 필요")

load_dotenv()
logger = logging.getLogger(__name__)


class ClickHouseManager:
    """ClickHouse 연결 및 SSH 터널링 관리"""

    def __init__(self):
        self.ssh_conf = self._load_ssh_conf()
        self.ch_conf = self._load_ch_conf()
        self.tunnel: Optional[SSHTunnelForwarder] = None

    @staticmethod
    def _load_ssh_conf() -> Dict[str, Any]:
        return {
            "host": os.getenv("SSH_HOST"),
            "port": int(os.getenv("SSH_PORT", "22")),
            "username": os.getenv("SSH_USERNAME"),
            "password": os.getenv("SSH_PASSWORD"),
        }

    @staticmethod
    def _load_ch_conf() -> Dict[str, Any]:
        return {
            "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
            "port": int(os.getenv("CLICKHOUSE_PORT", "9000")),
            "username": os.getenv("CLICKHOUSE_USERNAME", "default"),
            "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
        }

    # ---------------- SSH 터널 -----------------
    def _create_ssh_tunnel(self) -> Optional[SSHTunnelForwarder]:
        if not SSH_AVAILABLE or not self.ssh_conf["host"]:
            return None

        try:
            tunnel = SSHTunnelForwarder(
                (self.ssh_conf["host"], self.ssh_conf["port"]),
                ssh_username=self.ssh_conf["username"],
                ssh_password=self.ssh_conf["password"],
                remote_bind_address=(self.ch_conf["host"], self.ch_conf["port"]),
                local_bind_address=("localhost", 0),
            )
            tunnel.start()
            logger.info("SSH 터널 연결 성공, local_port=%s", tunnel.local_bind_port)
            return tunnel
        except Exception as exc:
            logger.error("SSH 터널 연결 실패: %s", exc)
            return None

    # ---------------- 클라이언트 ----------------
    def get_client(self, database: Optional[str] = None, use_ssh: Optional[bool] = None):
        if use_ssh is None:
            use_ssh = bool(self.ssh_conf["host"])  # SSH 정보가 있으면 사용

        try:
            if use_ssh:
                if not self.tunnel:
                    self.tunnel = self._create_ssh_tunnel()
                if self.tunnel:
                    host, port = "localhost", self.tunnel.local_bind_port
                else:
                    host, port = self.ch_conf["host"], self.ch_conf["port"]
            else:
                host, port = self.ch_conf["host"], self.ch_conf["port"]

            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=self.ch_conf["username"],
                password=self.ch_conf["password"],
                database=database,
            )
            logger.info("ClickHouse 연결 성공 (%s:%s, db=%s)", host, port, database)
            return client
        except Exception as exc:
            logger.error("ClickHouse 연결 실패: %s", exc)
            return None

    def execute_query(self, query: str, database: Optional[str] = None, use_ssh: Optional[bool] = None):
        client = self.get_client(database=database, use_ssh=use_ssh)
        if not client:
            return None
        try:
            result = client.query(query)
            logger.info("쿼리 실행 성공: %s rows", len(result.result_rows))
            return result
        except Exception as exc:
            logger.error("쿼리 실행 실패: %s", exc)
            return None

    def close(self):
        if self.tunnel:
            self.tunnel.stop()
            logger.info("SSH 터널 종료")
            self.tunnel = None

    # 싱글톤 접근용

_manager: Optional[ClickHouseManager] = None

def get_clickhouse_manager() -> ClickHouseManager:
    global _manager
    if _manager is None:
        _manager = ClickHouseManager()
    return _manager


def get_clickhouse_client(database: Optional[str] = None, use_ssh: Optional[bool] = None):
    return get_clickhouse_manager().get_client(database=database, use_ssh=use_ssh)


def execute_clickhouse_query(query: str, database: Optional[str] = None, use_ssh: Optional[bool] = None):
    return get_clickhouse_manager().execute_query(query=query, database=database, use_ssh=use_ssh) 