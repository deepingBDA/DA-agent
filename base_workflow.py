"""
Base Workflow 클래스
공통 기능들을 제공하는 추상 베이스 클래스
"""

import os
import sys
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

import clickhouse_connect
from dotenv import load_dotenv


class BaseWorkflow(ABC):
    """
    모든 워크플로우의 기본 클래스
    공통 기능들을 제공합니다.
    """
    
    def __init__(self, workflow_name: str = "base"):
        """
        Args:
            workflow_name: 워크플로우 이름 (로깅에 사용)
        """
        self.workflow_name = workflow_name
        load_dotenv()
        
        # 로깅 설정
        self.logger = self._setup_logging()
        self.logger.info(f"{workflow_name} 워크플로우 초기화")
        
        # ClickHouse 연결 정보
        self.clickhouse_config = {
            'host': os.getenv("CLICKHOUSE_HOST"),
            'port': os.getenv("CLICKHOUSE_PORT"), 
            'username': os.getenv("CLICKHOUSE_USER"),
            'password': os.getenv("CLICKHOUSE_PASSWORD")
        }
        
    def _setup_logging(self) -> logging.Logger:
        """로깅 설정 (내부 메서드)"""
        log_dir = Path("results/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"{self.workflow_name}_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        logger = logging.getLogger(self.workflow_name)
        logger.info(f"로그 파일 생성: {log_file}")
        return logger
    
    def _get_clickhouse_client(self, database: Optional[str] = None) -> Optional[clickhouse_connect.Client]:
        """ClickHouse 클라이언트 생성 (내부 메서드)"""
        try:
            client = clickhouse_connect.get_client(
                host=self.clickhouse_config['host'],
                port=self.clickhouse_config['port'],
                username=self.clickhouse_config['username'],
                password=self.clickhouse_config['password'],
                database=database
            )
            return client
        except Exception as e:
            self.logger.error(f"ClickHouse 연결 실패: {e}")
            return None
    
    def _execute_query(self, query: str, database: Optional[str] = None) -> Optional[Any]:
        """ClickHouse 쿼리 실행 (내부 메서드)"""
        client = self._get_clickhouse_client(database)
        if client is None:
            return None
            
        try:
            result = client.query(query)
            self.logger.info(f"쿼리 실행 성공: {len(result.result_rows)}개 행 반환")
            return result
        except Exception as e:
            self.logger.error(f"쿼리 실행 실패: {e}")
            return None
    
    @abstractmethod
    def run(self, query: str, **kwargs) -> str:
        """
        워크플로우 실행 메서드
        하위 클래스에서 반드시 구현해야 합니다.
        """
        pass 