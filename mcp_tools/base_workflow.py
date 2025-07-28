"""
Base Workflow 클래스
워크플로우의 기본 구조만 제공하는 추상 베이스 클래스
"""

import sys
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, TypedDict, Generic, TypeVar

from langgraph.graph import StateGraph


# Generic State Type
StateType = TypeVar('StateType', bound='BaseState')


class BaseState(TypedDict):
    """
    모든 워크플로우의 기본 상태 정의
    최소한의 공통 필드만 포함
    """
    user_prompt: str        # 사용자 요청
    workflow_id: str        # 워크플로우 고유 ID
    timestamp: str          # 실행 시작 시간


class BaseWorkflow(ABC, Generic[StateType]):
    """
    모든 워크플로우의 기본 클래스
    핵심 구조만 제공하고 구체적 구현은 하위 클래스에 위임
    """
    
    def __init__(self, workflow_name: str = "base"):
        """
        Args:
            workflow_name: 워크플로우 이름 (로깅에 사용)
        """
        self.workflow_name = workflow_name
        
        # 로깅 설정
        self.logger = self._setup_logging()
        self.logger.info(f"{workflow_name} 워크플로우 초기화")
        
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
    


    def create_initial_state(self, user_prompt: str, **kwargs) -> StateType:
        """
        초기 상태 생성
        하위 클래스에서 오버라이드하여 추가 필드 설정 가능
        """
        base_state = {
            "user_prompt": user_prompt,
            "workflow_id": f"{self.workflow_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "timestamp": datetime.now().isoformat(),
        }
        return base_state  # type: ignore
    

        
    @abstractmethod
    def run(self, user_prompt: str, **kwargs) -> str:
        """
        워크플로우 실행 메서드
        하위 클래스에서 반드시 구현해야 합니다.
        """
        pass

    @abstractmethod 
    def _build_workflow(self) -> StateGraph:
        """
        LangGraph 워크플로우 구성
        하위 클래스에서 반드시 구현해야 합니다.
        """
        pass 