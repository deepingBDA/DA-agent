"""
State Manager
=============

멀티 에이전트 워크플로우의 상태를 관리합니다.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import json
import logging
from agents.base_agent import AgentMessage, MessageType

logger = logging.getLogger(__name__)


class StateManager:
    """워크플로우 상태 관리자"""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.state_history = []
        self.current_state = self._initialize_state()
        self.checkpoints = {}
    
    def _initialize_state(self) -> Dict[str, Any]:
        """초기 상태 생성"""
        return {
            "session_id": self.session_id,
            "query": "",
            "intent": "",
            "context": {},
            "agent_responses": {},
            "current_agent": "",
            "step_count": 0,
            "errors": [],
            "completed_tasks": [],
            "confidence_score": 0.5,
            "created_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "current_message": None,
            "final_result": None
        }
    
    def update_state(self, updates: Dict[str, Any]) -> None:
        """상태 업데이트"""
        try:
            # 상태 백업
            self._backup_state()
            
            # 업데이트 적용
            for key, value in updates.items():
                if key in self.current_state:
                    self.current_state[key] = value
                else:
                    logger.warning(f"Unknown state key: {key}")
            
            # 타임스탬프 업데이트
            self.current_state["last_updated"] = datetime.now().isoformat()
            
            logger.debug(f"State updated for session {self.session_id}")
            
        except Exception as e:
            logger.error(f"State update failed: {e}")
            self._restore_state()
    
    def _backup_state(self) -> None:
        """현재 상태 백업"""
        self.state_history.append(self.current_state.copy())
    
    def _restore_state(self) -> None:
        """마지막 백업된 상태로 복구"""
        if self.state_history:
            self.current_state = self.state_history.pop()
    
    def get_state(self) -> Dict[str, Any]:
        """현재 상태 반환"""
        return self.current_state.copy()
    
    def set_checkpoint(self, name: str) -> None:
        """체크포인트 생성"""
        self.checkpoints[name] = {
            "state": self.current_state.copy(),
            "timestamp": datetime.now().isoformat(),
            "step_count": self.current_state["step_count"]
        }
        logger.info(f"Checkpoint '{name}' created for session {self.session_id}")
    
    def restore_checkpoint(self, name: str) -> bool:
        """체크포인트로 복구"""
        try:
            if name in self.checkpoints:
                checkpoint = self.checkpoints[name]
                self.current_state = checkpoint["state"].copy()
                logger.info(f"Restored to checkpoint '{name}' for session {self.session_id}")
                return True
            else:
                logger.warning(f"Checkpoint '{name}' not found")
                return False
        except Exception as e:
            logger.error(f"Checkpoint restore failed: {e}")
            return False
    
    def add_agent_response(self, agent_name: str, response: AgentMessage) -> None:
        """에이전트 응답 추가"""
        self.current_state["agent_responses"][agent_name] = {
            "content": response.content,
            "type": response.type.value,
            "confidence": response.confidence,
            "timestamp": response.timestamp
        }
        
        # 작업 완료 목록 업데이트
        if response.type == MessageType.RESULT and "task" in response.content:
            task_name = response.content.get("task", f"{agent_name}_task")
            if task_name not in self.current_state["completed_tasks"]:
                self.current_state["completed_tasks"].append(task_name)
    
    def add_error(self, error: str, agent_name: str = None) -> None:
        """오류 추가"""
        error_info = {
            "error": error,
            "agent": agent_name,
            "timestamp": datetime.now().isoformat(),
            "step": self.current_state["step_count"]
        }
        self.current_state["errors"].append(error_info)
        logger.error(f"Error added to state: {error}")
    
    def update_confidence(self, confidence: float) -> None:
        """신뢰도 업데이트"""
        if 0.0 <= confidence <= 1.0:
            self.current_state["confidence_score"] = confidence
        else:
            logger.warning(f"Invalid confidence score: {confidence}")
    
    def get_summary(self) -> Dict[str, Any]:
        """상태 요약 반환"""
        return {
            "session_id": self.session_id,
            "step_count": self.current_state["step_count"],
            "current_agent": self.current_state["current_agent"],
            "completed_tasks": len(self.current_state["completed_tasks"]),
            "errors": len(self.current_state["errors"]),
            "confidence_score": self.current_state["confidence_score"],
            "duration": self._calculate_duration(),
            "checkpoints": list(self.checkpoints.keys())
        }
    
    def _calculate_duration(self) -> str:
        """처리 시간 계산"""
        try:
            start_time = datetime.fromisoformat(self.current_state["created_at"])
            end_time = datetime.fromisoformat(self.current_state["last_updated"])
            duration = (end_time - start_time).total_seconds()
            return f"{duration:.2f}초"
        except:
            return "unknown"
    
    def export_state(self) -> str:
        """상태를 JSON으로 내보내기"""
        try:
            return json.dumps(self.current_state, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"State export failed: {e}")
            return "{}"
    
    def import_state(self, state_json: str) -> bool:
        """JSON에서 상태 가져오기"""
        try:
            imported_state = json.loads(state_json)
            
            # 기본 구조 검증
            required_keys = ["session_id", "step_count", "agent_responses"]
            if all(key in imported_state for key in required_keys):
                self.current_state = imported_state
                logger.info(f"State imported for session {self.session_id}")
                return True
            else:
                logger.error("Invalid state structure")
                return False
                
        except Exception as e:
            logger.error(f"State import failed: {e}")
            return False