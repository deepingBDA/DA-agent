"""
Base Agent Class
================

모든 전문 에이전트의 기본 클래스입니다.
공통 인터페이스와 기본 기능을 정의합니다.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
import logging
import uuid

class AgentType(Enum):
    """에이전트 타입 열거형"""
    ORCHESTRATOR = "orchestrator"
    DATA_ANALYST = "data_analyst" 
    INSIGHT_GENERATOR = "insight_generator"
    RECOMMENDATION = "recommendation"
    ANOMALY_DETECTOR = "anomaly_detector"
    TREND_PREDICTOR = "trend_predictor"

class MessageType(Enum):
    """메시지 타입 열거형"""
    REQUEST = "request"
    RESPONSE = "response"
    ERROR = "error"
    INFO = "info"

@dataclass
class AgentMessage:
    """에이전트 간 통신 메시지 구조"""
    id: str
    sender: str
    receiver: str
    message_type: MessageType
    content: Dict[str, Any]
    priority: int = 1  # 1(높음) ~ 5(낮음)
    timestamp: datetime = None
    correlation_id: Optional[str] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.id is None:
            self.id = str(uuid.uuid4())

@dataclass 
class AgentCapability:
    """에이전트 능력 정의"""
    name: str
    description: str
    input_requirements: List[str]
    output_format: str
    confidence_threshold: float = 0.7

class BaseAgent(ABC):
    """모든 에이전트의 기본 클래스"""
    
    def __init__(self, 
                 agent_type: AgentType,
                 agent_id: str = None,
                 capabilities: List[AgentCapability] = None):
        """
        Args:
            agent_type: 에이전트 타입
            agent_id: 고유 식별자 (None시 자동 생성)
            capabilities: 에이전트 능력 목록
        """
        self.agent_type = agent_type
        self.agent_id = agent_id or f"{agent_type.value}_{str(uuid.uuid4())[:8]}"
        self.capabilities = capabilities or []
        
        # 상태 관리
        self.is_active = True
        self.current_tasks = {}
        self.message_history = []
        
        # 로깅 설정
        self.logger = self._setup_logging()
        self.logger.info(f"{self.agent_type.value} 에이전트 초기화 완료: {self.agent_id}")
    
    def _setup_logging(self) -> logging.Logger:
        """로깅 설정"""
        logger = logging.getLogger(f"agent.{self.agent_type.value}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                f'%(asctime)s - {self.agent_type.value} - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    @abstractmethod
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        """
        메시지 처리 (각 에이전트에서 구현)
        
        Args:
            message: 처리할 메시지
            
        Returns:
            AgentMessage: 응답 메시지
        """
        pass
    
    @abstractmethod
    def get_capabilities(self) -> List[AgentCapability]:
        """에이전트 능력 목록 반환"""
        pass
    
    async def can_handle(self, task: Dict[str, Any]) -> bool:
        """
        주어진 작업을 처리할 수 있는지 확인
        
        Args:
            task: 작업 정의
            
        Returns:
            bool: 처리 가능 여부
        """
        task_type = task.get("type", "")
        required_capabilities = task.get("required_capabilities", [])
        
        # 능력 기반 매칭
        agent_capability_names = [cap.name for cap in self.capabilities]
        
        return all(cap in agent_capability_names for cap in required_capabilities)
    
    def create_response_message(self,
                              original_message: AgentMessage,
                              content: Dict[str, Any],
                              message_type: MessageType = MessageType.RESPONSE) -> AgentMessage:
        """응답 메시지 생성"""
        return AgentMessage(
            id=str(uuid.uuid4()),
            sender=self.agent_id,
            receiver=original_message.sender,
            message_type=message_type,
            content=content,
            correlation_id=original_message.id,
            priority=original_message.priority
        )
    
    def log_message(self, message: AgentMessage, direction: str = "received"):
        """메시지 로깅"""
        self.message_history.append({
            "timestamp": datetime.now(),
            "direction": direction,
            "message": message
        })
        
        self.logger.info(
            f"Message {direction}: {message.message_type.value} "
            f"from {message.sender} to {message.receiver}"
        )
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """성능 메트릭 반환"""
        return {
            "agent_id": self.agent_id,
            "agent_type": self.agent_type.value,
            "is_active": self.is_active,
            "total_messages": len(self.message_history),
            "current_tasks": len(self.current_tasks),
            "capabilities_count": len(self.capabilities)
        }
    
    def validate_input(self, data: Dict[str, Any], required_fields: List[str]) -> bool:
        """입력 데이터 검증"""
        missing_fields = [field for field in required_fields if field not in data]
        
        if missing_fields:
            self.logger.warning(f"Missing required fields: {missing_fields}")
            return False
        
        return True
    
    def calculate_confidence(self, 
                           data_quality: float,
                           sample_size: int,
                           method_reliability: float) -> float:
        """신뢰도 점수 계산"""
        # 데이터 품질 (0.0 ~ 1.0)
        quality_score = min(data_quality, 1.0)
        
        # 샘플 크기 점수 (로그 스케일)
        import math
        size_score = min(math.log10(max(sample_size, 1) + 1) / 3, 1.0)
        
        # 방법론 신뢰도 (0.0 ~ 1.0)
        method_score = min(method_reliability, 1.0)
        
        # 가중 평균
        confidence = (quality_score * 0.4 + size_score * 0.3 + method_score * 0.3)
        
        return round(confidence, 3)
    
    async def health_check(self) -> Dict[str, Any]:
        """헬스 체크"""
        return {
            "agent_id": self.agent_id,
            "status": "healthy" if self.is_active else "inactive",
            "timestamp": datetime.now().isoformat(),
            "capabilities": len(self.capabilities),
            "current_load": len(self.current_tasks)
        }

# 특화된 기본 에이전트들

class AnalyticsBaseAgent(BaseAgent):
    """분석 전문 에이전트의 기본 클래스"""
    
    def __init__(self, agent_type: AgentType, **kwargs):
        super().__init__(agent_type, **kwargs)
        
        # 분석 관련 공통 설정
        self.confidence_threshold = 0.7
        self.min_sample_size = 30
        self.statistical_significance = 0.05
    
    def is_statistically_significant(self, p_value: float) -> bool:
        """통계적 유의성 검증"""
        return p_value < self.statistical_significance
    
    def categorize_performance(self, value: float, thresholds: Dict[str, float]) -> str:
        """성과 카테고리 분류"""
        if value >= thresholds.get("excellent", float('inf')):
            return "excellent"
        elif value >= thresholds.get("good", 0):
            return "good"
        elif value >= thresholds.get("needs_attention", 0):
            return "needs_attention"
        else:
            return "critical"

class DataProcessingMixin:
    """데이터 처리 관련 공통 기능"""
    
    def clean_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """데이터 정리"""
        cleaned = []
        for record in data:
            # 빈 값 제거
            if all(v is not None for v in record.values()):
                cleaned.append(record)
        return cleaned
    
    def aggregate_by_period(self, 
                          data: List[Dict[str, Any]], 
                          date_field: str = "date",
                          period: str = "day") -> Dict[str, List[Dict[str, Any]]]:
        """기간별 데이터 집계"""
        from datetime import datetime
        import calendar
        
        aggregated = {}
        
        for record in data:
            date_value = record.get(date_field)
            if not date_value:
                continue
                
            if isinstance(date_value, str):
                date_obj = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            else:
                date_obj = date_value
            
            if period == "day":
                key = date_obj.strftime("%Y-%m-%d")
            elif period == "week":
                week_num = date_obj.isocalendar()[1]
                key = f"{date_obj.year}-W{week_num:02d}"
            elif period == "month":
                key = date_obj.strftime("%Y-%m")
            else:
                key = str(date_obj)
            
            if key not in aggregated:
                aggregated[key] = []
            aggregated[key].append(record)
        
        return aggregated

# 성능 최적화를 위한 캐시 믹신
class CacheMixin:
    """캐싱 기능 믹신"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache = {}
        self._cache_ttl = 300  # 5분
    
    def _get_cache_key(self, *args, **kwargs) -> str:
        """캐시 키 생성"""
        import hashlib
        key_string = f"{args}_{sorted(kwargs.items())}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _is_cache_valid(self, timestamp: datetime) -> bool:
        """캐시 유효성 검사"""
        return (datetime.now() - timestamp).seconds < self._cache_ttl
    
    def get_cached_result(self, cache_key: str) -> Optional[Any]:
        """캐시된 결과 조회"""
        if cache_key in self._cache:
            data, timestamp = self._cache[cache_key]
            if self._is_cache_valid(timestamp):
                return data
            else:
                del self._cache[cache_key]
        return None
    
    def set_cache(self, cache_key: str, data: Any):
        """결과 캐싱"""
        self._cache[cache_key] = (data, datetime.now())

if __name__ == "__main__":
    # 기본 테스트
    class TestAgent(BaseAgent):
        def __init__(self):
            super().__init__(AgentType.DATA_ANALYST)
        
        async def process_message(self, message: AgentMessage) -> AgentMessage:
            return self.create_response_message(
                message, 
                {"status": "processed", "agent": self.agent_id}
            )
        
        def get_capabilities(self) -> List[AgentCapability]:
            return [
                AgentCapability(
                    name="test_capability",
                    description="테스트용 능력",
                    input_requirements=["data"],
                    output_format="json"
                )
            ]
    
    # 테스트 실행
    import asyncio
    
    async def test_agent():
        agent = TestAgent()
        
        test_message = AgentMessage(
            id=str(uuid.uuid4()),
            sender="test_sender",
            receiver=agent.agent_id,
            message_type=MessageType.REQUEST,
            content={"test": "data"}
        )
        
        response = await agent.process_message(test_message)
        print(f"Response: {response.content}")
        
        health = await agent.health_check()
        print(f"Health: {health}")
    
    # asyncio.run(test_agent())