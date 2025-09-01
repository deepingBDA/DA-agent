"""
Multi-Agent System for Retail Intelligence
==========================================

GPT-5 기반 멀티 에이전트 시스템의 메인 패키지입니다.
각 전문 에이전트들이 협력하여 종합적인 소매 인텔리전스를 제공합니다.
"""

from .base_agent import BaseAgent, AgentType
from .orchestrator import OrchestratorAgent
from .specialists.data_analyst import DataAnalystAgent
from .specialists.insight_generator import InsightGeneratorAgent
from .specialists.recommendation_engine import RecommendationAgent
from .specialists.anomaly_detector import AnomalyDetectorAgent
from .specialists.trend_predictor import TrendPredictorAgent

__version__ = "1.0.0"
__author__ = "DA-Agent Team"

# 에이전트 레지스트리
AGENT_REGISTRY = {
    "orchestrator": OrchestratorAgent,
    "data_analyst": DataAnalystAgent,
    "insight_generator": InsightGeneratorAgent,
    "recommendation": RecommendationAgent,
    "anomaly_detector": AnomalyDetectorAgent,
    "trend_predictor": TrendPredictorAgent
}

# 기본 에이전트 구성
DEFAULT_AGENTS = [
    "orchestrator",
    "data_analyst", 
    "insight_generator",
    "recommendation"
]

def create_agent_system(agents: list = None):
    """에이전트 시스템 생성"""
    if agents is None:
        agents = DEFAULT_AGENTS
    
    agent_instances = {}
    for agent_name in agents:
        if agent_name in AGENT_REGISTRY:
            agent_instances[agent_name] = AGENT_REGISTRY[agent_name]()
    
    return agent_instances

__all__ = [
    'BaseAgent',
    'AgentType', 
    'OrchestratorAgent',
    'DataAnalystAgent',
    'InsightGeneratorAgent',
    'RecommendationAgent',
    'AnomalyDetectorAgent',
    'TrendPredictorAgent',
    'AGENT_REGISTRY',
    'DEFAULT_AGENTS',
    'create_agent_system'
]