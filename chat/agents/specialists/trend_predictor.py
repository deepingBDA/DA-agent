"""
Trend Predictor Agent (Simplified)
===================================

트렌드 예측 전문 에이전트 (간소화된 구현)
"""

from typing import Dict, Any, List
from ..base_agent import BaseAgent, AgentType, AgentMessage, MessageType, AgentCapability, AnalyticsBaseAgent

class TrendPredictorAgent(AnalyticsBaseAgent):
    def __init__(self):
        capabilities = [
            AgentCapability(
                name="trend_prediction",
                description="시계열 트렌드 예측",
                input_requirements=["historical_data"],
                output_format="trend_forecast"
            )
        ]
        super().__init__(AgentType.DATA_ANALYST, capabilities=capabilities)
    
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        return self.create_response_message(message, {"status": "success", "result": {"forecast": "stable_growth"}})
    
    def get_capabilities(self) -> List[AgentCapability]:
        return self.capabilities