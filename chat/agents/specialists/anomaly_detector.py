"""
Anomaly Detector Agent (Simplified)
===================================

이상치 탐지 전문 에이전트 (간소화된 구현)
"""

from typing import Dict, Any, List
from ..base_agent import BaseAgent, AgentType, AgentMessage, MessageType, AgentCapability, AnalyticsBaseAgent

class AnomalyDetectorAgent(AnalyticsBaseAgent):
    def __init__(self):
        capabilities = [
            AgentCapability(
                name="anomaly_detection",
                description="통계적 이상치 탐지",
                input_requirements=["time_series_data"],
                output_format="anomaly_report"
            )
        ]
        super().__init__(AgentType.DATA_ANALYST, capabilities=capabilities)
    
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        return self.create_response_message(message, {"status": "success", "result": {"anomalies": []}})
    
    def get_capabilities(self) -> List[AgentCapability]:
        return self.capabilities