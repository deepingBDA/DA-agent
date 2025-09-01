"""
Orchestrator Agent
==================

메인 오케스트레이터 에이전트입니다.
사용자 요청을 분석하고 적절한 전문 에이전트들에게 작업을 위임합니다.
"""

import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import re
import json

from .base_agent import (
    BaseAgent, AgentType, AgentMessage, MessageType, 
    AgentCapability, AnalyticsBaseAgent
)

class IntentType:
    """의도 분류"""
    DIAGNOSTIC = "diagnostic"          # 현재 상태 진단
    COMPARATIVE = "comparative"        # 비교 분석  
    TREND_ANALYSIS = "trend_analysis"  # 트렌드 분석
    PREDICTIVE = "predictive"          # 예측 분석
    OPTIMIZATION = "optimization"      # 최적화 제안
    ANOMALY_DETECTION = "anomaly"      # 이상 탐지
    DEEP_DIVE = "deep_dive"           # 심층 분석

class TaskPriority:
    """작업 우선순위"""
    CRITICAL = 1
    HIGH = 2  
    MEDIUM = 3
    LOW = 4

class OrchestratorAgent(AnalyticsBaseAgent):
    """오케스트레이터 에이전트 - 전체 시스템 조율"""
    
    def __init__(self):
        capabilities = [
            AgentCapability(
                name="intent_classification",
                description="사용자 의도 분석 및 분류",
                input_requirements=["user_query"],
                output_format="structured_intent"
            ),
            AgentCapability(
                name="task_decomposition", 
                description="복합 질의를 하위 작업으로 분해",
                input_requirements=["intent", "query_complexity"],
                output_format="task_list"
            ),
            AgentCapability(
                name="agent_routing",
                description="적절한 전문 에이전트 선택 및 작업 위임",
                input_requirements=["tasks", "agent_capabilities"],
                output_format="routing_plan"
            ),
            AgentCapability(
                name="result_synthesis",
                description="여러 에이전트 결과를 통합하여 최종 응답 생성",
                input_requirements=["agent_responses"],
                output_format="comprehensive_insight"
            )
        ]
        
        super().__init__(AgentType.ORCHESTRATOR, capabilities=capabilities)
        
        # 전문 에이전트 레지스트리
        self.specialist_agents = {}
        
        # 작업 히스토리
        self.task_history = []
        
        # 의도 분류 패턴
        self.intent_patterns = self._build_intent_patterns()
        
    def _build_intent_patterns(self) -> Dict[str, List[str]]:
        """의도 분류를 위한 키워드 패턴 구축"""
        return {
            IntentType.DIAGNOSTIC: [
                r"진단|현재\s*상태|어떻게\s*되고\s*있|현황|상황|분석해줘",
                r"지금|현재|오늘|이번\s*주|최근|상태",
                r"어떤\s*문제|무엇이\s*문제|왜\s*이렇게"
            ],
            IntentType.COMPARATIVE: [
                r"비교|대비|차이|vs|보다|비해|전주|전월|작년",
                r"어느\s*것이\s*좋|어떤\s*것이\s*나은|더\s*높|더\s*낮",
                r"지난\s*주|지난\s*달|이전\s*기간"
            ],
            IntentType.TREND_ANALYSIS: [
                r"트렌드|추세|변화|패턴|흐름|경향",
                r"증가|감소|상승|하락|변동|추이",
                r"시간에\s*따라|기간별|일별|주별|월별"
            ],
            IntentType.PREDICTIVE: [
                r"예측|전망|예상|앞으로|미래|다음\s*주|다음\s*달",
                r"될\s*것|할\s*것|것\s*같|예상되|전망",
                r"forecast|predict|future|next"
            ],
            IntentType.OPTIMIZATION: [
                r"개선|최적화|향상|높이|늘리|증대|개선\s*방안",
                r"어떻게\s*하면|방법|전략|해결책|개선안",
                r"더\s*좋게|더\s*많이|더\s*효율"
            ],
            IntentType.ANOMALY_DETECTION: [
                r"이상|비정상|문제|급증|급감|갑자기",
                r"왜\s*갑자기|무슨\s*일|어떤\s*문제|이상한",
                r"anomaly|outlier|unusual"
            ],
            IntentType.DEEP_DIVE: [
                r"자세히|상세히|깊이|심층|구체적|세부적",
                r"원인|이유|why|근본적|심화\s*분석",
                r"더\s*자세히|더\s*깊이|더\s*구체적"
            ]
        }
    
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        """메인 메시지 처리 로직"""
        try:
            self.log_message(message, "received")
            
            # 1단계: 의도 분석
            intent_result = await self._analyze_intent(message.content)
            
            # 2단계: 작업 분해
            tasks = await self._decompose_tasks(intent_result)
            
            # 3단계: 에이전트 라우팅 및 실행
            agent_results = await self._orchestrate_agents(tasks)
            
            # 4단계: 결과 통합
            final_insight = await self._synthesize_results(
                intent_result, tasks, agent_results
            )
            
            response = self.create_response_message(
                message,
                {
                    "status": "success",
                    "intent": intent_result,
                    "final_insight": final_insight,
                    "confidence": self._calculate_overall_confidence(agent_results),
                    "processing_time": self._get_processing_time()
                }
            )
            
            self.log_message(response, "sent")
            return response
            
        except Exception as e:
            self.logger.error(f"Processing error: {str(e)}")
            error_response = self.create_response_message(
                message,
                {
                    "status": "error",
                    "error": str(e),
                    "fallback_message": "죄송합니다. 요청을 처리하는 중 오류가 발생했습니다."
                },
                MessageType.ERROR
            )
            return error_response
    
    async def _analyze_intent(self, content: Dict[str, Any]) -> Dict[str, Any]:
        """사용자 의도 분석"""
        user_query = content.get("query", "")
        
        # 키워드 기반 의도 분류
        detected_intents = []
        confidence_scores = {}
        
        for intent_type, patterns in self.intent_patterns.items():
            score = 0
            for pattern in patterns:
                matches = len(re.findall(pattern, user_query, re.IGNORECASE))
                score += matches
            
            if score > 0:
                detected_intents.append(intent_type)
                confidence_scores[intent_type] = min(score / len(patterns), 1.0)
        
        # 기본 분류가 없으면 진단으로 분류
        if not detected_intents:
            detected_intents = [IntentType.DIAGNOSTIC]
            confidence_scores[IntentType.DIAGNOSTIC] = 0.5
        
        # 주요 의도 결정
        primary_intent = max(confidence_scores.items(), key=lambda x: x[1])[0]
        
        # 메타데이터 추출
        metadata = self._extract_metadata(user_query)
        
        return {
            "primary_intent": primary_intent,
            "all_intents": detected_intents,
            "confidence_scores": confidence_scores,
            "metadata": metadata,
            "original_query": user_query
        }
    
    def _extract_metadata(self, query: str) -> Dict[str, Any]:
        """쿼리에서 메타데이터 추출"""
        metadata = {
            "time_period": None,
            "metrics": [],
            "entities": [],
            "urgency": "normal"
        }
        
        # 시간 기간 패턴
        time_patterns = {
            "today": r"오늘|today",
            "yesterday": r"어제|yesterday", 
            "this_week": r"이번\s*주|this\s*week",
            "last_week": r"지난\s*주|저번\s*주|last\s*week",
            "this_month": r"이번\s*달|this\s*month",
            "last_month": r"지난\s*달|저번\s*달|last\s*month"
        }
        
        for period, pattern in time_patterns.items():
            if re.search(pattern, query, re.IGNORECASE):
                metadata["time_period"] = period
                break
        
        # 메트릭 패턴
        metric_patterns = [
            r"방문객|visitor|traffic",
            r"매출|sales|revenue", 
            r"전환율|conversion",
            r"픽업|pickup",
            r"체류\s*시간|dwell\s*time"
        ]
        
        for pattern in metric_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                metadata["metrics"].append(pattern.split("|")[0])
        
        # 긴급도 판단
        urgency_patterns = [
            r"급하|urgent|즉시|바로|지금\s*당장",
            r"문제|problem|issue|오류|error"
        ]
        
        for pattern in urgency_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                metadata["urgency"] = "high" 
                break
        
        return metadata
    
    async def _decompose_tasks(self, intent_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """복합 질의를 세부 작업으로 분해"""
        primary_intent = intent_result["primary_intent"]
        metadata = intent_result["metadata"]
        
        tasks = []
        
        # 의도별 작업 분해 전략
        if primary_intent == IntentType.DIAGNOSTIC:
            tasks.extend([
                {
                    "type": "data_collection",
                    "agent": "data_analyst", 
                    "priority": TaskPriority.HIGH,
                    "params": {
                        "metrics": metadata.get("metrics", ["traffic", "conversion"]),
                        "time_period": metadata.get("time_period", "this_week")
                    }
                },
                {
                    "type": "performance_analysis", 
                    "agent": "insight_generator",
                    "priority": TaskPriority.HIGH,
                    "params": {
                        "analysis_type": "diagnostic",
                        "context": metadata
                    }
                }
            ])
        
        elif primary_intent == IntentType.COMPARATIVE:
            tasks.extend([
                {
                    "type": "comparative_data_collection",
                    "agent": "data_analyst",
                    "priority": TaskPriority.HIGH, 
                    "params": {
                        "comparison_periods": ["current", "previous"],
                        "metrics": metadata.get("metrics", [])
                    }
                },
                {
                    "type": "comparative_analysis",
                    "agent": "insight_generator",
                    "priority": TaskPriority.HIGH,
                    "params": {
                        "analysis_type": "comparative"
                    }
                }
            ])
        
        elif primary_intent == IntentType.TREND_ANALYSIS:
            tasks.extend([
                {
                    "type": "time_series_collection",
                    "agent": "data_analyst",
                    "priority": TaskPriority.HIGH,
                    "params": {
                        "time_range": "extended",
                        "granularity": "daily"
                    }
                },
                {
                    "type": "trend_analysis",
                    "agent": "trend_predictor",
                    "priority": TaskPriority.HIGH,
                    "params": {
                        "analysis_type": "trend"
                    }
                }
            ])
        
        elif primary_intent == IntentType.PREDICTIVE:
            tasks.extend([
                {
                    "type": "historical_data_collection", 
                    "agent": "data_analyst",
                    "priority": TaskPriority.HIGH,
                    "params": {
                        "lookback_period": "3_months",
                        "forecast_horizon": "1_month"
                    }
                },
                {
                    "type": "predictive_modeling",
                    "agent": "trend_predictor",
                    "priority": TaskPriority.HIGH,
                    "params": {
                        "model_type": "forecast",
                        "confidence_interval": 0.95
                    }
                }
            ])
        
        elif primary_intent == IntentType.ANOMALY_DETECTION:
            tasks.extend([
                {
                    "type": "anomaly_data_scan",
                    "agent": "anomaly_detector", 
                    "priority": TaskPriority.CRITICAL,
                    "params": {
                        "scan_period": metadata.get("time_period", "last_7_days"),
                        "sensitivity": "high"
                    }
                }
            ])
        
        elif primary_intent == IntentType.OPTIMIZATION:
            tasks.extend([
                {
                    "type": "current_performance_analysis",
                    "agent": "data_analyst",
                    "priority": TaskPriority.HIGH,
                    "params": {
                        "focus_areas": metadata.get("metrics", []),
                        "benchmark_comparison": True
                    }
                },
                {
                    "type": "optimization_recommendations",
                    "agent": "recommendation",
                    "priority": TaskPriority.HIGH, 
                    "params": {
                        "optimization_target": "performance",
                        "constraints": metadata.get("constraints", [])
                    }
                }
            ])
        
        # 기본 추천 작업 항상 포함
        if primary_intent != IntentType.OPTIMIZATION:
            tasks.append({
                "type": "generate_recommendations",
                "agent": "recommendation",
                "priority": TaskPriority.MEDIUM,
                "params": {
                    "context": primary_intent,
                    "urgency": metadata.get("urgency", "normal")
                }
            })
        
        # 작업에 고유 ID 부여
        for i, task in enumerate(tasks):
            task["task_id"] = f"task_{i+1}_{datetime.now().strftime('%H%M%S')}"
        
        return tasks
    
    async def _orchestrate_agents(self, tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """전문 에이전트들에게 작업 위임 및 결과 수집"""
        agent_results = {}
        
        # 우선순위별 작업 그룹화
        priority_groups = {}
        for task in tasks:
            priority = task["priority"]
            if priority not in priority_groups:
                priority_groups[priority] = []
            priority_groups[priority].append(task)
        
        # 우선순위 순으로 실행 (높은 우선순위 먼저)
        for priority in sorted(priority_groups.keys()):
            group_tasks = priority_groups[priority]
            
            # 같은 우선순위 작업들은 병렬 실행
            parallel_results = await asyncio.gather(
                *[self._execute_task(task) for task in group_tasks],
                return_exceptions=True
            )
            
            # 결과 저장
            for task, result in zip(group_tasks, parallel_results):
                task_id = task["task_id"]
                if isinstance(result, Exception):
                    self.logger.error(f"Task {task_id} failed: {result}")
                    agent_results[task_id] = {
                        "status": "error",
                        "error": str(result),
                        "task": task
                    }
                else:
                    agent_results[task_id] = {
                        "status": "success", 
                        "result": result,
                        "task": task
                    }
        
        return agent_results
    
    async def _execute_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """개별 작업 실행"""
        agent_type = task["agent"]
        task_type = task["type"]
        params = task.get("params", {})
        
        self.logger.info(f"Executing task {task['task_id']} with {agent_type}")
        
        # 여기서는 시뮬레이션된 결과 반환
        # 실제로는 해당 전문 에이전트에게 메시지 전송
        
        if agent_type == "data_analyst":
            return await self._simulate_data_analyst_result(task_type, params)
        elif agent_type == "insight_generator":
            return await self._simulate_insight_generator_result(task_type, params)
        elif agent_type == "recommendation":
            return await self._simulate_recommendation_result(task_type, params)
        elif agent_type == "trend_predictor":
            return await self._simulate_trend_predictor_result(task_type, params)
        elif agent_type == "anomaly_detector":
            return await self._simulate_anomaly_detector_result(task_type, params)
        else:
            raise ValueError(f"Unknown agent type: {agent_type}")
    
    async def _simulate_data_analyst_result(self, task_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 분석가 에이전트 시뮬레이션"""
        # 실제로는 MCP 툴들을 호출하여 데이터 수집
        await asyncio.sleep(0.5)  # 처리 시간 시뮬레이션
        
        return {
            "task_type": task_type,
            "data": {
                "daily_visitors": [1200, 1150, 1300, 1250, 1400, 1100, 980],
                "conversion_rate": 0.34,
                "pickup_rate": 0.12,
                "avg_dwell_time": "8.5분"
            },
            "data_quality": 0.85,
            "sample_size": 7892,
            "confidence": 0.92
        }
    
    async def _simulate_insight_generator_result(self, task_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """인사이트 생성기 에이전트 시뮬레이션"""
        await asyncio.sleep(0.7)
        
        return {
            "task_type": task_type,
            "insights": [
                "주중 방문객이 주말 대비 15% 증가하는 패턴 확인",
                "오후 2-4시 시간대 픽업률이 평균 대비 23% 높음",
                "전환율이 업계 평균(28%) 대비 6% 우수"
            ],
            "key_findings": {
                "performance_level": "good",
                "trend_direction": "stable_positive",
                "critical_issues": []
            },
            "confidence": 0.88
        }
    
    async def _simulate_recommendation_result(self, task_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """추천 엔진 에이전트 시뮬레이션"""
        await asyncio.sleep(0.6)
        
        return {
            "task_type": task_type,
            "recommendations": [
                {
                    "priority": "HIGH",
                    "action": "오후 피크타임 맞춤 진열대 재배치",
                    "expected_impact": "픽업률 15-20% 증가",
                    "implementation_time": "2-3일",
                    "roi_estimate": "월 매출 8-12% 증대"
                },
                {
                    "priority": "MEDIUM", 
                    "action": "주말 대비 주중 프로모션 강화",
                    "expected_impact": "주중 방문객 추가 10% 증가",
                    "implementation_time": "1주일",
                    "roi_estimate": "월 순이익 5-7% 증가"
                }
            ],
            "confidence": 0.79
        }
    
    async def _simulate_trend_predictor_result(self, task_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """트렌드 예측기 에이전트 시뮬레이션"""
        await asyncio.sleep(0.8)
        
        return {
            "task_type": task_type,
            "predictions": {
                "next_week_visitors": {
                    "forecast": 8450,
                    "confidence_interval": [7890, 9010],
                    "probability": 0.85
                },
                "trend_direction": "stable_growth",
                "seasonal_factors": {
                    "weather_impact": "positive",
                    "event_influence": "neutral"
                }
            },
            "confidence": 0.81
        }
    
    async def _simulate_anomaly_detector_result(self, task_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """이상 탐지기 에이전트 시뮬레이션"""
        await asyncio.sleep(0.4)
        
        return {
            "task_type": task_type,
            "anomalies": [
                {
                    "date": "2024-12-30",
                    "metric": "방문객수",
                    "anomaly_type": "positive_spike",
                    "deviation": "+47%",
                    "possible_causes": ["연말 프로모션 효과", "날씨 영향"],
                    "severity": "medium"
                }
            ],
            "anomaly_score": 0.72,
            "confidence": 0.94
        }
    
    async def _synthesize_results(self, 
                                intent_result: Dict[str, Any],
                                tasks: List[Dict[str, Any]], 
                                agent_results: Dict[str, Any]) -> str:
        """여러 에이전트 결과를 통합하여 최종 인사이트 생성"""
        
        primary_intent = intent_result["primary_intent"]
        
        # Executive Summary 생성
        summary_parts = []
        
        # 데이터 기반 요약
        data_results = [r for r in agent_results.values() 
                       if r.get("status") == "success" and 
                       r.get("task", {}).get("agent") == "data_analyst"]
        
        if data_results:
            data = data_results[0]["result"]["data"]
            summary_parts.append(
                f"📊 **현재 성과**: 일평균 방문객 {data['daily_visitors'][-1]:,}명, "
                f"전환율 {data['conversion_rate']:.1%}, 픽업률 {data['pickup_rate']:.1%}"
            )
        
        # 인사이트 통합
        insight_results = [r for r in agent_results.values() 
                          if r.get("status") == "success" and 
                          r.get("task", {}).get("agent") == "insight_generator"]
        
        key_insights = []
        if insight_results:
            insights = insight_results[0]["result"]["insights"]
            key_insights.extend(insights)
        
        # 추천사항 통합
        recommendation_results = [r for r in agent_results.values() 
                                 if r.get("status") == "success" and 
                                 r.get("task", {}).get("agent") == "recommendation"]
        
        recommendations = []
        if recommendation_results:
            recs = recommendation_results[0]["result"]["recommendations"]
            recommendations.extend(recs)
        
        # 최종 응답 생성
        response_parts = []
        
        response_parts.append("# 📊 Executive Summary")
        if summary_parts:
            response_parts.extend(summary_parts)
        
        if key_insights:
            response_parts.append("\n## 🔍 Key Insights")
            for i, insight in enumerate(key_insights, 1):
                response_parts.append(f"**{i}.** {insight}")
        
        if recommendations:
            response_parts.append("\n## 💡 실행 방안")
            for rec in recommendations:
                response_parts.append(
                    f"🎯 **{rec['priority']}**: {rec['action']}\n"
                    f"   • 예상 효과: {rec['expected_impact']}\n"
                    f"   • 소요 시간: {rec['implementation_time']}\n"
                    f"   • ROI: {rec.get('roi_estimate', 'TBD')}"
                )
        
        # 신뢰도 정보
        overall_confidence = self._calculate_overall_confidence(agent_results)
        response_parts.append(f"\n**전체 분석 신뢰도**: {overall_confidence:.1%}")
        
        return "\n".join(response_parts)
    
    def _calculate_overall_confidence(self, agent_results: Dict[str, Any]) -> float:
        """전체 신뢰도 계산"""
        confidences = []
        for result in agent_results.values():
            if result.get("status") == "success":
                conf = result.get("result", {}).get("confidence", 0.5)
                confidences.append(conf)
        
        if not confidences:
            return 0.5
        
        # 가중 평균 (더 많은 결과가 있을수록 신뢰도 증가)
        weight = min(len(confidences) / 5, 1.0)  # 최대 5개 결과까지 가중치 적용
        base_confidence = sum(confidences) / len(confidences)
        
        return base_confidence * weight + 0.1 * (1 - weight)
    
    def _get_processing_time(self) -> str:
        """처리 시간 계산 (시뮬레이션)"""
        return "2.3초"
    
    def get_capabilities(self) -> List[AgentCapability]:
        """에이전트 능력 목록 반환"""
        return self.capabilities
    
    def register_specialist_agent(self, agent_type: str, agent_instance):
        """전문 에이전트 등록"""
        self.specialist_agents[agent_type] = agent_instance
        self.logger.info(f"Registered specialist agent: {agent_type}")
    
    def get_task_history(self) -> List[Dict[str, Any]]:
        """작업 히스토리 반환"""
        return self.task_history

if __name__ == "__main__":
    # 테스트 실행
    async def test_orchestrator():
        orchestrator = OrchestratorAgent()
        
        test_message = AgentMessage(
            id="test_001",
            sender="user",
            receiver=orchestrator.agent_id,
            message_type=MessageType.REQUEST,
            content={
                "query": "이번 주 방문객수가 어떻게 되고 있나요? 지난 주와 비교해서 개선 방안도 알려주세요."
            }
        )
        
        response = await orchestrator.process_message(test_message)
        print(f"Response Status: {response.content['status']}")
        if response.content['status'] == 'success':
            print(f"Final Insight:\n{response.content['final_insight']}")
    
    import asyncio
    # asyncio.run(test_orchestrator())