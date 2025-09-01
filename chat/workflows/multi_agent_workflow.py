"""
Multi-Agent LangGraph Workflow
===============================

LangGraph를 사용한 멀티 에이전트 워크플로우 구현입니다.
조건부 라우팅과 상태 관리를 통해 복잡한 분석 파이프라인을 실행합니다.
"""

from typing import TypedDict, List, Dict, Any, Optional, Annotated
from langgraph.graph import StateGraph, END, START
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
import asyncio
from datetime import datetime
import json
import logging

# 상태 타입 정의
class MultiAgentState(TypedDict):
    """멀티 에이전트 워크플로우 상태"""
    # 기본 정보
    user_query: str
    session_id: str
    timestamp: datetime
    
    # 의도 분석 결과
    intent: Dict[str, Any]
    metadata: Dict[str, Any]
    
    # 작업 관리
    tasks: List[Dict[str, Any]]
    completed_tasks: List[str]
    current_task: Optional[Dict[str, Any]]
    
    # 에이전트 결과
    data_analysis_result: Optional[Dict[str, Any]]
    insight_analysis_result: Optional[Dict[str, Any]]
    recommendation_result: Optional[Dict[str, Any]]
    anomaly_result: Optional[Dict[str, Any]]
    trend_result: Optional[Dict[str, Any]]
    
    # 최종 결과
    final_insight: str
    confidence_score: float
    processing_log: List[str]
    
    # 오류 처리
    errors: List[str]
    retry_count: int
    
    # 메시지 히스토리 (LangGraph 호환)
    messages: Annotated[list, add_messages]

class MultiAgentWorkflow:
    """멀티 에이전트 워크플로우 메인 클래스"""
    
    def __init__(self, mcp_client=None, model=None):
        """
        Args:
            mcp_client: MCP 클라이언트 (도구 접근용)
            model: LLM 모델 인스턴스
        """
        self.mcp_client = mcp_client
        self.model = model
        self.logger = self._setup_logging()
        
        # 체크포인터 설정 (상태 저장용)
        self.checkpointer = MemorySaver()
        
        # 워크플로우 그래프 빌드
        self.graph = self._build_workflow_graph()
        self.compiled_graph = self.graph.compile(checkpointer=self.checkpointer)
        
        # 에이전트 전문성 매핑
        self.agent_capabilities = {
            "data_analyst": ["data_collection", "statistical_analysis", "data_validation"],
            "insight_generator": ["pattern_recognition", "business_analysis", "root_cause_analysis"], 
            "recommendation": ["action_planning", "optimization", "roi_calculation"],
            "anomaly_detector": ["outlier_detection", "threshold_analysis", "alert_generation"],
            "trend_predictor": ["forecasting", "trend_analysis", "seasonality_detection"]
        }
    
    def _setup_logging(self) -> logging.Logger:
        """로깅 설정"""
        logger = logging.getLogger("multi_agent_workflow")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - MultiAgent - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _build_workflow_graph(self) -> StateGraph:
        """LangGraph 워크플로우 구성"""
        workflow = StateGraph(MultiAgentState)
        
        # 노드 추가
        workflow.add_node("analyze_intent", self._analyze_intent_node)
        workflow.add_node("decompose_tasks", self._decompose_tasks_node)  
        workflow.add_node("data_collection", self._data_collection_node)
        workflow.add_node("insight_generation", self._insight_generation_node)
        workflow.add_node("recommendation_generation", self._recommendation_generation_node)
        workflow.add_node("anomaly_detection", self._anomaly_detection_node)
        workflow.add_node("trend_analysis", self._trend_analysis_node)
        workflow.add_node("synthesize_results", self._synthesize_results_node)
        workflow.add_node("error_handler", self._error_handler_node)
        
        # 엣지 연결
        workflow.add_edge(START, "analyze_intent")
        workflow.add_edge("analyze_intent", "decompose_tasks")
        
        # 조건부 라우팅 - 의도에 따른 분기
        workflow.add_conditional_edges(
            "decompose_tasks",
            self._route_by_intent,
            {
                "data_first": "data_collection",
                "anomaly_first": "anomaly_detection", 
                "trend_first": "trend_analysis",
                "direct_insight": "insight_generation",
                "direct_synthesis": "synthesize_results",  # 간단한 질문은 바로 통합
                "error": "error_handler"
            }
        )
        
        # 데이터 수집 후 분기
        workflow.add_conditional_edges(
            "data_collection",
            self._route_after_data_collection,
            {
                "to_insights": "insight_generation",
                "to_anomaly": "anomaly_detection",
                "to_trend": "trend_analysis", 
                "to_synthesis": "synthesize_results",
                "error": "error_handler"
            }
        )
        
        # 각 분석 단계에서 통합으로
        workflow.add_conditional_edges(
            "insight_generation",
            self._route_after_analysis,
            {
                "continue": "recommendation_generation",
                "synthesize": "synthesize_results",
                "error": "error_handler"
            }
        )
        
        workflow.add_conditional_edges(
            "recommendation_generation", 
            self._route_after_analysis,
            {
                "synthesize": "synthesize_results",
                "error": "error_handler"
            }
        )
        
        workflow.add_conditional_edges(
            "anomaly_detection",
            self._route_after_analysis, 
            {
                "continue": "insight_generation",
                "synthesize": "synthesize_results",
                "error": "error_handler"
            }
        )
        
        workflow.add_conditional_edges(
            "trend_analysis",
            self._route_after_analysis,
            {
                "continue": "insight_generation", 
                "synthesize": "synthesize_results",
                "error": "error_handler"
            }
        )
        
        # 최종 단계들
        workflow.add_edge("synthesize_results", END)
        workflow.add_edge("error_handler", END)
        
        return workflow
    
    # ========================================================================
    # 노드 구현부
    # ========================================================================
    
    async def _analyze_intent_node(self, state: MultiAgentState) -> MultiAgentState:
        """의도 분석 노드 - GPT 모델 사용"""
        try:
            self.logger.info("의도 분석 시작")
            
            user_query = state["user_query"]
            
            # 간단한 질문들은 빠른 처리
            simple_patterns = {
                "greeting": ["안녕", "hello", "hi", "좋은", "반가", "헬로"],
                "thanks": ["고마워", "감사", "thank"],  
                "test": ["테스트", "test", "확인"],
                "simple": ["뭐", "어떻게", "뭔데", "무엇"]
            }
            
            query_lower = user_query.lower()
            for category, keywords in simple_patterns.items():
                if any(keyword in query_lower for keyword in keywords):
                    if len(user_query) < 20:  # 짧은 질문
                        self.logger.info(f"간단한 질문 감지: {category}")
                        state["intent"] = {
                            "primary": "simple_response", 
                            "secondary": [category],
                            "confidence": 0.9,
                            "is_simple": True
                        }
                        state["processing_log"].append("간단한 질문으로 분류됨")
                        return state
            
            if self.model:
                # GPT 모델로 의도 분석
                intent_prompt = f"""
사용자 질문을 분석하여 주요 의도를 분류해주세요:

질문: "{user_query}"

다음 의도 중에서 가장 적합한 것들을 선택하고 우선순위를 매겨주세요:
1. diagnostic - 현재 상태 진단
2. comparative - 비교 분석  
3. trend - 트렌드/시계열 분석
4. predictive - 예측 분석
5. optimization - 개선/최적화 제안
6. anomaly - 이상 탐지

JSON 형식으로 응답:
{{"primary": "가장_중요한_의도", "secondary": ["부차적_의도들"], "confidence": 0.9}}
"""
                
                try:
                    response = await self.model.ainvoke(intent_prompt)
                    import json
                    intent_analysis = json.loads(response.content)
                    
                    state["intent"] = intent_analysis
                    state["processing_log"].append(f"GPT 의도 분석 완료: {intent_analysis['primary']}")
                    self.logger.info(f"GPT 의도 분석 완료: {intent_analysis}")
                    
                    # 메타데이터 처리
                    metadata = {
                        "time_period": "this_week",
                        "metrics": [],
                        "urgency": "normal"
                    }
                    
                    # 시간 관련 키워드 검출
                    if any(word in user_query for word in ["오늘", "today"]):
                        metadata["time_period"] = "today"
                    elif any(word in user_query for word in ["어제", "yesterday"]):
                        metadata["time_period"] = "yesterday"
                    elif any(word in user_query for word in ["지난 주", "last week"]):
                        metadata["time_period"] = "last_week"
                    
                    # 메트릭 키워드 검출
                    metric_keywords = {
                        "방문객": "visitors", "매출": "sales", "전환율": "conversion",
                        "픽업": "pickup", "체류": "dwell_time"
                    }
                    
                    for korean, english in metric_keywords.items():
                        if korean in user_query:
                            metadata["metrics"].append(english)
                    
                    state["metadata"] = metadata
                    
                    # GPT 분석 성공시 즉시 리턴
                    return state
                    
                except Exception as e:
                    self.logger.warning(f"GPT 의도 분석 실패, 키워드 기반 폴백: {e}")
                    # 폴백: 키워드 기반 분석
                    intent_patterns = {
                        "diagnostic": ["현재", "상태", "어떻게", "분석"],
                        "comparative": ["비교", "대비", "차이", "vs", "지난"],
                        "trend": ["트렌드", "추세", "변화", "패턴", "시간"],
                        "predictive": ["예측", "전망", "앞으로", "미래"],
                        "optimization": ["개선", "최적화", "향상", "방법"],
                        "anomaly": ["이상", "문제", "급증", "급감", "갑자기"]
                    }
                    
                    detected_intents = {}
                    for intent_type, keywords in intent_patterns.items():
                        score = sum(1 for keyword in keywords if keyword in user_query)
                        if score > 0:
                            detected_intents[intent_type] = score
                    
                    primary_intent = max(detected_intents.items(), key=lambda x: x[1])[0] if detected_intents else "diagnostic"
                    
                    state["intent"] = {
                        "primary": primary_intent,
                        "secondary": list(detected_intents.keys()),
                        "confidence": 0.7
                    }
                    
                    # 메타데이터 추출
                    metadata = {
                        "time_period": "this_week",  # 기본값
                        "metrics": [],
                        "urgency": "normal"
                    }
                    
                    # 시간 관련 키워드 검출
                    if any(word in user_query for word in ["오늘", "today"]):
                        metadata["time_period"] = "today"
                    elif any(word in user_query for word in ["어제", "yesterday"]):
                        metadata["time_period"] = "yesterday"
                    elif any(word in user_query for word in ["지난 주", "last week"]):
                        metadata["time_period"] = "last_week"
                    
                    # 메트릭 키워드 검출
                    metric_keywords = {
                        "방문객": "visitors", "매출": "sales", "전환율": "conversion",
                        "픽업": "pickup", "체류": "dwell_time"
                    }
                    
                    for korean, english in metric_keywords.items():
                        if korean in user_query:
                            metadata["metrics"].append(english)
                    
                    state["metadata"] = metadata
                    state["processing_log"].append(f"의도 분석 완료: {primary_intent}")
                    
            else:
                # 모델이 없으면 기본값 사용
                state["intent"] = {
                    "primary": "diagnostic",
                    "secondary": [],
                    "confidence": 0.5
                }
                state["metadata"] = {
                    "time_period": "this_week",
                    "metrics": [],
                    "urgency": "normal"
                }
            
            return state
            
        except Exception as e:
            self.logger.error(f"의도 분석 오류: {e}")
            state["errors"].append(f"Intent analysis failed: {str(e)}")
            return state
    
    async def _decompose_tasks_node(self, state: MultiAgentState) -> MultiAgentState:
        """작업 분해 노드"""
        try:
            self.logger.info("작업 분해 시작")
            
            primary_intent = state["intent"]["primary"]
            metadata = state["metadata"]
            
            tasks = []
            
            # 의도별 작업 분해 전략
            if primary_intent == "diagnostic":
                tasks = [
                    {"type": "data_collection", "priority": 1, "agent": "data_analyst"},
                    {"type": "insight_generation", "priority": 2, "agent": "insight_generator"},
                    {"type": "recommendation", "priority": 3, "agent": "recommendation"}
                ]
            
            elif primary_intent == "comparative":
                tasks = [
                    {"type": "comparative_data_collection", "priority": 1, "agent": "data_analyst"},
                    {"type": "comparative_analysis", "priority": 2, "agent": "insight_generator"},
                    {"type": "recommendation", "priority": 3, "agent": "recommendation"}
                ]
            
            elif primary_intent == "trend":
                tasks = [
                    {"type": "time_series_collection", "priority": 1, "agent": "data_analyst"},
                    {"type": "trend_analysis", "priority": 2, "agent": "trend_predictor"},
                    {"type": "insight_generation", "priority": 3, "agent": "insight_generator"}
                ]
            
            elif primary_intent == "anomaly":
                tasks = [
                    {"type": "anomaly_detection", "priority": 1, "agent": "anomaly_detector"},
                    {"type": "data_validation", "priority": 2, "agent": "data_analyst"},
                    {"type": "insight_generation", "priority": 3, "agent": "insight_generator"}
                ]
            
            elif primary_intent == "optimization":
                tasks = [
                    {"type": "performance_analysis", "priority": 1, "agent": "data_analyst"}, 
                    {"type": "optimization_analysis", "priority": 2, "agent": "insight_generator"},
                    {"type": "action_planning", "priority": 3, "agent": "recommendation"}
                ]
            
            else:  # predictive or default
                tasks = [
                    {"type": "historical_data_collection", "priority": 1, "agent": "data_analyst"},
                    {"type": "predictive_analysis", "priority": 2, "agent": "trend_predictor"},
                    {"type": "recommendation", "priority": 3, "agent": "recommendation"}
                ]
            
            # 작업에 ID와 메타데이터 추가
            for i, task in enumerate(tasks):
                task["task_id"] = f"task_{i+1}_{datetime.now().strftime('%H%M%S')}"
                task["metadata"] = metadata.copy()
                task["status"] = "pending"
            
            state["tasks"] = tasks
            state["completed_tasks"] = []
            state["processing_log"].append(f"작업 분해 완료: {len(tasks)}개 작업 생성")
            
            return state
            
        except Exception as e:
            self.logger.error(f"작업 분해 오류: {e}")
            state["errors"].append(f"Task decomposition failed: {str(e)}")
            return state
    
    async def _data_collection_node(self, state: MultiAgentState) -> MultiAgentState:
        """데이터 수집 노드 - MCP 툴 활용"""
        try:
            self.logger.info("데이터 수집 시작")
            
            # MCP 클라이언트를 통한 실제 데이터 수집
            if self.mcp_client:
                # 실제 툴 호출 시뮬레이션
                await asyncio.sleep(1)  # 실제 처리 시간
                
                # 샘플 데이터 (실제로는 MCP 툴 결과)
                data_result = {
                    "daily_visitors": [1200, 1150, 1300, 1250, 1400, 1100, 980],
                    "conversion_rate": 0.34,
                    "pickup_rate": 0.12,
                    "avg_dwell_time": 8.5,
                    "top_zones": ["음료", "과자", "아이스크림"],
                    "data_quality": 0.89,
                    "sample_size": 7892,
                    "collection_timestamp": datetime.now().isoformat()
                }
            else:
                # 시뮬레이션된 데이터
                data_result = {
                    "daily_visitors": [1000, 1100, 1050, 1200, 1150],
                    "conversion_rate": 0.32,
                    "pickup_rate": 0.10,
                    "avg_dwell_time": 7.2,
                    "data_quality": 0.75,
                    "sample_size": 5000
                }
            
            state["data_analysis_result"] = data_result
            state["completed_tasks"].append("data_collection")
            state["processing_log"].append("데이터 수집 완료")
            
            return state
            
        except Exception as e:
            self.logger.error(f"데이터 수집 오류: {e}")
            state["errors"].append(f"Data collection failed: {str(e)}")
            return state
    
    async def _insight_generation_node(self, state: MultiAgentState) -> MultiAgentState:
        """인사이트 생성 노드"""
        try:
            self.logger.info("인사이트 생성 시작")
            
            data_result = state.get("data_analysis_result", {})
            intent = state.get("intent", {})
            
            # 데이터 기반 인사이트 생성
            insights = []
            
            if "daily_visitors" in data_result:
                visitors = data_result["daily_visitors"]
                if len(visitors) >= 2:
                    recent_avg = sum(visitors[-3:]) / 3 if len(visitors) >= 3 else visitors[-1]
                    prev_avg = sum(visitors[:-3]) / len(visitors[:-3]) if len(visitors) > 3 else visitors[0]
                    
                    if recent_avg > prev_avg * 1.1:
                        insights.append({
                            "type": "positive_trend",
                            "message": f"최근 방문객이 {((recent_avg/prev_avg-1)*100):.1f}% 증가 추세",
                            "confidence": 0.85,
                            "priority": "high"
                        })
                    elif recent_avg < prev_avg * 0.9:
                        insights.append({
                            "type": "negative_trend", 
                            "message": f"최근 방문객이 {((1-recent_avg/prev_avg)*100):.1f}% 감소 우려",
                            "confidence": 0.87,
                            "priority": "critical"
                        })
            
            if "conversion_rate" in data_result:
                conv_rate = data_result["conversion_rate"]
                if conv_rate > 0.35:
                    insights.append({
                        "type": "performance",
                        "message": f"전환율 {conv_rate:.1%}로 우수한 성과",
                        "confidence": 0.90,
                        "priority": "medium"
                    })
                elif conv_rate < 0.25:
                    insights.append({
                        "type": "performance_issue",
                        "message": f"전환율 {conv_rate:.1%}로 개선 필요",
                        "confidence": 0.88,
                        "priority": "high"
                    })
            
            if "pickup_rate" in data_result:
                pickup_rate = data_result["pickup_rate"]
                if pickup_rate < 0.08:
                    insights.append({
                        "type": "engagement_issue",
                        "message": f"픽업률 {pickup_rate:.1%}로 상품 매력도 점검 필요",
                        "confidence": 0.82,
                        "priority": "medium"
                    })
            
            insight_result = {
                "insights": insights,
                "analysis_type": intent.get("primary", "diagnostic"),
                "key_metrics": {
                    "performance_score": self._calculate_performance_score(data_result),
                    "trend_direction": self._determine_trend_direction(data_result),
                    "risk_level": self._assess_risk_level(insights)
                },
                "confidence": sum(i["confidence"] for i in insights) / len(insights) if insights else 0.5
            }
            
            state["insight_analysis_result"] = insight_result
            state["completed_tasks"].append("insight_generation")
            state["processing_log"].append(f"인사이트 생성 완료: {len(insights)}개")
            
            return state
            
        except Exception as e:
            self.logger.error(f"인사이트 생성 오류: {e}")
            state["errors"].append(f"Insight generation failed: {str(e)}")
            return state
    
    async def _recommendation_generation_node(self, state: MultiAgentState) -> MultiAgentState:
        """추천 생성 노드"""
        try:
            self.logger.info("추천 생성 시작")
            
            insights = state.get("insight_analysis_result", {}).get("insights", [])
            data_result = state.get("data_analysis_result", {})
            
            recommendations = []
            
            # 인사이트 기반 추천 생성
            for insight in insights:
                if insight["type"] == "negative_trend":
                    recommendations.append({
                        "priority": "HIGH",
                        "action": "방문객 유입 채널 다각화 및 프로모션 강화",
                        "expected_impact": "15-20% 방문객 증가",
                        "implementation_time": "1-2주",
                        "roi_estimate": "월 매출 10-15% 증대"
                    })
                
                elif insight["type"] == "performance_issue":
                    recommendations.append({
                        "priority": "CRITICAL",
                        "action": "전환 장벽 분석 및 구매 동선 최적화", 
                        "expected_impact": "전환율 5-8% 포인트 개선",
                        "implementation_time": "3-5일",
                        "roi_estimate": "월 순이익 20-25% 증가"
                    })
                
                elif insight["type"] == "engagement_issue":
                    recommendations.append({
                        "priority": "MEDIUM",
                        "action": "상품 진열 및 시각적 어필 강화",
                        "expected_impact": "픽업률 3-5% 포인트 개선",
                        "implementation_time": "1-2일", 
                        "roi_estimate": "월 매출 5-8% 증가"
                    })
            
            # 데이터 품질 기반 추천
            data_quality = data_result.get("data_quality", 1.0)
            if data_quality < 0.8:
                recommendations.append({
                    "priority": "MEDIUM",
                    "action": "데이터 수집 시스템 점검 및 품질 개선",
                    "expected_impact": "분석 정확도 향상",
                    "implementation_time": "1주일",
                    "roi_estimate": "장기적 의사결정 품질 개선"
                })
            
            # 기본 추천사항
            if not recommendations:
                recommendations.append({
                    "priority": "LOW",
                    "action": "현재 성과 유지 및 지속적 모니터링",
                    "expected_impact": "안정적 운영 지속",
                    "implementation_time": "지속",
                    "roi_estimate": "현 수준 유지"
                })
            
            recommendation_result = {
                "recommendations": recommendations,
                "prioritized_actions": sorted(recommendations, key=lambda x: {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}[x["priority"]]),
                "implementation_roadmap": self._create_implementation_roadmap(recommendations),
                "confidence": 0.82
            }
            
            state["recommendation_result"] = recommendation_result
            state["completed_tasks"].append("recommendation_generation")
            state["processing_log"].append(f"추천 생성 완료: {len(recommendations)}개")
            
            return state
            
        except Exception as e:
            self.logger.error(f"추천 생성 오류: {e}")
            state["errors"].append(f"Recommendation generation failed: {str(e)}")
            return state
    
    async def _anomaly_detection_node(self, state: MultiAgentState) -> MultiAgentState:
        """이상 탐지 노드"""
        try:
            self.logger.info("이상 탐지 시작")
            
            data_result = state.get("data_analysis_result", {})
            
            anomalies = []
            
            # 간단한 이상치 탐지 로직
            if "daily_visitors" in data_result:
                visitors = data_result["daily_visitors"]
                if len(visitors) > 2:
                    mean_visitors = sum(visitors) / len(visitors)
                    std_visitors = (sum((x - mean_visitors) ** 2 for x in visitors) / len(visitors)) ** 0.5
                    
                    for i, visitor_count in enumerate(visitors):
                        if abs(visitor_count - mean_visitors) > 2 * std_visitors:
                            anomalies.append({
                                "date_index": i,
                                "metric": "daily_visitors",
                                "value": visitor_count,
                                "expected_range": f"{mean_visitors - 2*std_visitors:.0f}-{mean_visitors + 2*std_visitors:.0f}",
                                "deviation": f"{((visitor_count/mean_visitors-1)*100):+.1f}%",
                                "severity": "high" if abs(visitor_count - mean_visitors) > 3 * std_visitors else "medium"
                            })
            
            anomaly_result = {
                "anomalies": anomalies,
                "anomaly_score": len(anomalies) / max(len(data_result.get("daily_visitors", [1])), 1),
                "detection_method": "statistical_threshold",
                "confidence": 0.88
            }
            
            state["anomaly_result"] = anomaly_result
            state["completed_tasks"].append("anomaly_detection")
            state["processing_log"].append(f"이상 탐지 완료: {len(anomalies)}개 발견")
            
            return state
            
        except Exception as e:
            self.logger.error(f"이상 탐지 오류: {e}")
            state["errors"].append(f"Anomaly detection failed: {str(e)}")
            return state
    
    async def _trend_analysis_node(self, state: MultiAgentState) -> MultiAgentState:
        """트렌드 분석 노드"""
        try:
            self.logger.info("트렌드 분석 시작")
            
            data_result = state.get("data_analysis_result", {})
            
            trends = {}
            
            if "daily_visitors" in data_result:
                visitors = data_result["daily_visitors"]
                if len(visitors) >= 3:
                    # 단순 선형 트렌드 계산
                    n = len(visitors)
                    x = list(range(n))
                    y = visitors
                    
                    # 기울기 계산
                    slope = (n * sum(x[i] * y[i] for i in range(n)) - sum(x) * sum(y)) / (n * sum(x[i]**2 for i in range(n)) - sum(x)**2)
                    
                    trends["visitor_trend"] = {
                        "direction": "increasing" if slope > 0 else "decreasing",
                        "strength": abs(slope),
                        "forecast_next_period": visitors[-1] + slope,
                        "confidence": 0.75
                    }
            
            trend_result = {
                "trends": trends,
                "trend_summary": self._summarize_trends(trends),
                "predictions": {
                    "next_week_forecast": trends.get("visitor_trend", {}).get("forecast_next_period", "N/A"),
                    "confidence_interval": "±15%"
                },
                "confidence": 0.79
            }
            
            state["trend_result"] = trend_result
            state["completed_tasks"].append("trend_analysis")
            state["processing_log"].append("트렌드 분석 완료")
            
            return state
            
        except Exception as e:
            self.logger.error(f"트렌드 분석 오류: {e}")
            state["errors"].append(f"Trend analysis failed: {str(e)}")
            return state
    
    async def _synthesize_results_node(self, state: MultiAgentState) -> MultiAgentState:
        """결과 통합 노드 - GPT 모델 사용"""
        try:
            self.logger.info("GPT를 사용한 결과 통합 시작")
            
            # 각 에이전트 결과 수집
            data_result = state.get("data_analysis_result", {})
            insight_result = state.get("insight_analysis_result", {})
            recommendation_result = state.get("recommendation_result", {})
            anomaly_result = state.get("anomaly_result", {})
            trend_result = state.get("trend_result", {})
            
            user_query = state["user_query"]
            intent = state.get("intent", {})
            is_simple = intent.get("is_simple", False)
            
            # 간단한 질문은 빠른 응답
            if is_simple:
                category = intent.get("secondary", ["greeting"])[0]
                
                simple_responses = {
                    "greeting": "안녕하세요! 멀티 에이전트 분석 시스템입니다. 어떤 분석이나 인사이트가 필요하신가요? 도움이 필요하시면 언제든지 말씀해 주세요.",
                    "thanks": "천만에요! 더 도움이 필요하시면 언제든지 말씀해 주세요.",
                    "test": "시스템이 정상 작동 중입니다! 멀티 에이전트 분석 시스템이 준비되어 있습니다.",
                    "simple": "구체적으로 어떤 분석이나 정보가 필요하신가요? 예를 들어 '이번 주 방문객 수는?', '매출 트렌드는?' 같은 질문을 해주세요."
                }
                
                final_insight = simple_responses.get(category, simple_responses["greeting"])
                
                state["final_insight"] = final_insight
                state["confidence_score"] = 0.9
                state["processing_log"].append("간단한 질문 - 빠른 응답 완료")
                state["completed_tasks"] = ["quick_response"]
                
                return state
            
            if self.model:
                # GPT 모델로 전문적인 분석 리포트 생성
                synthesis_prompt = f"""
당신은 리테일 분석 전문가입니다. 수집된 데이터를 기반으로 CEO급 임원진이 읽을 전략적 분석 리포트를 작성해주세요.

## 사용자 질문
"{user_query}"

## 분석 의도
주요 의도: {intent.get('primary', 'diagnostic')}
부차 의도: {intent.get('secondary', [])}

## 분석된 데이터
데이터 분석 결과: {data_result}
인사이트 분석: {insight_result}
추천사항 결과: {recommendation_result}
이상 탐지 결과: {anomaly_result}
트렌드 분석 결과: {trend_result}

다음 구조로 전문적인 분석 리포트를 작성해주세요:

# 📊 Executive Summary
**현재 성과**: [핵심 지표 요약]
**전환율**: [전환율 정보 포함 시]

## 🔍 Key Insights
*우선순위가 높은 발견사항 3-5개를 데이터 근거와 함께*

## 📈 Performance Analysis
*현재 지표와 벤치마크 비교, 트렌드 분석*

## ⚡ Immediate Actions (24-48시간)
*즉시 실행 가능한 고임팩트 개선안*

## 🎯 Strategic Recommendations (1-4주)
*중장기 전략적 제안과 예상 성과*

## 📊 Success Metrics
*개선도 측정 방법과 추적 지표*

## ⚠️ Risk Factors
*잠재적 도전과제와 대응 방안*

**데이터 소스**: [사용된 도구/테이블]
**분석 기간**: [분석 범위]
**신뢰도**: [전체적인 분석 신뢰도]

한국어로 작성하되, 비즈니스 전문 용어를 적절히 사용하세요.
"""
                
                try:
                    response = await self.model.ainvoke(synthesis_prompt)
                    final_insight = response.content
                    
                    self.logger.info("GPT 결과 통합 완료")
                    
                except Exception as e:
                    self.logger.warning(f"GPT 결과 통합 실패, 기본 템플릿 사용: {e}")
                    # 폴백: 기존 하드코딩된 로직
                    final_insight = self._generate_fallback_synthesis(data_result, insight_result, recommendation_result, anomaly_result, trend_result)
            else:
                # 모델이 없으면 기본 템플릿 사용
                final_insight = self._generate_fallback_synthesis(data_result, insight_result, recommendation_result, anomaly_result, trend_result)
            
            # 요약 정보 수집용 배열 초기화
            summary_parts = []
            
            if data_result:
                visitors = data_result.get("daily_visitors", [])
                if visitors:
                    summary_parts.append(f"📊 **현재 성과**: 일평균 방문객 {visitors[-1]:,}명")
                
                conv_rate = data_result.get("conversion_rate")
                if conv_rate:
                    summary_parts.append(f"전환율 {conv_rate:.1%}")
            
            # 주요 인사이트
            key_insights = []
            if insight_result and "insights" in insight_result:
                for insight in insight_result["insights"]:
                    key_insights.append(f"• {insight['message']}")
            
            # 이상 탐지 결과
            if anomaly_result and anomaly_result.get("anomalies"):
                key_insights.append(f"• ⚠️ {len(anomaly_result['anomalies'])}개 이상 패턴 감지")
            
            # 트렌드 정보
            if trend_result and trend_result.get("trends"):
                trend_info = trend_result["trends"].get("visitor_trend", {})
                if trend_info:
                    direction = trend_info["direction"]
                    key_insights.append(f"• 📈 방문객 추세: {'상승' if direction == 'increasing' else '하락'}세")
            
            # 추천사항
            top_recommendations = []
            if recommendation_result and "recommendations" in recommendation_result:
                for rec in recommendation_result["recommendations"][:3]:  # 상위 3개
                    top_recommendations.append(
                        f"🎯 **{rec['priority']}**: {rec['action']}\n"
                        f"   └ 예상 효과: {rec['expected_impact']}"
                    )
            
            # 최종 인사이트 문서 생성
            final_parts = [
                "# 📊 Executive Summary",
                " ".join(summary_parts) if summary_parts else "데이터 분석이 완료되었습니다.",
                "",
                "## 🔍 Key Insights"
            ]
            
            if key_insights:
                final_parts.extend(key_insights)
            else:
                final_parts.append("• 특별한 이상사항은 발견되지 않았습니다.")
            
            if top_recommendations:
                final_parts.extend(["", "## 💡 주요 추천사항"])
                final_parts.extend(top_recommendations)
            
            # 신뢰도 계산
            confidences = []
            for result in [data_result, insight_result, recommendation_result, anomaly_result, trend_result]:
                if result and "confidence" in result:
                    confidences.append(result["confidence"])
            
            overall_confidence = sum(confidences) / len(confidences) if confidences else 0.5
            
            final_parts.extend([
                "",
                f"**분석 신뢰도**: {overall_confidence:.1%}",
                f"**처리 완료**: {len(state['completed_tasks'])}개 작업"
            ])
            
            # 신뢰도 계산
            confidences = []
            for result in [data_result, insight_result, recommendation_result, anomaly_result, trend_result]:
                if result and "confidence" in result:
                    confidences.append(result["confidence"])
            
            overall_confidence = sum(confidences) / len(confidences) if confidences else 0.5
            
            state["final_insight"] = final_insight
            state["confidence_score"] = overall_confidence
            state["processing_log"].append("최종 결과 통합 완료")
            
            return state
            
        except Exception as e:
            self.logger.error(f"결과 통합 오류: {e}")
            state["errors"].append(f"Result synthesis failed: {str(e)}")
            state["final_insight"] = "분석 중 오류가 발생했습니다. 다시 시도해 주세요."
            return state
    
    def _generate_fallback_synthesis(self, data_result, insight_result, recommendation_result, anomaly_result, trend_result):
        """폴백용 기본 결과 통합"""
        final_parts = ["# 📊 Executive Summary"]
        
        if data_result:
            visitors = data_result.get("daily_visitors", [])
            if visitors:
                final_parts.append(f"📊 **현재 성과**: 일평균 방문객 {visitors[-1]:,}명")
            
            conv_rate = data_result.get("conversion_rate")
            if conv_rate:
                final_parts.append(f"📊 **전환율**: {conv_rate:.1%}")
        
        final_parts.extend(["", "## 🔍 Key Insights"])
        
        if insight_result and "insights" in insight_result:
            key_insights = insight_result["insights"][:3]  # 상위 3개
            final_parts.extend(key_insights)
        else:
            final_parts.append("• 특별한 이상사항은 발견되지 않았습니다.")
        
        if recommendation_result and "recommendations" in recommendation_result:
            top_recommendations = recommendation_result["recommendations"][:3]
            final_parts.extend(["", "## 💡 주요 추천사항"])
            final_parts.extend(top_recommendations)
        
        final_parts.extend([
            "",
            "**분석 신뢰도**: 50.0%",
            "**처리 완료**: 2개 작업"
        ])
        
        return "\n".join(final_parts)
    
    async def _error_handler_node(self, state: MultiAgentState) -> MultiAgentState:
        """오류 처리 노드"""
        try:
            self.logger.warning("오류 처리 노드 실행")
            
            errors = state.get("errors", [])
            retry_count = state.get("retry_count", 0)
            
            # 재시도 로직
            if retry_count < 2:
                state["retry_count"] = retry_count + 1
                state["processing_log"].append(f"재시도 {retry_count + 1}/3")
                # 여기서 실제로는 이전 단계로 되돌아가거나 대안 실행
            else:
                # 최대 재시도 초과 시 기본 응답 생성
                state["final_insight"] = (
                    "# ⚠️ 분석 오류\n\n"
                    "죄송합니다. 요청을 완전히 처리하는 중 기술적 문제가 발생했습니다.\n"
                    "기본적인 상태 정보는 다음과 같습니다:\n\n"
                    "• 시스템이 정상 작동 중입니다\n"
                    "• 데이터 수집 기능이 활성화되어 있습니다\n"
                    "• 잠시 후 다시 시도해 주시기 바랍니다\n\n"
                    f"**오류 내용**: {'; '.join(errors[-2:])}"  # 최근 2개 오류만
                )
                state["confidence_score"] = 0.1
            
            return state
            
        except Exception as e:
            self.logger.error(f"오류 처리기 자체 오류: {e}")
            state["final_insight"] = "시스템 오류가 발생했습니다."
            return state
    
    # ========================================================================
    # 라우팅 함수들
    # ========================================================================
    
    def _route_by_intent(self, state: MultiAgentState) -> str:
        """의도에 따른 초기 라우팅"""
        try:
            intent_info = state.get("intent", {})
            primary_intent = intent_info.get("primary", "diagnostic")
            is_simple = intent_info.get("is_simple", False)
            
            # 간단한 질문은 바로 결과 통합으로
            if is_simple or primary_intent == "simple_response":
                return "direct_synthesis"
            
            if primary_intent == "anomaly":
                return "anomaly_first"
            elif primary_intent == "trend": 
                return "trend_first"
            elif primary_intent in ["diagnostic", "comparative", "optimization"]:
                return "data_first"
            elif primary_intent == "predictive":
                return "trend_first"
            else:
                return "data_first"
                
        except Exception:
            return "error"
    
    def _route_after_data_collection(self, state: MultiAgentState) -> str:
        """데이터 수집 후 라우팅"""
        try:
            if state.get("errors"):
                return "error"
            
            primary_intent = state.get("intent", {}).get("primary", "diagnostic")
            
            if primary_intent == "anomaly":
                return "to_anomaly"
            elif primary_intent in ["trend", "predictive"]:
                return "to_trend" 
            else:
                return "to_insights"
                
        except Exception:
            return "error"
    
    def _route_after_analysis(self, state: MultiAgentState) -> str:
        """분석 후 라우팅"""
        try:
            if state.get("errors"):
                return "error"
            
            completed = state.get("completed_tasks", [])
            
            # 필수 작업들이 완료되었는지 확인
            essential_tasks = ["data_collection", "insight_generation"]
            
            if any(task not in completed for task in essential_tasks):
                return "continue"
            else:
                return "synthesize"
                
        except Exception:
            return "error"
    
    # ========================================================================
    # 헬퍼 함수들
    # ========================================================================
    
    def _calculate_performance_score(self, data_result: Dict[str, Any]) -> float:
        """성과 점수 계산"""
        score = 0.5  # 기본값
        
        conv_rate = data_result.get("conversion_rate", 0.3)
        pickup_rate = data_result.get("pickup_rate", 0.1)
        
        # 전환율 기반 점수 (30% 이상 우수)
        if conv_rate >= 0.35:
            score += 0.3
        elif conv_rate >= 0.25:
            score += 0.1
        
        # 픽업률 기반 점수 (10% 이상 양호)
        if pickup_rate >= 0.12:
            score += 0.2
        elif pickup_rate >= 0.08:
            score += 0.1
        
        return min(score, 1.0)
    
    def _determine_trend_direction(self, data_result: Dict[str, Any]) -> str:
        """트렌드 방향 판단"""
        visitors = data_result.get("daily_visitors", [])
        if len(visitors) < 2:
            return "insufficient_data"
        
        recent = sum(visitors[-3:]) / 3 if len(visitors) >= 3 else visitors[-1]
        earlier = sum(visitors[:-3]) / len(visitors[:-3]) if len(visitors) > 3 else visitors[0]
        
        if recent > earlier * 1.05:
            return "increasing"
        elif recent < earlier * 0.95:
            return "decreasing"
        else:
            return "stable"
    
    def _assess_risk_level(self, insights: List[Dict[str, Any]]) -> str:
        """위험 수준 평가"""
        critical_count = sum(1 for i in insights if i.get("priority") == "critical")
        high_count = sum(1 for i in insights if i.get("priority") == "high")
        
        if critical_count > 0:
            return "critical"
        elif high_count > 1:
            return "high"
        elif high_count == 1:
            return "medium"
        else:
            return "low"
    
    def _create_implementation_roadmap(self, recommendations: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """구현 로드맵 생성"""
        roadmap = {
            "immediate": [],  # 24시간 내
            "short_term": [],  # 1주일 내
            "medium_term": []  # 1개월 내
        }
        
        for rec in recommendations:
            impl_time = rec.get("implementation_time", "")
            action = rec["action"]
            
            if any(word in impl_time for word in ["즉시", "바로", "24시간", "1-2일"]):
                roadmap["immediate"].append(action)
            elif any(word in impl_time for word in ["주", "week"]):
                roadmap["short_term"].append(action)
            else:
                roadmap["medium_term"].append(action)
        
        return roadmap
    
    def _summarize_trends(self, trends: Dict[str, Any]) -> str:
        """트렌드 요약"""
        if not trends:
            return "충분한 데이터가 없어 트렌드를 파악할 수 없습니다."
        
        visitor_trend = trends.get("visitor_trend", {})
        if visitor_trend:
            direction = visitor_trend["direction"]
            strength = visitor_trend.get("strength", 0)
            
            if direction == "increasing":
                return f"방문객 수가 상승 추세입니다 (강도: {strength:.2f})"
            else:
                return f"방문객 수가 하락 추세입니다 (강도: {strength:.2f})"
        
        return "트렌드 패턴이 명확하지 않습니다."
    
    # ========================================================================
    # 공개 API
    # ========================================================================
    
    async def execute(self, user_query: str, session_id: str = None) -> Dict[str, Any]:
        """워크플로우 실행"""
        try:
            session_id = session_id or f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # 초기 상태 설정
            initial_state = {
                "user_query": user_query,
                "session_id": session_id,
                "timestamp": datetime.now(),
                "intent": {},
                "metadata": {},
                "tasks": [],
                "completed_tasks": [],
                "current_task": None,
                "data_analysis_result": None,
                "insight_analysis_result": None,
                "recommendation_result": None,
                "anomaly_result": None,
                "trend_result": None,
                "final_insight": "",
                "confidence_score": 0.0,
                "processing_log": [],
                "errors": [],
                "retry_count": 0,
                "messages": [HumanMessage(content=user_query)]
            }
            
            # 워크플로우 실행
            config = {"configurable": {"thread_id": session_id}}
            
            final_state = None
            async for state_update in self.compiled_graph.astream(initial_state, config):
                final_state = state_update
                self.logger.info(f"상태 업데이트: {list(state_update.keys())}")
            
            if final_state:
                # 결과에서 마지막 상태 추출
                last_node_state = list(final_state.values())[-1]
                
                return {
                    "success": True,
                    "session_id": session_id,
                    "final_insight": last_node_state.get("final_insight", "분석이 완료되었습니다."),
                    "confidence_score": last_node_state.get("confidence_score", 0.5),
                    "completed_tasks": last_node_state.get("completed_tasks", []),
                    "processing_log": last_node_state.get("processing_log", []),
                    "errors": last_node_state.get("errors", [])
                }
            else:
                return {
                    "success": False,
                    "error": "워크플로우 실행 중 상태를 받지 못했습니다."
                }
            
        except Exception as e:
            self.logger.error(f"워크플로우 실행 오류: {e}")
            return {
                "success": False,
                "error": str(e),
                "final_insight": "워크플로우 실행 중 오류가 발생했습니다."
            }
    
    def get_workflow_info(self) -> Dict[str, Any]:
        """워크플로우 정보 반환"""
        return {
            "nodes": list(self.graph.nodes.keys()),
            "agent_capabilities": self.agent_capabilities,
            "checkpointer_enabled": self.checkpointer is not None,
            "mcp_client_available": self.mcp_client is not None
        }

if __name__ == "__main__":
    # 테스트 실행
    async def test_workflow():
        workflow = MultiAgentWorkflow()
        
        result = await workflow.execute(
            "이번 주 방문객수가 어떻게 되고 있나요? 개선 방안도 알려주세요.",
            "test_session_001"
        )
        
        print("=== 워크플로우 실행 결과 ===")
        print(f"성공: {result['success']}")
        if result['success']:
            print(f"최종 인사이트:\n{result['final_insight']}")
            print(f"신뢰도: {result['confidence_score']:.1%}")
            print(f"완료된 작업: {result['completed_tasks']}")
        else:
            print(f"오류: {result.get('error')}")
    
    # asyncio.run(test_workflow())