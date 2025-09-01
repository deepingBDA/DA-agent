"""
Insight Generator Agent
=======================

데이터 분석 결과를 비즈니스 인사이트로 변환하는 전문 에이전트입니다.
패턴 인식, 원인 분석, 비즈니스 임팩트 평가를 수행합니다.
"""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
import statistics
import json

from ..base_agent import (
    BaseAgent, AgentType, AgentMessage, MessageType, 
    AgentCapability, AnalyticsBaseAgent
)

# 인사이트 템플릿 import
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from knowledge.insight_templates import insight_generator, InsightType, Priority
from knowledge.schema_context import BUSINESS_METRICS, INSIGHT_RULES, ACTION_FRAMEWORK

class InsightGeneratorAgent(AnalyticsBaseAgent):
    """인사이트 생성 전문 에이전트"""
    
    def __init__(self):
        capabilities = [
            AgentCapability(
                name="pattern_recognition",
                description="데이터에서 비즈니스 패턴과 트렌드 식별",
                input_requirements=["statistical_data", "time_series"],
                output_format="business_insights",
                confidence_threshold=0.75
            ),
            AgentCapability(
                name="root_cause_analysis", 
                description="성과 변화의 근본 원인 분석",
                input_requirements=["performance_data", "context"],
                output_format="causal_analysis",
                confidence_threshold=0.7
            ),
            AgentCapability(
                name="business_impact_assessment",
                description="데이터 변화의 비즈니스 임팩트 평가",
                input_requirements=["metrics", "benchmarks"],
                output_format="impact_assessment",
                confidence_threshold=0.8
            ),
            AgentCapability(
                name="competitive_analysis",
                description="벤치마크 대비 성과 분석",
                input_requirements=["performance_metrics", "industry_benchmarks"],
                output_format="competitive_insights",
                confidence_threshold=0.75
            ),
            AgentCapability(
                name="trend_interpretation",
                description="시간에 따른 변화 패턴 해석",
                input_requirements=["time_series_data"],
                output_format="trend_insights",
                confidence_threshold=0.7
            )
        ]
        
        super().__init__(AgentType.INSIGHT_GENERATOR, capabilities=capabilities)
        
        # 비즈니스 컨텍스트
        self.business_context = BUSINESS_METRICS
        self.insight_rules = INSIGHT_RULES
        
        # 성과 벤치마크
        self.benchmarks = {
            "conversion_rate": {
                "excellent": 0.35,
                "good": 0.28,
                "average": 0.20,
                "poor": 0.15
            },
            "pickup_rate": {
                "excellent": 0.15,
                "good": 0.12,
                "average": 0.08,
                "poor": 0.05
            },
            "dwell_time": {
                "optimal_min": 3.0,
                "optimal_max": 10.0,
                "concerning_max": 20.0
            }
        }
        
        # 임계값 설정
        self.significance_thresholds = {
            "trend_change": 0.1,      # 10% 변화
            "anomaly_std": 2.0,       # 2 표준편차
            "correlation_min": 0.6,   # 최소 상관관계
            "sample_size_min": 30     # 최소 샘플 수
        }
    
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        """메시지 처리 메인 로직"""
        try:
            self.log_message(message, "received")
            
            task_type = message.content.get("task_type", "insight_generation")
            params = message.content.get("params", {})
            data = message.content.get("data", {})
            
            # 작업 타입별 처리
            if task_type == "performance_analysis":
                result = await self._handle_performance_analysis(data, params)
            elif task_type == "comparative_analysis":
                result = await self._handle_comparative_analysis(data, params)
            elif task_type == "pattern_analysis":
                result = await self._handle_pattern_analysis(data, params)
            elif task_type == "optimization_analysis":
                result = await self._handle_optimization_analysis(data, params)
            elif task_type == "diagnostic_analysis":
                result = await self._handle_diagnostic_analysis(data, params)
            else:
                # 기본 인사이트 생성
                result = await self._handle_general_insight_generation(data, params)
            
            response = self.create_response_message(
                message,
                {
                    "status": "success",
                    "result": result
                }
            )
            
            self.log_message(response, "sent")
            return response
            
        except Exception as e:
            self.logger.error(f"Insight generation error: {str(e)}")
            error_response = self.create_response_message(
                message,
                {
                    "status": "error",
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                MessageType.ERROR
            )
            return error_response
    
    async def _handle_performance_analysis(self, data: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """성과 분석 처리"""
        self.logger.info("성과 분석 인사이트 생성 시작")
        
        insights = []
        
        # 전환율 분석
        if "conversion_rate" in data:
            conv_insights = self._analyze_conversion_performance(data["conversion_rate"])
            insights.extend(conv_insights)
        
        # 픽업률 분석
        if "pickup_rate" in data:
            pickup_insights = self._analyze_pickup_performance(data["pickup_rate"])
            insights.extend(pickup_insights)
        
        # 방문객 트렌드 분석
        if "daily_visitors" in data:
            visitor_insights = self._analyze_visitor_trends(data["daily_visitors"])
            insights.extend(visitor_insights)
        
        # 종합 성과 평가
        overall_assessment = self._assess_overall_performance(data)
        
        return {
            "analysis_type": "performance_analysis",
            "insights": insights,
            "overall_assessment": overall_assessment,
            "key_metrics_status": self._categorize_all_metrics(data),
            "confidence": self._calculate_insight_confidence(insights),
            "analysis_timestamp": datetime.now().isoformat()
        }
    
    async def _handle_comparative_analysis(self, data: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """비교 분석 처리"""
        self.logger.info("비교 분석 인사이트 생성 시작")
        
        insights = []
        
        # 기간별 비교가 있는 경우
        if "comparative_analysis" in data:
            comp_data = data["comparative_analysis"]
            
            # 방문객 변화 분석
            if "visitor_comparison" in comp_data:
                visitor_comp_insights = self._analyze_visitor_comparison(comp_data["visitor_comparison"])
                insights.extend(visitor_comp_insights)
            
            # 전환율 변화 분석
            if "conversion_comparison" in comp_data:
                conv_comp_insights = self._analyze_conversion_comparison(comp_data["conversion_comparison"])
                insights.extend(conv_comp_insights)
        
        # 벤치마크 비교
        benchmark_insights = self._compare_against_benchmarks(data)
        insights.extend(benchmark_insights)
        
        return {
            "analysis_type": "comparative_analysis",
            "insights": insights,
            "comparison_summary": self._create_comparison_summary(data),
            "performance_changes": self._identify_significant_changes(data),
            "confidence": self._calculate_insight_confidence(insights),
            "analysis_timestamp": datetime.now().isoformat()
        }
    
    async def _handle_diagnostic_analysis(self, data: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """진단 분석 처리"""
        self.logger.info("진단 분석 인사이트 생성 시작")
        
        insights = []
        
        # 현재 상태 진단
        current_status = self._diagnose_current_status(data)
        insights.append({
            "type": "diagnostic_summary",
            "message": current_status["summary"],
            "priority": current_status["priority"],
            "confidence": current_status["confidence"]
        })
        
        # 문제점 식별
        issues = self._identify_issues(data)
        for issue in issues:
            insights.append({
                "type": "issue_identification",
                "message": issue["description"],
                "severity": issue["severity"],
                "priority": issue["priority"],
                "confidence": issue["confidence"]
            })
        
        # 강점 식별
        strengths = self._identify_strengths(data)
        for strength in strengths:
            insights.append({
                "type": "strength_identification",
                "message": strength["description"],
                "priority": "medium",
                "confidence": strength["confidence"]
            })
        
        return {
            "analysis_type": "diagnostic_analysis",
            "insights": insights,
            "current_status": current_status,
            "critical_issues": [i for i in issues if i["severity"] == "critical"],
            "key_strengths": strengths,
            "confidence": self._calculate_insight_confidence(insights),
            "analysis_timestamp": datetime.now().isoformat()
        }
    
    async def _handle_general_insight_generation(self, data: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """일반적인 인사이트 생성"""
        self.logger.info("일반 인사이트 생성 시작")
        
        insights = []
        
        # 데이터 기반 패턴 인식
        patterns = self._recognize_patterns(data)
        for pattern in patterns:
            insights.append({
                "type": "pattern_recognition",
                "message": pattern["description"],
                "significance": pattern["significance"],
                "priority": pattern["priority"],
                "confidence": pattern["confidence"]
            })
        
        # 상관관계 분석
        correlations = self._analyze_correlations(data)
        for corr in correlations:
            insights.append({
                "type": "correlation",
                "message": corr["description"],
                "strength": corr["strength"],
                "priority": "medium",
                "confidence": corr["confidence"]
            })
        
        return {
            "analysis_type": "general_insights",
            "insights": insights,
            "pattern_summary": self._summarize_patterns(patterns),
            "confidence": self._calculate_insight_confidence(insights),
            "analysis_timestamp": datetime.now().isoformat()
        }
    
    # ========================================================================
    # 구체적 분석 메서드들
    # ========================================================================
    
    def _analyze_conversion_performance(self, conversion_rate: float) -> List[Dict[str, Any]]:
        """전환율 성과 분석"""
        insights = []
        
        # 벤치마크 대비 평가
        benchmarks = self.benchmarks["conversion_rate"]
        
        if conversion_rate >= benchmarks["excellent"]:
            insights.append({
                "type": "performance_excellent",
                "message": f"전환율 {conversion_rate:.1%}로 업계 최상위 수준입니다",
                "priority": "medium",
                "confidence": 0.9,
                "impact": "positive"
            })
        elif conversion_rate >= benchmarks["good"]:
            insights.append({
                "type": "performance_good", 
                "message": f"전환율 {conversion_rate:.1%}로 양호한 성과를 보이고 있습니다",
                "priority": "low",
                "confidence": 0.85,
                "impact": "positive"
            })
        elif conversion_rate >= benchmarks["average"]:
            insights.append({
                "type": "performance_average",
                "message": f"전환율 {conversion_rate:.1%}로 개선 여지가 있습니다",
                "priority": "medium",
                "confidence": 0.8,
                "impact": "neutral"
            })
        else:
            insights.append({
                "type": "performance_poor",
                "message": f"전환율 {conversion_rate:.1%}로 즉시 개선이 필요합니다",
                "priority": "high",
                "confidence": 0.95,
                "impact": "negative"
            })
            
            # 추가 개선 포인트 제시
            insights.append({
                "type": "improvement_needed",
                "message": "구매 장벽 분석과 고객 경험 개선이 시급합니다",
                "priority": "high",
                "confidence": 0.8,
                "impact": "actionable"
            })
        
        return insights
    
    def _analyze_pickup_performance(self, pickup_rate: float) -> List[Dict[str, Any]]:
        """픽업률 성과 분석"""
        insights = []
        
        benchmarks = self.benchmarks["pickup_rate"]
        
        if pickup_rate >= benchmarks["excellent"]:
            insights.append({
                "type": "engagement_excellent",
                "message": f"픽업률 {pickup_rate:.1%}로 상품 매력도가 매우 높습니다",
                "priority": "low",
                "confidence": 0.9,
                "impact": "positive"
            })
        elif pickup_rate >= benchmarks["good"]:
            insights.append({
                "type": "engagement_good",
                "message": f"픽업률 {pickup_rate:.1%}로 고객 관심도가 양호합니다",
                "priority": "low", 
                "confidence": 0.85,
                "impact": "positive"
            })
        elif pickup_rate >= benchmarks["average"]:
            insights.append({
                "type": "engagement_average",
                "message": f"픽업률 {pickup_rate:.1%}로 진열 최적화가 필요합니다",
                "priority": "medium",
                "confidence": 0.8,
                "impact": "neutral"
            })
        else:
            insights.append({
                "type": "engagement_poor",
                "message": f"픽업률 {pickup_rate:.1%}로 상품 어필력 강화가 시급합니다",
                "priority": "high",
                "confidence": 0.9,
                "impact": "negative"
            })
            
            # 구체적 개선 방향 제시
            insights.append({
                "type": "display_optimization",
                "message": "진열 위치, 시각적 어필, 접근성 개선을 검토하세요",
                "priority": "high",
                "confidence": 0.85,
                "impact": "actionable"
            })
        
        return insights
    
    def _analyze_visitor_trends(self, daily_visitors: List[int]) -> List[Dict[str, Any]]:
        """방문객 트렌드 분석"""
        insights = []
        
        if len(daily_visitors) < 3:
            return insights
        
        # 트렌드 방향 분석
        recent_avg = sum(daily_visitors[-3:]) / 3
        earlier_avg = sum(daily_visitors[:-3]) / len(daily_visitors[:-3]) if len(daily_visitors) > 3 else daily_visitors[0]
        
        change_pct = ((recent_avg - earlier_avg) / earlier_avg) * 100 if earlier_avg != 0 else 0
        
        if change_pct > 15:
            insights.append({
                "type": "trend_strong_positive",
                "message": f"방문객이 최근 강한 증가 추세 (+{change_pct:.1f}%)",
                "priority": "medium",
                "confidence": 0.85,
                "impact": "positive"
            })
        elif change_pct > 5:
            insights.append({
                "type": "trend_positive",
                "message": f"방문객이 증가 추세 (+{change_pct:.1f}%)",
                "priority": "low",
                "confidence": 0.8,
                "impact": "positive"
            })
        elif change_pct < -15:
            insights.append({
                "type": "trend_strong_negative",
                "message": f"방문객이 급감하고 있습니다 ({change_pct:.1f}%)",
                "priority": "critical",
                "confidence": 0.9,
                "impact": "negative"
            })
        elif change_pct < -5:
            insights.append({
                "type": "trend_negative",
                "message": f"방문객이 감소 추세입니다 ({change_pct:.1f}%)",
                "priority": "high", 
                "confidence": 0.8,
                "impact": "negative"
            })
        else:
            insights.append({
                "type": "trend_stable",
                "message": f"방문객이 안정적으로 유지되고 있습니다",
                "priority": "low",
                "confidence": 0.75,
                "impact": "neutral"
            })
        
        # 변동성 분석
        if len(daily_visitors) > 1:
            std_dev = statistics.stdev(daily_visitors)
            mean_visitors = statistics.mean(daily_visitors)
            cv = std_dev / mean_visitors if mean_visitors > 0 else 0
            
            if cv > 0.3:  # 변동 계수 30% 이상
                insights.append({
                    "type": "high_volatility",
                    "message": "방문객 변동이 큰 편입니다. 일관된 마케팅이 필요합니다",
                    "priority": "medium",
                    "confidence": 0.8,
                    "impact": "actionable"
                })
        
        return insights
    
    def _analyze_visitor_comparison(self, comparison_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """방문객 비교 분석"""
        insights = []
        
        change_pct = comparison_data.get("change_percentage", 0)
        significance = comparison_data.get("significance", "minor")
        
        if significance == "significant":
            if change_pct > 0:
                insights.append({
                    "type": "significant_improvement",
                    "message": f"방문객이 이전 기간 대비 {change_pct:.1f}% 크게 증가했습니다",
                    "priority": "medium",
                    "confidence": 0.9,
                    "impact": "positive"
                })
                
                # 성공 요인 분석 제안
                insights.append({
                    "type": "success_factor_analysis",
                    "message": "이 증가 요인을 분석하여 지속 가능한 전략을 수립하세요",
                    "priority": "medium",
                    "confidence": 0.75,
                    "impact": "actionable"
                })
            else:
                insights.append({
                    "type": "significant_decline",
                    "message": f"방문객이 이전 기간 대비 {abs(change_pct):.1f}% 크게 감소했습니다",
                    "priority": "critical",
                    "confidence": 0.95,
                    "impact": "negative"
                })
                
                # 원인 분석 필요성 제시
                insights.append({
                    "type": "root_cause_needed",
                    "message": "감소 원인 분석과 즉시 대응 조치가 필요합니다",
                    "priority": "critical",
                    "confidence": 0.9,
                    "impact": "actionable"
                })
        
        return insights
    
    def _diagnose_current_status(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """현재 상태 종합 진단"""
        status_indicators = []
        
        # 전환율 상태
        conv_rate = data.get("conversion_rate", 0)
        if conv_rate:
            conv_status = self._get_performance_level(conv_rate, self.benchmarks["conversion_rate"])
            status_indicators.append(f"전환율 {conv_status}")
        
        # 픽업률 상태
        pickup_rate = data.get("pickup_rate", 0)
        if pickup_rate:
            pickup_status = self._get_performance_level(pickup_rate, self.benchmarks["pickup_rate"])
            status_indicators.append(f"픽업률 {pickup_status}")
        
        # 방문객 트렌드 상태
        if "daily_visitors" in data:
            visitors = data["daily_visitors"]
            if len(visitors) >= 3:
                recent_avg = sum(visitors[-3:]) / 3
                earlier_avg = sum(visitors[:-3]) / len(visitors[:-3]) if len(visitors) > 3 else visitors[0]
                
                if recent_avg > earlier_avg * 1.1:
                    trend_status = "상승세"
                elif recent_avg < earlier_avg * 0.9:
                    trend_status = "하락세"
                else:
                    trend_status = "안정"
                
                status_indicators.append(f"방문객 {trend_status}")
        
        # 종합 평가
        if not status_indicators:
            summary = "데이터가 부족하여 상태를 정확히 진단할 수 없습니다"
            priority = "medium"
            confidence = 0.3
        else:
            summary = f"현재 매장 상태: {', '.join(status_indicators)}"
            
            # 우려 사항이 있는지 확인
            if any(word in summary for word in ["하락", "부진", "낮음"]):
                priority = "high"
                confidence = 0.85
            else:
                priority = "medium"
                confidence = 0.8
        
        return {
            "summary": summary,
            "indicators": status_indicators,
            "priority": priority,
            "confidence": confidence
        }
    
    # ========================================================================
    # 헬퍼 메서드들
    # ========================================================================
    
    def _get_performance_level(self, value: float, benchmarks: Dict[str, float]) -> str:
        """성과 레벨 분류"""
        if value >= benchmarks["excellent"]:
            return "우수"
        elif value >= benchmarks["good"]:
            return "양호"
        elif value >= benchmarks["average"]:
            return "보통"
        else:
            return "부진"
    
    def _calculate_insight_confidence(self, insights: List[Dict[str, Any]]) -> float:
        """인사이트 신뢰도 계산"""
        if not insights:
            return 0.5
        
        confidences = [insight.get("confidence", 0.5) for insight in insights]
        
        # 가중 평균 (더 많은 인사이트가 있을수록 신뢰도 증가)
        base_confidence = sum(confidences) / len(confidences)
        weight_factor = min(len(insights) / 5, 1.0)  # 최대 5개까지 가중치 적용
        
        return base_confidence * (0.7 + 0.3 * weight_factor)
    
    def get_capabilities(self) -> List[AgentCapability]:
        """에이전트 능력 목록 반환"""
        return self.capabilities

# 간소화된 나머지 메서드들 (실제로는 더 복잡한 로직 구현)
    def _identify_issues(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """문제점 식별"""
        issues = []
        
        # 전환율 문제
        conv_rate = data.get("conversion_rate", 0)
        if conv_rate and conv_rate < self.benchmarks["conversion_rate"]["poor"]:
            issues.append({
                "description": f"전환율 {conv_rate:.1%}가 업계 평균보다 현저히 낮습니다",
                "severity": "critical",
                "priority": "high",
                "confidence": 0.9
            })
        
        return issues
    
    def _identify_strengths(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """강점 식별"""
        strengths = []
        
        # 우수한 전환율
        conv_rate = data.get("conversion_rate", 0)
        if conv_rate and conv_rate >= self.benchmarks["conversion_rate"]["excellent"]:
            strengths.append({
                "description": f"전환율 {conv_rate:.1%}가 업계 최상위 수준입니다",
                "confidence": 0.9
            })
        
        return strengths
    
    def _recognize_patterns(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """패턴 인식 (간소화)"""
        return [
            {
                "description": "주중 방문객이 주말보다 일관되게 높은 패턴",
                "significance": "medium",
                "priority": "medium",
                "confidence": 0.8
            }
        ]
    
    def _analyze_correlations(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """상관관계 분석 (간소화)"""
        return []
    
    def _assess_overall_performance(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """전체 성과 평가 (간소화)"""
        return {"grade": "B+", "summary": "전반적으로 양호한 성과"}
    
    def _categorize_all_metrics(self, data: Dict[str, Any]) -> Dict[str, str]:
        """모든 메트릭 분류 (간소화)"""
        return {"conversion": "good", "pickup": "average"}
    
    def _create_comparison_summary(self, data: Dict[str, Any]) -> str:
        """비교 요약 (간소화)"""
        return "이전 기간 대비 전반적 개선"
    
    def _identify_significant_changes(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """유의미한 변화 식별 (간소화)"""
        return []
    
    def _compare_against_benchmarks(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """벤치마크 비교 (간소화)"""
        return []
    
    def _summarize_patterns(self, patterns: List[Dict[str, Any]]) -> str:
        """패턴 요약 (간소화)"""
        return f"{len(patterns)}개 패턴 식별됨"
    
    def _analyze_conversion_comparison(self, comp_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """전환율 비교 분석 (간소화)"""
        return []
    
    async def _handle_pattern_analysis(self, data: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """패턴 분석 처리 (간소화)"""
        return {"analysis_type": "pattern_analysis", "insights": []}
    
    async def _handle_optimization_analysis(self, data: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """최적화 분석 처리 (간소화)"""
        return {"analysis_type": "optimization_analysis", "insights": []}

if __name__ == "__main__":
    # 테스트 실행
    async def test_insight_generator():
        generator = InsightGeneratorAgent()
        
        test_data = {
            "conversion_rate": 0.32,
            "pickup_rate": 0.09,
            "daily_visitors": [1200, 1150, 1300, 1250, 1400, 1100, 980]
        }
        
        test_message = AgentMessage(
            id="test_insight_001",
            sender="orchestrator",
            receiver=generator.agent_id,
            message_type=MessageType.REQUEST,
            content={
                "task_type": "performance_analysis",
                "data": test_data,
                "params": {"analysis_depth": "comprehensive"}
            }
        )
        
        response = await generator.process_message(test_message)
        print(f"Response Status: {response.content['status']}")
        
        if response.content['status'] == 'success':
            result = response.content['result']
            print(f"인사이트 생성 완료:")
            print(f"- 분석 유형: {result['analysis_type']}")
            print(f"- 인사이트 수: {len(result['insights'])}")
            print(f"- 신뢰도: {result['confidence']:.1%}")
            
            for i, insight in enumerate(result['insights'][:3], 1):
                print(f"  {i}. {insight['message']} (우선순위: {insight['priority']})")
    
    import asyncio
    # asyncio.run(test_insight_generator())