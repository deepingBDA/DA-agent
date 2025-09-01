"""
Recommendation Engine Agent
===========================

분석 결과를 기반으로 구체적이고 실행 가능한 액션 아이템을 생성하는 전문 에이전트입니다.
우선순위, ROI 추정, 구현 계획을 포함한 종합적 추천을 제공합니다.
"""

import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import json

from ..base_agent import (
    BaseAgent, AgentType, AgentMessage, MessageType, 
    AgentCapability, AnalyticsBaseAgent
)

class RecommendationAgent(AnalyticsBaseAgent):
    """추천 엔진 전문 에이전트"""
    
    def __init__(self):
        capabilities = [
            AgentCapability(
                name="action_planning",
                description="분석 결과를 기반으로 구체적 액션 플랜 생성",
                input_requirements=["insights", "current_performance"],
                output_format="action_plan",
                confidence_threshold=0.75
            ),
            AgentCapability(
                name="roi_estimation",
                description="추천 액션의 예상 ROI 및 비즈니스 임팩트 계산",
                input_requirements=["action_items", "baseline_metrics"],
                output_format="roi_analysis",
                confidence_threshold=0.7
            ),
            AgentCapability(
                name="priority_optimization",
                description="리소스 제약을 고려한 액션 우선순위 최적화",
                input_requirements=["action_list", "constraints"],
                output_format="prioritized_roadmap",
                confidence_threshold=0.8
            ),
            AgentCapability(
                name="implementation_planning",
                description="단계별 구현 계획 및 성공 지표 정의",
                input_requirements=["prioritized_actions"],
                output_format="implementation_guide",
                confidence_threshold=0.75
            ),
            AgentCapability(
                name="risk_assessment",
                description="추천 액션의 리스크 평가 및 완화 방안",
                input_requirements=["action_plan"],
                output_format="risk_analysis",
                confidence_threshold=0.7
            )
        ]
        
        super().__init__(AgentType.DATA_ANALYST, capabilities=capabilities)  # Note: Using DATA_ANALYST type as RECOMMENDATION not in enum
        
        # 액션 템플릿 라이브러리
        self.action_templates = {
            "conversion_improvement": [
                {
                    "action": "구매 경로 단순화 및 장벽 제거",
                    "expected_impact": "전환율 15-25% 개선",
                    "implementation_time": "2-3주",
                    "effort_level": "medium",
                    "cost_estimate": "low"
                },
                {
                    "action": "개인화된 상품 추천 시스템 도입",
                    "expected_impact": "전환율 10-20% 개선", 
                    "implementation_time": "4-6주",
                    "effort_level": "high",
                    "cost_estimate": "medium"
                }
            ],
            "traffic_optimization": [
                {
                    "action": "피크 시간대 맞춤 프로모션 강화",
                    "expected_impact": "방문객 10-15% 증가",
                    "implementation_time": "1-2주",
                    "effort_level": "low",
                    "cost_estimate": "medium"
                },
                {
                    "action": "소셜미디어 타겟팅 광고 최적화",
                    "expected_impact": "신규 방문객 20-30% 증가",
                    "implementation_time": "2-4주", 
                    "effort_level": "medium",
                    "cost_estimate": "high"
                }
            ],
            "engagement_enhancement": [
                {
                    "action": "인터랙티브 체험 존 구축",
                    "expected_impact": "픽업률 25-40% 개선",
                    "implementation_time": "3-4주",
                    "effort_level": "high",
                    "cost_estimate": "high"
                },
                {
                    "action": "상품 진열 및 시각적 어필 강화",
                    "expected_impact": "픽업률 15-25% 개선", 
                    "implementation_time": "1-2주",
                    "effort_level": "medium",
                    "cost_estimate": "low"
                }
            ]
        }
        
        # ROI 계산 모델
        self.roi_models = {
            "conversion_improvement": {
                "revenue_multiplier": 1.2,  # 전환율 개선 시 매출 증가율
                "customer_lifetime_value": 150,  # 고객 생애 가치
                "implementation_cost_factor": 0.1  # 구현 비용 계수
            },
            "traffic_optimization": {
                "revenue_multiplier": 1.15,
                "acquisition_cost": 25,  # 고객 획득 비용
                "implementation_cost_factor": 0.15
            },
            "operational_efficiency": {
                "cost_reduction_factor": 0.1,  # 비용 절감 효과
                "time_savings_value": 50,  # 시간 절약 가치 (시간당)
                "implementation_cost_factor": 0.05
            }
        }
        
        # 우선순위 가중치
        self.priority_weights = {
            "impact": 0.4,      # 비즈니스 임팩트
            "effort": 0.2,      # 구현 노력
            "cost": 0.2,        # 비용
            "urgency": 0.1,     # 긴급성
            "risk": 0.1         # 리스크
        }
    
    async def process_message(self, message: AgentMessage) -> AgentMessage:
        """메시지 처리 메인 로직"""
        try:
            self.log_message(message, "received")
            
            task_type = message.content.get("task_type", "generate_recommendations")
            params = message.content.get("params", {})
            data = message.content.get("data", {})
            insights = message.content.get("insights", [])
            
            # 작업 타입별 처리
            if task_type == "generate_recommendations":
                result = await self._handle_recommendation_generation(data, insights, params)
            elif task_type == "action_planning":
                result = await self._handle_action_planning(data, insights, params)
            elif task_type == "optimization_recommendations":
                result = await self._handle_optimization_recommendations(data, insights, params)
            elif task_type == "priority_optimization":
                result = await self._handle_priority_optimization(data, params)
            elif task_type == "roi_analysis":
                result = await self._handle_roi_analysis(data, params)
            else:
                # 기본 추천 생성
                result = await self._handle_general_recommendations(data, insights, params)
            
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
            self.logger.error(f"Recommendation generation error: {str(e)}")
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
    
    async def _handle_recommendation_generation(self, data: Dict[str, Any], insights: List[Dict[str, Any]], params: Dict[str, Any]) -> Dict[str, Any]:
        """기본 추천 생성 처리"""
        self.logger.info("추천 생성 시작")
        
        # 인사이트 기반 문제 영역 식별
        problem_areas = self._identify_problem_areas(data, insights)
        
        # 각 문제 영역별 추천 생성
        recommendations = []
        for area in problem_areas:
            area_recommendations = self._generate_area_recommendations(area, data)
            recommendations.extend(area_recommendations)
        
        # ROI 및 우선순위 계산
        for rec in recommendations:
            rec["roi_estimate"] = self._calculate_roi(rec, data)
            rec["priority_score"] = self._calculate_priority_score(rec)
        
        # 우선순위순 정렬
        recommendations.sort(key=lambda x: x["priority_score"], reverse=True)
        
        # 구현 로드맵 생성
        implementation_roadmap = self._create_implementation_roadmap(recommendations)
        
        # 성공 지표 정의
        success_metrics = self._define_success_metrics(recommendations, data)
        
        return {
            "recommendation_type": "comprehensive",
            "problem_areas": problem_areas,
            "recommendations": recommendations[:8],  # 상위 8개만
            "implementation_roadmap": implementation_roadmap,
            "success_metrics": success_metrics,
            "total_estimated_roi": sum(r["roi_estimate"]["roi_percentage"] for r in recommendations[:5]),
            "confidence": self._calculate_recommendation_confidence(recommendations),
            "generation_timestamp": datetime.now().isoformat()
        }
    
    async def _handle_action_planning(self, data: Dict[str, Any], insights: List[Dict[str, Any]], params: Dict[str, Any]) -> Dict[str, Any]:
        """액션 플래닝 처리"""
        self.logger.info("액션 플래닝 시작")
        
        optimization_target = params.get("optimization_target", "performance")
        constraints = params.get("constraints", [])
        
        # 목표 기반 액션 생성
        if optimization_target == "conversion":
            actions = self._generate_conversion_actions(data, insights)
        elif optimization_target == "traffic":
            actions = self._generate_traffic_actions(data, insights)
        elif optimization_target == "engagement":
            actions = self._generate_engagement_actions(data, insights)
        else:
            actions = self._generate_performance_actions(data, insights)
        
        # 제약 조건 적용
        filtered_actions = self._apply_constraints(actions, constraints)
        
        # 액션별 세부 계획 생성
        detailed_plans = []
        for action in filtered_actions:
            plan = self._create_detailed_action_plan(action, data)
            detailed_plans.append(plan)
        
        return {
            "planning_type": "action_planning",
            "optimization_target": optimization_target,
            "constraints_applied": constraints,
            "action_plans": detailed_plans,
            "execution_timeline": self._create_execution_timeline(detailed_plans),
            "resource_requirements": self._calculate_resource_requirements(detailed_plans),
            "confidence": 0.82
        }
    
    async def _handle_optimization_recommendations(self, data: Dict[str, Any], insights: List[Dict[str, Any]], params: Dict[str, Any]) -> Dict[str, Any]:
        """최적화 추천 처리"""
        self.logger.info("최적화 추천 시작")
        
        # 현재 성과 분석
        current_performance = self._analyze_current_performance(data)
        
        # 최적화 기회 식별
        optimization_opportunities = self._identify_optimization_opportunities(data, insights)
        
        # 각 기회별 구체적 추천 생성
        recommendations = []
        for opportunity in optimization_opportunities:
            recs = self._generate_optimization_recommendations(opportunity, current_performance)
            recommendations.extend(recs)
        
        # Quick wins vs Long-term 분류
        quick_wins = [r for r in recommendations if r.get("implementation_time", "").startswith(("1-2일", "즉시", "1주일"))]
        long_term = [r for r in recommendations if r not in quick_wins]
        
        return {
            "optimization_type": "performance_optimization",
            "current_performance": current_performance,
            "optimization_opportunities": optimization_opportunities,
            "quick_wins": quick_wins,
            "long_term_initiatives": long_term,
            "total_potential_improvement": self._calculate_total_potential(recommendations),
            "confidence": 0.78
        }
    
    # ========================================================================
    # 핵심 추천 로직
    # ========================================================================
    
    def _identify_problem_areas(self, data: Dict[str, Any], insights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """문제 영역 식별"""
        problem_areas = []
        
        # 전환율 문제
        conv_rate = data.get("conversion_rate", 0)
        if conv_rate < 0.25:
            problem_areas.append({
                "area": "conversion_improvement",
                "severity": "high" if conv_rate < 0.15 else "medium",
                "current_value": conv_rate,
                "target_value": 0.30,
                "gap": 0.30 - conv_rate
            })
        
        # 픽업률 문제
        pickup_rate = data.get("pickup_rate", 0)
        if pickup_rate < 0.08:
            problem_areas.append({
                "area": "engagement_enhancement",
                "severity": "high" if pickup_rate < 0.05 else "medium",
                "current_value": pickup_rate,
                "target_value": 0.12,
                "gap": 0.12 - pickup_rate
            })
        
        # 방문객 트렌드 문제
        if "daily_visitors" in data:
            visitors = data["daily_visitors"]
            if len(visitors) >= 3:
                recent_avg = sum(visitors[-3:]) / 3
                earlier_avg = sum(visitors[:-3]) / len(visitors[:-3]) if len(visitors) > 3 else visitors[0]
                
                if recent_avg < earlier_avg * 0.9:  # 10% 이상 감소
                    problem_areas.append({
                        "area": "traffic_optimization",
                        "severity": "high" if recent_avg < earlier_avg * 0.8 else "medium",
                        "current_value": recent_avg,
                        "target_value": earlier_avg,
                        "gap": earlier_avg - recent_avg
                    })
        
        # 인사이트 기반 추가 문제 영역
        for insight in insights:
            if insight.get("impact") == "negative" and insight.get("priority") in ["high", "critical"]:
                if "전환" in insight.get("message", ""):
                    # 이미 추가되지 않았다면 추가
                    if not any(area["area"] == "conversion_improvement" for area in problem_areas):
                        problem_areas.append({
                            "area": "conversion_improvement",
                            "severity": "medium",
                            "insight_driven": True,
                            "source_insight": insight["message"]
                        })
        
        return problem_areas
    
    def _generate_area_recommendations(self, problem_area: Dict[str, Any], data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """영역별 구체적 추천 생성"""
        area_type = problem_area["area"]
        severity = problem_area["severity"]
        
        recommendations = []
        
        if area_type == "conversion_improvement":
            base_recs = self.action_templates["conversion_improvement"]
            for template in base_recs:
                rec = template.copy()
                rec["problem_area"] = area_type
                rec["severity"] = severity
                rec["priority"] = "HIGH" if severity == "high" else "MEDIUM"
                
                # 심각도에 따른 영향 조정
                if severity == "high":
                    # 더 적극적인 개선 기대
                    rec["expected_impact"] = rec["expected_impact"].replace("15-25%", "20-30%").replace("10-20%", "15-25%")
                
                recommendations.append(rec)
        
        elif area_type == "engagement_enhancement":
            base_recs = self.action_templates["engagement_enhancement"]
            for template in base_recs:
                rec = template.copy()
                rec["problem_area"] = area_type
                rec["severity"] = severity
                rec["priority"] = "HIGH" if severity == "high" else "MEDIUM"
                recommendations.append(rec)
        
        elif area_type == "traffic_optimization":
            base_recs = self.action_templates["traffic_optimization"]
            for template in base_recs:
                rec = template.copy()
                rec["problem_area"] = area_type
                rec["severity"] = severity
                rec["priority"] = "CRITICAL" if severity == "high" else "HIGH"
                recommendations.append(rec)
        
        # 맞춤형 추천 추가
        custom_recs = self._generate_custom_recommendations(problem_area, data)
        recommendations.extend(custom_recs)
        
        return recommendations
    
    def _generate_custom_recommendations(self, problem_area: Dict[str, Any], data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """문제 영역에 특화된 맞춤형 추천"""
        custom_recs = []
        
        area_type = problem_area["area"]
        
        if area_type == "conversion_improvement":
            # 전환율 데이터 기반 맞춤 추천
            conv_rate = problem_area.get("current_value", 0)
            if conv_rate < 0.15:  # 매우 낮은 경우
                custom_recs.append({
                    "action": "고객 구매 여정 전면 재설계",
                    "expected_impact": "전환율 50-100% 개선",
                    "implementation_time": "6-8주",
                    "effort_level": "high",
                    "cost_estimate": "high",
                    "priority": "CRITICAL",
                    "problem_area": area_type,
                    "custom": True,
                    "rationale": f"현재 전환율 {conv_rate:.1%}는 심각한 수준으로 근본적 개선이 필요"
                })
        
        elif area_type == "traffic_optimization":
            # 방문객 감소폭에 따른 맞춤 추천
            gap = problem_area.get("gap", 0)
            if gap > 200:  # 일일 200명 이상 감소
                custom_recs.append({
                    "action": "긴급 마케팅 캠페인 및 고객 재유치 프로그램",
                    "expected_impact": f"방문객 {gap:.0f}명 회복",
                    "implementation_time": "3-5일",
                    "effort_level": "high",
                    "cost_estimate": "high",
                    "priority": "CRITICAL",
                    "problem_area": area_type,
                    "custom": True,
                    "rationale": f"일일 {gap:.0f}명 감소는 즉각적 대응이 필요한 수준"
                })
        
        return custom_recs
    
    def _calculate_roi(self, recommendation: Dict[str, Any], baseline_data: Dict[str, Any]) -> Dict[str, Any]:
        """ROI 계산"""
        problem_area = recommendation.get("problem_area", "general")
        
        # 기본 메트릭
        current_visitors = baseline_data.get("total_visitors", 1000)
        current_conv_rate = baseline_data.get("conversion_rate", 0.3)
        avg_transaction_value = baseline_data.get("avg_transaction_value", 50)
        
        # 현재 월 매출 추정
        monthly_revenue = current_visitors * 4 * current_conv_rate * avg_transaction_value
        
        # 개선 효과 파싱
        impact_str = recommendation.get("expected_impact", "10%")
        
        try:
            # "15-25%" 형태에서 중간값 추출
            if "%" in impact_str and "-" in impact_str:
                range_part = impact_str.split("%")[0]
                if "-" in range_part:
                    min_val, max_val = map(float, range_part.split("-"))
                    improvement_rate = (min_val + max_val) / 200  # 평균값을 소수로
                else:
                    improvement_rate = float(range_part) / 100
            else:
                improvement_rate = 0.15  # 기본값 15%
        except:
            improvement_rate = 0.15
        
        # 예상 매출 증가
        revenue_increase = monthly_revenue * improvement_rate
        
        # 구현 비용 추정
        cost_level = recommendation.get("cost_estimate", "medium")
        effort_level = recommendation.get("effort_level", "medium")
        
        cost_multipliers = {"low": 0.05, "medium": 0.15, "high": 0.3}
        effort_multipliers = {"low": 1.0, "medium": 1.5, "high": 2.0}
        
        implementation_cost = monthly_revenue * cost_multipliers.get(cost_level, 0.15) * effort_multipliers.get(effort_level, 1.0)
        
        # ROI 계산
        monthly_profit = revenue_increase - (implementation_cost / 6)  # 6개월 감가상각
        roi_percentage = (monthly_profit / max(implementation_cost, 1)) * 100
        payback_months = implementation_cost / max(monthly_profit, 1)
        
        return {
            "monthly_revenue_increase": round(revenue_increase, 0),
            "implementation_cost": round(implementation_cost, 0),
            "monthly_profit": round(monthly_profit, 0),
            "roi_percentage": round(roi_percentage, 1),
            "payback_months": round(payback_months, 1),
            "annual_value": round(monthly_profit * 12, 0)
        }
    
    def _calculate_priority_score(self, recommendation: Dict[str, Any]) -> float:
        """우선순위 점수 계산"""
        # 기본 우선순위 매핑
        priority_map = {"CRITICAL": 1.0, "HIGH": 0.8, "MEDIUM": 0.6, "LOW": 0.4}
        
        # 각 요소별 점수
        impact_score = priority_map.get(recommendation.get("priority", "MEDIUM"), 0.6)
        
        # 노력 수준 (낮을수록 높은 점수)
        effort_map = {"low": 1.0, "medium": 0.7, "high": 0.4}
        effort_score = effort_map.get(recommendation.get("effort_level", "medium"), 0.7)
        
        # 비용 수준 (낮을수록 높은 점수)
        cost_map = {"low": 1.0, "medium": 0.7, "high": 0.4}
        cost_score = cost_map.get(recommendation.get("cost_estimate", "medium"), 0.7)
        
        # ROI 점수
        roi_percentage = recommendation.get("roi_estimate", {}).get("roi_percentage", 0)
        roi_score = min(roi_percentage / 100, 1.0) if roi_percentage > 0 else 0.5
        
        # 긴급성 점수
        urgency_score = 1.0 if recommendation.get("severity") == "high" else 0.7
        
        # 가중 평균
        weights = self.priority_weights
        final_score = (
            impact_score * weights["impact"] +
            effort_score * weights["effort"] +
            cost_score * weights["cost"] +
            roi_score * weights.get("roi", 0.1) +
            urgency_score * weights["urgency"]
        )
        
        return round(final_score, 3)
    
    def _create_implementation_roadmap(self, recommendations: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """구현 로드맵 생성"""
        roadmap = {
            "immediate": [],     # 1주일 내
            "short_term": [],    # 1개월 내
            "medium_term": [],   # 3개월 내
            "long_term": []      # 3개월 이상
        }
        
        for rec in recommendations:
            impl_time = rec.get("implementation_time", "")
            
            roadmap_item = {
                "action": rec["action"],
                "priority": rec.get("priority", "MEDIUM"),
                "expected_impact": rec.get("expected_impact", ""),
                "roi": rec.get("roi_estimate", {}).get("roi_percentage", 0)
            }
            
            if any(keyword in impl_time for keyword in ["즉시", "3-5일", "1주일"]):
                roadmap["immediate"].append(roadmap_item)
            elif any(keyword in impl_time for keyword in ["1-2주", "2-3주", "1개월"]):
                roadmap["short_term"].append(roadmap_item)
            elif any(keyword in impl_time for keyword in ["4-6주", "2-3개월"]):
                roadmap["medium_term"].append(roadmap_item)
            else:
                roadmap["long_term"].append(roadmap_item)
        
        return roadmap
    
    def _define_success_metrics(self, recommendations: List[Dict[str, Any]], baseline_data: Dict[str, Any]) -> Dict[str, Any]:
        """성공 지표 정의"""
        current_conv = baseline_data.get("conversion_rate", 0.3)
        current_pickup = baseline_data.get("pickup_rate", 0.1)
        current_visitors = baseline_data.get("avg_daily_visitors", 1000)
        
        # 추천사항들의 예상 개선 효과 합계
        total_conv_improvement = 0
        total_pickup_improvement = 0
        total_traffic_improvement = 0
        
        for rec in recommendations:
            impact = rec.get("expected_impact", "")
            if "전환율" in rec.get("action", "") or rec.get("problem_area") == "conversion_improvement":
                if "15-25%" in impact:
                    total_conv_improvement += 0.2  # 평균값
                elif "10-20%" in impact:
                    total_conv_improvement += 0.15
            
            if "픽업률" in rec.get("action", "") or rec.get("problem_area") == "engagement_enhancement":
                if "25-40%" in impact:
                    total_pickup_improvement += 0.325
                elif "15-25%" in impact:
                    total_pickup_improvement += 0.2
            
            if "방문객" in impact or rec.get("problem_area") == "traffic_optimization":
                if "20-30%" in impact:
                    total_traffic_improvement += 0.25
                elif "10-15%" in impact:
                    total_traffic_improvement += 0.125
        
        return {
            "baseline_metrics": {
                "conversion_rate": current_conv,
                "pickup_rate": current_pickup,
                "daily_visitors": current_visitors
            },
            "target_metrics": {
                "conversion_rate": min(current_conv * (1 + total_conv_improvement), 0.6),
                "pickup_rate": min(current_pickup * (1 + total_pickup_improvement), 0.3),
                "daily_visitors": current_visitors * (1 + total_traffic_improvement)
            },
            "improvement_targets": {
                "conversion_improvement": f"{total_conv_improvement*100:.1f}%",
                "pickup_improvement": f"{total_pickup_improvement*100:.1f}%",
                "traffic_improvement": f"{total_traffic_improvement*100:.1f}%"
            },
            "measurement_frequency": "weekly",
            "review_milestone": "4주"
        }
    
    def _calculate_recommendation_confidence(self, recommendations: List[Dict[str, Any]]) -> float:
        """추천 신뢰도 계산"""
        if not recommendations:
            return 0.5
        
        # ROI 기반 신뢰도
        roi_scores = [r.get("roi_estimate", {}).get("roi_percentage", 0) for r in recommendations]
        avg_roi = sum(roi_scores) / len(roi_scores) if roi_scores else 0
        roi_confidence = min(avg_roi / 50, 1.0)  # 50% ROI를 최고점으로
        
        # 우선순위 기반 신뢰도
        high_priority_count = sum(1 for r in recommendations if r.get("priority") in ["CRITICAL", "HIGH"])
        priority_confidence = min(high_priority_count / max(len(recommendations), 1), 1.0)
        
        # 맞춤화 수준 기반 신뢰도
        custom_count = sum(1 for r in recommendations if r.get("custom", False))
        customization_confidence = 0.8 + (custom_count / max(len(recommendations), 1)) * 0.2
        
        # 종합 신뢰도
        final_confidence = (roi_confidence * 0.4 + priority_confidence * 0.3 + customization_confidence * 0.3)
        
        return round(final_confidence, 3)
    
    def get_capabilities(self) -> List[AgentCapability]:
        """에이전트 능력 목록 반환"""
        return self.capabilities

    # 간소화된 헬퍼 메서드들
    def _generate_conversion_actions(self, data: Dict[str, Any], insights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """전환 최적화 액션 생성 (간소화)"""
        return self.action_templates["conversion_improvement"][:2]
    
    def _generate_traffic_actions(self, data: Dict[str, Any], insights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """트래픽 최적화 액션 생성 (간소화)"""
        return self.action_templates["traffic_optimization"][:2]
    
    def _generate_engagement_actions(self, data: Dict[str, Any], insights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """참여도 향상 액션 생성 (간소화)"""
        return self.action_templates["engagement_enhancement"][:2]
    
    def _generate_performance_actions(self, data: Dict[str, Any], insights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """전체 성과 향상 액션 생성 (간소화)"""
        all_actions = []
        for templates in self.action_templates.values():
            all_actions.extend(templates[:1])  # 각 카테고리에서 1개씩
        return all_actions
    
    def _apply_constraints(self, actions: List[Dict[str, Any]], constraints: List[str]) -> List[Dict[str, Any]]:
        """제약 조건 적용 (간소화)"""
        if "low_budget" in constraints:
            return [a for a in actions if a.get("cost_estimate") != "high"]
        return actions
    
    def _create_detailed_action_plan(self, action: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
        """세부 액션 플랜 생성 (간소화)"""
        return {
            **action,
            "detailed_steps": ["1단계: 현황 분석", "2단계: 계획 수립", "3단계: 실행", "4단계: 모니터링"],
            "success_criteria": "목표 달성도 80% 이상",
            "risk_factors": ["리소스 부족", "시장 변화"]
        }
    
    def _create_execution_timeline(self, plans: List[Dict[str, Any]]) -> Dict[str, str]:
        """실행 타임라인 생성 (간소화)"""
        return {"phase1": "즉시 실행", "phase2": "2주 후", "phase3": "1개월 후"}
    
    def _calculate_resource_requirements(self, plans: List[Dict[str, Any]]) -> Dict[str, Any]:
        """리소스 요구사항 계산 (간소화)"""
        return {"budget": "중간", "personnel": "2-3명", "time": "4-6주"}
    
    def _analyze_current_performance(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """현재 성과 분석 (간소화)"""
        return {"grade": "B", "strengths": ["높은 방문율"], "weaknesses": ["낮은 전환율"]}
    
    def _identify_optimization_opportunities(self, data: Dict[str, Any], insights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """최적화 기회 식별 (간소화)"""
        return [{"area": "conversion", "potential": "high", "effort": "medium"}]
    
    def _generate_optimization_recommendations(self, opportunity: Dict[str, Any], performance: Dict[str, Any]) -> List[Dict[str, Any]]:
        """최적화 추천 생성 (간소화)"""
        return [{"action": "A/B 테스트 실시", "expected_impact": "10-15% 개선"}]
    
    def _calculate_total_potential(self, recommendations: List[Dict[str, Any]]) -> str:
        """총 잠재 효과 계산 (간소화)"""
        return "월 매출 20-30% 증가 예상"
    
    async def _handle_general_recommendations(self, data: Dict[str, Any], insights: List[Dict[str, Any]], params: Dict[str, Any]) -> Dict[str, Any]:
        """일반 추천 처리 (간소화)"""
        return await self._handle_recommendation_generation(data, insights, params)
    
    async def _handle_priority_optimization(self, data: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """우선순위 최적화 처리 (간소화)"""
        return {"optimization_type": "priority", "recommendations": []}
    
    async def _handle_roi_analysis(self, data: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """ROI 분석 처리 (간소화)"""
        return {"analysis_type": "roi", "total_roi": "150%"}

if __name__ == "__main__":
    # 테스트 실행
    async def test_recommendation_engine():
        engine = RecommendationAgent()
        
        test_data = {
            "conversion_rate": 0.18,
            "pickup_rate": 0.06,
            "daily_visitors": [1200, 1150, 1000, 950, 900, 850, 800],
            "total_visitors": 6850,
            "avg_transaction_value": 45
        }
        
        test_insights = [
            {"message": "전환율이 업계 평균보다 낮습니다", "priority": "high", "impact": "negative"},
            {"message": "픽업률이 크게 부족합니다", "priority": "high", "impact": "negative"}
        ]
        
        test_message = AgentMessage(
            id="test_rec_001", 
            sender="orchestrator",
            receiver=engine.agent_id,
            message_type=MessageType.REQUEST,
            content={
                "task_type": "generate_recommendations",
                "data": test_data,
                "insights": test_insights,
                "params": {"focus": "comprehensive"}
            }
        )
        
        response = await engine.process_message(test_message)
        print(f"Response Status: {response.content['status']}")
        
        if response.content['status'] == 'success':
            result = response.content['result']
            print(f"추천 생성 완료:")
            print(f"- 추천 수: {len(result['recommendations'])}")
            print(f"- 총 예상 ROI: {result['total_estimated_roi']:.1f}%")
            print(f"- 신뢰도: {result['confidence']:.1%}")
            
            for i, rec in enumerate(result['recommendations'][:3], 1):
                print(f"  {i}. {rec['action']}")
                print(f"     예상 효과: {rec['expected_impact']}")
                print(f"     우선순위: {rec['priority']}")
                print(f"     ROI: {rec['roi_estimate']['roi_percentage']:.1f}%")
                print()
    
    import asyncio
    # asyncio.run(test_recommendation_engine())