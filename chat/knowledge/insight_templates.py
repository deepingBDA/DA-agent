"""
Insight Generation Templates
============================

GPT-5 에이전트가 데이터 분석 결과를 의미 있는 비즈니스 인사이트로 
변환하는데 사용하는 템플릿과 패턴을 정의합니다.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
import re

class InsightType(Enum):
    """인사이트 유형 분류"""
    TREND = "trend"
    ANOMALY = "anomaly" 
    CORRELATION = "correlation"
    COMPARISON = "comparison"
    PREDICTION = "prediction"
    RECOMMENDATION = "recommendation"

class Priority(Enum):
    """우선순위 레벨"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

@dataclass
class InsightTemplate:
    """인사이트 템플릿 구조"""
    insight_type: InsightType
    pattern: str
    template: str
    priority: Priority
    action_required: bool
    confidence_threshold: float

# =============================================================================
# 핵심 인사이트 템플릿
# =============================================================================

INSIGHT_TEMPLATES = {
    # 트렌드 분석 템플릿
    "trend_analysis": {
        "increasing_trend": InsightTemplate(
            insight_type=InsightType.TREND,
            pattern=r"(\d+\.?\d*)%\s*증가",
            template="📈 **{metric}**이 {period}동안 **{value}% 증가**했습니다.\n"
                     "• 현재 수치: {current_value}\n"
                     "• 이전 대비: +{change}% ({trend_strength})\n"
                     "• 예상 원인: {likely_causes}\n"
                     "• 비즈니스 임팩트: {business_impact}",
            priority=Priority.HIGH,
            action_required=True,
            confidence_threshold=0.8
        ),
        "decreasing_trend": InsightTemplate(
            insight_type=InsightType.TREND,
            pattern=r"(\d+\.?\d*)%\s*감소",
            template="📉 **{metric}**이 {period}동안 **{value}% 감소**했습니다.\n"
                     "• 현재 수치: {current_value}\n" 
                     "• 이전 대비: -{change}% ({severity})\n"
                     "• 주요 원인: {root_causes}\n"
                     "• 즉시 조치 필요: {urgent_actions}",
            priority=Priority.CRITICAL,
            action_required=True,
            confidence_threshold=0.85
        ),
        "stable_pattern": InsightTemplate(
            insight_type=InsightType.TREND,
            pattern=r"안정적|일정한|변화\s*없음",
            template="📊 **{metric}**이 {period}동안 **안정적**으로 유지되고 있습니다.\n"
                     "• 평균 수치: {average_value}\n"
                     "• 변동 폭: ±{variation}%\n"
                     "• 평가: {performance_assessment}\n"
                     "• 개선 기회: {optimization_opportunities}",
            priority=Priority.MEDIUM,
            action_required=False,
            confidence_threshold=0.7
        )
    },

    # 이상치 감지 템플릿
    "anomaly_detection": {
        "positive_anomaly": InsightTemplate(
            insight_type=InsightType.ANOMALY,
            pattern=r"이상\s*급증|비정상\s*높음",
            template="🚀 **{date}**에 **{metric}**에서 **비정상적 급증**이 감지되었습니다.\n"
                     "• 급증 수치: {peak_value} (평균 대비 +{deviation}%)\n"
                     "• 지속 시간: {duration}\n"
                     "• 가능한 원인: {potential_triggers}\n"
                     "• 활용 방안: {opportunity_analysis}",
            priority=Priority.HIGH,
            action_required=True,
            confidence_threshold=0.9
        ),
        "negative_anomaly": InsightTemplate(
            insight_type=InsightType.ANOMALY,
            pattern=r"이상\s*급감|비정상\s*낮음",
            template="⚠️ **{date}**에 **{metric}**에서 **비정상적 급감**이 발생했습니다.\n"
                     "• 급감 수치: {low_value} (평균 대비 -{deviation}%)\n"
                     "• 영향 범위: {impact_scope}\n"
                     "• 긴급 조치: {immediate_actions}\n"
                     "• 복구 계획: {recovery_plan}",
            priority=Priority.CRITICAL,
            action_required=True,
            confidence_threshold=0.95
        )
    },

    # 상관관계 분석 템플릿
    "correlation_analysis": {
        "strong_positive": InsightTemplate(
            insight_type=InsightType.CORRELATION,
            pattern=r"강한\s*양의\s*상관관계",
            template="🔗 **{metric1}**과 **{metric2}** 간 **강한 양의 상관관계**가 발견되었습니다.\n"
                     "• 상관계수: {correlation_coefficient}\n"
                     "• 통계적 유의성: {p_value}\n"
                     "• 실무 의미: {business_interpretation}\n"
                     "• 활용 전략: {strategic_implications}",
            priority=Priority.HIGH,
            action_required=True,
            confidence_threshold=0.85
        ),
        "strong_negative": InsightTemplate(
            insight_type=InsightType.CORRELATION,
            pattern=r"강한\s*음의\s*상관관계",
            template="⚖️ **{metric1}**과 **{metric2}** 간 **강한 음의 상관관계**가 확인되었습니다.\n"
                     "• 상관계수: {correlation_coefficient}\n"
                     "• 트레이드오프 관계: {tradeoff_analysis}\n"
                     "• 균형점 찾기: {optimization_strategy}\n"
                     "• 권장 조치: {balancing_actions}",
            priority=Priority.HIGH, 
            action_required=True,
            confidence_threshold=0.85
        )
    },

    # 비교 분석 템플릿
    "comparative_analysis": {
        "outperforming": InsightTemplate(
            insight_type=InsightType.COMPARISON,
            pattern=r"상위\s*성과|우수한\s*성과",
            template="🏆 **{entity}**가 **{benchmark}** 대비 **우수한 성과**를 보이고 있습니다.\n"
                     "• 성과 수치: {performance_value}\n"
                     "• 벤치마크 대비: +{outperformance}%\n"
                     "• 성공 요인: {success_factors}\n"
                     "• 확산 방안: {scaling_opportunities}",
            priority=Priority.MEDIUM,
            action_required=False,
            confidence_threshold=0.8
        ),
        "underperforming": InsightTemplate(
            insight_type=InsightType.COMPARISON,
            pattern=r"하위\s*성과|부진한\s*성과",
            template="📍 **{entity}**가 **{benchmark}** 대비 **개선이 필요**합니다.\n"
                     "• 현재 수치: {current_performance}\n"
                     "• 목표 대비: -{gap}%\n"
                     "• 주요 격차: {performance_gaps}\n"
                     "• 개선 방향: {improvement_roadmap}",
            priority=Priority.HIGH,
            action_required=True,
            confidence_threshold=0.8
        )
    },

    # 예측 및 전망 템플릿
    "prediction": {
        "growth_forecast": InsightTemplate(
            insight_type=InsightType.PREDICTION,
            pattern=r"증가\s*전망|성장\s*예상",
            template="🔮 **{metric}**의 **{forecast_period}** 전망이 **긍정적**입니다.\n"
                     "• 예측 수치: {predicted_value}\n"
                     "• 성장률: +{growth_rate}%\n"
                     "• 신뢰도: {confidence_level}%\n"
                     "• 성장 동력: {growth_drivers}\n"
                     "• 준비 사항: {preparation_needs}",
            priority=Priority.MEDIUM,
            action_required=False,
            confidence_threshold=0.75
        ),
        "decline_forecast": InsightTemplate(
            insight_type=InsightType.PREDICTION,
            pattern=r"감소\s*전망|하락\s*예상",
            template="⚠️ **{metric}**의 **{forecast_period}** 전망에 **주의**가 필요합니다.\n"
                     "• 예측 수치: {predicted_value}\n"
                     "• 감소율: -{decline_rate}%\n"
                     "• 위험도: {risk_level}\n"
                     "• 예방 조치: {preventive_measures}\n"
                     "• 대응 계획: {contingency_plan}",
            priority=Priority.HIGH,
            action_required=True,
            confidence_threshold=0.8
        )
    }
}

# =============================================================================
# 액션 추천 템플릿
# =============================================================================

RECOMMENDATION_TEMPLATES = {
    "immediate_actions": {
        "high_priority": """
🚨 **즉시 조치 필요** (24시간 내)
{immediate_action_list}

💡 **예상 효과**: {expected_impact}
⏱️ **소요 시간**: {implementation_time}
💰 **예상 비용**: {estimated_cost}
""",
        "medium_priority": """
📋 **단기 개선 과제** (1주일 내)
{short_term_actions}

📈 **성과 지표**: {success_metrics}
🎯 **목표 수치**: {target_values}
""",
        "strategic": """
🎯 **전략적 개선 방향** (1개월 내)
{strategic_initiatives}

🏗️ **구현 단계**: {implementation_phases}
📊 **진행률 추적**: {progress_tracking}
"""
    },

    "performance_improvement": {
        "conversion_optimization": """
💡 **전환율 개선 방안**

**현재 상황**: {current_conversion_rate}%
**목표 수치**: {target_conversion_rate}%
**개선 잠재력**: {improvement_potential}%

**구체적 액션**:
{conversion_actions}

**우선순위별 실행계획**:
1. 🔴 Critical: {critical_actions}
2. 🟠 High: {high_priority_actions}  
3. 🟡 Medium: {medium_priority_actions}
""",

        "traffic_optimization": """
🚶‍♂️ **고객 유입 최적화**

**트래픽 분석 결과**:
• 피크 시간대: {peak_hours}
• 저조한 시간대: {low_traffic_hours}
• 데드존: {dead_zones}

**최적화 전략**:
{traffic_strategies}

**측정 지표**:
• 일 평균 방문객: {daily_avg_visitors}명
• 목표 증가율: +{target_increase}%
""",

        "layout_optimization": """
🏬 **매장 레이아웃 최적화**

**현재 동선 분석**:
• 주요 이동 경로: {main_paths}
• 병목 구간: {bottlenecks}
• 활용도 낮은 구역: {underutilized_areas}

**레이아웃 개선안**:
{layout_improvements}

**기대 효과**:
• 매장 이용률: +{utilization_increase}%
• 교차 판매: +{cross_selling_increase}%
"""
    }
}

# =============================================================================
# 컨텍스트별 인사이트 생성기
# =============================================================================

class InsightGenerator:
    """인사이트 생성 클래스"""
    
    def __init__(self):
        self.templates = INSIGHT_TEMPLATES
        self.recommendations = RECOMMENDATION_TEMPLATES
    
    def generate_insight(self, 
                        insight_type: str,
                        sub_type: str, 
                        data: Dict[str, Any],
                        confidence: float = 0.8) -> str:
        """
        주어진 데이터와 타입에 맞는 인사이트 생성
        
        Args:
            insight_type: 인사이트 유형 (trend_analysis, anomaly_detection 등)
            sub_type: 세부 유형 (increasing_trend, positive_anomaly 등)
            data: 템플릿에 삽입할 데이터
            confidence: 신뢰도 (0.0 - 1.0)
        
        Returns:
            str: 생성된 인사이트 문자열
        """
        try:
            template_info = self.templates[insight_type][sub_type]
            
            if confidence < template_info.confidence_threshold:
                return f"⚠️ 신뢰도가 낮은 분석 결과입니다. (신뢰도: {confidence:.1%})"
            
            # 템플릿에 데이터 삽입
            insight_text = template_info.template.format(**data)
            
            # 우선순위 표시 추가
            priority_icon = {
                Priority.CRITICAL: "🚨",
                Priority.HIGH: "🔴", 
                Priority.MEDIUM: "🟡",
                Priority.LOW: "🟢"
            }
            
            priority_text = f"{priority_icon[template_info.priority]} 우선순위: {template_info.priority.value.upper()}"
            
            return f"{insight_text}\n\n{priority_text}"
            
        except KeyError as e:
            return f"❌ 인사이트 생성 실패: 알 수 없는 템플릿 유형 ({e})"
        except Exception as e:
            return f"❌ 인사이트 생성 중 오류: {str(e)}"

    def generate_recommendations(self,
                               recommendation_type: str,
                               sub_type: str,
                               data: Dict[str, Any]) -> str:
        """추천 액션 생성"""
        try:
            template = self.recommendations[recommendation_type][sub_type]
            return template.format(**data)
        except KeyError as e:
            return f"❌ 추천사항 생성 실패: {e}"
    
    def analyze_and_recommend(self, analysis_results: Dict[str, Any]) -> str:
        """분석 결과를 종합하여 인사이트와 추천사항 생성"""
        output = []
        
        # Executive Summary
        output.append("# 📊 분석 요약\n")
        
        if "summary" in analysis_results:
            output.append(f"**핵심 발견사항**: {analysis_results['summary']}\n")
        
        # Key Insights
        output.append("## 🔍 주요 인사이트\n")
        
        if "insights" in analysis_results:
            for i, insight in enumerate(analysis_results["insights"], 1):
                output.append(f"**{i}.** {insight}\n")
        
        # Recommendations
        output.append("## 💡 실행 방안\n")
        
        if "recommendations" in analysis_results:
            for rec in analysis_results["recommendations"]:
                output.append(f"• {rec}\n")
        
        # Risk Assessment
        if "risks" in analysis_results:
            output.append("## ⚠️ 위험 요소\n")
            for risk in analysis_results["risks"]:
                output.append(f"• {risk}\n")
        
        # Next Steps
        output.append("## 📋 다음 단계\n")
        if "next_steps" in analysis_results:
            for step in analysis_results["next_steps"]:
                output.append(f"• {step}\n")
        
        return "\n".join(output)

# =============================================================================
# 특화된 인사이트 패턴
# =============================================================================

SPECIALIZED_PATTERNS = {
    "seasonal_patterns": {
        "description": "계절성 패턴 인사이트",
        "triggers": ["월별", "계절별", "seasonal", "monthly"],
        "template": """
🗓️ **계절성 분석 결과**

**패턴 인식**: {seasonal_pattern}
• 성수기: {peak_seasons}
• 비수기: {low_seasons}
• 변동 폭: {seasonal_variance}%

**전략적 시사점**:
{strategic_implications}

**계절별 대응 방안**:
{seasonal_strategies}
"""
    },
    
    "customer_segmentation": {
        "description": "고객 세분화 인사이트",
        "triggers": ["연령", "성별", "demographic", "segment"],
        "template": """
👥 **고객 세분화 분석**

**주요 세그먼트**:
{customer_segments}

**세그먼트별 특성**:
{segment_characteristics}

**타겟팅 전략**:
{targeting_strategy}
"""
    },
    
    "competitive_analysis": {
        "description": "경쟁 분석 인사이트", 
        "triggers": ["경쟁", "비교", "벤치마크", "competitive"],
        "template": """
⚔️ **경쟁력 분석**

**포지셔닝**: {competitive_position}
**강점**: {competitive_advantages}
**개선 영역**: {improvement_areas}

**차별화 전략**: {differentiation_strategy}
"""
    }
}

# =============================================================================
# 유틸리티 함수
# =============================================================================

def detect_insight_patterns(text: str) -> List[str]:
    """텍스트에서 인사이트 패턴 감지"""
    detected = []
    
    for category, templates in INSIGHT_TEMPLATES.items():
        for sub_type, template_info in templates.items():
            if re.search(template_info.pattern, text, re.IGNORECASE):
                detected.append(f"{category}.{sub_type}")
    
    return detected

def get_priority_score(insight_type: str, sub_type: str) -> int:
    """인사이트의 우선순위 점수 반환 (1-4, 높을수록 중요)"""
    try:
        template_info = INSIGHT_TEMPLATES[insight_type][sub_type]
        priority_scores = {
            Priority.LOW: 1,
            Priority.MEDIUM: 2,
            Priority.HIGH: 3,
            Priority.CRITICAL: 4
        }
        return priority_scores[template_info.priority]
    except KeyError:
        return 1

def format_metric_change(current: float, previous: float, metric_name: str) -> str:
    """메트릭 변화를 포맷팅된 문자열로 반환"""
    if previous == 0:
        return f"{metric_name}: {current} (이전 데이터 없음)"
    
    change_pct = ((current - previous) / previous) * 100
    direction = "증가" if change_pct > 0 else "감소"
    
    return f"{metric_name}: {current} (전 기간 대비 {abs(change_pct):.1f}% {direction})"

# 인스턴스 생성
insight_generator = InsightGenerator()

if __name__ == "__main__":
    # 테스트 예제
    test_data = {
        "metric": "방문객 수",
        "period": "이번 주",
        "value": 15.3,
        "current_value": "1,234명",
        "change": 15.3,
        "trend_strength": "강한 상승세",
        "likely_causes": "날씨 개선, 프로모션 효과",
        "business_impact": "매출 증대 기대"
    }
    
    insight = insight_generator.generate_insight(
        "trend_analysis", 
        "increasing_trend", 
        test_data, 
        confidence=0.85
    )
    
    print("생성된 인사이트:")
    print(insight)