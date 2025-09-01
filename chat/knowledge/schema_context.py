"""
Schema Context & Business Intelligence
=====================================

오프라인 매장 데이터 스키마와 비즈니스 컨텍스트를 정의합니다.
GPT-5 에이전트가 데이터를 이해하고 의미 있는 인사이트를 생성하는데 사용됩니다.
"""

from typing import Dict, Any, List
from dataclasses import dataclass

# =============================================================================
# 데이터베이스 스키마 정의
# =============================================================================

SCHEMA_CONTEXT = {
    "databases": {
        "plusinsight": {
            "description": "메인 고객 행동 분석 데이터베이스",
            "tables": {
                "line_in_out_individual": {
                    "description": "방문객 입출입 개별 기록",
                    "key_fields": {
                        "person_seq": "개별 방문객 고유 ID",
                        "date": "방문 날짜",
                        "timestamp": "정확한 입출입 시각", 
                        "in_out": "입장(IN) 또는 퇴장(OUT)",
                        "is_staff": "직원 여부 (0: 고객, 1: 직원)"
                    },
                    "business_meaning": "매장 방문 패턴과 체류 시간 분석의 기초 데이터"
                },
                "customer_behavior_event": {
                    "description": "고객의 매장 내 행동 이벤트",
                    "key_fields": {
                        "person_seq": "방문객 ID",
                        "event_type": "행동 유형 (1: 픽업, 2: 시선, 3: 체류)",
                        "timestamp": "행동 발생 시각",
                        "customer_behavior_area_id": "행동 발생 구역 ID"
                    },
                    "business_meaning": "고객의 실제 관심도와 구매 의도를 파악하는 핵심 데이터"
                },
                "zone": {
                    "description": "매장 내 구역 정보",
                    "key_fields": {
                        "id": "구역 고유 ID",
                        "name": "구역명 (예: '음료', '과자', '시식대')",
                        "coords": "구역의 물리적 좌표"
                    },
                    "business_meaning": "매장 레이아웃과 동선 최적화의 기준"
                },
                "sales_funnel": {
                    "description": "방문-노출-픽업 퍼널 분석",
                    "key_fields": {
                        "shelf_name": "진열대명",
                        "date": "분석 날짜",
                        "visit": "방문 수",
                        "gaze1": "노출 수 (시선 집중)",
                        "pickup": "픽업 수 (실제 집어든 행동)"
                    },
                    "business_meaning": "상품의 매력도와 진열 효과성을 측정"
                },
                "two_step_flow": {
                    "description": "고객 동선 패턴 (3단계 이동 경로)",
                    "key_fields": {
                        "gender": "성별 (0: 남성, 1: 여성)",
                        "age_group": "연령대",
                        "zone1_id": "첫 번째 방문 구역",
                        "zone2_id": "두 번째 방문 구역", 
                        "zone3_id": "세 번째 방문 구역",
                        "num_people": "해당 패턴을 보인 고객 수"
                    },
                    "business_meaning": "고객 세그먼트별 매장 내 이동 패턴과 선호도 분석"
                },
                "detected_time": {
                    "description": "AI가 감지한 고객 속성 정보",
                    "key_fields": {
                        "person_seq": "고객 ID",
                        "age": "추정 연령",
                        "gender": "성별 (0: 남성, 1: 여성)"
                    },
                    "business_meaning": "인구통계학적 세분화 분석의 기초"
                }
            }
        },
        "cu_base": {
            "description": "POS 매출 데이터베이스",
            "tables": {
                "cu_revenue_total": {
                    "description": "편의점 매출 상세 데이터",
                    "key_fields": {
                        "store_nm": "매장명",
                        "tran_ymd": "거래 날짜",
                        "pos_no": "POS 번호",
                        "tran_no": "거래 번호",
                        "small_nm": "소분류 상품명",
                        "sale_amt": "판매 금액",
                        "sale_qty": "판매 수량"
                    },
                    "business_meaning": "실제 매출과 고객 행동 데이터 간의 연관성 분석"
                }
            }
        }
    }
}

# =============================================================================
# 비즈니스 메트릭 정의
# =============================================================================

BUSINESS_METRICS = {
    "conversion_metrics": {
        "pickup_rate": {
            "formula": "픽업 수 / 노출 수 * 100",
            "threshold": {
                "excellent": "> 15%",
                "good": "10-15%", 
                "needs_attention": "5-10%",
                "critical": "< 5%"
            },
            "insights": {
                "low": "진열 위치, 상품 패키징, 가격 경쟁력 검토 필요",
                "high": "성공 요인 분석 후 다른 상품에 적용"
            }
        },
        "purchase_conversion": {
            "formula": "구매 고객 수 / 전체 방문 고객 수 * 100", 
            "threshold": {
                "excellent": "> 40%",
                "good": "30-40%",
                "needs_attention": "20-30%",
                "critical": "< 20%"
            },
            "insights": {
                "low": "상품 구성, 가격 정책, 매장 환경 개선 필요",
                "high": "성공적인 매장 운영 모델"
            }
        }
    },
    "traffic_metrics": {
        "peak_hours": {
            "morning": "06:00-09:00",
            "lunch": "11:00-14:00", 
            "evening": "17:00-20:00",
            "late_night": "21:00-01:00"
        },
        "dwell_time": {
            "threshold": {
                "quick_visit": "< 3분",
                "normal_shopping": "3-10분",
                "extensive_browsing": "10-20분", 
                "excessive": "> 20분"
            },
            "insights": {
                "excessive": "매장 내 혼잡도나 상품 찾기 어려움 신호",
                "quick_visit": "목적성 구매 또는 매장 매력도 부족"
            }
        }
    },
    "spatial_metrics": {
        "zone_performance": {
            "hot_zone": "평균 대비 200% 이상 트래픽",
            "warm_zone": "평균 대비 100-200% 트래픽",
            "cold_zone": "평균 대비 50% 미만 트래픽",
            "dead_zone": "거의 방문하지 않는 구역"
        }
    }
}

# =============================================================================
# 인사이트 생성 규칙
# =============================================================================

INSIGHT_RULES = {
    "pattern_detection": {
        "trend_analysis": {
            "increasing": "지속적 증가 패턴 (3일 이상)",
            "decreasing": "지속적 감소 패턴 (3일 이상)", 
            "cyclical": "요일별/시간별 주기적 패턴",
            "seasonal": "월별/계절별 패턴"
        },
        "anomaly_detection": {
            "threshold": "평균 ± 2 표준편차 벗어난 값",
            "consecutive_anomalies": "연속 2일 이상 이상치",
            "sudden_drop": "전일 대비 30% 이상 감소",
            "sudden_spike": "전일 대비 50% 이상 증가"
        }
    },
    "correlation_analysis": {
        "weather_impact": "날씨와 방문객 수 상관관계",
        "promotion_effect": "프로모션 기간 vs 판매량 상관관계",
        "layout_efficiency": "구역 배치와 고객 동선의 최적성"
    },
    "predictive_indicators": {
        "churn_signals": [
            "방문 빈도 급격한 감소",
            "체류 시간 단축",
            "픽업 없는 방문 증가"
        ],
        "growth_signals": [
            "신규 고객 증가",
            "재방문율 상승", 
            "객단가 증가"
        ]
    }
}

# =============================================================================
# 액션 추천 프레임워크
# =============================================================================

ACTION_FRAMEWORK = {
    "immediate_actions": {
        "low_conversion": [
            {
                "action": "진열대 재배치",
                "priority": "High",
                "expected_impact": "10-15% 픽업률 개선",
                "implementation_time": "1-2일"
            },
            {
                "action": "프로모션 가격표 시각적 강화",
                "priority": "Medium", 
                "expected_impact": "5-10% 매출 증가",
                "implementation_time": "즉시"
            }
        ],
        "traffic_optimization": [
            {
                "action": "데드존 활용을 위한 유도 장치 설치",
                "priority": "Medium",
                "expected_impact": "매장 전체 이용률 15% 증대",
                "implementation_time": "3-5일"
            }
        ]
    },
    "strategic_actions": {
        "layout_optimization": [
            {
                "action": "고빈도 이동 경로 기반 상품 재배치",
                "priority": "High",
                "expected_impact": "20-30% 교차 판매 증대",
                "implementation_time": "1-2주"
            }
        ],
        "customer_experience": [
            {
                "action": "연령/성별별 맞춤 상품 구성",
                "priority": "Medium",
                "expected_impact": "고객 만족도 및 재방문율 향상",
                "implementation_time": "2-4주"
            }
        ]
    }
}

# =============================================================================
# 분석 체크리스트
# =============================================================================

ANALYSIS_CHECKLIST = {
    "data_validation": [
        "데이터 완정성 확인 (누락된 날짜/시간대 없음)",
        "이상치 검증 (비현실적 수치 확인)",
        "직원 데이터 제외 확인",
        "중복 데이터 제거 확인"
    ],
    "context_consideration": [
        "요일별 패턴 고려",
        "날씨/계절 영향 고려", 
        "프로모션/이벤트 기간 고려",
        "매장 운영 시간 고려"
    ],
    "insight_quality": [
        "구체적 수치 기반 근거 제시",
        "비교 기준점 명시 (전주/전월/평균)",
        "실행 가능한 개선안 포함",
        "예상 효과 정량적 표현"
    ]
}

# =============================================================================
# 유틸리티 함수
# =============================================================================

def get_table_context(table_name: str) -> Dict[str, Any]:
    """특정 테이블의 컨텍스트 정보를 반환"""
    for db_name, db_info in SCHEMA_CONTEXT["databases"].items():
        if table_name in db_info["tables"]:
            return {
                "database": db_name,
                "table_info": db_info["tables"][table_name],
                "related_metrics": _get_related_metrics(table_name)
            }
    return {}

def get_metric_threshold(metric_name: str) -> Dict[str, Any]:
    """메트릭의 임계값과 해석 정보를 반환"""
    for category, metrics in BUSINESS_METRICS.items():
        if metric_name in metrics:
            return metrics[metric_name]
    return {}

def get_recommended_actions(condition: str) -> List[Dict[str, Any]]:
    """특정 상황에 대한 추천 액션을 반환"""
    actions = []
    for action_type, action_categories in ACTION_FRAMEWORK.items():
        for category, category_actions in action_categories.items():
            if condition in category:
                actions.extend(category_actions)
    return actions

def _get_related_metrics(table_name: str) -> List[str]:
    """테이블과 관련된 메트릭 목록을 반환"""
    table_metric_map = {
        "sales_funnel": ["pickup_rate", "conversion_rate"],
        "line_in_out_individual": ["traffic_volume", "dwell_time"],
        "customer_behavior_event": ["engagement_rate", "attention_duration"],
        "two_step_flow": ["path_efficiency", "zone_transition_rate"]
    }
    return table_metric_map.get(table_name, [])

# =============================================================================
# 메인 컨텍스트 빌더
# =============================================================================

def build_analysis_context(query_type: str, tables_used: List[str]) -> str:
    """분석 유형과 사용된 테이블 기반으로 컨텍스트 문자열 생성"""
    context_parts = [
        "# 데이터 분석 컨텍스트\n",
        f"분석 유형: {query_type}\n",
        f"사용 테이블: {', '.join(tables_used)}\n\n"
    ]
    
    # 테이블별 상세 정보
    context_parts.append("## 테이블 정보:\n")
    for table in tables_used:
        table_ctx = get_table_context(table)
        if table_ctx:
            context_parts.append(f"**{table}**: {table_ctx['table_info']['description']}\n")
            context_parts.append(f"핵심 필드: {list(table_ctx['table_info']['key_fields'].keys())}\n")
            context_parts.append(f"비즈니스 의미: {table_ctx['table_info']['business_meaning']}\n\n")
    
    # 관련 메트릭과 임계값
    context_parts.append("## 주요 성과 지표:\n")
    for category, metrics in BUSINESS_METRICS.items():
        context_parts.append(f"**{category}**:\n")
        for metric, info in metrics.items():
            if "formula" in info:
                context_parts.append(f"- {metric}: {info['formula']}\n")
    
    return "".join(context_parts)

if __name__ == "__main__":
    # 테스트용 컨텍스트 생성
    context = build_analysis_context("conversion_analysis", ["sales_funnel", "customer_behavior_event"])
    print(context)