"""
Schema Context & Business Intelligence
=====================================

오프라인 매장 데이터 스키마와 비즈니스 컨텍스트를 정의합니다.
JSON 스키마 파일들을 읽어서 동적으로 컨텍스트를 생성합니다.
"""

import json
from typing import Dict, Any, List
from pathlib import Path

# =============================================================================
# 스키마 로더
# =============================================================================

def load_database_schemas() -> Dict[str, Any]:
    """JSON 스키마 파일들을 읽어서 데이터베이스 스키마 컨텍스트 생성"""
    schema_dir = Path(__file__).parent / "schema"
    schemas = {}
    
    # JSON 파일들 읽기
    for schema_file in schema_dir.glob("*_schema.json"):
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_data = json.load(f)
                db_name = schema_data["database"]
                schemas[db_name] = {
                    "description": _get_database_description(db_name),
                    "extracted_at": schema_data["extracted_at"],
                    "tables": {}
                }
                
                # 테이블 정보 처리
                for table_name, columns in schema_data["tables"].items():
                    schemas[db_name]["tables"][table_name] = {
                        "description": _get_table_description(db_name, table_name),
                        "columns": columns,
                        "business_meaning": _get_business_meaning(db_name, table_name)
                    }
        except Exception as e:
            print(f"스키마 파일 로딩 오류 {schema_file}: {e}")
    
    return schemas

def _get_database_description(db_name: str) -> str:
    """데이터베이스별 설명"""
    descriptions = {
        "plusinsight": "메인 고객 행동 분석 데이터베이스 - AI 카메라를 통한 매장 내 고객 행동, 동선, 관심도 분석",
        "cu_base": "POS 매출 데이터베이스 - 실제 거래 및 매출 데이터"
    }
    return descriptions.get(db_name, f"{db_name} 데이터베이스")

def _get_table_description(db_name: str, table_name: str) -> str:
    """테이블별 설명 (핵심 테이블들만)"""
    descriptions = {
        "plusinsight": {
            "line_in_out_individual": "방문객 입출입 개별 기록",
            "customer_behavior_event": "고객의 매장 내 행동 이벤트",
            "zone": "매장 내 구역 정보",
            "sales_funnel": "방문-노출-픽업 퍼널 분석",
            "two_step_flow": "고객 동선 패턴 (3단계 이동 경로)",
            "detected_time": "AI가 감지한 고객 속성 정보",
            "action": "고객 행동 인식 데이터",
            "stay": "고객 체류 정보",
            "zone_in_out_individual": "구역별 개별 출입 기록",
            "zone_dwell_range": "구역별 체류 시간 범위",
            "dwelling_bin": "위치별 체류 집계 데이터"
        },
        "cu_base": {
            "cu_revenue_total": "편의점 매출 상세 데이터 - 거래별 상품 판매 정보"
        }
    }
    return descriptions.get(db_name, {}).get(table_name, f"{table_name} 테이블")

def _get_business_meaning(db_name: str, table_name: str) -> str:
    """테이블의 비즈니스 의미"""
    meanings = {
        "plusinsight": {
            "line_in_out_individual": "매장 방문 패턴과 체류 시간 분석의 기초 데이터",
            "customer_behavior_event": "고객의 실제 관심도와 구매 의도를 파악하는 핵심 데이터",
            "zone": "매장 레이아웃과 동선 최적화의 기준",
            "sales_funnel": "상품의 매력도와 진열 효과성을 측정",
            "two_step_flow": "고객 세그먼트별 매장 내 이동 패턴과 선호도 분석",
            "detected_time": "인구통계학적 세분화 분석의 기초",
            "action": "고객의 실제 행동 패턴과 상품 관심도 분석",
            "stay": "고객 체류 패턴과 관심 구역 분석",
            "zone_in_out_individual": "구역별 트래픽과 인기도 측정",
            "zone_dwell_range": "구역별 고객 만족도와 매력도 지표"
        },
        "cu_base": {
            "cu_revenue_total": "실제 매출과 고객 행동 데이터 간의 연관성 분석"
        }
    }
    return meanings.get(db_name, {}).get(table_name, f"{table_name}의 비즈니스 활용 데이터")

# 전역 스키마 컨텍스트 로드
SCHEMA_CONTEXT = {
    "databases": load_database_schemas()
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
            table_info = table_ctx['table_info']
            context_parts.append(f"**{table}**: {table_info['description']}\n")
            context_parts.append(f"비즈니스 의미: {table_info['business_meaning']}\n")
            if 'columns' in table_info:
                column_names = [col['name'] for col in table_info['columns']]
                context_parts.append(f"주요 컬럼: {', '.join(column_names[:10])}\n\n")
    
    # 관련 메트릭과 임계값
    context_parts.append("## 주요 성과 지표:\n")
    for category, metrics in BUSINESS_METRICS.items():
        context_parts.append(f"**{category}**:\n")
        for metric, info in metrics.items():
            if "formula" in info:
                context_parts.append(f"- {metric}: {info['formula']}\n")
    
    return "".join(context_parts)

def get_full_schema_context() -> str:
    """전체 스키마 컨텍스트를 문자열로 반환 (시스템 프롬프트용)"""
    context_parts = ["# 데이터베이스 스키마 컨텍스트\n\n"]
    
    for db_name, db_info in SCHEMA_CONTEXT["databases"].items():
        context_parts.append(f"## {db_name} 데이터베이스\n")
        context_parts.append(f"{db_info['description']}\n\n")
        
        context_parts.append("### 테이블 목록:\n")
        for table_name, table_info in db_info["tables"].items():
            context_parts.append(f"**{table_name}**: {table_info['description']}\n")
            
            # 주요 컬럼들 (처음 10개만)
            if 'columns' in table_info and table_info['columns']:
                columns = [f"{col['name']}({col['type']})" for col in table_info['columns'][:10]]
                context_parts.append(f"  - 주요 컬럼: {', '.join(columns)}\n")
            
            context_parts.append(f"  - 비즈니스 의미: {table_info['business_meaning']}\n\n")
    
    return "".join(context_parts)

if __name__ == "__main__":
    # 테스트용 컨텍스트 생성
    print("=== 전체 스키마 컨텍스트 ===")
    print(get_full_schema_context())
    
    print("\n=== 특정 분석 컨텍스트 ===")
    context = build_analysis_context("conversion_analysis", ["sales_funnel", "customer_behavior_event"])
    print(context)