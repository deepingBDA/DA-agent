"""매장 방문객수 진단 보고서 작성을 도와주는 워크플로우"""

import json
from typing import Dict, Any, TypedDict

from langchain.chat_models import ChatOpenAI
from langchain.schema import BaseOutputParser
from langchain.tools import tool
from langgraph.graph import StateGraph, END

from base_workflow import BaseWorkflow


class VisitorDiagnoseState(TypedDict):
    """워크플로우 상태 정의"""
    query: str
    store_name: str
    period: str
    visitor_data: dict
    analysis_results: dict
    report: str
    check_results: dict


class VisitorDiagnoseWorkflow(BaseWorkflow):
    """방문객 진단 워크플로우 클래스 - BaseWorkflow 상속"""

    def __init__(self):
        super().__init__(workflow_name="visitor_diagnose")
        
        # LLM 설정
        self.llm = ChatOpenAI(model="gpt-4o", temperature=0.1)
        
        # 워크플로우 그래프
        self.workflow_app = self._build_workflow()

    def run(
        self, 
        query: str, 
        store_name: str = "테스트매장", 
        period: str = "2024년 1월"
    ) -> str:
        """워크플로우 실행 메서드"""
        self.logger.info(f"워크플로우 실행 시작: {query}")
        
        initial_state: VisitorDiagnoseState = {
            "query": query,
            "store_name": store_name,
            "period": period,
            "visitor_data": {},
            "analysis_results": {},
            "report": "",
            "check_results": {},
        }

        try:
            final_state = self.workflow_app.invoke(initial_state)
            self.logger.info("워크플로우 실행 완료")
            return final_state["report"]
        except Exception as e:
            self.logger.error(f"워크플로우 실행 실패: {e}")
            return f"워크플로우 실행 실패: {e}"

    def _build_workflow(self) -> StateGraph:
        """LangGraph 워크플로우 구성"""
        builder = StateGraph(VisitorDiagnoseState)

        # 노드 추가
        builder.add_node("query_data", self._query_data_node)
        builder.add_node("analyze_patterns", self._analyze_patterns_node)
        builder.add_node("generate_report", self._generate_report_node)
        builder.add_node("check_data", self._check_data_node)
        builder.add_node("check_template", self._check_template_node)
        builder.add_node("reflect", self._reflect_node)
        builder.add_node("output", self._output_node)

        # 진입점 설정
        builder.set_entry_point("query_data")

        # 흐름 설정
        builder.add_edge("query_data", "analyze_patterns")
        builder.add_edge("analyze_patterns", "generate_report")
        builder.add_edge("generate_report", "check_data")
        builder.add_edge("check_data", "check_template")

        # 조건부 엣지
        builder.add_conditional_edges(
            "check_template",
            self._evaluate_next,
            {"output": "output", "reflect": "reflect"},
        )
        builder.add_edge("reflect", "query_data")

        # 종료점 설정
        builder.set_finish_point("output")

        return builder.compile()

    def _query_data_node(self, state: VisitorDiagnoseState) -> VisitorDiagnoseState:
        """방문객 데이터 조회 노드"""
        self.logger.info(f"데이터 조회: {state['store_name']}, {state['period']}")
        
        # TODO: 여기에 데이터 조회 로직 구현
        # self._execute_query() 사용 가능
        
        return state

    def _analyze_patterns_node(
        self, state: VisitorDiagnoseState
    ) -> VisitorDiagnoseState:
        """방문객 패턴 분석 노드"""
        self.logger.info("방문객 패턴 분석 시작")
        
        # TODO: 여기에 패턴 분석 로직 구현
        
        return state

    def _generate_report_node(
        self, state: VisitorDiagnoseState
    ) -> VisitorDiagnoseState:
        """보고서 생성 노드"""
        self.logger.info("보고서 생성 시작")
        
        # TODO: 여기에 보고서 생성 로직 구현
        
        return state

    def _check_data_node(self, state: VisitorDiagnoseState) -> VisitorDiagnoseState:
        """데이터 정확성 검사 노드"""
        self.logger.info("데이터 정확성 검사 시작")
        
        prompt = f"""아래 방문객 데이터가 올바른지 판단해줘.
데이터에 이상한 값이나 논리적 오류가 있으면 'NO'로 답하고 이유를 설명해줘.

방문객 데이터: {state['visitor_data']}
생성된 보고서: {state['report']}

응답 형식:
{{"result": "YES" or "NO", "reason": "..."}}"""

        try:
            response = self.llm.invoke(prompt).content
            state.setdefault("check_results", {})["data_check"] = response
            self.logger.info("데이터 정확성 검사 완료")
        except Exception as e:
            self.logger.error(f"데이터 정확성 검사 실패: {e}")
            state.setdefault("check_results", {})["data_check"] = (
                '{"result": "NO", "reason": "검사 실행 실패"}'
            )

        return state

    def _check_template_node(
        self, state: VisitorDiagnoseState
    ) -> VisitorDiagnoseState:
        """보고서 형식 검사 노드"""
        self.logger.info("보고서 형식 검사 시작")
        
        template_format = """방문객수 진단 보고서:
- 매장명: ...
- 분석 기간: ...
- 총 방문객수: ...명
- 성별 분석: 남성 ...명, 여성 ...명
- 연령대 분석: ...
- 주요 문제점: ...
- 개선 권장사항: ..."""

        prompt = f"""아래 보고서가 지정된 템플릿 형식과 일치하는지 판단해줘.
필수 항목이 누락되거나 형식이 틀렸으면 'NO'로 답해줘.

기준 템플릿:
{template_format}

생성된 보고서:
{state['report']}

응답 형식:
{{"result": "YES" or "NO", "reason": "..."}}"""

        try:
            response = self.llm.invoke(prompt).content
            state["check_results"]["format_check"] = response
            self.logger.info("보고서 형식 검사 완료")
        except Exception as e:
            self.logger.error(f"보고서 형식 검사 실패: {e}")
            state.setdefault("check_results", {})["format_check"] = (
                '{"result": "NO", "reason": "검사 실행 실패"}'
            )

        return state

    def _reflect_node(self, state: VisitorDiagnoseState) -> VisitorDiagnoseState:
        """Reflection 노드"""
        self.logger.warning("🔁 Reflection: 검증 실패, 재시도 중...")
        print("🔁 Reflection: 검증 실패, 데이터 및 보고서 재생성 중...")
        
        # 상태 정리
        state.pop("check_results", None)
        return state

    def _output_node(self, state: VisitorDiagnoseState) -> VisitorDiagnoseState:
        """최종 출력 노드"""
        self.logger.info("✅ 모든 검증 통과. 최종 보고서 출력")
        print("✅ 모든 검증 통과. 최종 보고서:")
        print("=" * 50)
        print(state["report"])
        print("=" * 50)
        return state

    def _evaluate_next(self, state: VisitorDiagnoseState) -> str:
        """다음 단계 평가"""
        try:
            check_results = state.get("check_results", {})
            
            data_check = json.loads(
                check_results.get("data_check", '{"result": "NO"}')
            )["result"]
            format_check = json.loads(
                check_results.get("format_check", '{"result": "NO"}')
            )["result"]

            if data_check == "YES" and format_check == "YES":
                self.logger.info("모든 검증 통과 -> 출력으로 이동")
                return "output"
            else:
                self.logger.info("검증 실패 -> 반영으로 이동")
                return "reflect"
        except Exception as e:
            self.logger.error(f"다음 단계 결정 실패: {e}")
            return "reflect"


if __name__ == "__main__":
    # 워크플로우 실행 테스트
    workflow = VisitorDiagnoseWorkflow()
    
    result = workflow.run(
        query="방문객 데이터 분석 및 보고서 작성",
        store_name="홍대점",
        period="2024년 1월",
    )
    
    print("\n최종 결과:")
    print(result)
