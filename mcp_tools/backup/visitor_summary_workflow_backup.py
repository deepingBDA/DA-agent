"""
Visitor Summary Workflow (HTML Tabs)

요구사항 요약:
- 방문 현황 요약 통계 워크플로우로 구성
- GPT-5 + ReAct 스타일 프롬프트로 표 데이터 요약 생성
- 스펙 확장 가능한 구조(Visitor, Touch Point, Dwelling Time). 현재는 Visitor만 구현
- 호출 매개변수: spec, 기준일(end_date), 이용 매장(stores)
- HTML 템플릿은 필수 섹션 누락 없이 안정적으로 출력
- 스파크라인은 `mcp_tools/reports/svg_renderer.py` 활용, 증감률 시리즈 변환은 `mcp_tools/reports/weekly_domain.py` 활용

주의:
- Touch Point / Dwelling Time은 추후 구현(스켈레톤으로 명시)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, TypedDict
import math
import re

from dotenv import load_dotenv
from langgraph.graph import END, START, StateGraph
from langchain_openai import ChatOpenAI

from base_workflow import BaseWorkflow, BaseState
from reports.svg_renderer import svg_sparkline
from reports.weekly_domain import to_pct_series
from database_manager import get_all_sites, get_site_client

# 이미 검증된 데이터 수집 함수는 기존 CLI 스크립트에서 재사용
# CLI 관련 함수들은 이 파일에 직접 구현


# ----------------------------- 타입 및 상수 -----------------------------

SPEC_VISITOR = "visitor"
SPEC_TOUCH_POINT = "touch_point"
SPEC_DWELLING_TIME = "dwelling_time"


class VisitorSummaryState(BaseState):
    spec: str
    end_date: str
    stores: List[str]
    periods: List[int]  # 예: [7, 30]
    compare_lag: int
    period_label: str
    prev_label: str
    rows_by_period: Dict[int, List[Dict[str, Optional[float]]]]
    html_content: str
    llm_summary: str
    final_result: str


@dataclass
class RenderSeries:
    weekday: List[float]
    weekend: List[float]
    total: List[float]


class VisitorSummaryWorkflow(BaseWorkflow[VisitorSummaryState]):
    def __init__(self) -> None:
        super().__init__(workflow_name="visitor_summary")
        load_dotenv()
        # gpt-5, ReAct 지향 프롬프트 사용(사고는 내부, 출력은 요약 결과만)
        # GPT-5는 temperature 파라미터를 지원하지 않으므로 제거
        self.llm = ChatOpenAI(model="gpt-5")
        self._summary_prompt_tpl = (
            """

        [3줄 요약 지침]

        1. 증감률 지속 감소 매장: 최근 4주 연속 하락추세인 매장명과 어떤 지표(평일/주말/총)인지 간단 표기.
        2. 총 증감률 감소 매장: 감소율 상위 매장명과 %를 나열, 공통적인 감소 양상 요약.
        3. 총 증감률 증가 매장: 증가율 상위 매장명과 %를 나열, 평일/주말 증감률 차이가 10% 이상일 때만 주요 요인(평일/주말) 표기, 그렇지 않으면 퍼센테이지만 표기.

        [3줄 요약 지침 + 스타일]

        1) 지속 감소 매장:
        - 출력 형식: <li class="trend-red"><span class="badge">지속 감소 매장: [매장명] (어떤 지표·몇 주 연속)</span></li>
        - 규칙:
            - 리스트 최상단에 위치  
            - 글자 끝에서 박스 마무리 (inline-block)  
            - 스타일: 배경 옅은 빨강(#fee2e2), 왼쪽 빨간 테두리(#ef4444), 진한 빨강 텍스트(#7f1d1d)
            - 불릿 제거

        2) 총 증감률 증가 매장:
        - 출력 형식: <li><span class="pct-pos">▲증가 매장</span>: [매장명](<span class="pct-pos">+x.x%</span>, 평일/주말 증감률 차이가 10% 이상일 때만 "평일 요인", "주말 요인" 등으로 표기, 그렇지 않으면 퍼센테이지만 표기)</li>
        - 규칙: "▲증가 매장" 문구와 증가율은 <span class="pct-pos">로 감싸 빨간색 표시
        - 주의: 증가 매장, 감소매장 문구 이후에 여러 개의 매장 정보 출력 

        3) 총 증감률 감소 매장:
        - 출력 형식: <li><span class="pct-neg">▼감소 매장</span>: [매장명](<span class="pct-neg">-x.x%</span>)</li>
        - 규칙: "▼감소 매장" 문구와 감소율은 <span class="pct-neg">로 감싸 파란색 표시

        [추가 규칙]
        - 단, 각 항목에 해당하는 값이 없다면 동일한 <li> 형식을 유지하고 "해당없음"으로 출력하세요.
        - 총 증감률이 1% ~ -1%인 매장은 출력하지 않도록 한다.
        - 모든 매장이 증가 또는 감소 매장에 들어가야한다.

        예: <li class="trend-red"><span class="badge">지속 감소 매장: 해당없음</span></li>
            <li><span class="pct-pos">▲증가 매장</span>: 해당없음</li>
            <li><span class="pct-neg">▼감소 매장</span>: 해당없음</li>

            데이터:
            {table_text}
            """
        )

        # 페어 추천용 LLM 프롬프트 (동적 데이터 기반)
        self._pair_prompt_tpl = (
            """
            [다음 단계 지침 + 스타일]

            당신의 작업:
            - 주간 리포트 테이블을 바탕으로 매장 성과 매트릭스를 분석하여 "매장 페어"를 선정하고 HTML 카드로 출력한다.

            선정 규칙:
            1) 테이블의 "금주 방문객" 컬럼 기준으로 방문객 수가 유사한 두 매장을 페어로 묶는다.
            • **유사성 기준**: 두 매장의 금주 방문객 수 차이가 평균 대비 30% 이하인 경우
            • [평균 방문객수] = (매장 A 금주 방문객 + 매장 B 금주 방문객) / 2
            • **중요**: 테이블에서 "금주 방문객" 컬럼의 값을 읽어야 함
            • 이 값은 반드시 양의 정수여야 하며, 소수점이 있으면 반올림
            2) 전주 대비 총 증감률이 반대 흐름(한쪽 증가, 한쪽 감소)이면 → "전주 대비 방문객 증감률 반대 흐름"
            3) 두 매장 모두 증가(또는 감소)이지만 증감 폭 차이가 크면 → "전주 대비 방문객 증감 폭 차이 큼"

            출력 형식 (HTML):
            <ul class="pair-list">
            <li class="pair-item">
                <div class="pair-head">
                <div class="pair-names">매장A vs 매장B</div>
                <span class="criteria-badge">선정 기준 요약</span>
                </div>
                <div class="pair-note">두 매장 평균 방문객: <span class="pct-pos"><b>[평균 방문객수]명</b></span>, [선정 기준 상세 설명]</div>
            </li>
            … (페어 개수만큼 반복)
            </ul>

            스타일 규칙:
            - [평균 방문객수]명은 <span class="pct-pos"><b>…</b></span>로 감싸 파란색 + 볼드 처리
            - 불필요한 설명/코드블록 없이 오직 <ul class="pair-list"> … </ul> 구조만 출력한다.


            데이터:
            {table_text}
            """
        )

        self.workflow_app = self._build_workflow()

    # ----------------------------- Public API -----------------------------
    def run(
        self,
        *,
        spec: str,
        end_date: str,
        stores: Union[str, Sequence[str]],
        periods: int = 7,
        compare_lag: Optional[int] = None,
        user_prompt: str = "주간 방문 리포트",
    ) -> str:
        # 입력 정규화
        if isinstance(stores, str):
            stores_list = [s.strip() for s in stores.replace("，", ",").split(",") if s.strip()]
        else:
            stores_list = [str(s).strip() for s in stores if str(s).strip()]
        if not stores_list:
            raise ValueError("stores가 비어 있습니다")

        end_iso = clamp_end_date_to_yesterday(end_date)
        # periods 는 int 하나로 받는다. 내부 로직 호환을 위해 리스트로 변환
        periods_list = [periods]
        lag_val = compare_lag if compare_lag is not None else periods

        # 기간 라벨 계산
        if periods == 1:
            period_label = "당일"
            lag_val = 7  # 1일 모드에서는 항상 전주 같은 요일과 비교
            prev_label = "전주 동일 요일"
        else:
            period_label = f"최근{periods}일"
            prev_label = f"전주{lag_val}일" if lag_val == 7 else f"전기간{lag_val}일"
        
        initial_state: VisitorSummaryState = {
            "user_prompt": user_prompt,
            "workflow_id": f"{self.workflow_name}_{end_iso}",
            "timestamp": date.today().isoformat(),
            "spec": spec.lower(),
            "end_date": end_iso,
            "stores": stores_list,
            "periods": periods_list,
            "compare_lag": lag_val,
            "period_label": period_label,
            "prev_label": prev_label,
            "rows_by_period": {},
            "html_content": "",
            "llm_summary": "",
            "final_result": "",
        }  # type: ignore

        result = self.workflow_app.invoke(initial_state)
        return result.get("final_result", "워크플로우 실행 완료")

    # ----------------------------- Graph -----------------------------
    def _build_workflow(self) -> StateGraph:
        builder = StateGraph(VisitorSummaryState)
        builder.add_node("fetch", self._fetch_node)
        builder.add_node("summarize", self._summarize_node)
        builder.add_node("generate_html", self._generate_html_node)
        builder.add_node("save", self._save_node)

        builder.add_edge(START, "fetch")
        builder.add_edge("fetch", "summarize")
        builder.add_edge("summarize", "generate_html")
        builder.add_edge("generate_html", "save")
        builder.add_edge("save", END)
        return builder.compile()

    # ----------------------------- Nodes -----------------------------
    def _fetch_node(self, state: VisitorSummaryState) -> VisitorSummaryState:
        spec = state["spec"]
        end_iso = state["end_date"]
        stores = state["stores"]
        periods = state["periods"]

        rows_by_period: Dict[int, List[Dict[str, Optional[float]]]] = {}

        if spec == SPEC_VISITOR:
            for days in periods:
                rows: List[Dict[str, Optional[float]]] = []
                for st in stores:
                    try:
                        summ = summarize_period_rates(st, end_iso, days)
                    except Exception as e:
                        self.logger.warning(f"요약 수집 실패: {st}, {e}")
                        summ = {
                            "site": st,
                            "end_date": end_iso,
                            "curr_total": None,
                            "prev_total": None,
                            "weekday_delta_pct": None,
                            "weekend_delta_pct": None,
                            "total_delta_pct": None,
                        }
                    rows.append(
                        {
                            "site": summ.get("site", st),
                            "curr_total": summ.get("curr_total"),
                            "prev_total": summ.get("prev_total"),
                            "weekday_delta_pct": summ.get("weekday_delta_pct"),
                            "weekend_delta_pct": summ.get("weekend_delta_pct"),
                            "total_delta_pct": summ.get("total_delta_pct"),
                        }
                    )
                rows_by_period[days] = rows
        elif spec in (SPEC_TOUCH_POINT, SPEC_DWELLING_TIME):
            # TODO: 추후 구현 - 동일한 테이블 스키마로 값을 매핑하도록 확장
            raise NotImplementedError(f"spec '{spec}' 은(는) 아직 미구현입니다. 현재는 '{SPEC_VISITOR}'만 지원합니다.")
        else:
            raise ValueError(f"알 수 없는 spec: {spec}")

        state["rows_by_period"] = rows_by_period
        return state

    def _generate_html_node(self, state: VisitorSummaryState) -> VisitorSummaryState:
        end_iso = state["end_date"]
        sections: List[str] = []
        
        # 디버깅을 위한 로그 추가
        llm_summary = state.get("llm_summary", "")
        self.logger.info(f"HTML 생성 시 llm_summary 길이: {len(llm_summary)}")
        self.logger.info(f"HTML 생성 시 llm_summary 내용: {llm_summary[:200]}...")
        
        for days in state["periods"]:
            rows = state["rows_by_period"].get(days, [])
            sections.append(
                self._build_tab_section_html(
                    section_id=f"section-{days}",
                    title_suffix=f"최근 {days}일 vs 이전 {days}일",
                    end_iso=end_iso,
                    days=days,
                    rows=rows,
                    llm_summary=llm_summary,
                    state=state,
                )
            )

        body_html = "\n".join(sections)
        
        # daily 옵션일 때 요일 추가
        title = f"방문 현황 요약 통계({end_iso})"
        if state["periods"] == [1]:  # daily 옵션
            weekday_kr = self._get_weekday_korean(end_iso)
            title = f"방문 현황 요약 통계({end_iso} {weekday_kr})"
        
        html = self._build_html_page(title=title, body_html=body_html, periods=state["periods"])
        state["html_content"] = html
        return state

    def _summarize_node(self, state: VisitorSummaryState) -> VisitorSummaryState:
        # LLM 요약을 위한 테이블 텍스트 구성(간결·일관된 포맷)
        base_days = min(state["periods"]) if state["periods"] else 7
        
        if state["compare_lag"] == 7 and base_days == 1:
            # 일자별 모드: 평일/주말 구분 없음
            lines: List[str] = [f"매장명\t{state['period_label']}방문객\t{state['prev_label']}방문객\t증감%"]
            for r in state["rows_by_period"].get(base_days, []):
                lines.append(
                    "\t".join(
                        [
                            str(r.get("site", "")),
                            self._fmt_int(r.get("curr_total")),
                            self._fmt_int(r.get("prev_total")),
                            self._fmt_pct(r.get("total_delta_pct")),
                        ]
                    )
                )
        else:
            # 주간 모드: 기존 평일/주말 구분
            lines: List[str] = [f"매장명\t{state['period_label']}방문객\t{state['prev_label']}방문객\t평일증감%\t주말증감%\t총증감%"]
            for r in state["rows_by_period"].get(base_days, []):
                lines.append(
                    "\t".join(
                        [
                            str(r.get("site", "")),
                            self._fmt_int(r.get("curr_total")),
                            self._fmt_int(r.get("prev_total")),
                            self._fmt_pct(r.get("weekday_delta_pct")),
                            self._fmt_pct(r.get("weekend_delta_pct")),
                            self._fmt_pct(r.get("total_delta_pct")),
                        ]
                    )
                )

        table_text = "\n".join(lines)
        prompt = self._summary_prompt_tpl.format(table_text=table_text)
        
        # 디버깅을 위한 로그 추가
        self.logger.info(f"LLM 요약 프롬프트 생성: {len(table_text)} 문자")
        self.logger.info(f"테이블 데이터: {table_text}")
        print(f"=== 테이블 데이터 ===")
        print(table_text)
        print(f"===================")
        
        try:
            resp = self.llm.invoke(prompt)
            content = (resp.content or "").strip()
            state["llm_summary"] = content
            
            # 디버깅을 위한 로그 추가
            self.logger.info(f"LLM 응답 성공: {len(content)} 문자")
            self.logger.info(f"LLM 응답 내용: {content[:200]}...")
            
        except Exception as e:
            self.logger.error(f"LLM 요약 실패: {e}")
            state["llm_summary"] = "요약 생성 실패"
        
        return state

    def _save_node(self, state: VisitorSummaryState) -> VisitorSummaryState:
        html = state.get("html_content", "")
        if not html:
            state["final_result"] = "HTML 콘텐츠가 없음"
            return state

        # 중앙 설정에서 경로 가져오기
        from config.html_output_config import get_full_html_path
        
        # 저장 경로: 1일은 daily, 7일은 weekly
        if state["periods"] == [1]:
            report_type = 'visitor_daily'
        else:
            report_type = 'visitor_weekly'
        
        out_path, latest_path = get_full_html_path(
            report_type=report_type,
            end_date=state['end_date'],
            use_unified=False  # 각 폴더별로 분리
        )
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(html)
            try:
                from shutil import copyfile
                copyfile(out_path, latest_path)
            except Exception:
                pass
            web_url = f"/reports/weekly/{os.path.basename(out_path)}"
            state["final_result"] = (
                "📊 방문 현황 요약 통계 생성 완료!\n\n" f"🔗 [웹에서 보기]({web_url})\n\n" + (state.get("llm_summary", "") or "")
            )
        except Exception as e:
            self.logger.error(f"HTML 저장 실패: {e}")
            state["final_result"] = f"HTML 저장 실패: {e}"
        return state

    # ----------------------------- HTML Builders -----------------------------
    def _build_tab_section_html(self, *, section_id: str, title_suffix: str, end_iso: str, days: int, rows: List[Dict[str, Optional[float]]], llm_summary: str, state: VisitorSummaryState) -> str:
        rows_sorted = sorted(rows, key=lambda r: (0 if r.get("total_delta_pct") is not None else 1, -(r.get("total_delta_pct") or 0)))
        return (
            """
<section id="{section_id}" class="tab-section" data-period="{section_id}">
  {summary}
  {table}
  {scatter}
  {next}
  {explain}
</section>
"""
        ).replace("{section_id}", section_id)\
         .replace("{summary}", self._build_summary_card_html(rows_sorted, llm_summary))\
         .replace("{table}", self._build_table_html(rows_sorted, end_iso, days, state))\
         .replace("{scatter}", self._build_scatter_card_html(rows_sorted))\
         .replace("{next}", self._build_next_actions_card_html(rows_sorted, llm_summary, end_iso))\
         .replace("{explain}", self._build_explanation_card_html(title_suffix))

    def _build_html_page(self, *, title: str, body_html: str, periods: List[int]) -> str:
        # labels_html, inputs_html, css_rules = self._build_tabs(periods)
        css_rules = ""
        return f"""
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Noto Sans KR', Arial, sans-serif; margin: 0; background: #fafafa; color: #111; }}
    .container {{ max-width: 1080px; margin: 24px auto; padding: 0 16px; }}
    header.page-header {{ margin-bottom: 16px; }}
    header.page-header h1 {{ font-size: 22px; margin: 0 0 4px; }}
    .desc {{ color: #666; font-size: 13px; }}
    .card {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 10px; padding: 16px; margin: 12px 0; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }}
    .card h2, .card h3 {{ margin: 0 0 8px; font-size: 18px; }}
    .card-header {{ margin-bottom: 16px; }}
    .card-header h3 {{ margin: 0 0 4px; font-size: 18px; color: #111; }}
    .card-subtitle {{ margin: 0; color: #6b7280; font-size: 13px; line-height: 1.4; }}
    .muted {{ color: #6b7280; font-size: 13px; }}
    .bullets {{ margin: 8px 0 8px 16px; padding: 0; }}
    .bullets li {{ margin: 4px 0; }}
    .table-wrap {{ overflow-x: auto; }}
    table.table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    table.table th, table.table td {{ border-top: 1px solid #f3f4f6; padding: 8px 10px; text-align: center; }}
    table.table th {{ background: #f9fafb; color: #374151; font-weight: 600; }}
    td.num {{ text-align: center; }}
    th.sep-left, td.sep-left {{ border-left: 1px solid #e5e7eb; }}
    th.sep-right, td.sep-right {{ border-right: 1px solid #e5e7eb; }}
    td.sep-left, th.sep-left {{ padding-left: 10px; }}
    td.sep-right, th.sep-right {{ padding-right: 10px; }}
    .pct-with-chart {{ display: inline-flex; align-items: center; gap: 8px; }}
    .pct-with-chart .spark {{ display: inline-flex; align-items: center; gap: 6px; padding: 2px 0; border: none; background: transparent; }}
    .pct-with-chart .spark svg {{ display: block; }}
    .col-note {{ font-size: 10px; color: #6b7280; font-weight: 400; margin-top: 2px; }}
    .pct-pos {{ color: #dc2626; }}
    .pct-neg {{ color: #1d4ed8; }}
    .pct-zero {{ color: #374151; }}
    .tabs {{ display: flex; gap: 8px; margin: 8px 0 16px; }}
    .tab-label {{ padding: 8px 12px; background: #eef2ff; color: #3730a3; border-radius: 8px; cursor: pointer; user-select: none; }}
    .tab-label:hover {{ background: #e0e7ff; }}
    input[type="radio"].tab-input {{ display: none; }}
    .tab-section {{ display: block; }}
    {css_rules}
    /* Summary readability */
    .summary-list {{ margin: 0; padding: 0; line-height: 1.6; }}
    .summary-list li {{ margin: 4px 0; text-align: left; list-style: none; display: list-item; }}
    
    /* 프롬프트 기반 요약 스타일 */
    .trend-red {{ color: #7f1d1d; }}
    .badge {{ 
        display: inline-block;
        background: #fee2e2; 
        border-left: 3px solid #ef4444; 
        color: #7f1d1d;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: 500;
    }}
    
    /* Pair recommendations */
    .pair-list {{ list-style: none; padding: 0; margin: 8px 0; display: grid; grid-template-columns: 1fr; gap: 10px; }}
    @media (min-width: 720px) {{ .pair-list {{ grid-template-columns: 1fr 1fr; }} }}
    .pair-item {{ border: 1px solid #e5e7eb; border-radius: 10px; padding: 10px 12px; background: #fff; }}
    .pair-head {{ display: flex; align-items: center; justify-content: space-between; gap: 8px; }}
    .pair-names {{ font-weight: 600; font-size: 14px; color: #111; }}
    .criteria-badge {{ font-size: 11px; color: #3730a3; background: #eef2ff; border: 1px solid #e0e7ff; padding: 2px 8px; border-radius: 999px; white-space: nowrap; }}
    .pair-note {{ margin-top: 6px; color: #374151; font-size: 13px; line-height: 1.5; }}
    .pair-question {{ display: block; margin-top: 6px; color: #6b7280; font-size: 12px; }}
  </style>
  <!-- section:head -->
</head>
<body>
  <div class="container">
    <header class="page-header">
      <h1>{title}</h1>
    </header>
    <div class="sections">
      {body_html}
    </div>
  </div>
</body>
</html>
"""

    # def _build_tabs(self, periods: List[int]) -> Tuple[str, str, str]:
    #     if not periods:
    #         periods = [7]
    #     labels: List[str] = []
    #     inputs: List[str] = []
    #     css_rules: List[str] = []
    #     for idx, p in enumerate(periods):
    #         labels.append(f"<label for=\"tab-{p}\" class=\"tab-label\">최근 {p}일</label>")
    #         checked = " checked" if idx == 0 else ""
    #         inputs.append(f"<input id=\"tab-{p}\" class=\"tab-input\" type=\"radio\" name=\"tabs\"{checked} />")
    #         css_rules.append(f"#tab-{p}:checked ~ .sections #section-{p} {{ display: block; }}")
    #     return "\n".join(labels), "\n".join(inputs), "\n".join(css_rules)

    def _build_explanation_card_html(self, title_suffix: str) -> str:
        return (
            """
<section class="card">
  <h2>지표 설명</h2>
  <p class="muted">이 표는 각 매장의 평일 증감률, 주말 증감률, 그리고 전체 기간(총) 증감률을 나타냅니다.</p>
  <ul class="bullets">
    <li><b>평일 증감률</b>: 해당 매장의 평일 방문자 수가 전 기간 대비 얼마나 증가 감소했는지를 백분율로 표시</li>
    <li><b>주말 증감률</b>: 주말(토·일) 방문자 수의 변동률</li>
    <li><b>총 증감률</b>: 평일과 주말을 합산한 전체 기간 대비 변동률</li>
    <li><b>주차별 평일/주말/총 증감률</b>: 최근 4주(금주 포함)의 전주 대비 방문율을 주차별로 비교</li>
  </ul>
  <p class="muted">매장별 상승·하락 추세를 진단하고, 최근 4주간의 증감률 변화를 통해 해당 추세가 일시적인지, 지속적인지 판단할 수 있습니다.</p>
  <p class="muted">지속적인 추세를 보이는 매장은 면밀히 관찰이 필요합니다.</p>
  <!-- section:explanation -->
</section>
"""
        )

    def _build_scatter_card_html(self, rows: List[Dict[str, Optional[float]]]) -> str:
        # 산점도: x=금주 방문객(curr_total), y=총 증감률(total_delta_pct)
        # 민맥스 스케일, 축 눈금값, 사분면 구분선(세로: 방문객 중위값, 가로: 0%), 굵은 라벨
        width, height = 1000, 600
        padding_left, padding_right = 120, 60
        padding_top, padding_bottom = 60, 120
        plot_w = width - padding_left - padding_right
        plot_h = height - padding_top - padding_bottom

        xs: List[float] = []
        ys: List[float] = []
        for r in rows:
            cx = r.get("curr_total")
            ty = r.get("total_delta_pct")
            if cx is not None and ty is not None:
                try:
                    xs.append(float(cx))
                    ys.append(float(ty))
                except Exception:
                    pass
        if not xs or not ys:
            return """
<section class=\"card\"> 
  <h3>매장 성과</h3>
  <p class=\"muted\">표시할 데이터가 부족합니다.</p>
</section>
"""

        # 1) 데이터 기반 최소/최대 및 10% 여백
        x_min_data, x_max_data = min(xs), max(xs)
        y_min_data, y_max_data = min(ys), max(ys)
        x_range = x_max_data - x_min_data or 1.0
        y_range = y_max_data - y_min_data or 1.0
        y_min_pad = y_min_data - y_range * 0.10
        y_max_pad = y_max_data + y_range * 0.10

        # 2) 알잘딱 Nice Scale로 깔끔한 축 경계/간격 계산
        def _nice_num(x: float, round_to: bool) -> float:
            if x <= 0:
                return 1.0
            exp = math.floor(math.log10(x))
            f = x / (10 ** exp)
            if round_to:
                if f < 1.5:
                    nf = 1
                elif f < 3:
                    nf = 2
                elif f < 7:
                    nf = 5
                else:
                    nf = 10
            else:
                if f <= 1:
                    nf = 1
                elif f <= 2:
                    nf = 2
                elif f <= 5:
                    nf = 5
                else:
                    nf = 10
            return nf * (10 ** exp)

        def _nice_scale(vmin: float, vmax: float, max_ticks: int = 5) -> tuple[float, float, float]:
            rng = _nice_num(max(vmax - vmin, 1e-6), False)
            tick = _nice_num(rng / max(1, (max_ticks - 1)), True)
            nice_min = math.floor(vmin / tick) * tick
            nice_max = math.ceil(vmax / tick) * tick
            return nice_min, nice_max, tick

        # X축 방문객 수: 중간값을 중심으로 대칭하게 스케일링
        x_mid = (x_min_data + x_max_data) / 2.0
        x_range_sym = max(x_max_data - x_mid, x_mid - x_min_data) * 1.15  # 15% 여백
        x_min_sym = x_mid - x_range_sym
        x_max_sym = x_mid + x_range_sym
        x_min, x_max, x_step = _nice_scale(x_min_sym, x_max_sym, 5)
        
        # Y축 증감률도 Nice scale로 적응적 설정 (큰 범위도 자동 대응)
        y_min, y_max, y_step = _nice_scale(y_min_pad, y_max_pad, 5)

        def sx(x: float) -> float:
            x_range = x_max - x_min or 1.0
            return padding_left + (x - x_min) / x_range * plot_w

        def sy(y: float) -> float:
            y_range = y_max - y_min or 1.0
            return padding_top + (1 - (y - y_min) / y_range) * plot_h

        # 가로 0% 기준선
        zero_y = sy(0) if (y_min <= 0 <= y_max) else None

        # 세로선: 방문객 수 최대값과 최소값의 평균 (이미 위에서 계산됨)
        mid_x_svg = sx(x_mid)

        # 3) 눈금 배열 생성
        x_ticks: List[float] = []
        v = x_min
        while v <= x_max + 1e-6:
            x_ticks.append(v)
            v += x_step
        y_ticks: List[float] = []
        v = y_min
        while v <= y_max + 1e-6:
            y_ticks.append(v)
            v += y_step

        def fmt_x(v: float) -> str:
            return f"{int(round(v)):,}"

        def fmt_y(v: float) -> str:
            return f"{v:.1f}%"

        grid_parts: List[str] = []
        for yv in y_ticks:
            gy = sy(yv)
            grid_parts.append(f"<line x1={padding_left} y1={gy:.1f} x2={width - padding_right} y2={gy:.1f} stroke=\"#eee\" />")
            is_zero = abs(yv) < 1e-6
            label_color = "#cbd5e1" if is_zero else "#6b7280"
            label_text = "0%" if is_zero else fmt_y(yv)
            grid_parts.append(f"<text x={padding_left-10} y={gy+4:.1f} font-size=\"12\" fill=\"{label_color}\" text-anchor=\"end\">{label_text}</text>")

        for xv in x_ticks:
            gx = sx(xv)
            grid_parts.append(f"<line x1={gx:.1f} y1={padding_top} x2={gx:.1f} y2={height - padding_bottom} stroke=\"#eee\" />")
            grid_parts.append(f"<text x={gx:.1f} y={height - padding_bottom + 24} font-size=\"12\" fill=\"#6b7280\" text-anchor=\"middle\">{fmt_x(xv)}</text>")

        # 축선 + 틱 마크
        axis_parts: List[str] = []
        x_axis_y = height - padding_bottom
        axis_parts.append(f"<line x1={padding_left} y1={x_axis_y:.1f} x2={width - padding_right} y2={x_axis_y:.1f} stroke=\"#111\" stroke-width=\"1.6\" />")
        axis_parts.append(f"<line x1={padding_left:.1f} y1={padding_top} x2={padding_left:.1f} y2={height - padding_bottom} stroke=\"#111\" stroke-width=\"1.6\" />")
        for yv in y_ticks:
            gy = sy(yv)
            axis_parts.append(f"<line x1={padding_left-6} y1={gy:.1f} x2={padding_left} y2={gy:.1f} stroke=\"#111\" stroke-width=\"1\" />")
        for xv in x_ticks:
            gx = sx(xv)
            axis_parts.append(f"<line x1={gx:.1f} y1={x_axis_y:.1f} x2={gx:.1f} y2={x_axis_y+6:.1f} stroke=\"#111\" stroke-width=\"1\" />")

        # 사분면 구분선
        divider_parts: List[str] = []
        if zero_y is not None:
            divider_parts.append(f"<line x1={padding_left} y1={zero_y:.1f} x2={width - padding_right} y2={zero_y:.1f} stroke=\"#cbd5e1\" stroke-width=\"1.2\" />")
        divider_parts.append(f"<line x1={mid_x_svg:.1f} y1={padding_top} x2={mid_x_svg:.1f} y2={height - padding_bottom} stroke=\"#cbd5e1\" stroke-width=\"1.4\" />")
        
        # 중앙값 라벨 (기존 X축 눈금과 겹치지 않을 때만 표시)
        mid_label = ""
        min_distance = 80  # 최소 거리 (픽셀)
        should_show = True
        for xv in x_ticks:
            if abs(x_mid - xv) < min_distance:
                should_show = False
                break
        
        if should_show:
            mid_label = f"<text x=\"{mid_x_svg:.1f}\" y=\"{height - padding_bottom + 24}\" font-size=\"12\" fill=\"#cbd5e1\" text-anchor=\"middle\">{int(round(x_mid)):,}명</text>"

        # 점 + 2줄 라벨(굵은 매장명 / 괄호에 값)
        points: List[str] = []
        labels: List[str] = []
        for r in rows:
            site = str(r.get("site", ""))
            cx = r.get("curr_total")
            ty = r.get("total_delta_pct")
            if cx is None or ty is None:
                continue
            try:
                x = sx(float(cx))
                y = sy(float(ty))
            except Exception:
                continue
            pct = float(ty)
            color = "#dc2626" if pct >= 10 else ("#10b981" if pct >= 0 else "#1d4ed8")
            points.append(f"<circle cx={x:.1f} cy={y:.1f} r=11 fill=\"{color}\" fill-opacity=\"0.9\" />")
            val_text = f"({int(round(float(cx))):,}, {pct:.1f}%)"
            labels.append(
                f"<text x={x:.1f} y={y-22:.1f} font-size=\"14\" text-anchor=\"middle\" fill=\"{color}\">"
                f"<tspan x={x:.1f} dy=\"0\" font-weight=\"700\">{self._escape_html(site)}</tspan>"
                f"<tspan x={x:.1f} dy=\"14\">{self._escape_html(val_text)}</tspan>"
                f"</text>"
            )

        # 범례 추가 (오른쪽 위 구석에 탁 박아넣기, 120x80 크기)
        legend_y = padding_top + 10
        legend_x_start = padding_left + plot_w - 130  # 오른쪽에서 130px
        
        svg = f"""
<svg width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\" xmlns=\"http://www.w3.org/2000/svg\">\n  <rect x=\"1\" y=\"1\" width=\"{width-2}\" height=\"{height-2}\" fill=\"#fff\" stroke=\"#e5e7eb\" rx=\"10\" />\n  {''.join(grid_parts)}\n  {''.join(axis_parts)}\n  {''.join(divider_parts)}\n  {mid_label}\n  {''.join(points)}\n  {''.join(labels)}\n  <text x=\"{padding_left/2}\" y=\"{padding_top+plot_h/2}\" transform=\"rotate(-90 {padding_left/2},{padding_top+plot_h/2})\" font-size=\"19\" font-weight=\"600\" fill=\"#374151\" text-anchor=\"middle\">증감률 (%)</text>\n  <text x=\"{padding_left+plot_w/2}\" y=\"{height-30}\" font-size=\"19\" font-weight=\"600\" fill=\"#374151\" text-anchor=\"middle\">방문객 수 (명)</text>\n  
  <!-- 범례 -->
  <rect x=\"{legend_x_start}\" y=\"{legend_y}\" width=\"120\" height=\"70\" fill=\"#f9fafb\" stroke=\"#e5e7eb\" rx=\"5\" />
  <!-- 고성장 (10% 이상) -->
  <rect x=\"{legend_x_start + 10}\" y=\"{legend_y + 12}\" width=\"10\" height=\"10\" fill=\"#dc2626\" />
  <text x=\"{legend_x_start + 25}\" y=\"{legend_y + 20}\" font-size=\"11\" fill=\"#374151\">고성장 (10%+)</text>
  <!-- 안정성장 (0~10%) -->
  <rect x=\"{legend_x_start + 10}\" y=\"{legend_y + 32}\" width=\"10\" height=\"10\" fill=\"#10b981\" />
  <text x=\"{legend_x_start + 25}\" y=\"{legend_y + 40}\" font-size=\"11\" fill=\"#374151\">안정성장 (0~10%)</text>
  <!-- 하락 (0% 이하) -->
  <rect x=\"{legend_x_start + 10}\" y=\"{legend_y + 52}\" width=\"10\" height=\"10\" fill=\"#1d4ed8\" />
  <text x=\"{legend_x_start + 25}\" y=\"{legend_y + 60}\" font-size=\"11\" fill=\"#374151\">하락 (0% 이하)</text>
</svg>\n"""

        return f"""
<section class=\"card\">
  <div class=\"card-header\">
    <h3>매장 성과</h3>
    <p class=\"card-subtitle\">방문객 수와 전 기간 대비 방문객 증감률을 기준으로 매장별 성과와 위치를 한눈에 확인할 수 있습니다.</p>
  </div>
  <div style=\"text-align: center; margin-top: 16px;\">{svg}</div>
</section>
""".replace("{svg}", svg)

    def _build_next_actions_card_html(self, rows: List[Dict[str, Optional[float]]], llm_summary: str, end_iso: Optional[str] = None) -> str:
        # LLM 기반 동적 페어 추천
        # 테이블 텍스트 구성: 매장\t금주방문객\t전주방문객\t평일%\t주말%\t총%\t최근4주총%
        lines: List[str] = ["매장\t금주방문객\t전주방문객\t평일%\t주말%\t총%\t최근4주총%"]
        def fmt_pct(v: Optional[float]) -> str:
            return "" if v is None else f"{float(v):.1f}%"
        def fmt_int(v: Optional[float]) -> str:
            return "" if v is None else f"{int(v):,}"
        for r in rows:
            site = str(r.get("site", ""))
            curr = fmt_int(r.get("curr_total"))
            prev = fmt_int(r.get("prev_total"))
            wd = fmt_pct(r.get("weekday_delta_pct"))
            we = fmt_pct(r.get("weekend_delta_pct"))
            tot = fmt_pct(r.get("total_delta_pct"))
            series_str = ""
            if end_iso:
                try:
                    weekly = fetch_weekly_series(site, end_iso, weeks=5)
                    s_tot = to_pct_series(weekly.get("total", []))[-4:]
                    while len(s_tot) < 4:
                        s_tot.insert(0, 0.0)
                    series_str = "|".join(f"{v:.1f}%" for v in s_tot)
                except Exception:
                    series_str = ""
            lines.append("\t".join([site, curr, prev, wd, we, tot, series_str]))

        table_text = "\n".join(lines)
        content = ""
        try:
            prompt = self._pair_prompt_tpl.format(table_text=table_text)
            resp = self.llm.invoke(prompt)
            content = (resp.content or "").strip()
            
            # 코드펜스 제거
            if content.startswith("```") and content.endswith("```"):
                content = "\n".join(content.splitlines()[1:-1]).strip()
            
            # HTML 그대로 or <li>만 온 경우 감싸기
            if "<ul" in content and "<li" in content:
                pass # Use as is if HTML
            elif content.startswith("<li") and "</li>" in content:
                content = f"<ul class=\"pair-list\">{content}</ul>"
            else:
                # 마크다운 불릿을 HTML 리스트로 변환
                lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
                items = []
                for ln in lines:
                    if ln.startswith("- "):
                        items.append(ln[2:].strip())
                    else:
                        items.append(ln)
                li_html = "\n".join(f"<li>{self._escape_html(it)}</li>" for it in items)
                content = f"<ul class=\"pair-list\">{li_html}</ul>"
        except Exception as e:
            # 실패 시 기본 안내문
            content = """
            <div style="text-align: center; padding: 20px; color: #6b7280;">
              <p style="margin: 0; font-size: 14px;">🔄 <strong>매장 페어 추천</strong></p>
              <p style="margin: 8px 0 0 0; font-size: 12px;">AI가 매장별 성과를 분석하여<br>비교 분석 대상 페어를 추천합니다</p>
            </div>
            """

        return f"""
<section class="card">
  <h3>다음 단계</h3>
  <p class="card-subtitle">방문객 수가 유사하고, 전주 대비 방문객 증감률이 반대 흐름을 보이는 두 매장을 비교합니다.</p>
  {content}
  <!-- section:next -->
</section>
"""

    def _build_summary_card_html(self, rows: List[Dict[str, Optional[float]]], llm_summary: str) -> str:
        # 디버깅을 위한 로그 추가
        print(f"DEBUG: _build_summary_card_html 호출됨")
        print(f"DEBUG: llm_summary 길이: {len(llm_summary) if llm_summary else 0}")
        print(f"DEBUG: llm_summary 내용: {llm_summary[:200] if llm_summary else 'None'}...")
        
        # LLM 요약을 HTML로 렌더링
        if llm_summary and llm_summary.strip():
            raw = llm_summary.strip()
            # 코드펜스 제거
            if raw.startswith("```") and raw.endswith("```"):
                raw = "\n".join(raw.splitlines()[1:-1]).strip()
            # HTML 그대로 사용 (불릿은 CSS로 제거)
            if "<ul" in raw and "<li" in raw:
                content = raw
            elif raw.startswith("<li") and "</li>" in raw:
                content = f"<ul class=\"summary-list\">{raw}</ul>"
            else:
                # 마크다운 불릿을 HTML li 태그로 변환
                lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
                items = []
                for ln in lines:
                    if ln.startswith("- "):
                        items.append(ln[2:].strip())
                    else:
                        items.append(ln)
                li_html = "\n".join(f"<li>{self._escape_html(it)}</li>" for it in items)
                content = f"<ul class=\"summary-list\">{li_html}</ul>"
                print(f"DEBUG: 마크다운을 HTML로 변환")
        else:
            content = """
            <div style="text-align: center; padding: 12px; color: #6b7280;">
              <p style="margin: 0; font-size: 14px;">📊 <strong>AI 분석 요약</strong></p>
              <p style="margin: 6px 0 0 0; font-size: 12px;">매장별 방문 데이터를 분석하여<br>핵심 인사이트를 제공합니다</p>
            </div>
            """
            print(f"DEBUG: 기본 안내문 사용")

        return f"""
<section class="card"> 
  <h3 style="margin: 0 0 8px 0;">요약</h3>
  <div style="margin-top: 0;">
    {content}
  </div>
  <!-- section:summary -->
</section>
"""

    def _build_table_html(self, rows: List[Dict[str, Optional[float]]], end_iso: str, days: int, state: VisitorSummaryState) -> str:
        # 공통 스케일 계산을 위해 모든 시리즈 수집
        collected: List[Tuple[Dict[str, Optional[float]], RenderSeries]] = []
        minmax = {
            "wd_min": None, "wd_max": None,
            "we_min": None, "we_max": None,
            "tot_min": None, "tot_max": None,
        }  # type: ignore

        for r in rows:
            site = str(r.get("site", ""))
            try:
                if state["compare_lag"] == 7 and days == 1:
                    # 1일 모드: 같은 요일 데이터만 가져와서 스파크라인 생성
                    weekly = fetch_same_weekday_series(site, end_iso, weeks=5)
                    s_tot = to_pct_series(weekly.get("total", []))[-4:] if len(weekly.get("total", [])) >= 4 else [0] * 4
                    s_wd = [0] * 4  # 1일 모드에서는 평일/주말 스파크라인 없음
                    s_we = [0] * 4
                    # 4포인트 보장
                    while len(s_tot) < 4:
                        s_tot.insert(0, 0.0)
                    s_tot = s_tot[-4:]
                else:
                    # 7일 모드: 기존 주간 데이터 사용
                    weekly = fetch_weekly_series(site, end_iso, weeks=5)
                    s_wd = to_pct_series(weekly.get("weekday", []))[-4:]
                    s_we = to_pct_series(weekly.get("weekend", []))[-4:]
                    s_tot = to_pct_series(weekly.get("total", []))[-4:]
                    # 최소 4포인트 보장
                    while len(s_wd) < 4:
                        s_wd.insert(0, 0.0)
                    while len(s_we) < 4:
                        s_we.insert(0, 0.0)
                    while len(s_tot) < 4:
                        s_tot.insert(0, 0.0)
            except Exception:
                if state["compare_lag"] == 7 and days == 1:
                    s_wd = [0.0] * 7
                    s_we = [0.0] * 7
                    s_tot = [0.0] * 7
                else:
                    s_wd = [0.0, 0.0, 0.0, 0.0]
                    s_we = [0.0, 0.0, 0.0, 0.0]
                    s_tot = [0.0, 0.0, 0.0, 0.0]

            collected.append((r, RenderSeries(s_wd, s_we, s_tot)))
            for label, series in (("wd", s_wd), ("we", s_we), ("tot", s_tot)):
                for v in series:
                    key_min = f"{label}_min"
                    key_max = f"{label}_max"
                    if minmax[key_min] is None or v < minmax[key_min]:
                        minmax[key_min] = v
                    if minmax[key_max] is None or v > minmax[key_max]:
                        minmax[key_max] = v

        # 헤더
        # timedelta는 이미 상단에서 import됨
        end_d = date.fromisoformat(end_iso)
        curr_start = end_d - timedelta(days=days - 1)
        curr_end = end_d
        prev_start = end_d - timedelta(days=(2 * days - 1))
        # 1일 모드에서는 7일 전 같은 요일, 다른 모드에서는 기간만큼 이전
        if state["compare_lag"] == 7 and days == 1:
            prev_end = end_d - timedelta(days=7)  # 전주 같은 요일
        else:
            prev_end = end_d - timedelta(days=days)
        
        # periods=1이면 단일 날짜, 아니면 범위 표시
        if state["compare_lag"] == 7 and days == 1:
            curr_range = curr_end.isoformat()
            prev_range = prev_end.isoformat()
        else:
            curr_range = f"{curr_start.isoformat()}<br>~ {curr_end.isoformat()}"
            prev_range = f"{prev_start.isoformat()}<br>~ {prev_end.isoformat()}"
        # periods=1일 때는 평일/주말 분류가 의미없으므로 컬럼 구조 변경
        if state["compare_lag"] == 7 and days == 1:
            period_type = "일자별"
            header_html = """
<section class=\"card\">
  <div class=\"card-header\">
    <h3>방문객 증감 요약</h3>
    <p class=\"card-subtitle\">{period_label}과 {prev_label} 대비를 비교해 매장별 방문 추세를 한눈에 파악합니다.</p>
  </div>
  <div class=\"table-wrap\">
    <table class=\"table\">
      <thead>
        <tr>
          <th>매장명</th>
          <th>{period_label} 방문객<div class=\"col-note\">{curr_range}</div></th>
          <th>{prev_label} 방문객<div class=\"col-note\">{prev_range}</div></th>
          <th>증감률</th>
          <th>주간 증감률 추이</th>
        </tr>
      </thead>
      <tbody>
"""
        else:
            period_type = "주차별"
            header_html = """
<section class=\"card\">
  <div class=\"card-header\">
    <h3>방문객 증감 요약</h3>
    <p class=\"card-subtitle\">최근 {days}일과 전 기간 대비를 비교해 매장별 방문 추세와 최근 4주의 변동을 한눈에 파악합니다.</p>
  </div>
  <div class=\"table-wrap\">
    <table class=\"table\">
      <thead>
        <tr>
          <th>매장명</th>
          <th>{period_label} 방문객<div class=\"col-note\">{curr_range}</div></th>
          <th>{prev_label} 방문객<div class=\"col-note\">{prev_range}</div></th>
          <th>평일<br>증감률</th>
          <th>주말<br>증감률</th>
          <th>총<br>증감률</th>
          <th class=\"sep-left\">주차별 평일<br>증감률<div class=\"col-note\">max: {wd_max}%<br>min: {wd_min}%</div></th>
          <th>주차별 주말<br>증감률<div class=\"col-note\">max: {we_max}%<br>min: {we_min}%</div></th>
          <th>주차별 총<br>증감률<div class=\"col-note\">max: {tot_max}%<br>min: {tot_min}%</div></th>
        </tr>
      </thead>
      <tbody>
"""
        
        # 템플릿 변수 치환
        header = header_html.replace("{curr_range}", curr_range).replace("{prev_range}", prev_range).replace(
            "{period_label}", state["period_label"]).replace("{prev_label}", state["prev_label"]).replace("{days}", str(days))
        
        # 주간 모드일 때만 min/max 값 치환
        if not (state["compare_lag"] == 1 and days == 1):
            header = header.replace(
                "{wd_min}", f"{(minmax['wd_min'] or 0):.1f}"
            ).replace("{wd_max}", f"{(minmax['wd_max'] or 0):.1f}").replace(
                "{we_min}", f"{(minmax['we_min'] or 0):.1f}"
            ).replace("{we_max}", f"{(minmax['we_max'] or 0):.1f}").replace(
                "{tot_min}", f"{(minmax['tot_min'] or 0):.1f}"
            ).replace("{tot_max}", f"{(minmax['tot_max'] or 0):.1f}")

        # 바디
        body_rows: List[str] = []
        for r, ser in collected:
            if state["compare_lag"] == 7 and days == 1:
                # 일자별 모드: 총 증감률 + 7일 스파크라인 표시
                row_html = """
        <tr>
          <td>{site}</td>
          <td class="num">{curr}</td>
          <td class="num">{prev}</td>
          <td class="num"><b><span class="{tot_cls}">{tot}</span></b></td>
          <td class="num"><div class="pct-with-chart"><span class="spark">{spark_daily}</span></div></td>
        </tr>
"""
                body_rows.append(
                    row_html
                    .replace("{site}", str(r.get("site", "")))
                    .replace("{curr}", self._fmt_int(r.get("curr_total")))
                    .replace("{prev}", self._fmt_int(r.get("prev_total")))
                    .replace("{tot}", self._fmt_pct(r.get("total_delta_pct")))
                    .replace(
                        "{tot_cls}",
                        "pct-pos"
                        if (r.get("total_delta_pct") or 0) > 0
                        else ("pct-neg" if (r.get("total_delta_pct") or 0) < 0 else "pct-zero"),
                    )
                    .replace("{spark_daily}", svg_sparkline(ser.total))  # 7일간 총 증감률 사용
                )
            else:
                # 주간 모드: 기존 전체 컬럼 표시
                row_html = """
        <tr>
          <td>{site}</td>
          <td class="num">{curr}</td>
          <td class="num">{prev}</td>
          <td class="num"><span class="{wd_cls}">{wd}</span></td>
          <td class="num"><span class="{we_cls}">{we}</span></td>
          <td class="num sep-right"><b><span class="{tot_cls}">{tot}</span></b></td>
          <td class="num sep-left"><div class="pct-with-chart"><span class="spark">{spark_wd}</span></div></td>
          <td class="num"><div class="pct-with-chart"><span class="spark">{spark_we}</span></div></td>
          <td class="num"><div class="pct-with-chart"><span class="spark">{spark_tot}</span></div></td>
        </tr>
"""
                body_rows.append(
                    row_html
                    .replace("{site}", str(r.get("site", "")))
                    .replace("{curr}", self._fmt_int(r.get("curr_total")))
                    .replace("{prev}", self._fmt_int(r.get("prev_total")))
                    .replace("{wd}", self._fmt_pct(r.get("weekday_delta_pct")))
                    .replace("{we}", self._fmt_pct(r.get("weekend_delta_pct")))
                    .replace("{tot}", self._fmt_pct(r.get("total_delta_pct")))
                    .replace(
                        "{wd_cls}",
                        "pct-pos"
                        if (r.get("weekday_delta_pct") or 0) > 0
                        else ("pct-neg" if (r.get("weekday_delta_pct") or 0) < 0 else "pct-zero"),
                    )
                    .replace(
                        "{we_cls}",
                        "pct-pos"
                        if (r.get("weekend_delta_pct") or 0) > 0
                        else ("pct-neg" if (r.get("weekend_delta_pct") or 0) < 0 else "pct-zero"),
                    )
                    .replace(
                        "{tot_cls}",
                        "pct-pos"
                        if (r.get("total_delta_pct") or 0) > 0
                        else ("pct-neg" if (r.get("total_delta_pct") or 0) < 0 else "pct-zero"),
                    )
                    .replace("{spark_wd}", svg_sparkline(ser.weekday))
                    .replace("{spark_we}", svg_sparkline(ser.weekend))
                    .replace("{spark_tot}", svg_sparkline(ser.total))
                )

        footer = """
      </tbody>
    </table>
  </div>
  <!-- section:table -->
</section>
"""
        return header + "\n".join(body_rows) + footer

    # ----------------------------- Utils -----------------------------
    @staticmethod
    def _fmt_int(v: Optional[float]) -> str:
        return "" if v is None else f"{int(v):,}"

    @staticmethod
    def _fmt_pct(v: Optional[float]) -> str:
        if v is None:
            return ""
        elif v > 0:
            return f"+{float(v):.1f}%"
        else:
            return f"{float(v):.1f}%"

    @staticmethod
    def _get_weekday_korean(date_iso: str) -> str:
        """날짜 문자열에서 요일을 한글로 반환"""
        from datetime import datetime
        try:
            date_obj = datetime.fromisoformat(date_iso)
            weekday_num = date_obj.weekday()  # 0=월요일, 6=일요일
            weekdays_kr = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"]
            return weekdays_kr[weekday_num]
        except Exception:
            return ""

    @staticmethod
    def _escape_html(text: str) -> str:
        return (
            (text or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )


# ----------------------------- CLI Utils -----------------------------
def clamp_end_date_to_yesterday(end_date_iso: str) -> str:
    """기준일이 오늘이거나 미래인 경우 어제로 조정"""
    end_d = date.fromisoformat(end_date_iso)
    today = date.today()
    if end_d >= today:
        return (today - timedelta(days=1)).isoformat()
    return end_date_iso


def _build_sql_period_agg(end_date_iso: str, days: int) -> str:
    """ClickHouse SQL: 주기(days) 단위로 최근/이전 동일기간 합계 및 평일/주말 분리 집계"""
    return f"""
WITH
  toDate('{end_date_iso}') AS req_end,
  if(req_end >= today(), addDays(today(), -1), req_end) AS target_end,
  {days} AS win,
  addDays(target_end, -(win-1))          AS curr_start,
  addDays(target_end, -(2*win-1))        AS prev_start,
  addDays(target_end, -win)              AS prev_end,

  base AS (
    SELECT lioi.date, lioi.person_seq
    FROM line_in_out_individual AS lioi
    INNER JOIN line AS l
      ON l.id = lioi.triggered_line_id
     AND l.entrance = 1
    WHERE lioi.date BETWEEN prev_start AND target_end
      AND lioi.is_staff = 0
      AND upper(lioi.in_out) = 'IN'
  ),
  daily AS (
    SELECT date, uniqExact(person_seq) AS uv
    FROM base
    GROUP BY date
  ),
  labeled AS (
    SELECT
      date,
      uv,
      if(date BETWEEN curr_start AND target_end, 1, 0) AS is_curr,
      if(date BETWEEN prev_start AND prev_end,   1, 0) AS is_prev,
      if(toDayOfWeek(date) IN (6, 7), 'weekend', 'weekday') AS day_type
    FROM daily
    WHERE date BETWEEN prev_start AND target_end
  ),
  agg AS (
    SELECT
      sumIf(uv, is_curr = 1)                                        AS curr_total,
      sumIf(uv, is_prev = 1)                                        AS prev_total,
      sumIf(uv, is_curr = 1 AND day_type = 'weekday')               AS curr_weekday_total,
      sumIf(uv, is_prev = 1 AND day_type = 'weekday')               AS prev_weekday_total,
      sumIf(uv, is_curr = 1 AND day_type = 'weekend')               AS curr_weekend_total,
      sumIf(uv, is_prev = 1 AND day_type = 'weekend')               AS prev_weekend_total
    FROM labeled
  )
SELECT
  curr_total,
  prev_total,
  curr_weekday_total,
  prev_weekday_total,
  curr_weekend_total,
  prev_weekend_total,
  if(prev_weekday_total = 0, NULL,
     (curr_weekday_total - prev_weekday_total) / prev_weekday_total * 100) AS weekday_delta_pct,
  if(prev_weekend_total = 0, NULL,
     (curr_weekend_total - prev_weekend_total) / prev_weekend_total * 100) AS weekend_delta_pct,
  if(prev_total = 0, NULL,
     (curr_total - prev_total) / prev_total * 100)                      AS total_delta_pct
FROM agg
"""


def _build_sql_weekly_series(end_date_iso: str, num_weeks: int = 5) -> str:
    """ClickHouse SQL: 최근 주차별(week_idx 0=금주, 1=전주, ...) 합계 산출"""
    return f"""
WITH
  toDate('{end_date_iso}') AS req_end,
  if(req_end >= today(), addDays(today(), -1), req_end) AS target_end,
  {num_weeks} AS wcnt,
  base AS (
    SELECT lioi.date, lioi.person_seq
    FROM line_in_out_individual AS lioi
    INNER JOIN line AS l
      ON l.id = lioi.triggered_line_id
     AND l.entrance = 1
    WHERE lioi.date <= target_end
      AND lioi.date >= addDays(target_end, -90) -- 안전범위(약 3개월)로 제한
      AND lioi.is_staff = 0
      AND upper(lioi.in_out) = 'IN'
  ),
  daily AS (
    SELECT date, uniqExact(person_seq) AS uv
    FROM base
    GROUP BY date
  ),
  weekly AS (
    SELECT
      toYearWeek(date) AS yearweek,
      toStartOfWeek(date) AS week_start,
      sumIf(uv, toDayOfWeek(date) IN (6, 7)) AS weekend_total,
      sumIf(uv, toDayOfWeek(date) NOT IN (6, 7)) AS weekday_total,
      sum(uv) AS total_total
    FROM daily
    GROUP BY yearweek, week_start
    ORDER BY week_start DESC
    LIMIT wcnt
  ),
  indexed AS (
    SELECT
      row_number() OVER (ORDER BY week_start DESC) - 1 AS week_idx,
      weekend_total,
      weekday_total,
      total_total
    FROM weekly
  )
SELECT week_idx, weekday_total, weekend_total, total_total
FROM indexed
ORDER BY week_idx ASC
"""


def _build_sql_daily_same_weekday_agg(end_date_iso: str) -> str:
    """1일 모드: 당일 vs 전주 같은 요일 비교 SQL"""
    return f"""
WITH
  toDate('{end_date_iso}') AS req_end,
  if(req_end >= today(), addDays(today(), -1), req_end) AS target_end,
  addDays(target_end, -7) AS prev_same_weekday,

  base AS (
    SELECT lioi.date, lioi.person_seq
    FROM line_in_out_individual AS lioi
    INNER JOIN line AS l
      ON l.id = lioi.triggered_line_id
     AND l.entrance = 1
    WHERE lioi.date IN (target_end, prev_same_weekday)
      AND lioi.is_staff = 0
      AND upper(lioi.in_out) = 'IN'
  ),
  daily AS (
    SELECT date, uniqExact(person_seq) AS uv
    FROM base
    GROUP BY date
  ),
  agg AS (
    SELECT
      sumIf(uv, date = target_end) AS curr_total,
      sumIf(uv, date = prev_same_weekday) AS prev_total
    FROM daily
  )
SELECT
  curr_total,
  prev_total,
  0 AS curr_weekday_total,  -- 1일 모드에서는 평일/주말 구분 없음
  0 AS prev_weekday_total,
  0 AS curr_weekend_total,
  0 AS prev_weekend_total,
  NULL AS weekday_delta_pct,  -- 1일 모드에서는 평일/주말 증감률 없음
  NULL AS weekend_delta_pct,
  if(prev_total = 0, NULL,
     (curr_total - prev_total) / prev_total * 100) AS total_delta_pct
FROM agg
"""


def summarize_period_rates(site: str, end_date_iso: str, days: int) -> Dict[str, Optional[float]]:
    """지정된 기간에 대한 매장별 증감률 데이터를 가져온다"""
    # 1일 모드에서는 전주 같은 요일과 비교
    if days == 1:
        sql = _build_sql_daily_same_weekday_agg(end_date_iso)
    else:
        sql = _build_sql_period_agg(end_date_iso, days)
    client = get_site_client(site)
    if not client:
        raise RuntimeError(f"Failed to get client for site: {site}")
    try:
        res = client.query(sql)
        rows = res.result_rows or []
        if not rows:
            return {
                "site": site,
                "end_date": end_date_iso,
                "curr_total": 0,
                "prev_total": 0,
                "weekday_delta_pct": None,
                "weekend_delta_pct": None,
                "total_delta_pct": None,
            }
        (
            curr_total,
            prev_total,
            _curr_weekday_total,
            _prev_weekday_total,
            _curr_weekend_total,
            _prev_weekend_total,
            weekday_delta_pct,
            weekend_delta_pct,
            total_delta_pct,
        ) = rows[0]

        _today = date.today()
        target_end = min(date.fromisoformat(end_date_iso), _today - timedelta(days=1))
        return {
            "site": site,
            "end_date": target_end.isoformat(),
            "curr_total": int(curr_total or 0),
            "prev_total": int(prev_total or 0),
            "weekday_delta_pct": None if weekday_delta_pct is None else round(float(weekday_delta_pct), 2),
            "weekend_delta_pct": None if weekend_delta_pct is None else round(float(weekend_delta_pct), 2),
            "total_delta_pct": None if total_delta_pct is None else round(float(total_delta_pct), 2),
        }
    finally:
        try:
            client.close()
        except Exception:
            pass


def fetch_daily_series(site: str, end_date_iso: str, days: int = 7) -> Dict[str, List[int]]:
    """일별 방문 합계 시리즈를 가져온다 (1일 모드용)"""
    sql = f"""
WITH
  toDate('{end_date_iso}') AS req_end,
  if(req_end >= today(), addDays(today(), -1), req_end) AS target_end,
  addDays(target_end, -{days-1}) AS start_date,

  base AS (
    SELECT lioi.date, lioi.person_seq
    FROM line_in_out_individual AS lioi
    INNER JOIN line AS l
      ON l.id = lioi.triggered_line_id
     AND l.entrance = 1
    WHERE lioi.date BETWEEN start_date AND target_end
      AND lioi.is_staff = 0
      AND upper(lioi.in_out) = 'IN'
  ),
  daily AS (
    SELECT date, uniqExact(person_seq) AS uv
    FROM base
    GROUP BY date
  ),
  all_dates AS (
    SELECT arrayJoin(range(toUInt32(start_date), toUInt32(target_end) + 1)) AS date_num
  ),
  filled AS (
    SELECT 
      toDate(date_num) AS date,
      coalesce(d.uv, 0) AS uv,
      if(toDayOfWeek(toDate(date_num)) IN (6, 7), 'weekend', 'weekday') AS day_type
    FROM all_dates a
    LEFT JOIN daily d ON d.date = toDate(date_num)
    ORDER BY date
  )
SELECT 
  groupArray(uv) AS total_series,
  groupArray(if(day_type = 'weekday', uv, 0)) AS weekday_series,
  groupArray(if(day_type = 'weekend', uv, 0)) AS weekend_series
FROM filled
"""
    
    client = get_site_client(site)
    if not client:
        return {"total": [0] * days, "weekday": [0] * days, "weekend": [0] * days}
    
    try:
        result = client.execute(sql)
        if result:
            row = result[0]
            return {
                "total": list(row[0]),
                "weekday": list(row[1]), 
                "weekend": list(row[2])
            }
    except Exception:
        pass
    
    return {"total": [0] * days, "weekday": [0] * days, "weekend": [0] * days}


def fetch_weekly_series(site: str, end_date_iso: str, weeks: int = 4) -> Dict[str, List[int]]:
    """7일 기간별 방문 합계 시리즈를 가져온다 (테이블과 동일한 기준)"""
    # 7일 기간으로 5개 기간 데이터 가져오기
    sql = f"""
WITH
  toDate('{end_date_iso}') AS req_end,
  if(req_end >= today(), addDays(today(), -1), req_end) AS target_end,
  7 AS win,
  addDays(target_end, -(win-1))          AS curr_start,
  addDays(target_end, -(2*win-1))        AS prev_start,
  addDays(target_end, -win)              AS prev_end,
  addDays(target_end, -(3*win-1))        AS prev2_start,
  addDays(target_end, -(2*win))          AS prev2_end,
  addDays(target_end, -(4*win-1))        AS prev3_start,
  addDays(target_end, -(3*win))          AS prev3_end,
  addDays(target_end, -(5*win-1))        AS prev4_start,
  addDays(target_end, -(4*win))          AS prev4_end,

  base AS (
    SELECT lioi.date, lioi.person_seq
    FROM line_in_out_individual AS lioi
    INNER JOIN line AS l
      ON l.id = lioi.triggered_line_id
     AND l.entrance = 1
    WHERE lioi.date BETWEEN prev4_start AND target_end
      AND lioi.is_staff = 0
      AND upper(lioi.in_out) = 'IN'
  ),
  daily AS (
    SELECT date, uniqExact(person_seq) AS uv
    FROM base
    GROUP BY date
  ),
  labeled AS (
    SELECT
      date,
      uv,
      if(date BETWEEN curr_start AND target_end, 1, 0) AS is_curr,
      if(date BETWEEN prev_start AND prev_end, 1, 0) AS is_prev,
      if(date BETWEEN prev2_start AND prev2_end, 1, 0) AS is_prev2,
      if(date BETWEEN prev3_start AND prev3_end, 1, 0) AS is_prev3,
      if(date BETWEEN prev4_start AND prev4_end, 1, 0) AS is_prev4,
      if(toDayOfWeek(date) IN (6, 7), 'weekend', 'weekday') AS day_type
    FROM daily
    WHERE date BETWEEN prev4_start AND target_end
  ),
  agg AS (
    SELECT
      sumIf(uv, is_curr = 1) AS curr_total,
      sumIf(uv, is_prev = 1) AS prev_total,
      sumIf(uv, is_prev2 = 1) AS prev2_total,
      sumIf(uv, is_prev3 = 1) AS prev3_total,
      sumIf(uv, is_prev4 = 1) AS prev4_total,
      sumIf(uv, is_curr = 1 AND day_type = 'weekday') AS curr_weekday,
      sumIf(uv, is_prev = 1 AND day_type = 'weekday') AS prev_weekday,
      sumIf(uv, is_prev2 = 1 AND day_type = 'weekday') AS prev2_weekday,
      sumIf(uv, is_prev3 = 1 AND day_type = 'weekday') AS prev3_weekday,
      sumIf(uv, is_prev4 = 1 AND day_type = 'weekday') AS prev4_weekday,
      sumIf(uv, is_curr = 1 AND day_type = 'weekend') AS curr_weekend,
      sumIf(uv, is_prev = 1 AND day_type = 'weekend') AS prev_weekend,
      sumIf(uv, is_prev2 = 1 AND day_type = 'weekend') AS prev2_weekend,
      sumIf(uv, is_prev3 = 1 AND day_type = 'weekend') AS prev3_weekend,
      sumIf(uv, is_prev4 = 1 AND day_type = 'weekend') AS prev4_weekend
    FROM labeled
  )
SELECT
  curr_total, prev_total, prev2_total, prev3_total, prev4_total,
  curr_weekday, prev_weekday, prev2_weekday, prev3_weekday, prev4_weekday,
  curr_weekend, prev_weekend, prev2_weekend, prev3_weekend, prev4_weekend
FROM agg
"""
    
    client = get_site_client(site)
    if not client:
        raise RuntimeError(f"Failed to get client for site: {site}")
    try:
        res = client.query(sql)
        rows = list(res.result_rows or [])
        if not rows:
            return {"weekday": [0, 0, 0, 0, 0], "weekend": [0, 0, 0, 0, 0], "total": [0, 0, 0, 0, 0]}
        
        row = rows[0]
        (curr_total, prev_total, prev2_total, prev3_total, prev4_total,
         curr_weekday, prev_weekday, prev2_weekday, prev3_weekday, prev4_weekday,
         curr_weekend, prev_weekend, prev2_weekend, prev3_weekend, prev4_weekend) = row
        
        # 과거부터 최신 순서로 정렬 (to_pct_series와 맞추기 위해)
        values_tot = [prev4_total, prev3_total, prev2_total, prev_total, curr_total]
        values_wd = [prev4_weekday, prev3_weekday, prev2_weekday, prev_weekday, curr_weekday]
        values_we = [prev4_weekend, prev3_weekend, prev2_weekend, prev_weekend, curr_weekend]
        
        return {"weekday": values_wd, "weekend": values_we, "total": values_tot}
    finally:
        try:
            client.close()
        except Exception:
            pass


def fetch_same_weekday_series(site: str, end_date_iso: str, weeks: int = 4) -> Dict[str, List[int]]:
    """1일 모드: 같은 요일 데이터만 가져와서 스파크라인용 시리즈 생성"""
    # 기준일의 요일을 구해서, 과거 4주간의 같은 요일 데이터만 가져오기
    sql = f"""
WITH
  toDate('{end_date_iso}') AS req_end,
  if(req_end >= today(), addDays(today(), -1), req_end) AS target_end,
  toDayOfWeek(target_end) AS target_weekday,
  
  -- 과거 4주간의 같은 요일 날짜들
  addDays(target_end, -7) AS prev_week_same_day,
  addDays(target_end, -14) AS prev2_week_same_day,
  addDays(target_end, -21) AS prev3_week_same_day,
  addDays(target_end, -28) AS prev4_week_same_day,

  base AS (
    SELECT lioi.date, lioi.person_seq
    FROM line_in_out_individual AS lioi
    INNER JOIN line AS l
      ON l.id = lioi.triggered_line_id
     AND l.entrance = 1
    WHERE lioi.date IN (target_end, prev_week_same_day, prev2_week_same_day, prev3_week_same_day, prev4_week_same_day)
      AND lioi.is_staff = 0
      AND upper(lioi.in_out) = 'IN'
  ),
  daily AS (
    SELECT date, uniqExact(person_seq) AS uv
    FROM base
    GROUP BY date
  ),
  agg AS (
    SELECT
      sumIf(uv, date = target_end) AS curr_total,
      sumIf(uv, date = prev_week_same_day) AS prev_total,
      sumIf(uv, date = prev2_week_same_day) AS prev2_total,
      sumIf(uv, date = prev3_week_same_day) AS prev3_total,
      sumIf(uv, date = prev4_week_same_day) AS prev4_total
    FROM daily
  )
SELECT
  curr_total, prev_total, prev2_total, prev3_total, prev4_total
FROM agg
"""
    
    client = get_site_client(site)
    if not client:
        raise RuntimeError(f"Failed to get client for site: {site}")
    try:
        res = client.query(sql)
        rows = list(res.result_rows or [])
        if not rows:
            return {"total": [0, 0, 0, 0, 0]}
        
        row = rows[0]
        (curr_total, prev_total, prev2_total, prev3_total, prev4_total) = row
        
        # 과거부터 최신 순서로 정렬 (to_pct_series와 맞추기 위해)
        values_tot = [prev4_total, prev3_total, prev2_total, prev_total, curr_total]
        
        return {"total": values_tot}
    finally:
        try:
            client.close()
        except Exception:
            pass


def _collect_rows_for_period(stores: Sequence[str], end_iso: str, days: int) -> Tuple[List[Dict[str, Optional[float]]], str]:
    """지정된 기간에 대한 매장별 데이터를 수집"""
    rows: List[Dict[str, Optional[float]]] = []
    for st in stores:
        try:
            summ = summarize_period_rates(st, end_iso, days)
        except Exception:
            summ = {
                "site": st,
                "end_date": end_iso,
                "curr_total": None,
                "prev_total": None,
                "weekday_delta_pct": None,
                "weekend_delta_pct": None,
                "total_delta_pct": None,
            }
        rows.append(
            {
                "site": summ.get("site", st),
                "curr_total": summ.get("curr_total"),
                "prev_total": summ.get("prev_total"),
                "weekday_delta_pct": summ.get("weekday_delta_pct"),
                "weekend_delta_pct": summ.get("weekend_delta_pct"),
                "total_delta_pct": summ.get("total_delta_pct"),
            }
        )
    return rows, end_iso


# ----------------------------- FastMCP Tool -----------------------------
from fastmcp import FastMCP

mcp = FastMCP("visitor_summary")


@mcp.tool()
def visitor_summary_html(
    *,
    spec: str = SPEC_VISITOR,
    end_date: str,
    stores: str | list[str],
    periods: list[int] | None = None,
    user_prompt: str = "방문 현황 요약 통계(HTML)"
) -> str:
    """
    [VISITOR_SUMMARY] Generate a visitor summary HTML report using the specified spec.

    Parameters
    ----------
    - spec: one of [visitor, touch_point, dwelling_time]; currently only 'visitor' is implemented
    - end_date: 기준일(YYYY-MM-DD)
    - stores: 매장 목록(문자열 콤마 구분 또는 리스트)
    - periods: 탭 섹션 일수 목록(예: [7, 30])
    - user_prompt: 커스텀 프롬프트(선택)
    """
    wf = VisitorSummaryWorkflow()
    return wf.run(spec=spec, end_date=end_date, stores=stores, periods=periods, user_prompt=user_prompt)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Visitor Summary Workflow Runner")
    parser.add_argument("--spec", default=SPEC_VISITOR, help="visitor | touch_point | dwelling_time")
    parser.add_argument("--end", required=True, help="기준일(YYYY-MM-DD)")
    parser.add_argument("--stores", required=True, help="콤마로 구분된 매장 문자열")
    parser.add_argument("--periods", default="7,30", help="예: '7,30' 또는 '7'")
    parser.add_argument("--cli", action="store_true", help="FastMCP 서버 대신 1회 실행")
    parser.add_argument("--out", help="출력 HTML 파일 경로")
    args = parser.parse_args()

    if args.cli:
        periods = [int(p.strip()) for p in args.periods.split(",") if p.strip()]
        wf = VisitorSummaryWorkflow()
        
        # CLI 모드에서는 HTML을 직접 생성
        end_iso = clamp_end_date_to_yesterday(args.end)
        stores_list = [s.strip() for s in args.stores.replace("，", ",").split(",") if s.strip()]
        
        # 데이터 수집
        rows_by_period = {}
        for days in periods:
            rows, _ = _collect_rows_for_period(stores_list, end_iso, days)
            rows_by_period[days] = rows
        
        # LLM 요약 생성
        print("LLM 요약 생성 중...")
        llm_summary = ""
        try:
            # 가장 짧은 기간(보통 7일)을 기준으로 요약 생성
            base_days = min(periods) if periods else 7
            base_rows = rows_by_period.get(base_days, [])
            
            # 테이블 텍스트 구성
            lines = ["매장\t금주\t전주\t평일증감%\t주말증감%\t총증감%"]
            for r in base_rows:
                lines.append(
                    "\t".join(
                        [
                            str(r.get("site", "")),
                            wf._fmt_int(r.get("curr_total")),
                            wf._fmt_int(r.get("prev_total")),
                            wf._fmt_pct(r.get("weekday_delta_pct")),
                            wf._fmt_pct(r.get("weekend_delta_pct")),
                            wf._fmt_pct(r.get("total_delta_pct")),
                        ]
                    )
                )
            
            table_text = "\n".join(lines)
            prompt = wf._summary_prompt_tpl.format(table_text=table_text)
            
            print(f"LLM 프롬프트 생성 완료: {len(table_text)} 문자")
            resp = wf.llm.invoke(prompt)
            llm_summary = (resp.content or "").strip()
            print(f"LLM 응답 성공: {len(llm_summary)} 문자")
            
        except Exception as e:
            print(f"LLM 요약 생성 실패: {e}")
            llm_summary = "요약 생성 실패"
        
        # HTML 생성
        sections = []
        for days in periods:
            rows = rows_by_period.get(days, [])
            # CLI 모드에서는 더미 state 생성
            dummy_state = {
                "periods": periods,
                "compare_lag": 7 if days == 1 else days,
                "period_label": "당일" if days == 1 else f"최근{days}일",
                "prev_label": "전주 동일 요일" if days == 1 else f"전주{days}일"
            }
            sections.append(
                wf._build_tab_section_html(
                    section_id=f"section-{days}",
                    title_suffix=f"최근 {days}일 vs 이전 {days}일",
                    end_iso=end_iso,
                    days=days,
                    rows=rows,
                    llm_summary=llm_summary,
                    state=dummy_state,
                )
            )
        
        body_html = "\n".join(sections)
        
        # daily 옵션일 때 요일 추가
        title = f"방문 현황 요약 통계({end_iso})"
        if periods == [1]:  # daily 옵션
            weekday_kr = wf._get_weekday_korean(end_iso)
            print(f"DEBUG: periods={periods}, end_iso={end_iso}, weekday_kr={weekday_kr}")
            title = f"방문 현황 요약 통계({end_iso} {weekday_kr})"
            print(f"DEBUG: 최종 제목: {title}")
        
        html = wf._build_html_page(title=title, body_html=body_html, periods=periods)
        
        # 파일로 저장
        if args.out:
            out_path = args.out
        else:
            # 저장 경로: 1일은 daily, 7일은 weekly
            if periods == [1]:
                out_dir = os.path.abspath(os.path.join("html_report", "daily"))
                file_name = f"daily_report_{end_iso}.html"
            else:
                out_dir = os.path.abspath(os.path.join("html_report", "weekly"))
                file_name = f"visitor_summary_{end_iso}.html"
            
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, file_name)
        
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)
        
        print(f"HTML 저장: {out_path}")
        
        # latest.html 갱신
        if not args.out:
            latest_path = os.path.join(out_dir, "latest.html")
            try:
                from shutil import copyfile
                copyfile(out_path, latest_path)
                print(f"Latest 동기화: {latest_path}")
            except Exception:
                pass
    else:
        print("FastMCP 서버 시작 - visitor_summary")
        mcp.run()

