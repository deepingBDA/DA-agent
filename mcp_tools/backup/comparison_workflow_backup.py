"""
Comparison Analysis Workflow (HTML Cards)

요구사항 요약:
- 매장 간 비교 분석 워크플로우로 구성
- 4개 카드 섹션: 요약카드, 일별 방문 추이, 고객 구성 변화, 시간대 연령대별 패턴
- 현재는 빈 뼈대만 구현, 추후 내용 채워넣기
- GPT-5 모델을 사용하여 비교분석 생성
"""

from __future__ import annotations
from typing import Dict, List, Any, Optional
from datetime import date, timedelta
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI


class ComparisonWorkflow:
    """비교분석 워크플로우"""
    
    def __init__(self):
        load_dotenv()
        # gpt-5, 비교분석 전용 프롬프트 사용
        # GPT-5는 temperature 파라미터를 지원하지 않으므로 제거
        self.llm = ChatOpenAI(model="gpt-5")
        self._comparison_prompt_tpl = (
            """
            당신은 리테일 방문 데이터 비교 분석가입니다. 아래 표형 텍스트(매장별 금주/전주, 평일/주말/총 증감률)를 근거로 한국어로 간결한 비교 분석을 작성하세요.

            [비교분석 요약 지침]

            1. 매장 간 성과 차이: 금주 방문객 수와 증감률을 기준으로 매장별 성과 순위를 매기고 핵심 인사이트를 도출하세요.
            2. 평일/주말 패턴 분석: 평일과 주말의 증감률 차이를 분석하여 매장별 특성을 파악하세요.
            3. 성장/하락 추세: 증감률이 높은 매장과 낮은 매장을 구분하고, 각각의 특징을 요약하세요.
            4. 개선점 제시: 성과가 낮은 매장의 개선 방향을 구체적으로 제시하세요.

            [출력 형식 + 스타일]

            - 불릿 5~7개, 각 항목 25~50자. 중복 없이 핵심만.
            - 출력 형식: 각 항목을 한 줄로, "- "로 시작하는 마크다운 불릿 목록으로만 출력하세요.
            - 내부 추론(체인 오브 쏘트)은 출력하지 마세요.

            데이터:
            {table_text}
            """
        )
    
    def run(self, sites: List[str], end_date: str, periods: List[int]) -> Dict[str, Any]:
        """비교분석 워크플로우 실행"""
        # 실제 데이터 추출 (7일간 비교 분석 데이터)
        try:
            from reports.data_extractors.comparison_extractor import ComparisonDataExtractor
            data_extractor = ComparisonDataExtractor()
            self.comparison_data = data_extractor.extract_comparison_data(
                sites=sites,
                end_date=end_date,
                days=7  # 7일간 데이터
            )
        except Exception as e:
            print(f"실제 데이터 추출 실패, 더미 데이터 사용: {e}")
            self.comparison_data = {}
        
        # 더미 데이터 생성 (매장별로 다른 패턴)
        data_by_period = {}
        for days in periods:
            data = []
            for i, site in enumerate(sites):
                # 매장별로 다른 성과 패턴 생성
                base_growth = 3.0 + (i * 1.5)  # 매장별 차등 적용
                base_visitors = 1000 + (i * 200)  # 매장별 기본 방문객 수
                
                # 평일/주말 차이 (일부 매장은 주말이 더 높음)
                weekday_factor = 1.0 if i % 2 == 0 else 1.2
                weekend_factor = 1.0 if i % 2 == 0 else 0.8
                
                data.append({
                    "site": site,
                    "end_date": end_date,
                    "curr_total": int(base_visitors * (1 + base_growth / 100)),
                    "prev_total": base_visitors,
                    "weekday_delta_pct": round(base_growth * weekday_factor, 1),
                    "weekend_delta_pct": round(base_growth * weekend_factor, 1),
                    "total_delta_pct": round(base_growth, 1),
                })
            data_by_period[days] = data
        
        # LLM 비교분석 생성
        comparison_analysis = self._generate_comparison_analysis(data_by_period, periods)
        
        # HTML 생성
        html_content = self._generate_html(sites, end_date, data_by_period, comparison_analysis)
        
        return {
            "data_by_period": data_by_period,
            "comparison_analysis": comparison_analysis,
            "html_content": html_content,
            "status": "success"
        }
    
    def _generate_comparison_analysis(self, data_by_period: Dict[int, List[Dict[str, Any]]], periods: List[int]) -> str:
        """LLM을 사용하여 비교분석 생성"""
        try:
            # 첫 번째 기간의 데이터로 분석 (보통 7일)
            primary_period = periods[0] if periods else 7
            primary_data = data_by_period.get(primary_period, [])
            
            if not primary_data:
                return "데이터가 부족하여 비교분석을 수행할 수 없습니다."
            
            # 테이블 텍스트 구성
            table_text = self._build_comparison_table_text(primary_data)
            
            # LLM 호출
            prompt = self._comparison_prompt_tpl.format(table_text=table_text)
            response = self.llm.invoke(prompt)
            
            return response.content.strip()
            
        except Exception as e:
            return f"비교분석 생성 중 오류 발생: {str(e)}"
    
    def _build_comparison_table_text(self, data: List[Dict[str, Any]]) -> str:
        """비교분석을 위한 테이블 텍스트 구성"""
        if not data:
            return "데이터가 없습니다."
        
        lines = ["매장명\t금주방문객\t전주방문객\t평일증감%\t주말증감%\t총증감%"]
        for item in data:
            site = item.get("site", "Unknown")
            curr_total = item.get("curr_total", 0)
            prev_total = item.get("prev_total", 0)
            weekday_delta = item.get("weekday_delta_pct", 0)
            weekend_delta = item.get("weekend_delta_pct", 0)
            total_delta = item.get("total_delta_pct", 0)
            
            line = f"{site}\t{curr_total}\t{prev_total}\t{weekday_delta}%\t{weekend_delta}%\t{total_delta}%"
            lines.append(line)
        
        return "\n".join(lines)
    
    def _generate_html(self, sites: List[str], end_date: str, data_by_period: Dict[int, List[Dict[str, Any]]], comparison_analysis: str) -> str:
        """HTML 생성 - 4개 카드 뼈대"""
        # 제목에 사용할 매장명
        store_a = sites[0] if len(sites) > 0 else "A매장"
        store_b = sites[1] if len(sites) > 1 else (sites[0] if sites else "B매장")
        title = f"매장별 방문객 추이 비교 분석: {store_a} vs {store_b}"
        
        # 4개 카드 섹션
        summary_card = self._build_summary_card(comparison_analysis)
        daily_trends_card = self._build_daily_trends_card(sites)
        composition_card = self._build_customer_composition_card()
        time_age_card = self._build_time_age_pattern_card()
        
        html = f"""
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Noto Sans KR', Arial, sans-serif; margin: 0; background: #fafafa; color: #111; }}
    .container {{ max-width: 1080px; margin: 24px auto; padding: 0 16px; }}
    .page-title {{ margin: 0 0 6px; font-size: 24px; font-weight: 700; }}
    .page-subtitle {{ margin: 0 0 16px; color: #6b7280; font-size: 14px; }}
    .card {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 10px; padding: 16px; margin: 12px 0; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }}
    .card h3 {{ margin: 0 0 8px; font-size: 18px; }}
    .placeholder {{ color: #9ca3af; font-size: 13px; text-align: center; padding: 20px; }}
    .summary-list {{ margin: 8px 0 0 16px; padding-left: 16px; line-height: 1.6; }}
    .summary-list li {{ margin: 6px 0; text-align: left; list-style: disc; }}
    .chart-container {{ text-align: center; margin: 16px auto; width: 100%; display: flex; justify-content: center; align-items: center; }}
    .chart-container svg {{ width: 100%; height: auto; border: 1px solid #e5e7eb; border-radius: 8px; display: block; margin: 0 auto; }}
  </style>
</head>
<body>
  <div class="container">
    <h2 class="page-title">{title}</h2>
    <div class="page-subtitle">비교 분석합니다</div>
    
    {summary_card}
    {daily_trends_card}
    {composition_card}
    {time_age_card}
  </div>
</body>
</html>
"""
        return html
    
    def _build_summary_card(self, comparison_analysis: str) -> str:
        """1. 요약카드 - LLM 분석 결과 표시"""
        if comparison_analysis and comparison_analysis.strip():
            # 마크다운 불릿을 HTML 리스트로 변환
            lines = [ln.strip() for ln in comparison_analysis.splitlines() if ln.strip()]
            items = []
            for ln in lines:
                if ln.startswith('- '):
                    items.append(ln[2:].strip())
                else:
                    items.append(ln)
            
            if items:
                li_html = "\n".join(f"<li>{self._escape_html(it)}</li>" for it in items)
                content = f"<ul class=\"summary-list\">{li_html}</ul>"
            else:
                content = "<p class=\"placeholder\">분석 결과가 없습니다.</p>"
        else:
            content = """
            <div class="placeholder">
              📊 <strong>AI 비교 분석</strong><br>
              매장별 방문 데이터를 분석하여 핵심 인사이트를 제공합니다
            </div>
            """

        return f"""
<section class="card">
  <h3>요약카드</h3>
  {content}
</section>
"""
    
    def _generate_daily_trends_chart(self) -> str:
        """A매장 vs B매장 비교 차트 생성 (좌우 나란히)"""
        # 더미 데이터: A매장 vs B매장 비교
        dates = ["8/1", "8/2", "8/3", "8/4", "8/5", "8/6", "8/7"]
        weekdays = ["(목)", "(금)", "(토)", "(일)", "(월)", "(화)", "(수)"]
        
        # A매장 데이터 - 이미지와 동일하게 수정
        site_a_prev = [115, 130, 120, 140, 135, 170, 180]
        site_a_curr = [120, 135, 128, 142, 138, 180, 185]
        site_a_growth = [4.3, 3.8, 2.4, 1.4, 2.2, 5.9, 5.4]
        
        # B매장 데이터 - 이미지와 동일하게 수정
        site_b_prev = [95, 110, 120, 105, 115, 160, 160]
        site_b_curr = [98, 112, 122, 108, 118, 165, 165]
        site_b_growth = [3.2, 1.8, 1.7, 2.9, 2.6, 3.1, 3.1]
        
        # 전체 차트 크기 (두 개 차트를 나란히)
        total_width = 2600
        chart_width = 1100
        chart_height = 900
        padding = 100
        inner_padding = 100
        
        # 차트 간격 계산 (중앙 정렬을 위해)
        available_space = total_width - (2 * chart_width)
        margin = available_space // 3  # 좌측 여백, 차트 간격, 우측 여백을 동일하게
        
        # A매장 차트 (좌측)
        chart_a = self._generate_single_chart(
            dates, weekdays, site_a_prev, site_a_curr, site_a_growth,
            "A매장", chart_width, chart_height, padding
        )
        
        # B매장 차트 (우측)
        chart_b = self._generate_single_chart(
            dates, weekdays, site_b_prev, site_b_curr, site_b_growth,
            "B매장", chart_width, chart_height, padding
        )
        
        # 두 차트를 나란히 배치 (중앙 정렬)
        svg = f"""
<svg viewBox="0 0 {total_width} {chart_height + 2 * padding}" xmlns="http://www.w3.org/2000/svg" style="background: white;">

  
  <!-- A매장 차트 (좌측) - 상하 중앙 정렬 -->
  <g transform="translate({margin}, {padding//2})">
    {chart_a}
  </g>
  
  <!-- B매장 차트 (우측) - 상하 중앙 정렬 -->
  <g transform="translate({margin + chart_width + margin}, {padding//2})">
    {chart_b}
  </g>
</svg>
"""
        return svg

    
    def _generate_single_chart(self, dates: List[str], weekdays: List[str], 
                              prev_visitors: List[int], curr_visitors: List[int], 
                              growth_rates: List[float], site_name: str,
                              width: int, height: int, padding: int) -> str:
        """개별 매장 차트 생성 - 정의에 맞게 새로 작성"""
        # 차트 영역 계산
        chart_width = width - 2 * padding
        chart_height = height - 2 * padding
        # UI 스케일 (기본 500 높이 기준) - 1.4배 확대
        ui_scale = max(0.8, chart_height / 500) * 1.4
        
        # 동적 Y축 스케일 계산 (딱 떨어지는 눈금으로 정규화)
        visitor_min, visitor_max = min(min(prev_visitors), min(curr_visitors)), max(max(prev_visitors), max(curr_visitors))
        visitor_range = max(1, visitor_max - visitor_min)
        visitor_padding = visitor_range * 0.1  # 10% 여백
        padded_min = visitor_min - visitor_padding
        padded_max = visitor_max + visitor_padding
        # 5개 격자를 기본으로 하는 "nice" 스텝 계산 (1/2/5/10 계열)
        import math
        approx_step = max(1, (padded_max - padded_min) / 5)
        magnitude = 10 ** int(math.floor(math.log10(approx_step)))
        for m in (1, 2, 5, 10):
            nice_step = m * magnitude
            if nice_step >= approx_step:
                break
        ticks_min = int(math.floor(padded_min / nice_step) * nice_step)
        # 상단 여백 확보를 위해 한 스텝 추가
        ticks_max = int(math.ceil(padded_max / nice_step) * nice_step) + int(nice_step)
        ticks_range = max(1, ticks_max - ticks_min)
        visitor_scale = chart_height / ticks_range
        
        growth_min, growth_max = min(growth_rates), max(growth_rates)
        growth_range = max(0.1, growth_max - growth_min)
        # 여백 추가 (10%)
        growth_padding = growth_range * 0.1
        growth_scale_min = growth_min - growth_padding
        growth_scale_max = growth_max + growth_padding
        growth_scale_range = growth_scale_max - growth_scale_min
        growth_scale = chart_height / growth_scale_range
        
        # X축 스케일 (막대가 테두리와 겹치지 않도록 좌우 오프셋 적용)
        bar_offset = int(30 * ui_scale)
        x_origin = padding + bar_offset
        x_scale = (chart_width - 2 * bar_offset) / (len(dates) - 1) if len(dates) > 1 else (chart_width - 2 * bar_offset)
        # 막대 폭/간격 (x_scale 기반 비례)
        bar_width = max(14, min(int(x_scale * 0.22), int(40 * ui_scale)))
        bar_gap = max(4, int(x_scale * 0.06))
        
        svg_elements = []
        
        # 매장명 제목 - padding 기준 상대적 배치
        svg_elements.append(f'<text x="{width//2}" y="{padding//2}" font-size="{int(24*ui_scale)}" font-weight="bold" text-anchor="middle" fill="#1f2937">{site_name}</text>')
        
        # 그리드 라인 (방문자 수 기준) - 딱 떨어지는 눈금으로 분할
        visitor_step = int(nice_step)
        for i in range(ticks_min, ticks_max + 1, visitor_step):
            y = padding + (1 - (i - ticks_min) / ticks_range) * chart_height
            # 그리드 라인
            svg_elements.append(f'<line x1="{padding}" y1="{y}" x2="{width-padding}" y2="{y}" stroke="#f3f4f6" stroke-width="{max(1,int(1*ui_scale))}" />')
            # 좌측 Y축 눈금 선 (테두리 바깥으로)
            svg_elements.append(f'<line x1="{padding}" y1="{y}" x2="{padding-10}" y2="{y}" stroke="#6b7280" stroke-width="{max(1,int(1*ui_scale))}" />')
            # 좌측 Y축 라벨 - 눈금선에 가깝게 배치
            svg_elements.append(f'<text x="{padding-14}" y="{y+4}" font-size="{int(16*ui_scale)}" text-anchor="end" fill="#6b7280">{i}</text>')
        
        # 0% 기준선 (변화율) - 동적 범위에 맞춤
        if growth_min < 0 and growth_max > 0:
            zero_y = padding + (1 - (0 - (growth_min - growth_padding)) / (growth_range + 2 * growth_padding)) * chart_height
            svg_elements.append(f'<line x1="{padding}" y1="{zero_y}" x2="{width-padding}" y2="{zero_y}" stroke="#cbd5e1" stroke-width="1.5" stroke-dasharray="5,5" />')
        
        # 변화율 Y축 눈금 추가 - min/max 기반 간단한 스케일링
        if growth_range > 0:
            growth_step = max(1, int(growth_range // 4))  # 4-5개 눈금으로 분할
            for i in range(int(growth_scale_min), int(growth_scale_max + 1), growth_step):
                # Y 좌표 계산 (min/max 기반)
                y = padding + chart_height - (i - growth_scale_min) * growth_scale
                # 차트 영역 내에 있는 눈금만 표시
                if padding <= y <= padding + chart_height:
                    # 오른쪽 Y축 눈금 선 (테두리 바깥으로, 스케일링 적용)
                    tick_line_length = int(10 * ui_scale)
                    svg_elements.append(f'<line x1="{width-padding}" y1="{y}" x2="{width-padding+tick_line_length}" y2="{y}" stroke="#6b7280" stroke-width="{max(1,int(1*ui_scale))}" />')
                    # 오른쪽 Y축 눈금 라벨 - 눈금선에 가깝게 배치, 스케일링 적용
                    label_offset = int(14 * ui_scale)
                    svg_elements.append(f'<text x="{width-padding+label_offset}" y="{y+4}" font-size="{int(15*ui_scale)}" text-anchor="start" fill="#6b7280">{i}%</text>')
        
        # 막대그래프 (전주/금주)
        for i, (date_str, weekday) in enumerate(zip(dates, weekdays)):
            x_center = x_origin + i * x_scale
            
            # 전주 막대 (파란색) - 테두리 추가, 채도 높임, 동적 Y축 적용, 눈금과 겹치지 않도록 위치 조정
            prev_height = (prev_visitors[i] - ticks_min) * visitor_scale
            prev_y = padding + chart_height - prev_height
            prev_x = x_center - (bar_gap//2) - bar_width
            svg_elements.append(f'<rect x="{prev_x}" y="{prev_y}" width="{bar_width}" height="{prev_height}" fill="#93c5fd" stroke="#3b82f6" stroke-width="{max(1,int(2*ui_scale))}" />')
            # 방문자 수 라벨 (막대 상단)
            svg_elements.append(f'<text x="{prev_x + bar_width/2}" y="{prev_y-8}" font-size="{int(16*ui_scale)}" text-anchor="middle" fill="#1f2937" font-weight="bold">{prev_visitors[i]}</text>')
            
            # 금주 막대 (빨간색) - 테두리 추가, 채도 높임, 동적 Y축 적용, 눈금과 겹치지 않도록 위치 조정
            curr_height = (curr_visitors[i] - ticks_min) * visitor_scale
            curr_y = padding + chart_height - curr_height
            curr_x = x_center + (bar_gap//2)
            svg_elements.append(f'<rect x="{curr_x}" y="{curr_y}" width="{bar_width}" height="{curr_height}" fill="#fca5a5" stroke="#ef4444" stroke-width="{max(1,int(2*ui_scale))}" />')
            # 방문자 수 라벨 (막대 상단)
            svg_elements.append(f'<text x="{curr_x + bar_width/2}" y="{curr_y-8}" font-size="{int(16*ui_scale)}" text-anchor="middle" fill="#1f2937" font-weight="bold">{curr_visitors[i]}</text>')
            
            # X축 라벨 (날짜 + 요일)
            svg_elements.append(f'<text x="{x_center}" y="{height-padding+25}" font-size="{int(18*ui_scale)}" text-anchor="middle" fill="#6b7280">{date_str}<tspan x="{x_center}" dy="{int(25*ui_scale)}">{weekday}</tspan></text>')
        
        # 변화율 선 그래프 (초록색) - min/max 기반 간단한 스케일링
        points = []
        for i, rate in enumerate(growth_rates):
            x = x_origin + i * x_scale
            # min/max 기반으로 변화율 Y 좌표 계산
            y = padding + chart_height - (rate - growth_scale_min) * growth_scale
            points.append(f"{x},{y}")
            
            # 변화율 라벨 (빨간색, + 기호 포함, 소수점 한 자리)
            rate_text = f"+{rate:.1f}%" if rate > 0 else f"{rate:.1f}%"
            svg_elements.append(f'<text x="{x}" y="{y-15}" font-size="{int(14*ui_scale)}" text-anchor="middle" fill="#dc2626" font-weight="bold">{rate_text}</text>')
        
        if len(points) > 1:
            # 선 그래프
            path_d = " ".join(points)
            svg_elements.append(f'<polyline fill="none" stroke="#10b981" stroke-width="{max(2,int(3*ui_scale))}" points="{path_d}" />')
            
            # 원형 마커
            for point in points:
                x, y = map(float, point.split(','))
                svg_elements.append(f'<circle cx="{x}" cy="{y}" r="{int(4*ui_scale)}" fill="#10b981" stroke="#065f46" stroke-width="{max(1,int(1*ui_scale))}" />')
        
        # Y축 라벨
        svg_elements.append(f'<text x="30" y="{height//2}" font-size="{int(18*ui_scale)}" text-anchor="middle" fill="#6b7280" transform="rotate(-90, 30, {height//2})">방문자 수(명)</text>')
        svg_elements.append(f'<text x="{width-30}" y="{height//2}" font-size="{int(18*ui_scale)}" text-anchor="middle" fill="#6b7280" transform="rotate(90, {width-30}, {height//2})">변화율(%)</text>')
        
        # 플롯 영역(눈금+막대/선)만 정확히 감싸는 테두리
        svg_elements.append(f'<rect x="{padding}" y="{padding}" width="{chart_width}" height="{chart_height}" fill="none" stroke="#e5e7eb" stroke-width="{max(1,int(2*ui_scale))}" rx="{int(4*ui_scale)}" />')
        
        # 변화율 범례 - 차트 안 우상단 (타이트한 박스, 스케일 적용)
        category_y = padding + int(6*ui_scale)
        cat_w = int(70*ui_scale)
        cat_h = int(24*ui_scale)
        category_x = padding + chart_width - cat_w - int(6*ui_scale)
        svg_elements.extend([
            f'<rect x="{category_x}" y="{category_y}" width="{cat_w}" height="{cat_h}" fill="#f9fafb" stroke="#e5e7eb" rx="4" />',
            f'<line x1="{category_x+int(8*ui_scale)}" y1="{category_y+cat_h//2}" x2="{category_x+int(22*ui_scale)}" y2="{category_y+cat_h//2}" stroke="#10b981" stroke-width="{max(2,int(3*ui_scale))}" />',
            f'<text x="{category_x+int(30*ui_scale)}" y="{category_y+int(0.67*cat_h)}" font-size="{int(14*ui_scale)}" fill="#374151">변화율</text>'
        ])

        # 전주/금주 범례 - 차트 안 좌상단 (타이트한 박스, 스케일 적용)
        legend_y = padding + int(6*ui_scale)
        legend_x = padding + int(6*ui_scale)
        leg_w = int(96*ui_scale)
        leg_h = int(24*ui_scale)
        svg_elements.extend([
            f'<rect x="{legend_x}" y="{legend_y}" width="{leg_w}" height="{leg_h}" fill="#f9fafb" stroke="#e5e7eb" rx="4" />',
            f'<rect x="{legend_x+int(6*ui_scale)}" y="{legend_y+int(6*ui_scale)}" width="{int(10*ui_scale)}" height="{int(10*ui_scale)}" fill="#93c5fd" stroke="#3b82f6" stroke-width="0.5" />',
            f'<text x="{legend_x+int(20*ui_scale)}" y="{legend_y+int(0.65*leg_h)}" font-size="{int(13*ui_scale)}" fill="#374151">전주</text>',
            f'<rect x="{legend_x+int(52*ui_scale)}" y="{legend_y+int(6*ui_scale)}" width="{int(10*ui_scale)}" height="{int(10*ui_scale)}" fill="#fca5a5" stroke="#ef4444" stroke-width="0.5" />',
            f'<text x="{legend_x+int(66*ui_scale)}" y="{legend_y+int(0.65*leg_h)}" font-size="{int(13*ui_scale)}" fill="#374151">금주</text>'
        ])
        
        svg = f"""
<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
  {''.join(svg_elements)}
</svg>
"""
        return svg
    
    def _build_daily_trends_card(self, sites: List[str]) -> str:
        """2. 매장별 일별 방문 추이 - 매장별 분리 구성(2와 동일한 레이아웃)"""
        # 실제 데이터가 있으면 사용, 없으면 더미 데이터 사용
        if hasattr(self, 'comparison_data') and self.comparison_data:
            # 실제 데이터에서 추출
            sites = list(self.comparison_data.keys())
            if len(sites) >= 2:
                site_a, site_b = sites[0], sites[1]
                
                a_data = self.comparison_data[site_a]["daily_trends"]
                b_data = self.comparison_data[site_b]["daily_trends"]
                
                dates = a_data["dates"]
                weekdays = a_data["weekdays"]
                a_prev = a_data["previous"]
                a_curr = a_data["current"]
                a_growth = a_data["growth"]
                b_prev = b_data["previous"]
                b_curr = b_data["current"]
                b_growth = b_data["growth"]
            else:
                # 데이터가 부족한 경우 더미 데이터 사용
                dates = ["8/1", "8/2", "8/3", "8/4", "8/5", "8/6", "8/7"]
                weekdays = ["(목)", "(금)", "(토)", "(일)", "(월)", "(화)", "(수)"]
                a_prev = [115, 130, 120, 140, 135, 170, 180]
                a_curr = [120, 135, 128, 142, 138, 180, 185]
                a_growth = [4.3, 3.8, 2.4, 1.4, 2.2, 5.9, 5.4]
                b_prev = [95, 110, 120, 105, 115, 160, 160]
                b_curr = [98, 112, 122, 108, 118, 165, 165]
                b_growth = [3.2, 1.8, 1.7, 2.9, 2.6, 3.1, 3.1]
        else:
            # 더미 데이터 사용
            dates = ["8/1", "8/2", "8/3", "8/4", "8/5", "8/6", "8/7"]
            weekdays = ["(목)", "(금)", "(토)", "(일)", "(월)", "(화)", "(수)"]
            a_prev = [115, 130, 120, 140, 135, 170, 180]
            a_curr = [120, 135, 128, 142, 138, 180, 185]
            a_growth = [4.3, 3.8, 2.4, 1.4, 2.2, 5.9, 5.4]
            b_prev = [95, 110, 120, 105, 115, 160, 160]
            b_curr = [98, 112, 122, 108, 118, 165, 165]
            b_growth = [3.2, 1.8, 1.7, 2.9, 2.6, 3.1, 3.1]

        padding = 100
        single_w, single_h = 1100, 640
        # 실제 매장명 사용
        site_a_name = sites[0] if len(sites) > 0 else "A매장"
        site_b_name = sites[1] if len(sites) > 1 else "B매장"
        chart_a = self._generate_single_chart(dates, weekdays, a_prev, a_curr, a_growth, site_a_name, single_w, single_h, padding)
        chart_b = self._generate_single_chart(dates, weekdays, b_prev, b_curr, b_growth, site_b_name, single_w, single_h, padding)
        
        return f"""
<section class="card">
  <h3>매장별 일별 방문 추이 비교</h3>
  <div style="display:flex; gap:24px; align-items:flex-start;">
    <div style="flex:1; text-align:center;">
      <div class="chart-container">{chart_a}</div>
    </div>
    <div style="flex:1; text-align:center;">
      <div class="chart-container">{chart_b}</div>
    </div>
  </div>
</section>
"""
    
    def _build_customer_composition_card(self) -> str:
        """3. 고객 구성 변화 - 성별/연령대별 막대그래프"""
        chart_svg = self._generate_customer_composition_chart()
        
        return f"""
<section class="card">
  <h3>고객 구성 변화</h3>
  <div class="chart-container">
    {chart_svg}
  </div>
</section>
"""
    
    def _build_time_age_pattern_card(self) -> str:
        """4. 시간대 연령대별 방문 패턴 - 히트맵"""
        chart_svg = self._generate_time_age_heatmap()
        
        return f"""
<section class="card">
  <h3>시간대 연령대별 방문 패턴</h3>
  <div class="chart-container">
    {chart_svg}
  </div>
</section>
"""
    
    def _generate_customer_composition_chart(self) -> str:
        """고객 구성 변화 차트 생성 - 중앙 기준 분기형(왼쪽 남성, 오른쪽 여성) 수평 막대 + 비교 얇은 바.

        색상 규칙
        - 남성: 진한 파란색 #1d4ed8
        - 여성: 진한 하늘색 #38bdf8
        - 비교_남성: 연한 청록색 #5eead4
        - 비교_여성: 연한 민트색 #a7f3d0
        """
        # 연령대(Y축): 60대 이상 → 10세 미만 (상단→하단)
        age_labels = ["60세~", "50~59세", "40~49세", "30~39세", "20~29세", "10~19세", "0~9세"]
        # 더미 비율 (해당 매장 전체 100% 내 분포라고 가정)
        # 최대치 테스트용: 30~39세/10~19세 구간을 과감히 키워 바깥쪽 길이가 충분히 뻗도록 설정
        age_totals_a = [12, 18, 22, 35, 20, 28, 3]
        age_totals_b = [10, 17, 21, 32, 22, 30, 4]
        # 성별 비중(각 연령대 합 중 남성 비율)
        male_share_a = [0.52, 0.56, 0.51, 0.46, 0.42, 0.55, 0.50]
        male_share_b = [0.50, 0.53, 0.49, 0.47, 0.44, 0.56, 0.50]

        def split(age_totals: List[float], male_share: List[float]) -> tuple[List[float], List[float]]:
            m = [round(t * s, 1) for t, s in zip(age_totals, male_share)]
            f = [round(t - mv, 1) for t, mv in zip(age_totals, m)]
            return m, f

        a_m, a_f = split(age_totals_a, male_share_a)
        b_m, b_f = split(age_totals_b, male_share_b)
        # 비교(전기) 더미: 소폭 증감 반영
        a_m_cmp = [max(0.0, x * 0.9) for x in a_m]
        a_f_cmp = [max(0.0, x * 0.9) for x in a_f]
        b_m_cmp = [max(0.0, x * 0.88) for x in b_m]
        b_f_cmp = [max(0.0, x * 0.88) for x in b_f]

        # 주요 고객층(현재) 텍스트: 성별별 최댓값 연령대 추출
        def top_age(m: List[float], f: List[float]) -> tuple[str, str]:
            i_m = max(range(len(m)), key=lambda i: m[i])
            i_f = max(range(len(f)), key=lambda i: f[i])
            return age_labels[i_m], age_labels[i_f]

        a_top_m, a_top_f = top_age(a_m, a_f)
        b_top_m, b_top_f = top_age(b_m, b_f)

        # ---------------- 색상 규칙 ----------------
        # 30대 기준색 (금주/전주)
        base_m_curr_hex = "#3467E2"  # 남성 30대 기준색
        base_f_curr_hex = "#76CCCF"  # 여성 30대 기준색
        base_m_prev_hex = "#9BB4F0"  # 남성 30대 이전 주 기준색
        base_f_prev_hex = "#BAE5E7"  # 여성 30대 이전 주 기준색

        # 연령에 따른 채도 가중치(60+ → 0.75 ... 30대 → 1.0 ... 10세 미만 → 1.25)
        sat_factors = [0.75, 0.85, 0.93, 1.0, 1.12, 1.20, 1.25]

        # 색상 유틸
        def clamp01(x: float) -> float:
            return 0.0 if x < 0.0 else (1.0 if x > 1.0 else x)

        def hex_to_rgb(hex_str: str) -> tuple[float, float, float]:
            hex_str = hex_str.lstrip('#')
            r = int(hex_str[0:2], 16)
            g = int(hex_str[2:4], 16)
            b = int(hex_str[4:6], 16)
            return r/255.0, g/255.0, b/255.0

        def rgb_to_hex(r: float, g: float, b: float) -> str:
            return '#%02X%02X%02X' % (int(max(0, min(255, round(r*255)))), int(max(0, min(255, round(g*255)))), int(max(0, min(255, round(b*255)))))

        def rgb_to_hsl(r: float, g: float, b: float) -> tuple[float, float, float]:
            mx = max(r, g, b)
            mn = min(r, g, b)
            l = (mx + mn) / 2
            if mx == mn:
                h = s = 0.0
            else:
                d = mx - mn
                s = d / (2 - mx - mn) if l > 0.5 else d / (mx + mn)
                if mx == r:
                    h = (g - b) / d + (6 if g < b else 0)
                elif mx == g:
                    h = (b - r) / d + 2
                else:
                    h = (r - g) / d + 4
                h /= 6
            return h, s, l

        def hsl_to_rgb(h: float, s: float, l: float) -> tuple[float, float, float]:
            def hue2rgb(p: float, q: float, t: float) -> float:
                if t < 0: t += 1
                if t > 1: t -= 1
                if t < 1/6: return p + (q - p) * 6 * t
                if t < 1/2: return q
                if t < 2/3: return p + (q - p) * (2/3 - t) * 6
                return p
            if s == 0:
                r = g = b = l
            else:
                q = l * (1 + s) if l < 0.5 else (l + s - l * s)
                p = 2 * l - q
                r = hue2rgb(p, q, h + 1/3)
                g = hue2rgb(p, q, h)
                b = hue2rgb(p, q, h - 1/3)
            return r, g, b

        def build_color_pair(curr_hex: str, prev_hex: str) -> tuple[List[str], List[str]]:
            r, g, b = hex_to_rgb(curr_hex)
            h_c, s_c, l_c = rgb_to_hsl(r, g, b)
            r, g, b = hex_to_rgb(prev_hex)
            h_p, s_p, l_p = rgb_to_hsl(r, g, b)
            delta_s = s_p - s_c
            delta_l = l_p - l_c
            curr_colors: List[str] = []
            prev_colors: List[str] = []
            for f in sat_factors:
                s_curr = clamp01(s_c * f)
                s_prev = clamp01(s_curr + delta_s)
                l_curr = clamp01(l_c)  # 밝기는 기준 유지
                l_prev = clamp01(l_curr + delta_l)
                r1, g1, b1 = hsl_to_rgb(h_c, s_curr, l_curr)
                r2, g2, b2 = hsl_to_rgb(h_c, s_prev, l_prev)
                curr_colors.append(rgb_to_hex(r1, g1, b1))
                prev_colors.append(rgb_to_hex(r2, g2, b2))
            return curr_colors, prev_colors

        # 원본 이미지 팔레트에 맞춰 연령대별 고정 색상 팔레트 적용 (age_labels 순서)
        male_curr_colors = [
            "#A8BBF4",  # 60세~ (조금 더 진하게, 여전히 50~59세보단 밝게)
            "#8EABF2",  # 50~59세
            "#6E92ED",  # 40~49세
            "#3467E2",  # 30~39세
            "#2D58C8",  # 20~29세
            "#244AAD",  # 10~19세
            "#1C3F99",  # 0~9세
        ]
        male_prev_colors = [
            "#D6E0FA",  # 60세~ (조금 더 진하게)
            "#C9D6F9",  # 50~59세
            "#B7C9F7",  # 40~49세
            "#9BB4F0",  # 30~39세
            "#8EA9EE",  # 20~29세
            "#839FEA",  # 10~19세
            "#7A95E6",  # 0~9세
        ]
        female_curr_colors = [
            "#C8EEED",  # 60세~ (조금 더 진하게)
            "#B3E6E4",  # 50~59세
            "#95DBD7",  # 40~49세
            "#76CCCF",  # 30~39세
            "#64BFC4",  # 20~29세
            "#54AFB5",  # 10~19세
            "#469FA4",  # 0~9세
        ]
        female_prev_colors = [
            "#DDF3F2",  # 60세~ (조금 더 진하게)
            "#D8F2F1",  # 50~59세
            "#CBEDED",  # 40~49세
            "#BAE5E7",  # 30~39세
            "#AEDFE2",  # 20~29세
            "#A2D8DB",  # 10~19세
            "#99D2D5",  # 0~9세
        ]

        # 단일 매장: 중앙 분기형 렌더러
        def render_single(store: str, m: List[float], f: List[float], m_cmp: List[float], f_cmp: List[float], width: int, height: int) -> str:
            # 좌우 균형 유지, 내부 스케일은 크게 유지하면서 섹션 간 간격만 소폭 축소
            pad_left, pad_right, pad_top, pad_bottom = 120, 120, 90, 90
            plot_w = width - pad_left - pad_right
            plot_h = height - pad_top - pad_bottom
            center_x = pad_left + plot_w / 2
            bands = len(age_labels)
            band_h = plot_h / bands
            label_font = 22.0
            # 막대 두께를 소폭 감소
            bar_h = min(band_h * 0.60, label_font + 4)
            gap_y = 0.0  # 전주/금주 막대 간격 없음(딱 붙게)
            # 동적 축 최대치 (전체 값 중 최댓값을 5단위로 반올림)
            max_val = max(max(m), max(f), max(m_cmp), max(f_cmp)) if m and f and m_cmp and f_cmp else 30.0
            def round_up_to_5(x: float) -> float:
                import math
                return float(int(math.ceil(x / 5.0)) * 5)
            # 원래 로직 복원: 데이터 최대값(여유 5%)을 기준으로 30%~100% 범위에서 스케일링
            axis_max = max(30.0, min(100.0, round_up_to_5(max_val * 1.05)))

            def w_of(p: float) -> float:
                p = max(0.0, min(axis_max, p))
                return (plot_w / 2) * (p / axis_max)

            svg: List[str] = []
            svg.append(f"<rect x='0' y='0' width='{width}' height='{height}' fill='white' />")
            # 제목
            # 타이틀-차트 간 간격 소폭 축소
            svg.append(f"<text x='{width/2:.1f}' y='{pad_top-34}' font-size='24' fill='#111827' text-anchor='middle' font-weight='800'>{store}</text>")

            # 가이드 라인 및 라벨
            for i, label in enumerate(age_labels):
                y0 = pad_top + i * band_h
                y_mid = y0 + band_h / 2
                # (요청) 눈금/가이드 제거, 연령대 라벨은 중앙선 위에 배치
                svg.append(f"<text x='{center_x:.1f}' y='{y_mid + 2:.1f}' font-size='{label_font:.0f}' fill='#374151' font-weight='700' text-anchor='middle'>{label}</text>")

                # 얇은 비교 바(먼저 그려서 아래로 깔림) - 같은 중심선
                # 상단: 금주, 하단: 전주 (겹치지 않게 분리)
                thin_h = bar_h  # 전주도 동일 두께(쌍둥이 막대)
                # 금주(위 막대)
                curr_y = y_mid - bar_h
                # 연령대 인덱스 i에 맞춘 채도 색상 적용 (각진 막대: rx 제거)
                color_m_curr = male_curr_colors[i]
                color_f_curr = female_curr_colors[i]
                center_gap = max(56.0, band_h * 0.30)
                def w_of(p: float) -> float:
                    p = max(0.0, min(axis_max, p))
                    return (plot_w / 2) * (p / axis_max)
                svg.append(f"<rect x='{center_x - center_gap - w_of(m[i]):.1f}' y='{curr_y:.1f}' width='{w_of(m[i]):.1f}' height='{bar_h:.1f}' fill='{color_m_curr}' />")
                svg.append(f"<rect x='{center_x + center_gap:.1f}' y='{curr_y:.1f}' width='{w_of(f[i]):.1f}' height='{bar_h:.1f}' fill='{color_f_curr}' />")
                # 전주(아래 막대) - 동일 두께, 딱 붙임
                prev_y = y_mid
                color_m_prev = male_prev_colors[i]
                color_f_prev = female_prev_colors[i]
                svg.append(f"<rect x='{center_x - center_gap - w_of(m_cmp[i]):.1f}' y='{prev_y:.1f}' width='{w_of(m_cmp[i]):.1f}' height='{thin_h:.1f}' fill='{color_m_prev}' />")
                svg.append(f"<rect x='{center_x + center_gap:.1f}' y='{prev_y:.1f}' width='{w_of(f_cmp[i]):.1f}' height='{thin_h:.1f}' fill='{color_f_prev}' />")

                # (요청) 합계 라벨 제거

            # (요청) 눈금/라벨 제거

            # 중앙 기준선 제거 (요청)

            # 범례: 아래 중앙 정렬
            # 범례: 그래프와의 간격을 더 줄여 bars 아래로 가깝게 배치
            ly = height - pad_bottom + 24
            legends = [
                ("#1d4ed8", "남성"),
                ("#38bdf8", "비교_남성"),
                ("#5eead4", "여성"),
                ("#a7f3d0", "비교_여성"),
            ]
            # 아이템 간 고정 간격 20px로 재배치
            icon_r = 8
            icon_d = icon_r * 2
            gap_icon_text = 6
            gap_between = 20
            # 텍스트 폭 근사값 계산
            def approx_text_w(s: str) -> int:
                return max(24, int(len(s) * 12))
            items = []
            for c, name in legends:
                w = icon_d + gap_icon_text + approx_text_w(name)
                items.append((c, name, w))
            total_w = sum(w for _, _, w in items) + gap_between * (len(items) - 1)
            start_x = pad_left + (plot_w - total_w) / 2
            x_cursor = start_x
            for c, name, w in items:
                svg.append(f"<circle cx='{x_cursor + icon_r:.1f}' cy='{ly}' r='{icon_r}' fill='{c}' />")
                svg.append(f"<text x='{x_cursor + icon_d + gap_icon_text:.1f}' y='{ly + 6}' font-size='15' fill='#374151'>{name}</text>")
                x_cursor += w + gap_between

            return f"<svg width='{width}' height='{height}' viewBox='0 0 {width} {height}' xmlns='http://www.w3.org/2000/svg'>" + "".join(svg) + "</svg>"

        # 두 매장 블록: 텍스트는 각 차트 위에 별도 DOM 요소로 배치
        single_w, single_h = 1100, 640
        chart_a = render_single("A매장", a_m, a_f, a_m_cmp, a_f_cmp, single_w, single_h)
        chart_b = render_single("B매장", b_m, b_f, b_m_cmp, b_f_cmp, single_w, single_h)

        # 각 매장별 주요 고객층 텍스트 (금주/전주 병기)
        a_top_prev_m, a_top_prev_f = top_age(a_m_cmp, a_f_cmp)
        b_top_prev_m, b_top_prev_f = top_age(b_m_cmp, b_f_cmp)

        block_html = f"""
<div style=\"display:flex; gap:24px; align-items:flex-start;\">
  <div style=\"flex:1; text-align:center;\">
    <div style=\"margin:0 0 6px; font-size:14px; color:#374151; text-align:center;\">
      남성 <strong>{a_top_m}</strong>, 여성 <strong>{a_top_f}</strong>가 가장 많이 방문했습니다.
    </div>
    <div class=\"chart-container\">{chart_a}</div>
  </div>
  <div style=\"flex:1; text-align:center;\">
    <div style=\"margin:0 0 6px; font-size:14px; color:#374151; text-align:center;\">
      남성 <strong>{b_top_m}</strong>, 여성 <strong>{b_top_f}</strong>가 가장 많이 방문했습니다.
    </div>
    <div class=\"chart-container\">{chart_b}</div>
  </div>
</div>
"""
        return block_html

    def _generate_time_age_heatmap(self) -> str:
        """시간대 연령대별 방문 패턴 히트맵 생성 (24시간 × 7연령대)"""
        # 더미 데이터: 24시간 × 연령대(0~9세 추가)
        time_slots = list(range(24))  # 0~23시
        # 요청: 0~9세 → "10세 미만"으로 표기하고, "10대" 위에 오도록 배치
        age_groups = ["10세 미만", "10대", "20대", "30대", "40대", "50대", "60대+"]
        
        # 간단 생성기: 오전/점심/저녁 피크를 반영한 더미 데이터 생성
        def gen_store_pattern(mult: float = 1.0):
            data = []
            for h in time_slots:
                row = []
                base = 10 + 5 * (1 if 11 <= h <= 14 else 0) + 7 * (18 <= h <= 21) + 3 * (8 <= h <= 10)
                # 연령대별 가중 (30대/20대 높게, 60+ 낮게, 10세 미만 매우 낮게)
                # 순서: 10세 미만, 10대, 20대, 30대, 40대, 50대, 60+
                weights = [0.3, 1.1, 1.4, 1.6, 1.2, 0.9, 0.6]
                for w in weights:
                    row.append(int((base * w + (h % 3)) * mult))
                data.append(row)
            return data

        site_a_prev = gen_store_pattern(1.0)
        site_a_curr = gen_store_pattern(1.1)
        site_b_prev = gen_store_pattern(0.9)
        site_b_curr = gen_store_pattern(1.05)
        
        # 차트 크기 설정
        width = 1100  # 더 compact
        height = 520
        padding = 60
        
        # 히트맵 영역 계산
        heatmap_width = width - 2 * padding
        heatmap_height = height - 2 * padding
        
        # 셀 크기 계산
        cell_width = heatmap_width / len(time_slots)
        cell_height = heatmap_height / len(age_groups)
        # 칸 너비를 cell_width / 2로 설정하고 간격 0
        rect_w = cell_width / 2
        rect_dx = 0
        
        # UI 스케일
        ui_scale = max(0.8, min(cell_width, cell_height) / 50) * 1.2
        
        svg_elements = []
        
        # 히트맵 그리기 (A매장 전주) - 자주(#741443) → 주황(#E48356) → 흰색(#FFFFFF) 그라데이션
        for i, time_slot in enumerate(time_slots):
            for j, age_group in enumerate(age_groups):
                # 간격 보정: 현재 기준(축소된 너비)에 맞춰 x 스텝도 rect_w로 이동
                x = padding + (i * rect_w)
                y = padding + (j * cell_height)
                
                # 방문자 수에 따른 색상 강도 계산
                value = site_a_prev[i][j]
                max_value = max(max(row) for row in site_a_prev)
                intensity = value / max_value if max_value else 0
                # 보간: hot(#741443) ↔ mid(#E48356) ↔ cool(#FFFFFF)
                def hex_to_rgb(h):
                    h=h.lstrip('#'); return tuple(int(h[k:k+2],16) for k in (0,2,4))
                def lerp(a,b,t): return int(a+(b-a)*t)
                hot = hex_to_rgb('#741443'); mid = hex_to_rgb('#E48356'); cool = hex_to_rgb('#FFFFFF')
                if intensity <= 0.5:
                    t = intensity*2
                    r = lerp(hot[0], mid[0], t); g = lerp(hot[1], mid[1], t); b = lerp(hot[2], mid[2], t)
                else:
                    t = (intensity-0.5)*2
                    r = lerp(mid[0], cool[0], t); g = lerp(mid[1], cool[1], t); b = lerp(mid[2], cool[2], t)
                color = f"rgb({r},{g},{b})"
                
                # 셀 그리기
                svg_elements.append(f'<rect x="{x}" y="{y}" width="{rect_w}" height="{cell_height}" fill="{color}" />')
                
                # 값 표기는 24×7 격자에서는 과밀해 생략
        
        # B매장 히트맵 (금주) - 오른쪽에 배치 (동일 그라데이션)
        for i, time_slot in enumerate(time_slots):
            for j, age_group in enumerate(age_groups):
                x = padding + width//2 + (i * rect_w)
                y = padding + (j * cell_height)
                
                # 방문자 수에 따른 색상 강도 계산
                value = site_b_curr[i][j]
                max_value = max(max(row) for row in site_b_curr)
                intensity = value / max_value if max_value else 0
                def hex_to_rgb(h):
                    h=h.lstrip('#'); return tuple(int(h[k:k+2],16) for k in (0,2,4))
                def lerp(a,b,t): return int(a+(b-a)*t)
                hot = hex_to_rgb('#741443'); mid = hex_to_rgb('#E48356'); cool = hex_to_rgb('#FFFFFF')
                if intensity <= 0.5:
                    t = intensity*2
                    r = lerp(hot[0], mid[0], t); g = lerp(hot[1], mid[1], t); b = lerp(hot[2], mid[2], t)
                else:
                    t = (intensity-0.5)*2
                    r = lerp(mid[0], cool[0], t); g = lerp(mid[1], cool[1], t); b = lerp(mid[2], cool[2], t)
                color = f"rgb({r},{g},{b})"
                
                # 셀 그리기
                svg_elements.append(f'<rect x="{x}" y="{y}" width="{rect_w}" height="{cell_height}" fill="{color}" />')
                
                # 값 표기는 생략
        
        # X축 라벨 (시간대)
        for i, h in enumerate(time_slots):
            if h % 3 == 0:  # 3시간 간격 라벨만 표시
                x = padding + (i * rect_w) + rect_w/2
                y = height - padding + 18
                svg_elements.append(f'<text x="{x}" y="{y}" font-size="{int(12*ui_scale)}" text-anchor="middle" fill="#6b7280">{h}시</text>')
                x_b = padding + width//2 + (i * rect_w) + rect_w/2
                svg_elements.append(f'<text x="{x_b}" y="{y}" font-size="{int(12*ui_scale)}" text-anchor="middle" fill="#6b7280">{h}시</text>')
        
        # Y축 라벨 (연령대)
        for j, age_group in enumerate(age_groups):
            x = padding - 10
            y = padding + (j * cell_height) + cell_height//2 + 4
            svg_elements.append(f'<text x="{x}" y="{y}" font-size="{int(14*ui_scale)}" text-anchor="end" fill="#6b7280">{age_group}</text>')
        
        # 제목
        svg_elements.append(f'<text x="{width//4}" y="{padding//2}" font-size="{int(16*ui_scale)}" font-weight="bold" text-anchor="middle" fill="#1f2937">A매장 (전주)</text>')
        svg_elements.append(f'<text x="{width*3//4}" y="{padding//2}" font-size="{int(16*ui_scale)}" font-weight="bold" text-anchor="middle" fill="#1f2937">B매장 (금주)</text>')
        
        # 범례 제거 (요청)
        
        # 차트 테두리
        svg_elements.append(f'<rect x="{padding}" y="{padding}" width="{heatmap_width//2}" height="{heatmap_height}" fill="none" stroke="#e5e7eb" stroke-width="{max(1,int(2*ui_scale))}" rx="4" />')
        svg_elements.append(f'<rect x="{padding + width//2}" y="{padding}" width="{heatmap_width//2}" height="{heatmap_height}" fill="none" stroke="#e5e7eb" stroke-width="{max(1,int(2*ui_scale))}" rx="4" />')
        
        svg = f"""
<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" style="background: white;">
  {''.join(svg_elements)}
</svg>
"""
        return svg

    @staticmethod
    def _escape_html(text: str) -> str:
        """HTML 특수문자 이스케이프"""
        return (
            (text or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    def save_html(self, html_content: str, end_date: str) -> str:
        """HTML 파일을 comparison 폴더에 저장"""
        import os
        import sys
        # mcp_tools 디렉토리를 sys.path에 추가
        mcp_tools_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        if mcp_tools_path not in sys.path:
            sys.path.insert(0, mcp_tools_path)
        
        from config.html_output_config import get_full_html_path
        
        # 중앙 설정에서 경로 가져오기
        out_path, latest_path = get_full_html_path(
            report_type='comparison',
            end_date=end_date,
            use_unified=False  # 각 폴더별로 분리
        )
        
        try:
            # HTML 파일 저장
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            
            # latest.html 동기화
            try:
                from shutil import copyfile
                copyfile(out_path, latest_path)
            except Exception:
                pass
            
            print(f"✅ HTML 리포트 저장 완료: {out_path}")
            return out_path
            
        except Exception as e:
            print(f"❌ HTML 저장 실패: {e}")
            raise
