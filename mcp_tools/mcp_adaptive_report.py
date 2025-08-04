"""
Adaptive HTML Report Generator with MCP Server
============================================

어떤 형태의 데이터든 받아서 자동으로 HTML 보고서를 생성하는 적응형 도구입니다.
MCP 서버 기능과 핵심 로직이 모두 포함되어 있습니다.

지원하는 데이터 형태:
- Dict (중첩 구조 포함)
- List of Dict (테이블 형태)
- Pandas DataFrame
- CSV/JSON 파일 경로
- ClickHouse 쿼리 결과
- 기타 구조화된 데이터

특징:
- 데이터 구조를 자동 분석해서 적절한 HTML 테이블/카드 생성
- 반응형 디자인 (모바일 친화적)
- 다크모드 지원
- 차트/그래프 자동 생성 (선택적)
- 커스텀 템플릿 지원
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Union, Optional, Literal

# 새로운 데이터베이스 매니저 import
from database_manager import get_site_client
import re

from fastmcp import FastMCP
def _load_css_file():
    """CSS 파일 로드"""
    css_path = Path(__file__).parent / "styles" / "adaptive_report.css"
    if css_path.exists():
        return css_path.read_text(encoding="utf-8")
    else:
        # 기본 CSS (파일이 없을 때)
        return """
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: sans-serif; line-height: 1.6; color: #333; background: #f5f7fa; }
        .container { max-width: 1200px; margin: 0 auto; background: white; min-height: 100vh; }
        .report-header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 40px; text-align: center; }
        .report-content { padding: 40px; }
        .data-table { width: 100%; border-collapse: collapse; }
        .data-table th { background: #34495e; color: white; padding: 15px; }
        .data-table td { padding: 12px 15px; border-bottom: 1px solid #e9ecef; }
        """

CHART_JS_CDN = """
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
"""

# FastMCP 인스턴스
mcp = FastMCP("adaptive_report")

class AdaptiveReportBuilder:
    """적응형 HTML 보고서 생성기"""
    
    def __init__(self, theme: Literal["light", "dark", "auto"] = "light"):
        self.theme = theme
        self.custom_css = ""
        self.custom_js = ""
        
    def generate(
        self,
        data: Any,
        *,
        title: str = "자동 생성 보고서",
        description: str = "",
        output_dir: str | os.PathLike[str] = "report",
        filename: Optional[str] = None,
        save: bool = True,
        include_charts: bool = False,
        custom_template: Optional[str] = None,
    ) -> tuple[str, str | None]:
        """
        범용 데이터로부터 HTML 보고서 생성
        
        Parameters
        ----------
        data : Any
            보고서로 만들 데이터. Dict, List, DataFrame, 파일경로 등 지원
        title : str
            보고서 제목
        description : str
            보고서 설명
        output_dir : str | PathLike
            출력 디렉토리
        filename : str, optional
            파일명 (없으면 자동 생성)
        save : bool
            파일로 저장할지 여부
        include_charts : bool
            차트 포함 여부 (Chart.js 사용)
        custom_template : str, optional
            커스텀 HTML 템플릿
            
        Returns
        -------
        html : str
            생성된 HTML 문자열
        file_path : str | None
            저장된 파일 경로 (save=False면 None)
        """
        
        # 1. 데이터 구조 자동 분석
        data_info = self._analyze_data_structure(data)
        
        # 2. 데이터 정규화 (모든 형태를 표준 dict로 변환)
        normalized_data = self._normalize_data(data, data_info)
        
        # 3. HTML 생성
        if custom_template:
            html = self._render_custom_template(custom_template, normalized_data, title, description)
        else:
            html = self._render_default_template(normalized_data, data_info, title, description, include_charts)
        
        # 4. 파일 저장
        file_path = None
        if save:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"report_{timestamp}.html"
            
            output_path = Path(output_dir).expanduser().resolve()
            output_path.mkdir(parents=True, exist_ok=True)
            file_path = str(output_path / filename)
            Path(file_path).write_text(html, encoding="utf-8")
        
        return html, file_path
    
    def _analyze_data_structure(self, data: Any) -> Dict[str, Any]:
        """데이터 구조 자동 분석"""
        info = {
            "type": type(data).__name__,
            "structure": "unknown",
            "columns": [],
            "row_count": 0,
            "has_nested": False,
            "numeric_columns": [],
            "text_columns": [],
            "date_columns": [],
        }
        
        # 파일 경로인 경우
        if isinstance(data, (str, Path)) and Path(data).exists():
            return self._analyze_file_data(data, info)
        
        # DataFrame인 경우
        try:
            import pandas as pd
            if isinstance(data, pd.DataFrame):
                return self._analyze_dataframe(data, info)
        except ImportError:
            pass
        
        # Dict인 경우
        if isinstance(data, dict):
            return self._analyze_dict_data(data, info)
        
        # List인 경우
        if isinstance(data, list):
            return self._analyze_list_data(data, info)
        
        return info
    
    def _analyze_file_data(self, file_path: str | Path, info: Dict[str, Any]) -> Dict[str, Any]:
        """파일 데이터 분석"""
        path = Path(file_path)
        info["source_file"] = str(path)
        
        if path.suffix.lower() == ".csv":
            try:
                import pandas as pd
                df = pd.read_csv(path)
                return self._analyze_dataframe(df, info)
            except ImportError:
                # pandas 없으면 csv 모듈 사용
                import csv
                with open(path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    return self._analyze_list_data(rows, info)
        
        elif path.suffix.lower() == ".json":
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return self._analyze_dict_data(data, info)
                elif isinstance(data, list):
                    return self._analyze_list_data(data, info)
        
        return info
    
    def _analyze_dataframe(self, df, info: Dict[str, Any]) -> Dict[str, Any]:
        """DataFrame 분석"""
        info["structure"] = "dataframe"
        info["columns"] = list(df.columns)
        info["row_count"] = len(df)
        
        # 컬럼 타입 분석
        for col in df.columns:
            if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                info["numeric_columns"].append(col)
            elif df[col].dtype == 'object':
                # 날짜 형태인지 확인
                sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else ""
                if self._is_date_like(str(sample)):
                    info["date_columns"].append(col)
                else:
                    info["text_columns"].append(col)
        
        return info
    
    def _analyze_dict_data(self, data: dict, info: Dict[str, Any]) -> Dict[str, Any]:
        """Dict 데이터 분석"""
        info["structure"] = "nested_dict"
        info["columns"] = list(data.keys())
        
        # 중첩 구조 확인
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                info["has_nested"] = True
                break
        
        return info
    
    def _analyze_list_data(self, data: list, info: Dict[str, Any]) -> Dict[str, Any]:
        """List 데이터 분석"""
        info["row_count"] = len(data)
        
        if not data:
            return info
        
        # 첫 번째 요소로 구조 파악
        first_item = data[0]
        
        if isinstance(first_item, dict):
            info["structure"] = "table"
            info["columns"] = list(first_item.keys())
            
            # 컬럼 타입 분석 (샘플링)
            for col in info["columns"]:
                sample_values = [item.get(col) for item in data[:10] if col in item and item[col] is not None]
                if sample_values:
                    sample = sample_values[0]
                    if isinstance(sample, (int, float)):
                        info["numeric_columns"].append(col)
                    elif self._is_date_like(str(sample)):
                        info["date_columns"].append(col)
                    else:
                        info["text_columns"].append(col)
        else:
            info["structure"] = "simple_list"
        
        return info
    
    def _is_date_like(self, text: str) -> bool:
        """문자열이 날짜 형태인지 확인"""
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # 2024-01-01
            r'\d{4}/\d{2}/\d{2}',  # 2024/01/01
            r'\d{2}-\d{2}-\d{4}',  # 01-01-2024
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # 2024-01-01 12:00:00
        ]
        return any(re.match(pattern, text) for pattern in date_patterns)
    
    def _normalize_data(self, data: Any, data_info: Dict[str, Any]) -> Dict[str, Any]:
        """데이터를 표준 형태로 정규화"""
        
        # 파일 경로인 경우 로드
        if isinstance(data, (str, Path)) and Path(data).exists():
            data = self._load_file_data(data)
        
        # DataFrame인 경우
        try:
            import pandas as pd
            if isinstance(data, pd.DataFrame):
                return {
                    "type": "table",
                    "data": data.to_dict('records'),
                    "columns": list(data.columns),
                    "summary": self._generate_dataframe_summary(data),
                }
        except ImportError:
            pass
        
        # Dict인 경우
        if isinstance(data, dict):
            return {
                "type": "nested_dict",
                "data": data,
                "summary": self._generate_dict_summary(data),
            }
        
        # List인 경우
        if isinstance(data, list):
            if data and isinstance(data[0], dict):
                return {
                    "type": "table",
                    "data": data,
                    "columns": list(data[0].keys()) if data else [],
                    "summary": self._generate_table_summary(data),
                }
            else:
                return {
                    "type": "simple_list",
                    "data": data,
                    "summary": {"count": len(data)},
                }
        
        # 기타 단순 데이터
        return {
            "type": "simple",
            "data": data,
            "summary": {"value": str(data), "type": type(data).__name__},
        }
    
    def _load_file_data(self, file_path: str | Path):
        """파일에서 데이터 로드"""
        path = Path(file_path)
        
        if path.suffix.lower() == ".csv":
            try:
                import pandas as pd
                return pd.read_csv(path)
            except ImportError:
                import csv
                with open(path, 'r', newline='', encoding='utf-8') as f:
                    return list(csv.DictReader(f))
        
        elif path.suffix.lower() == ".json":
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        return None
    
    def _generate_dataframe_summary(self, df) -> Dict[str, Any]:
        """DataFrame 요약 통계"""
        summary = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": {}
        }
        
        for col in df.columns:
            col_summary = {
                "type": str(df[col].dtype),
                "null_count": df[col].isnull().sum(),
                "unique_count": df[col].nunique(),
            }
            
            if df[col].dtype in ['int64', 'float64']:
                col_summary.update({
                    "min": df[col].min(),
                    "max": df[col].max(),
                    "mean": df[col].mean(),
                })
            
            summary["columns"][col] = col_summary
        
        return summary
    
    def _generate_table_summary(self, data: List[Dict]) -> Dict[str, Any]:
        """테이블 형태 데이터 요약"""
        if not data:
            return {"row_count": 0}
        
        columns = list(data[0].keys())
        summary = {
            "row_count": len(data),
            "column_count": len(columns),
            "columns": {}
        }
        
        for col in columns:
            values = [row.get(col) for row in data if col in row and row[col] is not None]
            summary["columns"][col] = {
                "non_null_count": len(values),
                "unique_count": len(set(str(v) for v in values)),
                "sample_values": list(set(str(v) for v in values[:5])),
            }
        
        return summary
    
    def _generate_dict_summary(self, data: Dict) -> Dict[str, Any]:
        """Dict 데이터 요약"""
        return {
            "key_count": len(data),
            "keys": list(data.keys()),
            "nested_keys": [k for k, v in data.items() if isinstance(v, (dict, list))],
        }
    
    def _render_default_template(
        self, 
        normalized_data: Dict[str, Any], 
        data_info: Dict[str, Any],
        title: str, 
        description: str,
        include_charts: bool
    ) -> str:
        """기본 HTML 템플릿 렌더링"""
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 데이터 섹션 렌더링
        data_html = self._render_data_section(normalized_data, data_info, include_charts)
        
        # CSS 파일 로드
        css_content = _load_css_file()
        
        html = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>{css_content}</style>
    {CHART_JS_CDN if include_charts else ""}
</head>
<body>
    <div class="container">
        <header class="report-header">
            <h1>📊 {title}</h1>
            {f'<p class="description">{description}</p>' if description else ''}
            <div class="meta-info">
                <span>📅 생성일시: {current_time}</span>
                <span>📈 데이터 타입: {normalized_data['type']}</span>
            </div>
        </header>
        
        <main class="report-content">
            {data_html}
        </main>
        
        <footer class="report-footer">
            <p>🤖 이 보고서는 Adaptive HTML Report Builder에 의해 자동 생성되었습니다.</p>
        </footer>
    </div>
</body>
</html>
"""
        return html
    
    def _render_data_section(self, normalized_data: Dict[str, Any], data_info: Dict[str, Any], include_charts: bool) -> str:
        """데이터 섹션 렌더링"""
        
        data_type = normalized_data["type"]
        
        if data_type == "table":
            return self._render_table_section(normalized_data, include_charts)
        elif data_type == "nested_dict":
            return self._render_dict_section(normalized_data)
        elif data_type == "simple_list":
            return self._render_list_section(normalized_data)
        else:
            return self._render_simple_section(normalized_data)
    
    def _render_table_section(self, data: Dict[str, Any], include_charts: bool) -> str:
        """테이블 데이터 렌더링"""
        rows = data["data"]
        columns = data["columns"]
        summary = data.get("summary", {})
        
        if not rows:
            return "<p>데이터가 없습니다.</p>"
        
        # 요약 정보
        summary_html = f"""
        <section class="summary-section">
            <h2>📋 데이터 요약</h2>
            <div class="summary-grid">
                <div class="summary-item">
                    <span class="summary-label">총 행 수</span>
                    <span class="summary-value">{len(rows):,}</span>
                </div>
                <div class="summary-item">
                    <span class="summary-label">총 열 수</span>
                    <span class="summary-value">{len(columns)}</span>
                </div>
            </div>
        </section>
        """
        
        # 테이블
        table_html = """
        <section class="table-section">
            <h2>📊 데이터 테이블</h2>
            <div class="table-container">
                <table class="data-table">
                    <thead><tr>
        """
        
        for col in columns:
            table_html += f"<th>{col}</th>"
        
        table_html += "</tr></thead><tbody>"
        
        # 최대 100행만 표시 (성능을 위해)
        display_rows = rows[:100]
        for row in display_rows:
            table_html += "<tr>"
            for col in columns:
                value = row.get(col, "")
                # 긴 텍스트는 줄임
                if isinstance(value, str) and len(value) > 50:
                    value = value[:47] + "..."
                table_html += f"<td>{value}</td>"
            table_html += "</tr>"
        
        if len(rows) > 100:
            table_html += f"<tr><td colspan='{len(columns)}' class='more-rows'>... 총 {len(rows)}행 중 100행 표시</td></tr>"
        
        table_html += "</tbody></table></div></section>"
        
        # 차트 (선택적)
        chart_html = ""
        if include_charts:
            chart_html = self._render_charts(data)
        
        return summary_html + table_html + chart_html
    
    def _render_dict_section(self, data: Dict[str, Any]) -> str:
        """Dict 데이터 렌더링"""
        dict_data = data["data"]
        
        html = """
        <section class="dict-section">
            <h2>🗂️ 구조화된 데이터</h2>
            <div class="dict-container">
        """
        
        html += self._render_dict_recursive(dict_data, level=0)
        html += "</div></section>"
        
        return html
    
    def _render_dict_recursive(self, data: Any, level: int = 0) -> str:
        """Dict를 재귀적으로 렌더링"""
        if level > 3:  # 깊이 제한
            return "<span class='truncated'>...</span>"
        
        if isinstance(data, dict):
            html = "<div class='dict-level'>"
            for key, value in data.items():
                html += f"""
                <div class="dict-item">
                    <span class="dict-key">{key}:</span>
                    <span class="dict-value">{self._render_dict_recursive(value, level + 1)}</span>
                </div>
                """
            html += "</div>"
            return html
        
        elif isinstance(data, list):
            if len(data) > 10:  # 긴 리스트는 일부만 표시
                items = data[:10]
                html = "<div class='list-container'>"
                for i, item in enumerate(items):
                    html += f"<div class='list-item'>[{i}] {self._render_dict_recursive(item, level + 1)}</div>"
                html += f"<div class='list-more'>... 총 {len(data)}개 항목</div></div>"
                return html
            else:
                html = "<div class='list-container'>"
                for i, item in enumerate(data):
                    html += f"<div class='list-item'>[{i}] {self._render_dict_recursive(item, level + 1)}</div>"
                html += "</div>"
                return html
        
        else:
            # 단순 값
            value_str = str(data)
            if len(value_str) > 100:
                value_str = value_str[:97] + "..."
            return f"<span class='simple-value'>{value_str}</span>"
    
    def _render_list_section(self, data: Dict[str, Any]) -> str:
        """단순 리스트 렌더링"""
        list_data = data["data"]
        
        html = f"""
        <section class="list-section">
            <h2>📝 리스트 데이터 ({len(list_data)}개 항목)</h2>
            <div class="simple-list">
        """
        
        # 최대 50개 항목만 표시
        display_items = list_data[:50]
        for i, item in enumerate(display_items):
            html += f"<div class='list-item'>{i+1}. {item}</div>"
        
        if len(list_data) > 50:
            html += f"<div class='more-items'>... 총 {len(list_data)}개 항목 중 50개 표시</div>"
        
        html += "</div></section>"
        return html
    
    def _render_simple_section(self, data: Dict[str, Any]) -> str:
        """단순 데이터 렌더링"""
        value = data["data"]
        summary = data.get("summary", {})
        
        return f"""
        <section class="simple-section">
            <h2>📄 단순 데이터</h2>
            <div class="simple-data">
                <div class="data-type">타입: {summary.get('type', 'unknown')}</div>
                <div class="data-value">{value}</div>
            </div>
        </section>
        """
    
    def _render_charts(self, data: Dict[str, Any]) -> str:
        """차트 렌더링 (Chart.js 사용)"""
        # 간단한 차트 예시 - 숫자 컬럼이 있으면 히스토그램
        return """
        <section class="chart-section">
            <h2>📈 차트</h2>
            <div class="chart-container">
                <canvas id="dataChart"></canvas>
            </div>
            <script>
                // Chart.js 코드는 여기에 추가
                console.log('차트 기능은 추후 구현 예정');
            </script>
        </section>
        """
    
    def _render_custom_template(self, template: str, data: Dict[str, Any], title: str, description: str) -> str:
        """커스텀 템플릿 렌더링"""
        # 간단한 템플릿 변수 치환
        template = template.replace("{{title}}", title)
        template = template.replace("{{description}}", description)
        template = template.replace("{{data}}", json.dumps(data, ensure_ascii=False, indent=2))
        template = template.replace("{{timestamp}}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return template

# 편의 함수
def generate_report(
    data: Any,
    title: str = "자동 생성 보고서",
    description: str = "",
    output_dir: str = "report",
    **kwargs
) -> tuple[str, str | None]:
    """
    간편한 보고서 생성 함수
    
    Usage:
        html, path = generate_report(my_data, title="매출 분석", include_charts=True)
    """
    builder = AdaptiveReportBuilder()
    return builder.generate(
        data, 
        title=title, 
        description=description, 
        output_dir=output_dir,
        **kwargs
    )

def auto_detect_data_structure(data: Any) -> Dict[str, Any]:
    """데이터 구조 자동 감지 (분석용)"""
    builder = AdaptiveReportBuilder()
    return builder._analyze_data_structure(data)

# ============================================================================
# MCP 툴들
# ============================================================================

@mcp.tool()
def create_html_report(
    *,
    data: Union[Dict, List, str],
    title: str = "자동 생성 보고서",
    description: str = "",
    output_dir: str = "report",
    include_charts: bool = False,
    theme: str = "light",
    custom_template: Optional[str] = None,
    site: str) -> str:
    """
    **적응형 HTML 보고서 생성기** - 어떤 데이터든 받아서 자동으로 HTML 보고서 생성
    
    Trigger words (case-insensitive):
        - "보고서", "report", "html", "리포트"
        - "데이터 분석", "data analysis", "시각화", "visualization"
        - "표", "table", "차트", "chart", "그래프", "graph"
    
    이 도구는 다양한 형태의 데이터를 받아서 자동으로 구조를 분석하고
    아름다운 HTML 보고서를 생성합니다. 특정 데이터 형태에 종속되지 않으며
    어떤 구조의 데이터든 적절한 형태로 시각화합니다.
    
    지원하는 데이터 형태:
    - Dict (중첩 구조 포함): {"key1": "value1", "nested": {"key2": "value2"}}
    - List of Dict (테이블): [{"name": "A", "age": 25}, {"name": "B", "age": 30}]
    - 파일 경로: CSV, JSON 파일 경로를 넘기면 자동으로 로드
    - 단순 리스트: ["item1", "item2", "item3"]
    - 기타 구조화된 데이터
    
    Parameters
    ----------
    data : dict | list | str
        보고서로 만들 데이터. 
        - Dict/List: 직접 데이터 전달
        - str: 파일 경로 (CSV, JSON 지원)
    title : str, default="자동 생성 보고서"
        보고서 제목
    description : str, default=""
        보고서 설명/부제목
    output_dir : str, default="report"
        HTML 파일을 저장할 디렉토리
    include_charts : bool, default=False
        차트/그래프 포함 여부 (Chart.js 사용)
    theme : str, default="light"
        테마 ("light", "dark", "auto")
    custom_template : str, optional
        커스텀 HTML 템플릿 (고급 사용자용)
    
    Returns
    -------
    str
        생성된 HTML 보고서의 파일 경로와 웹 접근 URL
    
    Examples
    --------
    # 테이블 형태 데이터
    data = [
        {"매장": "강남점", "매출": 1500, "방문객": 320},
        {"매장": "홍대점", "매출": 1200, "방문객": 280}
    ]
    
    # 중첩 Dict 데이터
    data = {
        "매장정보": {"이름": "강남점", "주소": "서울시 강남구"},
        "통계": {"일평균매출": 1500, "월평균방문객": 9600}
    }
    
    # CSV 파일
    data = "/path/to/sales_data.csv"
    """
    
    try:
        # 문자열이 파일 경로인지 확인
        if isinstance(data, str) and not Path(data).exists():
            # JSON 문자열일 수도 있으니 파싱 시도
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                return f"❌ 오류: 파일을 찾을 수 없습니다: {data}"
        
        # 보고서 생성
        builder = AdaptiveReportBuilder(theme=theme)
        html, file_path = builder.generate(
            data,
            title=title,
            description=description,
            output_dir=output_dir,
            save=True,
            include_charts=include_charts,
            custom_template=custom_template,
        )
        
        if not file_path:
            return "❌ 보고서 생성 실패"
        
        # 웹 접근 가능한 URL 생성 (chat 디렉토리에도 복사)
        filename = Path(file_path).name
        chat_report_dir = Path("../chat/report")
        chat_report_dir.mkdir(parents=True, exist_ok=True)
        
        chat_file_path = chat_report_dir / filename
        chat_file_path.write_text(html, encoding="utf-8")
        
        web_url = f"/reports/{filename}"
        
        return f"""📊 **적응형 HTML 보고서 생성 완료!**

🔗 **[웹에서 보기]({web_url})**

📁 **파일 정보:**
- 로컬 경로: `{file_path}`
- 웹 경로: `{web_url}`
- 테마: {theme}
- 차트 포함: {'✅' if include_charts else '❌'}

💡 **생성된 보고서 특징:**
- 📱 반응형 디자인 (모바일 친화적)
- 🎨 현대적인 UI/UX
- 📊 데이터 구조 자동 분석
- 📈 요약 통계 포함

보고서를 클릭하여 새 탭에서 확인하세요!"""
        
    except Exception as e:
        return f"❌ 보고서 생성 중 오류 발생: {str(e)}"

@mcp.tool()
def analyze_data_structure(
    *,
    data: Union[Dict, List, str],
    site: str) -> str:
    """
    **데이터 구조 분석기** - 데이터의 구조와 특성을 자동으로 분석
    
    보고서를 생성하기 전에 데이터의 구조를 미리 파악하고 싶을 때 사용합니다.
    데이터 타입, 컬럼 정보, 행 수, 데이터 품질 등을 분석합니다.
    
    Parameters
    ----------
    data : dict | list | str
        분석할 데이터 (보고서 생성과 동일한 형태)
    
    Returns
    -------
    str
        데이터 구조 분석 결과 (마크다운 형태)
    """
    
    try:
        # 파일 경로인 경우 처리
        if isinstance(data, str):
            if Path(data).exists():
                pass  # 파일 경로 그대로 사용
            else:
                # JSON 문자열 파싱 시도
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    return f"❌ 오류: 유효하지 않은 데이터 형태입니다."
        
        # 구조 분석
        builder = AdaptiveReportBuilder()
        analysis = builder._analyze_data_structure(data)
        normalized = builder._normalize_data(data, analysis)
        
        # 분석 결과를 마크다운으로 포맷팅
        result = f"""# 📊 데이터 구조 분석 결과

## 🔍 기본 정보
- **데이터 타입**: `{analysis['type']}`
- **구조 형태**: `{analysis['structure']}`
- **행 수**: {analysis.get('row_count', 'N/A'):,}개
- **중첩 구조**: {'✅ 있음' if analysis.get('has_nested', False) else '❌ 없음'}

## 📋 컬럼 정보
"""
        
        if analysis.get('columns'):
            result += f"- **총 컬럼 수**: {len(analysis['columns'])}개\n"
            result += f"- **컬럼 목록**: {', '.join(analysis['columns'])}\n\n"
            
            if analysis.get('numeric_columns'):
                result += f"- **숫자 컬럼** ({len(analysis['numeric_columns'])}개): {', '.join(analysis['numeric_columns'])}\n"
            if analysis.get('text_columns'):
                result += f"- **텍스트 컬럼** ({len(analysis['text_columns'])}개): {', '.join(analysis['text_columns'])}\n"
            if analysis.get('date_columns'):
                result += f"- **날짜 컬럼** ({len(analysis['date_columns'])}개): {', '.join(analysis['date_columns'])}\n"
        else:
            result += "- 컬럼 정보 없음 (단순 데이터 구조)\n"
        
        # 요약 통계
        if 'summary' in normalized:
            summary = normalized['summary']
            result += f"\n## 📈 요약 통계\n"
            
            if normalized['type'] == 'table':
                result += f"- **총 행 수**: {summary.get('row_count', 0):,}개\n"
                result += f"- **총 열 수**: {summary.get('column_count', 0)}개\n"
                
                if 'columns' in summary:
                    result += f"\n### 컬럼별 상세 정보\n"
                    for col, info in summary['columns'].items():
                        result += f"- **{col}**: 비어있지않은값 {info.get('non_null_count', 0)}개, 고유값 {info.get('unique_count', 0)}개\n"
            
            elif normalized['type'] == 'nested_dict':
                result += f"- **키 개수**: {summary.get('key_count', 0)}개\n"
                if summary.get('nested_keys'):
                    result += f"- **중첩 키**: {', '.join(summary['nested_keys'])}\n"
        
        # 권장사항
        result += f"\n## 💡 보고서 생성 권장사항\n"
        
        if analysis['structure'] == 'table':
            result += "- ✅ **테이블 형태** - 표와 차트로 시각화하기 적합합니다\n"
            if analysis.get('numeric_columns'):
                result += "- 📊 **차트 생성 권장** - 숫자 컬럼이 있어 차트 생성이 유용할 것 같습니다\n"
        elif analysis['structure'] == 'nested_dict':
            result += "- 🗂️ **구조화된 데이터** - 계층형 카드 레이아웃으로 표시됩니다\n"
        elif analysis['structure'] == 'simple_list':
            result += "- 📝 **단순 리스트** - 목록 형태로 깔끔하게 표시됩니다\n"
        
        if analysis.get('row_count', 0) > 1000:
            result += "- ⚠️ **대용량 데이터** - 성능을 위해 일부 행만 표시될 수 있습니다\n"
        
        return result
        
    except Exception as e:
        return f"❌ 데이터 분석 중 오류 발생: {str(e)}"

@mcp.tool()
def create_report_from_clickhouse(
    *,
    query: str,
    title: str = "ClickHouse 쿼리 결과 보고서",
    description: str = "",
    database: str = "plusinsight",
    include_charts: bool = True,
    site: str) -> str:
    """
    **ClickHouse 쿼리 결과로 보고서 생성** - SQL 쿼리를 실행하고 결과를 HTML 보고서로 생성
    
    ClickHouse 데이터베이스에서 직접 쿼리를 실행하고 결과를 자동으로 분석하여
    아름다운 HTML 보고서를 생성합니다.
    
    Parameters
    ----------
    query : str
        실행할 SQL 쿼리
    title : str, default="ClickHouse 쿼리 결과 보고서"
        보고서 제목
    description : str, default=""
        보고서 설명
    database : str, default="plusinsight"
        사용할 데이터베이스명
    include_charts : bool, default=True
        차트 포함 여부
    
    Returns
    -------
    str
        생성된 보고서 정보
    """
    
    try:
        # ClickHouse 클라이언트 생성 (기존 워크플로우 로직 재사용)
        import clickhouse_connect
        from dotenv import load_dotenv
        
        load_dotenv()
        
        # 환경변수 로드
        CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
        CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
        CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
        CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
        
        if not all([CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD]):
            return "❌ ClickHouse 연결 정보가 설정되지 않았습니다. 환경변수를 확인해주세요."
        
        # 클라이언트 생성
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=int(CLICKHOUSE_PORT),
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=database,
        )
        
        # 쿼리 실행
        result = client.query(query)
        
        if not result.result_rows:
            return "❌ 쿼리 결과가 없습니다."
        
        # 결과를 List[Dict] 형태로 변환
        columns = [col[0] for col in result.column_names]
        data = []
        for row in result.result_rows:
            data.append(dict(zip(columns, row)))
        
        # 보고서 생성
        html, file_path = generate_report(
            data,
            title=title,
            description=f"{description}\n\n**실행된 쿼리:**\n```sql\n{query}\n```" if description else f"**실행된 쿼리:**\n```sql\n{query}\n```",
            include_charts=include_charts,
        )
        
        if not file_path:
            return "❌ 보고서 생성 실패"
        
        # 웹 접근 가능한 URL 생성
        filename = Path(file_path).name
        chat_report_dir = Path("../chat/report")
        chat_report_dir.mkdir(parents=True, exist_ok=True)
        
        chat_file_path = chat_report_dir / filename
        chat_file_path.write_text(html, encoding="utf-8")
        
        web_url = f"/reports/{filename}"
        
        return f"""📊 **ClickHouse 쿼리 결과 보고서 생성 완료!**

🔗 **[웹에서 보기]({web_url})**

📊 **쿼리 결과:**
- 총 {len(data):,}행 데이터
- {len(columns)}개 컬럼: {', '.join(columns)}
- 데이터베이스: `{database}`

💡 보고서를 클릭하여 새 탭에서 확인하세요!"""
        
    except Exception as e:
        return f"❌ ClickHouse 쿼리 실행 또는 보고서 생성 중 오류 발생: {str(e)}"

@mcp.tool()
def create_report_from_csv(
    *,
    csv_path: str,
    title: str = "CSV 데이터 보고서",
    description: str = "",
    include_charts: bool = True,
    encoding: str = "utf-8",
    site: str) -> str:
    """
    **CSV 파일로 보고서 생성** - CSV 파일을 읽어서 HTML 보고서 생성
    
    Parameters
    ----------
    csv_path : str
        CSV 파일 경로
    title : str, default="CSV 데이터 보고서"
        보고서 제목
    description : str, default=""
        보고서 설명
    include_charts : bool, default=True
        차트 포함 여부
    encoding : str, default="utf-8"
        파일 인코딩
    
    Returns
    -------
    str
        생성된 보고서 정보
    """
    
    try:
        if not Path(csv_path).exists():
            return f"❌ CSV 파일을 찾을 수 없습니다: {csv_path}"
        
        # CSV 파일 읽기
        import csv
        data = []
        with open(csv_path, 'r', encoding=encoding) as f:
            reader = csv.DictReader(f)
            data = list(reader)
        
        if not data:
            return "❌ CSV 파일이 비어있습니다."
        
        # 보고서 생성
        html, file_path = generate_report(
            data,
            title=title,
            description=f"{description}\n\n**소스 파일:** `{csv_path}`" if description else f"**소스 파일:** `{csv_path}`",
            include_charts=include_charts,
        )
        
        if not file_path:
            return "❌ 보고서 생성 실패"
        
        # 웹 접근
        filename = Path(file_path).name
        chat_report_dir = Path("../chat/report")
        chat_report_dir.mkdir(parents=True, exist_ok=True)
        
        chat_file_path = chat_report_dir / filename
        chat_file_path.write_text(html, encoding="utf-8")
        
        web_url = f"/reports/{filename}"
        
        return f"""📊 **CSV 보고서 생성 완료!**

🔗 **[웹에서 보기]({web_url})**

📁 **파일 정보:**
- 소스: `{csv_path}`
- 총 {len(data):,}행 데이터
- {len(data[0])}개 컬럼: {', '.join(data[0].keys())}

💡 보고서를 클릭하여 새 탭에서 확인하세요!"""
        
    except Exception as e:
        return f"❌ CSV 보고서 생성 중 오류 발생: {str(e)}"

# get_available_sites 기능은 mcp_agent_helper.py로 분리됨

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Adaptive Report MCP Server")
    parser.add_argument("--cli", action="store_true", help="CLI 테스트 모드")
    args = parser.parse_args()
    
    if args.cli:
        # CLI 테스트
        test_data = [
            {"매장": "강남점", "매출": 1500000, "방문객": 320, "평점": 4.5},
            {"매장": "홍대점", "매출": 1200000, "방문객": 280, "평점": 4.2},
            {"매장": "명동점", "매출": 1800000, "방문객": 450, "평점": 4.7},
            {"매장": "신촌점", "매출": 900000, "방문객": 210, "평점": 4.0},
        ]
        
        html, path = generate_report(
            test_data,
            title="매장별 성과 분석 보고서",
            description="주요 매장들의 매출, 방문객, 평점 데이터를 종합한 성과 분석 보고서입니다.",
            include_charts=True,
        )
        
        print(f"✅ 테스트 보고서 생성 완료: {path}")
        print(f"📊 데이터: {len(test_data)}행, {len(test_data[0])}열")
        
    else:
        # FastMCP 서버 실행
        print("Adaptive Report MCP 서버 시작...", file=sys.stderr)
        try:
            mcp.run()
        except Exception as e:
            print(f"서버 오류: {e}", file=sys.stderr)