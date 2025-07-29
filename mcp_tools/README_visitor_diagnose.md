# 방문객 진단 워크플로우 - 버전 가이드

## 📊 개요
매장 방문객 데이터를 분석하여 진단 보고서를 생성하는 워크플로우입니다.
현재 **HTML 버전(최신)**과 **Excel 버전(레거시)**을 모두 지원합니다.

## 🆕 HTML 버전 (권장) - `visitor_diagnose_workflow.py`

### ✨ 주요 특징
- **현대적인 웹 디자인**: 그라디언트, 카드 레이아웃, 반응형 디자인
- **즉시 확인 가능**: 웹 브라우저에서 바로 열어볼 수 있음
- **모바일 친화적**: 스마트폰, 태블릿에서 완벽하게 표시
- **컴팩트한 레이아웃**: 여러 매장을 한 줄에 효율적으로 표시
- **AI 하이라이트**: 중요한 지표를 글씨 색상으로 강조 (빨간색/파란색)
- **가벼운 파일**: HTML 파일로 빠른 로딩

### 🎯 사용법
```python
from visitor_diagnose_workflow import visitor_diagnose_html

# 단일 매장
result = visitor_diagnose_html(
    store_name="망우혜원점",
    start_date="2025-01-01", 
    end_date="2025-01-31"
)

# 여러 매장 (쉼표로 구분)
result = visitor_diagnose_html(
    store_name="망우혜원점,더미데이터점,매장A",
    start_date="2025-01-01",
    end_date="2025-01-31"
)
```

### 🎨 출력 결과
- **파일명**: `report/방문객진단_YYYYMMDD_HHMMSS.html`
- **내용**: 매장별 카드 + 비교 테이블 + AI 하이라이트
- **크기**: 약 30-50KB (매장 수에 따라)

## 📊 Excel 버전 (레거시) - `visitor_diagnose_workflow_legacy_excel.py`

### 📋 주요 특징
- **엑셀 파일 생성**: .xlsx 형식의 정교한 표 생성
- **복잡한 레이아웃**: 셀 병합, 테두리, 정렬 등 세밀한 포맷팅
- **Excel 호환성**: Microsoft Excel에서 완벽하게 열림
- **MCP 서버 연동**: excel-mcp-server를 통한 고급 엑셀 조작

### ⚠️ 제한사항
- **파일 크기 큼**: 복잡한 포맷팅으로 인한 용량 증가
- **의존성 많음**: openpyxl, excel-mcp-server 등 추가 라이브러리 필요
- **모바일 지원 제한**: 스마트폰에서 보기 어려움
- **즉시 확인 불가**: Excel 프로그램이 필요

### 🎯 사용법
```python
from visitor_diagnose_workflow_legacy_excel import visitor_diagnose_excel

result = visitor_diagnose_excel(
    store_name="망우혜원점",
    start_date="2025-01-01",
    end_date="2025-01-31"
)
```

## 🔄 버전 선택 가이드

| 기준 | HTML 버전 ✅ | Excel 버전 |
|------|-------------|------------|
| **즉시 확인** | ✅ 브라우저에서 바로 | ❌ Excel 프로그램 필요 |
| **모바일 지원** | ✅ 완벽 지원 | ❌ 제한적 |
| **파일 크기** | ✅ 가벼움 (30-50KB) | ❌ 무거움 (200KB+) |
| **공유 편의성** | ✅ 링크만 보내면 됨 | ❌ 파일 다운로드 필요 |
| **시각적 매력** | ✅ 현대적 디자인 | ⭕ 전통적 표 형식 |
| **의존성** | ✅ 최소한 | ❌ 많음 |
| **인쇄 품질** | ⭕ 좋음 | ✅ 매우 좋음 |
| **데이터 편집** | ❌ 불가능 | ✅ Excel에서 편집 가능 |

## 🚀 권장사항

### HTML 버전을 사용하세요 (대부분의 경우)
- 빠른 확인이 필요한 경우
- 모바일에서 볼 가능성이 있는 경우  
- 여러 사람과 공유해야 하는 경우
- 시각적으로 매력적인 보고서가 필요한 경우

### Excel 버전을 사용하세요 (특수한 경우만)
- 데이터를 추가로 편집해야 하는 경우
- 기존 Excel 워크플로우와 연동해야 하는 경우
- 고품질 인쇄가 필수인 경우
- 전통적인 표 형식을 선호하는 경우

## 🔧 설치 및 실행

### HTML 버전
```bash
# 기본 의존성만 필요
pip install fastmcp langchain-openai langgraph pandas

# 실행
python visitor_diagnose_workflow.py --cli --store "매장명"
```

### Excel 버전 (레거시)
```bash
# 추가 의존성 필요
pip install openpyxl
npm install -g @negokaz/excel-mcp-server

# 실행  
python visitor_diagnose_workflow_legacy_excel.py --cli --store "매장명"
```

---

💡 **결론**: 특별한 이유가 없다면 **HTML 버전**을 사용하는 것을 강력히 권장합니다! 