# 채팅 UI 실행방법

chat/.env 파일 생성 필요

```
OPENAI_API_KEY=your_api_key
VITE_API_URL=http://192.168.49.157:8501

# If you want to debug it
LANGSMITH_TRACING=true
LANGSMITH_API_KEY=your_api_key_if_tracing_is_true
LANGSMITH_ENDPOINT=https://api.smith.langchain.com
LANGSMITH_PROJECT=LangGraph-MCP-Agents
```

```shell
cd chat
bash build.sh # .env 파일을 생성한 다음에 해야함
```

# MCP 서버 환경 설정

`.env` 파일에 다음 변수를 설정:

```env
# OPENAI API KEY
OPENAI_API_KEY=your_api_key

# SSH 터널링 (선택사항 - SSH_HOST가 있으면 자동으로 SSH 터널 사용)
SSH_HOST=your-ssh-server.com
SSH_PORT=your-ssh-port
SSH_USERNAME=your-username
SSH_PASSWORD=your-password

# ClickHouse 데이터베이스 연결
CLICKHOUSE_HOST=your-host
CLICKHOUSE_PORT=your-port
CLICKHOUSE_USER=your-username
CLICKHOUSE_PASSWORD=your-password
```

## 설치

```bash
# 기본 패키지
pip install -r requirements-backend.txt

# SSH 터널링을 사용할 경우 추가 설치
pip install sshtunnel paramiko
```

## MCP 서버 연결 방식

각 MCP 서버가 독립적으로 `.env` 파일에서 설정을 읽어 ClickHouse에 연결합니다:

- SSH 설정이 있으면 자동으로 SSH 터널링 사용
- SSH 설정이 없거나 실패하면 직접 연결
- 각 프로세스별로 독립적인 터널 관리

# MCP Diagnose

MCP Diagnose는 FastMCP를 활용하여 편의점 데이터에 대한 다양한 진단 분석을 수행할 수 있는 도구입니다.

```json
{
  "mcpServers": {
    "diagnose": {
      "command": "python3",
      "args": ["mcp_diagnose.py"]
    }
  }
}
```

## 제공 도구

### get_db_name

편의점 이름과 데이터베이스 매핑 정보를 조회합니다.

```python
get_db_name() -> str
```

### diagnose_avg_in

일평균 방문객 수를 진단합니다. 성별, 연령대별 방문자 수를 포함합니다.

```python
diagnose_avg_in(start_date: str, end_date: str) -> str
```

### diagnose_avg_sales

일평균 판매 건수를 진단합니다.

```python
diagnose_avg_sales(start_date: str, end_date: str) -> str
```

### check_zero_visits

방문객수 데이터 이상(0명인 날짜 또는 시간대) 정보를 조회합니다.

```python
check_zero_visits(start_date: str, end_date: str, database: str) -> str
```

### diagnose_purchase_conversion_rate

구매전환율(방문객 대비 판매 건수)을 진단합니다.

```python
diagnose_purchase_conversion_rate(start_date: str, end_date: str) -> str
```

### diagnose_exploratory_tendency

고객의 탐색 경향성을 진단합니다. 1인당 진열대 방문, 노출, 픽업 데이터를 제공합니다.

```python
diagnose_exploratory_tendency(start_date: str, end_date: str) -> str
```

### diagnose_shelf

진열대 진단 정보를 제공합니다. 방문, 노출, 픽업 등이 많거나 적은 진열대를 분석합니다.

```python
diagnose_shelf(start_date: str, end_date: str) -> str
```

### diagnose_table_occupancy

시식대 혼잡도를 진단합니다. 평균 및 최대 점유 인원, 세션 정보를 포함합니다.

```python
diagnose_table_occupancy(start_date: str, end_date: str) -> str
```

# MCP Report

MCP Report는 FastMCP를 활용하여 파일 읽기/쓰기, 데이터 처리 및 기본적인 수학 연산을 수행할 수 있는 도구입니다.

```json
{
  "mcpServers": {
    "report": {
      "command": "python3",
      "args": ["mcp_report.py"]
    }
  }
}
```

## 제공 도구

### read_file

텍스트 파일 내용을 읽어옵니다. 토큰 제한을 초과하는 경우 이진 탐색으로 적절한 범위의 내용을 반환합니다.

```python
read_file(file_path: str, start_line: int = 0, end_line: int = 100) -> str
```

### read_excel

Excel 파일을 읽고 시트 정보와 데이터를 반환합니다. 토큰 제한을 고려하여 적절한 범위의 데이터를 표시합니다.

```python
read_excel(file_path: str, sheet_name: str = None, max_rows: int = 1000) -> str
```

### save_file

텍스트 내용을 파일에 저장합니다.

```python
save_file(file_path: str, content: str) -> str
```

### edit_file

파일의 특정 라인 범위를 지정된 내용으로 수정합니다.

```python
edit_file(file_path: str, start_line: int = 0, end_line: int = 100, content: str) -> str
```

### append_file

텍스트 내용을 파일에 추가합니다.

```python
append_file(file_path: str, content: str) -> str
```

### get_current_time

현재 시간을 반환합니다.

```python
get_current_time() -> str
```

### 기본 수학 연산

기본적인 사칙연산 도구를 제공합니다.

```python
sum(a: int, b: int) -> int
subtract(a: int, b: int) -> int
multiply(a: int, b: int) -> int
divide(a: int, b: int) -> float
```

# MCP Insight

MCP Insight는 FastMCP를 활용하여 ClickHouse 데이터베이스에서 고객 행동 분석 데이터를 조회하고 활용할 수 있는 도구입니다.

```json
{
  "mcpServers": {
    "insight": {
      "command": "python3",
      "args": ["mcp_insight.py"]
    }
  }
}
```

## 제공 도구

### pickup_transition

픽업 구역 전환 데이터를 조회합니다. 고객이 한 구역에서 다른 구역으로 이동한 패턴을 분석할 수 있습니다.

```python
pickup_transition(database: str, start_date: str, end_date: str) -> int
```

### sales_funnel_pickup_rate

sales_funnel 테이블에서 제품 진열대별 픽업 비율(pickup_rate)을 계산합니다.

```python
sales_funnel_pickup_rate(database: str, start_date: str, end_date: str) -> str
```

### representative_movement

고객의 대표적인 이동 경로를 조회하여 성별, 연령대별 대표 이동 패턴을 제공합니다.

```python
representative_movement(database: str, start_date: str, end_date: str, limit: int = 20) -> str
```

### inflow_by_entrance_line

입구 라인별 유입 데이터를 분석합니다. 각 입구별로 성별, 연령대별 방문자 수와 유입 비율을 제공합니다.

```python
inflow_by_entrance_line(database: str, start_date: str, end_date: str) -> str
```

# MCP Clickhouse

MCP Clickhouse는 ClickHouse 데이터베이스에 접근하여 데이터를 조회하고 쿼리를 실행할 수 있는 도구입니다.

```json
{
  "mcpServers": {
    "clickhouse": {
      "command": "python3",
      "args": ["mcp_clickhouse.py"]
    }
  }
}
```

## 제공 도구

### show_databases

데이터베이스 목록을 조회합니다.

```python
show_databases() -> str
```

### show_tables

특정 데이터베이스의 테이블 목록을 조회합니다.

```python
show_tables(database: str) -> str
```

### execute_query

데이터베이스에서 SQL 쿼리를 실행합니다.

```python
execute_query(database: str, query: str) -> str
```

# 기타 Public MCP의 Local Build

```json
{
  "mcpServers": {
    "sequential-thinking": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-sequential-thinking"]
    },
    "memory": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-memory"]
    }
  }
}
```
