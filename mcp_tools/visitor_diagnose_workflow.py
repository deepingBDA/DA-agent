"""
[MODERN HTML VERSION]
매장 방문객수 진단 보고서 작성을 도와주는 워크플로우 - HTML 출력 버전

✨ 이 파일은 최신 HTML 버전입니다.

기능:
- 현대적인 HTML 보고서 생성
- 반응형 디자인 (모바일 친화적)
- 브라우저에서 즉시 확인 가능
- AI 기반 하이라이트 (글씨 색상 강조)
- 컴팩트한 레이아웃으로 여러 매장 한 줄 표시
- 가벼운 파일 크기

엑셀 버전 대비 장점:
- 즉시 확인 가능 (웹 브라우저)
- 모바일에서 완벽 지원
- 의존성 최소화
- 공유 및 배포 용이
- 시각적으로 더 매력적

레거시 엑셀 버전: visitor_diagnose_workflow_legacy_excel.py
"""

import json
import os
import sys
from typing import Dict, Any, List, Union
from fastmcp import FastMCP  # FastMCP 툴 서버용

from dotenv import load_dotenv
from langchain.schema import BaseOutputParser
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END, START

from base_workflow import BaseWorkflow, BaseState


class VisitorDiagnoseState(BaseState):
    """방문객 진단 워크플로우 전용 상태 - BaseState 확장"""
    store_name: str  # 단일 매장명
    period: str
    visitor_data: Dict[str, Any]  # 단일 매장 데이터
    raw_answer: str  # diagnose_avg_in의 원시 응답
    metric_dict: Dict[str, Any]  # 파싱된 메트릭 데이터
    placements: List[Dict[str, Any]]  # 엑셀 셀 배치 정보
    final_result: str  # 최종 결과
    design_spec: List[Dict[str, Any]]  # 디자인 스타일 placements
    dataframe: Any | None = None       # pandas DataFrame 저장
    highlights: List[Dict[str, Any]] | None = None  # 하이라이트 셀 정보
    html_content: str | None = None # HTML 콘텐츠 저장


class VisitorDiagnoseWorkflow(BaseWorkflow[VisitorDiagnoseState]):
    """방문객 진단 워크플로우 클래스 - BaseWorkflow 상속"""

    def __init__(self):
        super().__init__(workflow_name="visitor_diagnose")
        
        # 환경변수 로드 및 LLM 설정
        load_dotenv()
        self.llm = ChatOpenAI(model="gpt-4o", temperature=0)

        # HTML 하이라이트용 프롬프트 템플릿
        self._highlight_prompt = (
            """<ROLE>
당신은 오프라인 매장 데이터 분석 전문가입니다. 
매장 방문객 데이터를 분석하여 경영진이 주목해야 할 개선점과 매장의 특징적인 데이터를 식별해야 합니다.
</ROLE>

<ANALYSIS_CRITERIA>
## 빨간색 하이라이트 (주목할 개선점 및 매장 특징)

### 개선 기회가 있는 지표:
- **방문객 수 불균형**: 평일과 주말 방문객 차이가 30% 이상인 경우
- **성별 편중**: 한쪽 성별이 55% 이상 집중된 경우 (타겟 확장 기회)
- **연령대 집중**: 특정 연령대에 35% 이상 집중된 경우 (다양화 필요)
- **시간대 집중**: 특정 시간대에 35% 이상 집중된 경우 (분산 필요)
- **비효율적 시간대**: 아침/심야 시간대가 주력인 경우 (운영 최적화)

### 매장의 특징적 패턴:
- **독특한 고객층**: 다른 매장과 20% 이상 다른 연령대/성별 분포
- **특이한 방문 패턴**: 일반적이지 않은 시간대가 주력인 경우
- **매장간 큰 차이**: 여러 매장 중 방문객 수가 상위/하위 20%에 속하는 경우
- **계절성/요일성**: 평일/주말 패턴이 일반적 예상과 다른 경우

### 운영 개선 포인트:
- **미활용 시간대**: 방문객이 적은 시간대의 활용 방안
- **고객층 확장**: 부족한 성별/연령대 유입 방안
- **효율성 개선**: 과도하게 집중된 시간대의 분산 방안
</ANALYSIS_CRITERIA>

<SELECTION_PRIORITY>
1. **개선 임팩트**: 개선 시 매출 증대 효과가 클 것으로 예상되는 지표 우선
2. **실행 가능성**: 마케팅이나 운영 변경으로 개선 가능한 지표 우선
3. **매장 특성**: 해당 매장만의 독특한 특징이 드러나는 지표
4. **비교 우위**: 여러 매장 중 상대적으로 특이한 패턴을 보이는 지표
5. **최대 4개 항목**: 핵심 개선점에 집중
</SELECTION_PRIORITY>

<OUTPUT_FORMAT>
반드시 아래 JSON 형식으로만 응답하세요:
{{
  "highlight": [
    {{"metric": "방문객수", "store": "매장명", "color": "red", "reason": "평일 방문객 부족"}},
    {{"metric": "성별경향", "store": "매장명", "color": "red", "reason": "남성 고객 편중"}},
    {{"metric": "연령대순위", "store": "매장명", "color": "red", "reason": "20대 집중 심함"}},
    {{"metric": "시간대경향", "store": "매장명", "color": "red", "reason": "심야 시간대 주력"}}
  ]
}}

주의사항:
- metric은 "방문객수", "성별경향", "연령대순위", "시간대경향" 중 하나
- store는 실제 매장명
- color는 "red" 사용 (개선점/특징 강조)
- reason은 구체적인 개선 포인트나 특징을 20자 이내로 설명
- JSON 외 다른 텍스트는 출력 금지
</OUTPUT_FORMAT>

<DATA_TABLE>
{table}
</DATA_TABLE>"""
        )
        
        # 워크플로우 그래프
        self.workflow_app = self._build_workflow()

    def run(
        self, 
        user_prompt: str, 
        store_name: str, 
        start_date: str,
        end_date: str
    ) -> str:
        """Agent가 호출하는 방문객 진단 워크플로우 실행"""
        self.logger.info(f"워크플로우 실행: {store_name} ({start_date}~{end_date})")
        
        # 초기 상태 설정
        initial_state = VisitorDiagnoseState(
            store_name=store_name,
            period=f"{start_date}~{end_date}",
            visitor_data={},
            raw_answer="",
            metric_dict={},
            placements=[],
            final_result="",
            design_spec=[]
        )
        
        # 워크플로우 실행
        result = self.workflow_app.invoke(initial_state)
        
        return result.get("final_result", "워크플로우 실행 완료")

    def _build_workflow(self) -> StateGraph:
        """LangGraph 워크플로우를 구성합니다."""
        builder = StateGraph(VisitorDiagnoseState)
        
        # 노드 추가 - HTML 출력으로 변경
        builder.add_node("fetch", self._query_db_node)
        builder.add_node("parse", self._parse_node)
        builder.add_node("generate_html", self._generate_html_node)
        builder.add_node("highlight", self._highlight_node)
        builder.add_node("save_html", self._save_html_node)
        
        # 엣지 추가 (순차 실행) - 하이라이트 노드 비활성화
        builder.add_edge(START, "fetch")
        builder.add_edge("fetch", "parse")
        builder.add_edge("parse", "generate_html")
        # builder.add_edge("generate_html", "highlight")  # 하이라이트 노드 비활성화
        # builder.add_edge("highlight", "save_html")
        builder.add_edge("generate_html", "save_html")  # 직접 save_html로 연결
        builder.add_edge("save_html", END)
        
        return builder.compile()

    # ----------------- 노드 구현 -----------------
    def _query_db_node(self, state: VisitorDiagnoseState) -> VisitorDiagnoseState:
        """diagnose_avg_in 로직을 직접 구현하여 DB에서 데이터 조회"""
        start, end = state["period"].split("~")
        
        # ClickHouse 클라이언트 생성
        client = self._create_clickhouse_client()
        if not client:
            state["raw_answer"] = "데이터베이스 연결 실패"
            return state
            
        # mcp_diagnose.py의 diagnose_avg_in 쿼리를 그대로 사용
        query = f"""
        WITH
df AS (
    SELECT
        li.date AS visit_date,
        li.timestamp,
        li.person_seq AS visitor_id,
        if(toDayOfWeek(li.date) IN (1,2,3,4,5), 'weekday', 'weekend')                     AS day_type,
        multiIf(
            toHour(li.timestamp) IN (22,23,0,1), '22-01',
            toHour(li.timestamp) BETWEEN 2  AND 5 , '02-05',
            toHour(li.timestamp) BETWEEN 6  AND 9 , '06-09',
            toHour(li.timestamp) BETWEEN 10 AND 13, '10-13',
            toHour(li.timestamp) BETWEEN 14 AND 17, '14-17',
            '18-21'
        ) AS time_range,
        multiIf(
            dt.age BETWEEN 0  AND  9 , '10대 미만',
            dt.age BETWEEN 10 AND 19, '10대',
            dt.age BETWEEN 20 AND 29, '20대',
            dt.age BETWEEN 30 AND 39, '30대',
            dt.age BETWEEN 40 AND 49, '40대',
            dt.age BETWEEN 50 AND 59, '50대',
            dt.age >= 60           , '60대 이상',
            'Unknown'
        ) AS age_group,
        if(dt.gender = '0', '남성', if(dt.gender='1','여성','Unknown'))                   AS gender
    FROM line_in_out_individual li
    LEFT JOIN detected_time dt ON li.person_seq = dt.person_seq
    LEFT JOIN line          l  ON li.triggered_line_id = l.id
    WHERE li.date BETWEEN '{start}' AND '{end}'
      AND li.is_staff = 0
      AND li.in_out   = 'IN'
      AND l.entrance  = 1
),
daily_all     AS (SELECT visit_date, uniqExact(visitor_id) AS ucnt FROM df GROUP BY visit_date),
avg_all       AS (SELECT toUInt64(round(avg(ucnt))) AS avg_cnt FROM daily_all),
daily_dayType AS (SELECT visit_date, day_type, uniqExact(visitor_id) AS ucnt FROM df GROUP BY visit_date, day_type),
avg_dayType   AS (SELECT day_type, toUInt64(round(avg(ucnt))) AS avg_cnt FROM daily_dayType GROUP BY day_type),
daily_gender  AS (SELECT visit_date, gender, uniqExact(visitor_id) AS ucnt FROM df GROUP BY visit_date, gender),
avg_gender    AS (SELECT gender, toUInt64(round(avg(ucnt))) AS avg_cnt FROM daily_gender GROUP BY gender),
daily_age     AS (SELECT visit_date, age_group, uniqExact(visitor_id) AS ucnt FROM df GROUP BY visit_date, age_group),
avg_age       AS (SELECT age_group, toUInt64(round(avg(ucnt))) AS avg_cnt FROM daily_age GROUP BY age_group),
rank_age      AS (SELECT *, row_number() OVER (ORDER BY avg_cnt DESC) AS rk FROM avg_age),
overall_cnt   AS (SELECT avg_cnt AS cnt FROM avg_all),
range_cnt AS (
    SELECT day_type, time_range, uniqExact(visitor_id) AS visit_cnt
    FROM df GROUP BY day_type, time_range
),
tot_cnt   AS (SELECT day_type, sum(visit_cnt) AS total_cnt FROM range_cnt GROUP BY day_type),
range_pct AS (
    SELECT r.day_type, r.time_range, r.visit_cnt,
           toUInt64(round(r.visit_cnt / t.total_cnt * 100)) AS pct
    FROM range_cnt r JOIN tot_cnt t USING(day_type)
),
rank_slot AS (
    SELECT *, row_number() OVER (PARTITION BY day_type ORDER BY pct DESC) AS rk
    FROM range_pct
),
final AS (
    SELECT '일평균' AS section, '전체' AS label,
           avg_cnt AS value_cnt, CAST(NULL AS Nullable(UInt64)) AS value_pct, 0 AS ord
    FROM avg_all
    UNION ALL
    SELECT '일평균', '평일', avg_cnt, CAST(NULL AS Nullable(UInt64)), 1
    FROM avg_dayType WHERE day_type='weekday'
    UNION ALL
    SELECT '일평균', '주말', avg_cnt, CAST(NULL AS Nullable(UInt64)), 2
    FROM avg_dayType WHERE day_type='weekend'
    UNION ALL
    SELECT '성별경향', gender, avg_cnt,
           toUInt64(round(avg_cnt / (SELECT cnt FROM overall_cnt) * 100)) AS value_pct,
           10 + if(gender='남성',0,1) AS ord
    FROM avg_gender WHERE gender IN ('남성','여성')
    UNION ALL
    SELECT '연령대경향',
           concat(toString(rk),'위_',age_group)                         AS label,
           avg_cnt,
           toUInt64(round(avg_cnt / (SELECT cnt FROM overall_cnt) * 100)) AS value_pct,
           20 + rk AS ord
    FROM rank_age WHERE rk<=3
    UNION ALL
    SELECT '시간대경향',
           concat('평일_',toString(rk),'_',time_range)                 AS label,
           visit_cnt, pct, 30 + rk
    FROM rank_slot WHERE day_type='weekday' AND rk<=3
    UNION ALL
    SELECT '시간대경향',
           concat('주말_',toString(rk),'_',time_range),
           visit_cnt, pct, 40 + rk
    FROM rank_slot WHERE day_type='weekend' AND rk<=3
)
SELECT section, label, value_cnt, value_pct
FROM final
ORDER BY ord
"""

        answer = ""
        # 요청된 매장들 처리 (더미데이터 포함)
        if isinstance(state["store_name"], str):
            # 쉼표로 구분된 문자열을 분리
            store_names = [name.strip() for name in state["store_name"].split(',')]
        else:
            store_names = state["store_name"]
        
        for store in store_names:
            # 더미데이터점들인 경우 가짜 데이터 생성
            if store.startswith("더미데이터점"):
                answer += self._generate_dummy_data_for_store(store)
                continue
                
            # 실제 매장인 경우 DB 조회
            database = 'plusinsight'  # 기본 데이터베이스
            try:
                # 특정 데이터베이스로 클라이언트 재생성
                store_client = self._create_clickhouse_client(database=database)
                result = store_client.query(query)

                if len(result.result_rows) > 0:
                    # 섹션별로 데이터 분류
                    sections = {
                        '일평균': [],
                        '성별경향': [],
                        '연령대경향': [],
                        '시간대경향': []
                    }
                    
                    for row in result.result_rows:
                        section, label, value_cnt, value_pct = row
                        sections[section].append((label, value_cnt, value_pct))
                    
                    # 표 형태로 포맷팅 (mcp_diagnose.py와 동일한 형식)
                    store_answer = f"\n=== {store} ===\n"
                    
                    # 1. 일평균 방문객수
                    store_answer += "일평균 방문객수\n"
                    for label, cnt, _ in sections['일평균']:
                        store_answer += f"  {label}: {cnt}명\n"
                    
                    # 2. 성별경향
                    store_answer += "\n성별경향\n"
                    for label, cnt, pct in sections['성별경향']:
                        gender_display = 'M' if label == '남성' else 'F'
                        store_answer += f"  {gender_display}: {pct}%\n"
                    
                    # 3. 연령대별 순위 (상위 3개)
                    store_answer += "\n연령대별 순위\n"
                    for label, cnt, pct in sections['연령대경향']:
                        rank = label.split('위_')[0]
                        age_group = label.split('위_')[1]
                        store_answer += f"  {rank}: {age_group} - {pct}%\n"
                    
                    # 4. 주요 방문시간대
                    store_answer += "\n주요 방문시간대\n"
                    
                    # 시간대 명칭 매핑
                    time_names = {
                        '22-01': '심야',
                        '02-05': '새벽',
                        '06-09': '아침',
                        '10-13': '낮',
                        '14-17': '오후',
                        '18-21': '저녁'
                    }
                    
                    # 평일 시간대 분리
                    weekday_slots = [item for item in sections['시간대경향'] if '평일_' in item[0]]
                    weekend_slots = [item for item in sections['시간대경향'] if '주말_' in item[0]]
                    
                    store_answer += "  평일:\n"
                    for label, cnt, pct in weekday_slots:
                        rank = label.split('_')[1]
                        time_range = label.split('_')[2]
                        time_name = time_names.get(time_range, time_range)
                        store_answer += f"    {rank}: {time_name}({time_range}) - {pct}%\n"
                    
                    store_answer += "  주말:\n"
                    for label, cnt, pct in weekend_slots:
                        rank = label.split('_')[1]
                        time_range = label.split('_')[2]
                        time_name = time_names.get(time_range, time_range)
                        store_answer += f"    {rank}: {time_name}({time_range}) - {pct}%\n"
                    
                    answer += store_answer
                else:
                    answer += f"\n{store} 데이터가 없습니다."
                
            except Exception as e:
                self.logger.error(f"{store} 데이터 조회 오류: {e}")
                answer += f"\n{store} 데이터 조회 오류: {e}"

        state["raw_answer"] = answer
        return state

    def _create_clickhouse_client(self, database="plusinsight"):
        """ClickHouse 클라이언트 생성 (mcp_diagnose.py의 로직을 그대로 사용)"""
        import clickhouse_connect
        from dotenv import load_dotenv
        
        load_dotenv()
        
        # 환경변수 로드
        CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
        CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
        CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
        CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
        SSH_HOST = os.getenv("SSH_HOST")
        SSH_PORT = int(os.getenv("SSH_PORT", "22"))
        SSH_USERNAME = os.getenv("SSH_USERNAME")
        SSH_PASSWORD = os.getenv("SSH_PASSWORD")
        
        # SSH 터널링이 필요한 경우 (sshtunnel 사용)
        try:
            from sshtunnel import SSHTunnelForwarder
            SSH_AVAILABLE = True
        except ImportError:
            SSH_AVAILABLE = False
            
        if SSH_AVAILABLE and SSH_HOST:
            try:
                ssh_tunnel = SSHTunnelForwarder(
                    (SSH_HOST, SSH_PORT),
                    ssh_username=SSH_USERNAME,
                    ssh_password=SSH_PASSWORD,
                    remote_bind_address=(CLICKHOUSE_HOST, int(CLICKHOUSE_PORT)),
                    local_bind_address=("localhost", 0),
                )
                ssh_tunnel.start()
                self.logger.info(f"SSH 터널 생성: localhost:{ssh_tunnel.local_bind_port}")
                
                host = "localhost"
                port = ssh_tunnel.local_bind_port
                
            except Exception as e:
                self.logger.error(f"SSH 터널 생성 실패: {e}, 직접 연결 시도")
                host = CLICKHOUSE_HOST
                port = int(CLICKHOUSE_PORT)
        else:
            # 직접 연결
            host = CLICKHOUSE_HOST
            port = int(CLICKHOUSE_PORT)
        
        try:
            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=database,
            )
            self.logger.info(f"ClickHouse 연결 성공: {host}:{port}, db={database}")
            return client
        except Exception as e:
            self.logger.error(f"ClickHouse 연결 실패: {e}")
            return None

    def _parse_node(self, state: VisitorDiagnoseState) -> VisitorDiagnoseState:
        """raw_text → metric_dict - 모든 데이터 파싱"""
        import re

        self.logger.info(f"파싱할 raw_answer 길이: {len(state['raw_answer'])}")
        
        metric_dict = {}
        current_store = None
        current_section = None
        
        for line in state["raw_answer"].splitlines():
            line = line.strip()
            if not line:
                continue
                
            # 매장명 파싱
            m = re.match(r"===\s(.+)\s===", line)
            if m:
                current_store = m.group(1)
                metric_dict[current_store] = {
                    'daily_avg': {},      # 일평균 방문객수
                    'gender': {},         # 성별경향  
                    'age_rank': {},       # 연령대별 순위
                    'time_slots': {}      # 주요 방문시간대
                }
                self.logger.info(f"매장 발견: {current_store}")
                continue
            
            # 섹션 헤더 파싱
            if line in ['일평균 방문객수', '성별경향', '연령대별 순위', '주요 방문시간대']:
                current_section = line
                continue
                
            if not current_store or not current_section:
                continue
                
            # 각 섹션별 데이터 파싱
            if current_section == '일평균 방문객수':
                m = re.match(r"(.+):\s+(\d+)명", line)
                if m:
                    label, count = m.groups()
                    metric_dict[current_store]['daily_avg'][label] = int(count)
                    
            elif current_section == '성별경향':
                m = re.match(r"([MF]):\s+(\d+)%", line)
                if m:
                    gender, pct = m.groups()
                    gender_name = '남성' if gender == 'M' else '여성'
                    metric_dict[current_store]['gender'][gender_name] = int(pct)
                    
            elif current_section == '연령대별 순위':
                m = re.match(r"(\d+):\s+(.+)\s+-\s+(\d+)%", line)
                if m:
                    rank, age_group, pct = m.groups()
                    metric_dict[current_store]['age_rank'][f"{rank}위_{age_group}"] = int(pct)
                    
            elif current_section == '주요 방문시간대':
                # 평일/주말 구분
                if line in ['평일:', '주말:']:
                    current_day_type = line.rstrip(':')
                    if current_day_type not in metric_dict[current_store]['time_slots']:
                        metric_dict[current_store]['time_slots'][current_day_type] = {}
                    continue
                    
                # 시간대 데이터 파싱
                m = re.match(r"(\d+):\s+(.+)\((.+)\)\s+-\s+(\d+)%", line)
                if m and 'current_day_type' in locals():
                    rank, time_name, time_range, pct = m.groups()
                    key = f"{rank}위_{time_name}_{time_range}"
                    metric_dict[current_store]['time_slots'][current_day_type][key] = int(pct)

        state["metric_dict"] = metric_dict
        self.logger.info(f"파싱 완료 - 매장 수: {len(metric_dict)}")
        for store, data in metric_dict.items():
            self.logger.info(f"{store} 파싱 결과:")
            self.logger.info(f"  일평균: {data['daily_avg']}")
            self.logger.info(f"  성별: {data['gender']}")
            self.logger.info(f"  연령대: {data['age_rank']}")
            self.logger.info(f"  시간대: {data['time_slots']}")
        return state

    def _generate_html_node(self, state: VisitorDiagnoseState) -> VisitorDiagnoseState:
        """
        방문객 진단 데이터를 HTML 테이블로 변환
        """
        import os
        from datetime import datetime
        
        metric_dict = state["metric_dict"]
        
        # report 디렉토리 생성
        if not os.path.exists("report"):
            os.makedirs("report", exist_ok=True)
        
        # HTML 템플릿 시작
        html_content = self._create_html_template(metric_dict, state["period"])
        
        state["html_content"] = html_content
        self.logger.info(f"HTML 콘텐츠 생성 완료: {len(html_content)} 문자")
        
        return state

    def _create_html_template(self, metric_dict: dict, period: str) -> str:
        """
        HTML 보고서 템플릿 생성
        """
        from datetime import datetime
        
        stores = list(metric_dict.keys())
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        html = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>매장 방문객 진단 보고서</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 2.5rem;
            margin-bottom: 10px;
            font-weight: 700;
        }}
        
        .header .period {{
            font-size: 1.2rem;
            opacity: 0.9;
            margin-bottom: 5px;
        }}
        
        .header .generated {{
            font-size: 0.9rem;
            opacity: 0.7;
        }}
        
        .content {{
            padding: 40px;
        }}
        
        .store-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}
        
        .store-card {{
            background: #fff;
            border-radius: 12px;
            box-shadow: 0 6px 20px rgba(0,0,0,0.08);
            border: 1px solid #e1e8ed;
            overflow: hidden;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }}
        
        .store-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 15px 35px rgba(0,0,0,0.15);
        }}
        
        .store-header {{
            background: linear-gradient(135deg, #3498db 0%, #2980b9 100%);
            color: white;
            padding: 15px;
            text-align: center;
        }}
        
        .store-name {{
            font-size: 1.3rem;
            font-weight: 600;
            margin-bottom: 3px;
        }}
        
        .store-body {{
            padding: 18px;
        }}
        
        .metric-section {{
            margin-bottom: 18px;
        }}
        
        .metric-title {{
            font-size: 1.0rem;
            font-weight: 600;
            color: #2c3e50;
            margin-bottom: 12px;
            padding-bottom: 6px;
            border-bottom: 2px solid #3498db;
        }}
        
        .metric-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));
            gap: 8px;
        }}
        
        .metric-item {{
            background: #f8f9fa;
            padding: 12px;
            border-radius: 8px;
            text-align: center;
            border: 1px solid #e9ecef;
        }}
        
        .metric-label {{
            font-size: 0.85rem;
            color: #6c757d;
            margin-bottom: 5px;
        }}
        
        .metric-value {{
            font-size: 1.2rem;
            font-weight: 700;
            color: #2c3e50;
        }}
        
        .metric-value.highlight-red {{
            color: #e74c3c;
            font-weight: 800;
        }}
        
        .metric-value.highlight-blue {{
            color: #3498db;
            font-weight: 800;
        }}
        
        .time-slots {{
            display: flex;
            justify-content: space-between;
            gap: 12px;
        }}
        
        .time-slot-group {{
            flex: 1;
            background: #f8f9fa;
            padding: 12px;
            border-radius: 8px;
            border: 1px solid #e9ecef;
        }}
        
        .time-slot-title {{
            font-weight: 600;
            color: #495057;
            margin-bottom: 10px;
            text-align: center;
        }}
        
        .time-slot-item {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 8px 0;
            border-bottom: 1px solid #dee2e6;
        }}
        
        .time-slot-item:last-child {{
            border-bottom: none;
        }}
        
        .time-slot-rank {{
            background: #6c757d;
            color: white;
            width: 20px;
            height: 20px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.8rem;
            font-weight: 600;
        }}
        
        .time-slot-time {{
            font-size: 0.9rem;
            color: #495057;
        }}
        
        .time-slot-percent {{
            font-weight: 600;
            color: #2c3e50;
        }}
        
        .time-slot-percent.highlight-red {{
            color: #e74c3c;
            font-weight: 800;
        }}
        
        .time-slot-percent.highlight-blue {{
            color: #3498db;
            font-weight: 800;
        }}
        
        .comparison-table {{
            width: 100%;
            margin-top: 40px;
            border-collapse: collapse;
            background: white;
            border-radius: 15px;
            overflow: hidden;
            box-shadow: 0 8px 25px rgba(0,0,0,0.08);
        }}
        
        .comparison-table th {{
            background: linear-gradient(135deg, #34495e 0%, #2c3e50 100%);
            color: white;
            padding: 20px;
            text-align: center;
            font-weight: 600;
        }}
        
        .comparison-table td {{
            padding: 15px;
            text-align: center;
            border-bottom: 1px solid #e1e8ed;
        }}
        
        .comparison-table tbody tr:hover {{
            background: #f8f9fa;
        }}
        
        .footer {{
            background: #2c3e50;
            color: white;
            text-align: center;
            padding: 20px;
            font-size: 0.9rem;
        }}
        
        @media (max-width: 768px) {{
            .container {{
                margin: 10px;
                border-radius: 15px;
            }}
            
            .content {{
                padding: 20px;
            }}
            
            .store-grid {{
                grid-template-columns: 1fr;
                gap: 20px;
            }}
            
            .time-slots {{
                flex-direction: column;
                gap: 15px;
            }}
            
            .header h1 {{
                font-size: 2rem;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏪 매장 방문객 진단 보고서</h1>
            <div class="period">📅 분석 기간: {period}</div>
            <div class="generated">⏰ 생성 시간: {current_time}</div>
        </div>
        
        <div class="content">
            <div class="store-grid">
"""
        
        # 각 매장별 카드 생성
        for store, data in metric_dict.items():
            html += self._create_store_card_html(store, data)
        
        html += """
            </div>
            
            <!-- 매장 간 비교 테이블 -->
            <h2 style="text-align: center; margin-bottom: 30px; color: #2c3e50; font-size: 2rem;">📊 매장 간 비교</h2>
"""
        
        # 비교 테이블 생성
        html += self._create_comparison_table_html(metric_dict)
        
        html += f"""
        </div>
        
        <div class="footer">
            <p>💡 이 보고서는 방문객 진단 워크플로우에 의해 자동 생성되었습니다.</p>
            <p>🔄 마지막 업데이트: {current_time}</p>
        </div>
    </div>
</body>
</html>
"""
        
        return html

    def _create_store_card_html(self, store_name: str, data: dict) -> str:
        """개별 매장 카드 HTML 생성"""
        daily_avg = data.get('daily_avg', {})
        gender = data.get('gender', {})
        age_rank = data.get('age_rank', {})
        time_slots = data.get('time_slots', {})
        
        html = f"""
                <div class="store-card">
                    <div class="store-header">
                        <div class="store-name">{store_name}</div>
                    </div>
                    <div class="store-body">
                        <!-- 일평균 방문객수 -->
                        <div class="metric-section">
                            <div class="metric-title">👥 일평균 방문객수</div>
                            <div class="metric-grid">
                                <div class="metric-item">
                                    <div class="metric-label">전체</div>
                                    <div class="metric-value">{daily_avg.get('전체', 0):,}명</div>
                                </div>
                                <div class="metric-item">
                                    <div class="metric-label">평일</div>
                                    <div class="metric-value">{daily_avg.get('평일', 0):,}명</div>
                                </div>
                                <div class="metric-item">
                                    <div class="metric-label">주말</div>
                                    <div class="metric-value">{daily_avg.get('주말', 0):,}명</div>
                                </div>
                            </div>
                        </div>
                        
                        <!-- 성별 경향 -->
                        <div class="metric-section">
                            <div class="metric-title">👫 성별 경향</div>
                            <div class="metric-grid">
                                <div class="metric-item">
                                    <div class="metric-label">남성</div>
                                    <div class="metric-value">{gender.get('남성', 0)}%</div>
                                </div>
                                <div class="metric-item">
                                    <div class="metric-label">여성</div>
                                    <div class="metric-value">{gender.get('여성', 0)}%</div>
                                </div>
                            </div>
                        </div>
                        
                        <!-- 연령대 순위 -->
                        <div class="metric-section">
                            <div class="metric-title">🎯 연령대별 순위</div>
                            <div class="metric-grid">
"""
        
        # 연령대 순위 데이터 추가
        for i in range(1, 4):
            rank_key = None
            for key in age_rank.keys():
                if key.startswith(f"{i}위_"):
                    rank_key = key
                    break

            if rank_key:
                age_group = rank_key.split('위_')[1]
                pct = age_rank.get(rank_key, 0)
                html += f"""
                                <div class="metric-item">
                                    <div class="metric-label">{i}위</div>
                                    <div class="metric-value">{age_group}<br><small>{pct}%</small></div>
                                </div>
"""
        
        html += """
                            </div>
                        </div>
                        
                        <!-- 시간대 경향 -->
                        <div class="metric-section">
                            <div class="metric-title">⏰ 주요 방문시간대</div>
                            <div class="time-slots">
"""
        
        # 평일/주말 시간대 데이터 추가
        for day_type in ['평일', '주말']:
            icon = '💼' if day_type == '평일' else '🎉'
            slots = time_slots.get(day_type, {})
            
            html += f"""
                                <div class="time-slot-group">
                                    <div class="time-slot-title">{icon} {day_type}</div>
"""
            
            for i in range(1, 4):
                rank_key = None
                for key in slots.keys():
                    if key.startswith(f"{i}위_"):
                        rank_key = key
                        break
                
                if rank_key:
                    parts = rank_key.split('_')
                    if len(parts) >= 3:
                        time_name = parts[1]
                        time_range = parts[2]
                        pct = slots.get(rank_key, 0)
                        
                        html += f"""
                                    <div class="time-slot-item">
                                        <div class="time-slot-rank">{i}</div>
                                        <div class="time-slot-time">{time_name}({time_range})</div>
                                        <div class="time-slot-percent">{pct}%</div>
                                    </div>
"""
            
            html += """
                                </div>
"""
        
        html += """
                            </div>
                        </div>
                    </div>
                </div>
"""
        
        return html

    def _create_comparison_table_html(self, metric_dict: dict) -> str:
        """매장 간 비교 테이블 HTML 생성"""
        stores = list(metric_dict.keys())
        
        html = """
            <table class="comparison-table">
                <thead>
                    <tr>
                        <th>구분</th>
                        <th>항목</th>
"""
        
        # 매장명 헤더
        for store in stores:
            html += f"<th>{store}</th>"
        
        html += """
                    </tr>
                </thead>
                <tbody>
"""
        
        # 일평균 방문객수 행들
        daily_items = [('전체', '전체'), ('평일', '평일'), ('주말', '주말')]
        for i, (label, key) in enumerate(daily_items):
            rowspan = 'rowspan="3"' if i == 0 else ''
            html += f"""
                    <tr>
                        {'<td ' + rowspan + '>일평균 방문객수</td>' if i == 0 else ''}
                        <td>{label}</td>
"""
            for store in stores:
                value = metric_dict[store].get('daily_avg', {}).get(key, 0)
                html += f"<td>{value:,}명</td>"
            html += "</tr>"
        
        # 성별 경향 행들
        gender_items = [('남성', '남성'), ('여성', '여성')]
        for i, (label, key) in enumerate(gender_items):
            rowspan = 'rowspan="2"' if i == 0 else ''
            html += f"""
                    <tr>
                        {'<td ' + rowspan + '>성별 경향</td>' if i == 0 else ''}
                        <td>{label}</td>
"""
            for store in stores:
                value = metric_dict[store].get('gender', {}).get(key, 0)
                html += f"<td>{value}%</td>"
            html += "</tr>"
        
        # 연령대 순위 행들
        for rank in range(1, 4):
            rowspan = 'rowspan="3"' if rank == 1 else ''
            html += f"""
                    <tr>
                        {'<td ' + rowspan + '>연령대 순위</td>' if rank == 1 else ''}
                        <td>{rank}위</td>
"""
            for store in stores:
                age_rank = metric_dict[store].get('age_rank', {})
                rank_key = None
                for key in age_rank.keys():
                    if key.startswith(f"{rank}위_"):
                        rank_key = key
                        break
                
                if rank_key:
                    age_group = rank_key.split('위_')[1]
                    pct = age_rank.get(rank_key, 0)
                    html += f"<td>{age_group} ({pct}%)</td>"
                else:
                    html += "<td>-</td>"
            html += "</tr>"
        
        html += """
                </tbody>
            </table>
"""
        
        return html

    def _save_html_node(self, state: VisitorDiagnoseState) -> VisitorDiagnoseState:
        """
        생성된 HTML을 파일로 저장
        """
        import os
        from datetime import datetime
        
        html_content = state.get("html_content", "")
        if not html_content:
            state["final_result"] = "HTML 콘텐츠가 없음"
            return state
        
        # 하이라이트 적용
        highlights = state.get("highlights", [])
        if highlights:
            html_content = self._apply_html_highlights(html_content, highlights)
        
        # HTML 파일 저장
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"방문객진단_{timestamp}.html"
        
        # 현재 디렉토리와 chat 디렉토리 모두에 저장 (백엔드 호환성)
        # /app/chat/mcp_tools/report  경로에 저장해야 FastAPI StaticFiles(/reports) 가 서빙
        html_path = f"report/{filename}"  # 로컬(작업 디렉토리) 보관
        chat_html_path = f"../chat/report/{filename}"
        
        try:
            # 로컬 report 디렉토리에 저장
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            # Chat 디렉토리(report)에도 저장 (백엔드 서빙용)
            chat_dir = os.path.dirname(chat_html_path)
            if not os.path.exists(chat_dir):
                os.makedirs(chat_dir, exist_ok=True)
            with open(chat_html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            self.logger.info(f"HTML 보고서 저장 완료: {html_path}")
            self.logger.info(f"백엔드 서빙용 저장 완료: {chat_html_path}")
            
            # 웹 접근 가능한 상대 경로 URL (컨테이너/리버스프록시 환경에서도 안정)
            web_url = f"/reports/{filename}"

            # 마크다운 링크로 반환 (코드블록 없이 한 줄 텍스트)
            state["final_result"] = (
                "📊 HTML 보고서 생성 완료!\n\n"
                f"🔗 [웹에서 보기]({web_url})\n\n"
                "보고서를 클릭하여 새 탭에서 확인하세요!"
            )

            # DEBUG: 로그로 raw 문자열 확인
            self.logger.info(f"FINAL_RESULT_RAW: {repr(state['final_result'])}")
            
        except Exception as e:
            self.logger.error(f"HTML 파일 저장 실패: {e}")
            state["final_result"] = f"HTML 파일 저장 실패: {e}"
        
        return state

    def _apply_html_highlights(self, html_content: str, highlights: list) -> str:
        """
        HTML에 하이라이트 효과 적용
        """
        import re
        
        for highlight in highlights:
            metric = highlight.get("metric", "")
            store = highlight.get("store", "")
            color = highlight.get("color", "red")
            reason = highlight.get("reason", "")
            
            self.logger.info(f"하이라이트 적용: {store} - {metric} ({color}) - {reason}")
            
            # 매장명과 메트릭에 따라 해당 섹션을 찾아서 하이라이트 적용
            if metric == "방문객수":
                # 방문객수 섹션의 metric-value들을 하이라이트
                pattern = f'(<div class="store-name">{re.escape(store)}</div>.*?<div class="metric-title">👥 일평균 방문객수</div>.*?<div class="metric-value">)([^<]+)(</div>)'
                html_content = re.sub(
                    pattern,
                    f'\\1<span class="highlight-{color}">\\2</span>\\3',
                    html_content,
                    flags=re.DOTALL,
                    count=1
                )
            
            elif metric == "성별경향":
                # 성별경향 섹션을 하이라이트
                pattern = f'(<div class="store-name">{re.escape(store)}</div>.*?<div class="metric-title">👫 성별 경향</div>.*?<div class="metric-grid">.*?</div>)'
                def add_highlight(match):
                    content = match.group(1)
                    # 모든 metric-value에 하이라이트 클래스 추가
                    content = content.replace('class="metric-value"', f'class="metric-value highlight-{color}"')
                    return content
                
                html_content = re.sub(pattern, add_highlight, html_content, flags=re.DOTALL, count=1)
                
            elif metric == "연령대순위":
                # 연령대 순위 섹션을 하이라이트
                pattern = f'(<div class="store-name">{re.escape(store)}</div>.*?<div class="metric-title">🎯 연령대별 순위</div>.*?<div class="metric-grid">.*?</div>)'
                def add_highlight(match):
                    content = match.group(1)
                    content = content.replace('class="metric-value"', f'class="metric-value highlight-{color}"')
                    return content
                
                html_content = re.sub(pattern, add_highlight, html_content, flags=re.DOTALL, count=1)
                
            elif metric == "시간대경향":
                # 시간대 경향 섹션을 하이라이트
                pattern = f'(<div class="store-name">{re.escape(store)}</div>.*?<div class="metric-title">⏰ 주요 방문시간대</div>.*?<div class="time-slots">.*?</div>.*?</div>)'
                def add_highlight(match):
                    content = match.group(1)
                    # time-slot-percent에 하이라이트 클래스 추가
                    content = content.replace('class="time-slot-percent"', f'class="time-slot-percent highlight-{color}"')
                    return content
                
                html_content = re.sub(pattern, add_highlight, html_content, flags=re.DOTALL, count=1)
        
        return html_content

    def _highlight_node(self, state: VisitorDiagnoseState) -> VisitorDiagnoseState:
        """LLM을 사용해 하이라이트 대상 메트릭을 결정"""
        try:
            import json as _json

            metric_dict = state.get("metric_dict", {})
            if not metric_dict:
                self.logger.warning("metric_dict가 없어 highlight 스킵")
                state["highlights"] = []
                return state

            # metric_dict를 표 형식으로 변환
            table_text = self._format_metrics_for_highlight(metric_dict)

            prompt = self._highlight_prompt.format(table=table_text)
            self.logger.info(f"하이라이트 프롬프트 길이: {len(prompt)}")
            self.logger.info(f"프롬프트 샘플: {prompt[:500]}...")
            
            response = self.llm.invoke(prompt)
            self.logger.info(f"LLM 응답 타입: {type(response.content)}")
            self.logger.info(f"LLM 응답 내용: '{response.content}'")
            
            highlights = []
            try:
                # 마크다운 코드 블록 제거
                content = response.content.strip()
                if content.startswith("```json"):
                    content = content[7:]  # ```json 제거
                if content.endswith("```"):
                    content = content[:-3]  # ``` 제거
                content = content.strip()
                
                highlights = _json.loads(content)["highlight"]
            except Exception as e:
                self.logger.error(f"highlight JSON 파싱 실패: {e}")
                self.logger.error(f"파싱 시도한 내용: '{content[:200] if 'content' in locals() else response.content[:200]}'")

            state["highlights"] = highlights
            self.logger.info(f"하이라이트 선정: {highlights}")
            return state
        except Exception as e:
            self.logger.error(f"highlight 노드 오류: {e}")
            state["highlights"] = []
            return state

    def _format_metrics_for_highlight(self, metric_dict: dict) -> str:
        """
        metric_dict를 LLM이 분석하기 좋은 표 형식으로 변환
        """
        table_lines = []
        table_lines.append("매장별 방문객 진단 데이터:")
        table_lines.append("=" * 50)
        
        for store, data in metric_dict.items():
            table_lines.append(f"\n[{store}]")
            
            # 일평균 방문객수
            daily_avg = data.get('daily_avg', {})
            table_lines.append(f"일평균 방문객수: 전체 {daily_avg.get('전체', 0)}명, 평일 {daily_avg.get('평일', 0)}명, 주말 {daily_avg.get('주말', 0)}명")
            
            # 성별 경향
            gender = data.get('gender', {})
            table_lines.append(f"성별 경향: 남성 {gender.get('남성', 0)}%, 여성 {gender.get('여성', 0)}%")
            
            # 연령대 순위
            age_rank = data.get('age_rank', {})
            age_info = []
            for i in range(1, 4):
                for key, pct in age_rank.items():
                    if key.startswith(f"{i}위_"):
                        age_group = key.split('위_')[1]
                        age_info.append(f"{i}위: {age_group} ({pct}%)")
                        break
            table_lines.append(f"연령대 순위: {', '.join(age_info)}")
            
            # 시간대 경향
            time_slots = data.get('time_slots', {})
            for day_type in ['평일', '주말']:
                slots = time_slots.get(day_type, {})
                time_info = []
                for i in range(1, 4):
                    for key, pct in slots.items():
                        if key.startswith(f"{i}위_"):
                            parts = key.split('_')
                            if len(parts) >= 3:
                                time_name = parts[1]
                                time_range = parts[2]
                                time_info.append(f"{i}위: {time_name}({time_range}) {pct}%")
                            break
                table_lines.append(f"{day_type} 시간대: {', '.join(time_info)}")
        
        return "\n".join(table_lines)

# 기존 엑셀 관련 메서드들은 HTML 버전으로 대체되었습니다.

    def _generate_dummy_data_for_store(self, store_name: str) -> str:
        """더미데이터점들을 위한 가짜 데이터 생성 (각 매장마다 다른 특성)"""
        self.logger.info(f"{store_name} 더미 데이터 생성")
        
        # 매장별로 다른 더미 데이터 패턴
        dummy_patterns = {
            "더미데이터점": {
                "daily": {"전체": 380, "평일": 320, "주말": 450},  # 평일/주말 40% 차이 (개선점)
                "gender": {"M": 65, "F": 35},  # 성별 편중 65% (하이라이트 대상)
                "age": [("40대", 38), ("50대", 32), ("30대", 20)],  # 40대 38% 집중 (하이라이트 대상)
                "time_weekday": [("오후(14-17)", 38), ("저녁(18-21)", 24), ("낮(10-13)", 19)],  # 시간대 집중 38%
                "time_weekend": [("저녁(18-21)", 35), ("오후(14-17)", 28), ("낮(10-13)", 22)]
            },
            "더미데이터점1": {
                "daily": {"전체": 320, "평일": 310, "주말": 340},  # 균형잡힌 패턴
                "gender": {"M": 42, "F": 58},  # 균형잡힌 성별 분포
                "age": [("30대", 32), ("20대", 26), ("40대", 24)],  # 균형잡힌 연령대
                "time_weekday": [("아침(06-09)", 35), ("오후(14-17)", 25), ("저녁(18-21)", 20)],  # 아침 시간대 주력 (특이함)
                "time_weekend": [("심야(22-01)", 32), ("저녁(18-21)", 28), ("오후(14-17)", 25)]  # 심야 시간대 주력 (특이함)
            },
            "더미데이터점2": {
                "daily": {"전체": 450, "평일": 480, "주말": 400},  # 평일이 더 많음 (특이함)
                "gender": {"M": 25, "F": 75},  # 여성 극도 편중 75% (하이라이트 대상)
                "age": [("50대", 45), ("60대이상", 30), ("40대", 15)],  # 50대 45% 집중 (하이라이트 대상)
                "time_weekday": [("낮(10-13)", 40), ("오후(14-17)", 25), ("저녁(18-21)", 20)],  # 낮 시간대 40% 집중
                "time_weekend": [("낮(10-13)", 38), ("오후(14-17)", 28), ("저녁(18-21)", 22)]
            },
            "더미데이터점3": {
                "daily": {"전체": 280, "평일": 180, "주말": 420},  # 극심한 평일/주말 차이 (하이라이트 대상)
                "gender": {"M": 72, "F": 28},  # 남성 극도 편중 72% (하이라이트 대상)
                "age": [("20대", 42), ("10대", 35), ("30대", 18)],  # 20대 42% 집중 (하이라이트 대상)
                "time_weekday": [("아침(06-09)", 28), ("낮(10-13)", 25), ("오후(14-17)", 24)],  # 아침 시간대 주력
                "time_weekend": [("심야(22-01)", 45), ("저녁(18-21)", 30), ("오후(14-17)", 15)]  # 심야 45% 집중 (특이함)
            },
            "더미데이터점4": {
                "daily": {"전체": 520, "평일": 530, "주말": 500},  # 높은 방문객 수
                "gender": {"M": 48, "F": 52},  # 균형잡힌 성별
                "age": [("30대", 28), ("40대", 26), ("20대", 24)],  # 균형잡힌 연령대
                "time_weekday": [("오후(14-17)", 32), ("저녁(18-21)", 28), ("낮(10-13)", 25)],
                "time_weekend": [("저녁(18-21)", 34), ("오후(14-17)", 30), ("낮(10-13)", 26)]
            }
        }
        
        # 해당 매장의 패턴 가져오기 (없으면 기본 더미데이터점 패턴 사용)
        pattern = dummy_patterns.get(store_name, dummy_patterns["더미데이터점"])
        
        # 더미 데이터 생성
        dummy_data = f"""
=== {store_name} ===
일평균 방문객수
  전체: {pattern['daily']['전체']}명
  평일: {pattern['daily']['평일']}명
  주말: {pattern['daily']['주말']}명

성별경향
  M: {pattern['gender']['M']}%
  F: {pattern['gender']['F']}%

연령대별 순위
  1: {pattern['age'][0][0]} - {pattern['age'][0][1]}%
  2: {pattern['age'][1][0]} - {pattern['age'][1][1]}%
  3: {pattern['age'][2][0]} - {pattern['age'][2][1]}%

주요 방문시간대
  평일:
    1: {pattern['time_weekday'][0][0]} - {pattern['time_weekday'][0][1]}%
    2: {pattern['time_weekday'][1][0]} - {pattern['time_weekday'][1][1]}%
    3: {pattern['time_weekday'][2][0]} - {pattern['time_weekday'][2][1]}%
  주말:
    1: {pattern['time_weekend'][0][0]} - {pattern['time_weekend'][0][1]}%
    2: {pattern['time_weekend'][1][0]} - {pattern['time_weekend'][1][1]}%
    3: {pattern['time_weekend'][2][0]} - {pattern['time_weekend'][2][1]}%
"""
        return dummy_data


# FastMCP 인스턴스 (툴 서버 등록용)
mcp = FastMCP("visitor_diagnose_excel")


@mcp.tool()  # FastMCP 서버 전용
def visitor_diagnose_html(
    *,
    store_name: Union[str, List[str]],
    start_date: str,
    end_date: str,
    user_prompt: str = "매장 방문객 진단 HTML 보고서"
) -> str:
    """[HTML_REPORT] Generate a **modern HTML report** for *visitor diagnostics*.

    Trigger words (case-insensitive):
        - "html", "웹", "web", "보고서", "report", "진단"
        - Combinations like "방문객 진단 html", "visitor diagnose report" etc.

    Use this when the user asks for a visitor analysis report. Creates a beautiful,
    responsive HTML report with interactive features including:
    - Individual store cards with metrics
    - Comparison tables between stores  
    - Visual highlights for important data
    - Mobile-friendly responsive design

    Parameters
    ----------
    store_name : str | list[str]
        Store name(s) to diagnose. Accepts a single string or a list.
        Special values:
        - "더미데이터" or "더미" or "dummy": Automatically generates multi-store dummy data
    start_date : str
        Start date (YYYY-MM-DD).
    end_date : str
        End date (YYYY-MM-DD).
    user_prompt : str, optional
        Custom prompt for LLM. Defaults to "매장 방문객 진단 HTML 보고서".

    Returns
    -------
    str
        Result message containing the absolute path to the generated HTML file.
    """
    
    # 더미데이터 자동 확장 기능
    if isinstance(store_name, str):
        dummy_keywords = ["더미데이터", "더미", "dummy", "테스트", "test"]
        if any(keyword in store_name.lower() for keyword in dummy_keywords):
            # 더미데이터 요청 시 여러 매장으로 자동 확장 (test_visitor_workflow.py --multi와 동일)
            store_name = ["더미데이터점", "더미데이터점1", "더미데이터점2", "더미데이터점3", "더미데이터점4"]
            print(f"🎯 더미데이터 요청 감지 - 다중 매장 비교 모드로 변환: {store_name}")

    workflow = VisitorDiagnoseWorkflow()
    return workflow.run(
        user_prompt=user_prompt,
        store_name=store_name,
        start_date=start_date,
        end_date=end_date,
    )


@mcp.tool()  # 더미데이터 전용 함수
def visitor_diagnose_dummy_multi(
    *,
    start_date: str,
    end_date: str,
    user_prompt: str = "더미데이터 매장들 비교 진단 보고서"
) -> str:
    """[DUMMY_DATA] Generate a **multi-store comparison HTML report** using *dummy test data*.

    Trigger words (case-insensitive):
        - "더미", "dummy", "테스트", "test", "샘플", "sample", "예시", "example"
        - Combinations like "더미데이터 보고서", "테스트 매장", "샘플 보고서" etc.

    Use this when the user wants to see a demo/test report with multiple dummy stores.
    Automatically includes 5 different dummy stores with diverse visitor patterns:
    - 더미데이터점: 평일/주말 불균형, 성별/연령대 편중 (하이라이트 많음)
    - 더미데이터점1: 아침/심야 시간대 주력 (특이한 패턴)
    - 더미데이터점2: 여성/중장년층 편중 (특정 고객층 집중)
    - 더미데이터점3: 극심한 평일/주말 차이, 젊은층 집중 (문제점 많음)
    - 더미데이터점4: 균형잡힌 운영 (비교 기준점)

    Parameters
    ----------
    start_date : str
        Start date (YYYY-MM-DD).
    end_date : str
        End date (YYYY-MM-DD).
    user_prompt : str, optional
        Custom prompt for LLM. Defaults to "더미데이터 매장들 비교 진단 보고서".

    Returns
    -------
    str
        Result message containing the HTML report with comparison of all dummy stores.
    """
    
    # 미리 정의된 더미매장들 (test_visitor_workflow.py --multi와 동일)
    dummy_stores = ["망우혜원점", "더미데이터점", "더미데이터점1", "더미데이터점2", "더미데이터점3", "더미데이터점4"]
    
    print(f"🎯 더미데이터 다중매장 비교 모드: {dummy_stores}")
    
    workflow = VisitorDiagnoseWorkflow()
    return workflow.run(
        user_prompt=user_prompt,
        store_name=dummy_stores,
        start_date=start_date,
        end_date=end_date,
    )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Visitor Diagnose Workflow runner")
    parser.add_argument("--cli", action="store_true", help="Run a one-off workflow instead of starting FastMCP server")
    parser.add_argument("--store", default="더미데이터점", help="Store name for test run")
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD (default: 지난주 월요일)")
    parser.add_argument("--end",   default=None, help="End date YYYY-MM-DD (default: 지난주 일요일)")
    args = parser.parse_args()

    if args.cli:
        import datetime as dt

        # 기간 기본값 계산(지난주)
        if not args.start or not args.end:
            today = dt.date.today()
            this_mon = today - dt.timedelta(days=today.weekday())
            start = this_mon - dt.timedelta(days=7)
            end = start + dt.timedelta(days=6)
            start_date = start.isoformat()
            end_date   = end.isoformat()
        else:
            start_date = args.start
            end_date   = args.end

        wf = VisitorDiagnoseWorkflow()
        result = wf.run(
            user_prompt="CLI test run",
            store_name=args.store,
            start_date=start_date,
            end_date=end_date,
        )
        print("Workflow finished:", result)
    else:
        # FastMCP 서버 실행
        print("FastMCP 서버 시작 - visitor_diagnose", file=sys.stderr)
        try:
            mcp.run()
        except Exception as e:
            print(f"서버 오류 발생: {e}", file=sys.stderr)
