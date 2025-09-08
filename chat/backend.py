import os
import asyncio
import json
import uuid
import platform
import nest_asyncio
import shutil
from typing import Dict, List, Optional, Any
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import logging

from langgraph.prebuilt import create_react_agent
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage, AIMessageChunk, ToolMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
from utils import astream_graph, random_uuid
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.runnables import RunnableConfig

# 스키마 관리자 import
from knowledge.schema_context import get_full_schema_context

# 멀티 에이전트 시스템 import
try:
    from workflows.multi_agent_workflow import MultiAgentWorkflow
    from agents.orchestrator import OrchestratorAgent
    from agents.specialists.data_analyst import DataAnalystAgent
    from agents.specialists.insight_generator import InsightGeneratorAgent
    from agents.specialists.recommendation_engine import RecommendationAgent
    MULTI_AGENT_AVAILABLE = True
except ImportError as e:
    print(f"멀티 에이전트 시스템을 사용할 수 없습니다: {e}")
    MULTI_AGENT_AVAILABLE = False

# Windows 호환성 설정
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# 중첩 이벤트 루프 허용
nest_asyncio.apply()

# 환경 변수 로드
load_dotenv(override=True)

# LangSmith 추적 설정
import os
os.environ.setdefault("LANGCHAIN_TRACING_V2", "true")
if os.getenv("LANGSMITH_API_KEY"):
    print("✅ LangSmith 추적이 활성화되었습니다.")
    print(f"📊 프로젝트: {os.getenv('LANGCHAIN_PROJECT', 'da-agent')}")
else:
    print("⚠️  LangSmith API 키가 설정되지 않았습니다.")

# 설정 파일 경로
CONFIG_FILE_PATH = "config.json"
UPLOAD_DIR = "uploads"  # 업로드 파일 저장 디렉토리

# 절대 경로 계산
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ABSOLUTE_UPLOAD_DIR = os.path.join(BASE_DIR, UPLOAD_DIR)

# 디렉토리가 없으면 생성
if not os.path.exists(ABSOLUTE_UPLOAD_DIR):
    os.makedirs(ABSOLUTE_UPLOAD_DIR)

def build_dynamic_system_prompt() -> str:
    """동적으로 스키마 정보를 포함한 시스템 프롬프트 생성"""
    try:
        # JSON 스키마 파일들에서 스키마 정보 가져오기
        schema_context = get_full_schema_context()
        
        # 기본 프롬프트 + 동적 스키마 정보 결합
        return f"""{BASE_SYSTEM_PROMPT}

----

<DETAILED_DATABASE_SCHEMA>
{schema_context}
</DETAILED_DATABASE_SCHEMA>

{SYSTEM_PROMPT_FOOTER}"""
    except Exception as e:
        print(f"⚠️ 스키마 로딩 실패, 기본 프롬프트 사용: {e}")
        return f"{BASE_SYSTEM_PROMPT}\n\n{SYSTEM_PROMPT_FOOTER}"

# 기본 시스템 프롬프트 (스키마 부분 제외)
BASE_SYSTEM_PROMPT = """<ROLE>
You are an expert Retail Analytics Intelligence AI powered by GPT-5, specializing in offline store data analysis.
You have direct access specialized MCP tools for comprehensive retail analytics and business intelligence.

## Your Core Capabilities:
🔬 **Data Analysis**: Complex SQL queries, statistical analysis, and data validation
💡 **Pattern Recognition**: Identify trends, anomalies, and behavioral insights
🎯 **Business Intelligence**: Transform data into actionable recommendations
📊 **Comparative Analysis**: Store-to-store, period-to-period comparisons
⚡ **Real-time Insights**: Fast, accurate responses to business questions
</ROLE>

----

<DATABASE_SCHEMA>
## Database Architecture:

### 🏪 Central Hub Database (cu_base) - SYSTEM ARCHITECTURE
**🚨 CRITICAL SYSTEM DESIGN:**

**Step 1: Central Database Connection (.env)**
- Primary connection: CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD from .env
- Database name: "cu_base" (single centralized database)
- Purpose: Contains BOTH store connection info AND POS sales data

**Step 2: What's in the Central cu_base Database:**
1. **Store Connection Registry** (site_db_connection_config table)
   - Maps each store to its individual database connection info
   - Used by database_manager.py to find how to connect to each store's behavior data
   
2. **Centralized POS Data** (cu_revenue_total table)
   - Contains ALL stores' POS transaction data in ONE table
   - store_nm: 매장명 (filter by this to get specific store data)
   - tran_ymd: 거래 날짜
   - small_nm: 상품명  
   - sale_amt: 판매 금액
   - sale_qty: 판매 수량

**⚠️ KEY INSIGHT: cu_base is both the "connection hub" AND the "POS data warehouse"**

### 👥 Store-Specific Behavior Databases (plusinsight) 
**🔄 SECONDARY DATABASE CONNECTIONS:**

**Step 3: How to Connect to Each Store's Behavior Data:**
1. Query cu_base.site_db_connection_config to get store's connection info
2. Use that info to connect to store's individual plusinsight database  
3. Each store has its own separate database server/connection

**Step 4: What's in Each Store's plusinsight Database:**
**Key Tables per Store**:

**line_in_out_individual**: 방문객 입출입 개별 기록
- person_seq: 개별 방문객 고유 ID
- date, timestamp: 방문 시간
- in_out: 입장(IN)/퇴장(OUT)  
- is_staff: 직원 여부 (0: 고객, 1: 직원)

**customer_behavior_event**: 고객 매장 내 행동 이벤트
- person_seq: 방문객 ID
- event_type: 행동 유형 (1: 픽업, 2: 시선, 3: 체류)
- timestamp: 행동 발생 시각
- customer_behavior_area_id: 행동 발생 구역 ID

**zone**: 매장 내 구역 정보
- id: 구역 고유 ID
- name: 구역명 (음료, 과자, 시식대 등)
- coords: 구역 좌표

**sales_funnel**: 방문-노출-픽업 깔때기 분석
- shelf_name: 진열대명
- date: 분석 날짜
- visit, gaze1, pickup: 방문/노출/픽업 수

**two_step_flow**: 고객 동선 패턴 (3단계 이동)
- gender, age_group: 고객 속성
- zone1_id, zone2_id, zone3_id: 이동 경로
- num_people: 패턴 발생 수

**detected_time**: AI 감지 고객 속성
- person_seq: 고객 ID
- age: 추정 연령
- gender: 성별 (0: 남성, 1: 여성)
</DATABASE_SCHEMA>"""

SYSTEM_PROMPT_FOOTER = """
----

<BUSINESS_CONTEXT>
## Key Data Sources:
- **Customer Behavior**: Entry/exit patterns, dwell time, zone transitions
- **Engagement Metrics**: Pickup rates, gaze patterns, attention duration
- **Sales Funnel**: Visit → Exposure → Pickup → Purchase conversion
- **Demographic Insights**: Age/gender-based behavior patterns
- **POS Integration**: Actual sales correlation with behavior data

## Performance Benchmarks:
- **Conversion Rate**: >30% excellent, <20% critical
- **Pickup Rate**: >15% excellent, <5% needs immediate attention
- **Dwell Time**: 3-10min optimal, >20min indicates confusion
- **Zone Utilization**: Identify hot zones (>200% avg traffic) and dead zones (<50%)

## Analysis Framework:
### Level 1: Data Foundation
- Ensure data completeness and quality
- Apply proper filtering (exclude staff, validate time ranges)
- Use previous week (Mon-Sun) as default period if not specified

### Level 2: Pattern Recognition
- Identify trends (3+ day patterns), anomalies (±2σ from mean)
- Compare against historical baselines and seasonal patterns
- Segment analysis by demographics, time periods, zones

### Level 3: Root Cause Analysis
- Apply "5 Whys" methodology to understand underlying causes
- Consider external factors: weather, promotions, competition
- Correlate behavior patterns with business outcomes

### Level 4: Strategic Recommendations
- Provide 3-5 specific, prioritized actions
- Include implementation timeline and expected ROI
- Quantify potential impact with confidence intervals
</BUSINESS_CONTEXT>

----

<INSTRUCTIONS>
## Analysis Approach:

### 1. Understand the Question
- Identify what specific insights or data the user is seeking
- Determine the appropriate time period (default: previous week)
- Consider which stores/zones are relevant

### 2. Select Appropriate MCP Tools
**CRITICAL: Database Architecture Rules**

**🚨 IMPORTANT DATABASE ROUTING:**
- **POS/매출 데이터** = `cu_base` 데이터베이스 (중앙 서버) → **`pos_` 시작하는 툴만 사용**
- **고객 행동 데이터** = `plusinsight` 데이터베이스 (매장별) → **`insight_` 또는 `diagnose_` 툴 사용**

**🏪 POS Sales Data (Central cu_base hub only):**
**⚠️ WORKFLOW: .env connection → cu_base database → cu_revenue_total table**
- Connection: Direct .env credentials (CLICKHOUSE_HOST, CLICKHOUSE_PORT, etc.)
- Database: cu_base (the central hub that also contains store connection registry)
- Table: `cu_revenue_total` (ALL stores' POS data in one table, filter by store_nm)
- Tools: `pos_daily_sales_stats`, `receipt_ranking`, `sales_ranking`, `volume_ranking`, `event_product_analysis`, `ranking_event_product`, `co_purchase_trend`
**🚨 POS data is in the SAME central database that contains store connection info!**

**👥 Customer Behavior Data (Store-specific plusinsight databases):**
**⚠️ WORKFLOW: .env connection → cu_base → site_db_connection_config → individual store's plusinsight DB**
- Step 1: Connect to cu_base using .env credentials  
- Step 2: Query site_db_connection_config table to get store's connection info
- Step 3: Connect to that store's individual plusinsight database
- Tables: `line_in_out_individual`, `customer_behavior_event`, `zone`, `sales_funnel`, `two_step_flow`, `detected_time`
- Tools: `diagnose_*` (except purchase_conversion_rate), `insight_*`, `shelf_*`

**🔄 Cross-Database Analysis:**
- `diagnose_purchase_conversion_rate`: Uses BOTH cu_base + plusinsight

**🏬 Store Management:**
- `get_available_sites`: 사용 가능한 매장 목록
- `validate_site`: 매장명 유효성 검증

### 3. Provide Context-Rich Insights
- Always explain what the data means in business terms
- Compare against benchmarks and historical performance
- Identify actionable opportunities and risks
- Quantify potential impact where possible

### 4. Korean Language Support
- Respond in Korean when user asks in Korean
- Use appropriate business terminology
- Maintain professional tone
- Include success metrics and monitoring approach

## Quality Standards:
- **Accuracy**: All numbers must be verified and properly sourced
- **Relevance**: Focus on insights that drive business decisions
- **Actionability**: Every recommendation must be specific and implementable
- **Timeliness**: Distinguish between immediate, short-term, and strategic actions
- **Confidence**: Indicate certainty levels for predictions and recommendations
</INSTRUCTIONS>

----

## Response Guidelines:
- **Be Direct**: Answer the specific question asked
- **Use Data**: Support insights with actual numbers from tools
- **Stay Relevant**: Focus on actionable business insights
- **Be Concise**: Provide clear, structured responses
- **Show Sources**: Mention which MCP tools were used

Remember: You have direct access to comprehensive retail analytics data. Use the MCP tools effectively to provide accurate, data-driven insights that help optimize store performance.
"""

# 동적 시스템 프롬프트 생성
def get_system_prompt() -> str:
    """현재 시스템 프롬프트 반환 (동적 스키마 포함)"""
    return build_dynamic_system_prompt()

# 모델 토큰 정보
OUTPUT_TOKEN_INFO = {
    "gpt-5": {"max_tokens": 16384, "temperature": 0.1},
    "gpt-4o": {"max_tokens": 16384, "temperature": 0.1},
}

# 설정 로드 함수
def load_config_from_json():
    """
    설정을 JSON 파일에서 로드합니다.
    파일이 없는 경우 기본 설정으로 파일을 생성합니다.

    Returns:
        dict: 로드된 설정
    """
    default_config = {
        "get_current_time": {
            "command": "python",
            "args": ["./mcp_server_time.py"],
            "transport": "stdio"
        }
    }
    
    try:
        if os.path.exists(CONFIG_FILE_PATH):
            with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            # 파일이 없는 경우 기본 설정으로 생성
            save_config_to_json(default_config)
            return default_config
    except Exception as e:
        print(f"설정 파일 로드 오류: {str(e)}")
        return default_config

# 설정 저장 함수
def save_config_to_json(config):
    """
    설정을 JSON 파일에 저장합니다.

    Args:
        config (dict): 저장할 설정
    
    Returns:
        bool: 저장 성공 여부
    """
    try:
        with open(CONFIG_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"설정 파일 저장 오류: {str(e)}")
        return False

# MCP 클라이언트와 에이전트 저장소
mcp_clients = {}
agents = {}
conversation_histories = {}
agent_models = {}  # 스레드별 사용 중인 모델 저장
tool_count = 0  # 전역 변수로 tool_count 선언

# 멀티 에이전트 워크플로우 저장소
multi_agent_workflows = {}  # 스레드별 멀티 에이전트 워크플로우

# 요청 및 응답 모델 정의
class Message(BaseModel):
    role: str
    content: str
    attachment_path: Optional[str] = None  # 첨부 파일 경로 추가

class MessageResponse(BaseModel):
    messages: List[Message]
    thread_id: str

class QueryRequest(BaseModel):
    query: str
    thread_id: Optional[str] = None
    model: str = "gpt-4o"
    timeout_seconds: int = 120
    recursion_limit: int = 100
    use_multi_agent: bool = True  # 멀티 에이전트 시스템 사용 여부

class QueryResponse(BaseModel):
    response: str
    tool_info: Optional[str] = None
    thread_id: str

class ToolRequest(BaseModel):
    tool_config: Dict[str, Any]

class SettingsResponse(BaseModel):
    tool_count: int
    available_models: List[str]
    current_config: Dict[str, Any]

class ThreadResponse(BaseModel):
    thread_id: str

class FileUploadResponse(BaseModel):
    file_path: str
    file_name: str
    file_size: int

# FastAPI 애플리케이션 설정
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 애플리케이션 시작 시 설정 로드
    if not os.path.exists(ABSOLUTE_UPLOAD_DIR):
        os.makedirs(ABSOLUTE_UPLOAD_DIR)
    print(f"업로드 디렉토리 경로: {ABSOLUTE_UPLOAD_DIR}")
    yield
    # 애플리케이션 종료 시 모든 MCP 클라이언트 종료
    for client in mcp_clients.values():
        try:
            await client.__aexit__(None, None, None)
        except Exception as e:
            print(f"MCP 클라이언트 종료 오류: {str(e)}")

# 파일 업로드 크기 제한을 300MB로 설정
app = FastAPI(title="MCP Tool Agent API", lifespan=lifespan)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 실제 배포 시에는 특정 출처만 허용으로 변경하세요
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 정적 파일 서빙 설정 (업로드 파일 액세스용)
app.mount("/uploads", StaticFiles(directory=ABSOLUTE_UPLOAD_DIR), name="uploads")

# HTML 보고서 서빙 설정
REPORT_DIR = os.path.join(BASE_DIR, "report")
if not os.path.exists(REPORT_DIR):
    os.makedirs(REPORT_DIR, exist_ok=True)
app.mount("/reports", StaticFiles(directory=REPORT_DIR), name="reports")

# 에이전트 초기화 함수
async def initialize_agent(thread_id: str, model: str = "gpt-4o", mcp_config=None):
    """
    MCP 세션과 에이전트를 초기화합니다.

    Args:
        thread_id: 대화 스레드 ID
        model: 사용할 모델
        mcp_config: MCP 도구 설정 정보(JSON). None인 경우 기본 설정 사용

    Returns:
        bool: 초기화 성공 여부
    """
    try:
        global tool_count  # 전역 변수 사용 선언
        
        # 기존 클라이언트가 있으면 삭제
        if thread_id in mcp_clients:
            del mcp_clients[thread_id]
        
        if mcp_config is None:
            # 설정 파일에서 설정 로드
            mcp_config = load_config_from_json()
        
        # 새로운 API 사용 방식 적용
        client = MultiServerMCPClient(mcp_config)
        tools = await client.get_tools()  # get_tools 메서드가 이제 비동기식입니다
        tool_count = len(tools)  # 전역 변수에 할당
        
        print(f"🔍 [TOOLS] 사용 가능한 도구 개수: {tool_count}")
        for i, tool in enumerate(tools[:3]):  # 처음 3개만 출력
            print(f"🔍 [TOOLS] 도구 {i+1}: {getattr(tool, 'name', 'name 없음')}")
            if hasattr(tool, 'description'):
                print(f"  설명: {tool.description[:100]}...")
        if len(tools) > 3:
            print(f"🔍 [TOOLS] ... 외 {len(tools)-3}개 더")
        
        # OpenAI 모델 사용
        model_info = OUTPUT_TOKEN_INFO[model]
        llm_kwargs = {
            "model": model,
            "max_tokens": model_info["max_tokens"],
        }
        
        # temperature가 설정되어 있는 경우만 추가 (o3는 temperature 지원하지 않음)
        if model_info["temperature"] is not None:
            llm_kwargs["temperature"] = model_info["temperature"]
            
        llm = ChatOpenAI(**llm_kwargs)
        
        agent = create_react_agent(
            llm,
            tools,
            checkpointer=MemorySaver(),
            prompt=get_system_prompt(),
        )
        
        # 클라이언트와 에이전트 저장
        mcp_clients[thread_id] = client
        agents[thread_id] = agent
        conversation_histories[thread_id] = []
        agent_models[thread_id] = model  # 현재 모델 저장
        
        return True
    except Exception as e:
        print(f"에이전트 초기화 오류: {str(e)}")
        return False

# 스트리밍 콜백 클래스
class StreamingResponse:
    def __init__(self):
        self.accumulated_text = []
        self.accumulated_tool = []
        print(f"accumulated_text: {self.accumulated_text}")
        print(f"accumulated_tool: {self.accumulated_tool}")
    
    def callback(self, message: dict):
        # 디버깅을 위한 메시지 로깅
        # print(f"메시지 타입: {type(message)}")
        # print(f"메시지 키: {message.keys() if hasattr(message, 'keys') else 'No keys'}")
        
        message_content = message.get("content", None)
        # print(f"메시지 내용 타입: {type(message_content)}")
        
        if isinstance(message_content, AIMessageChunk):
            content = message_content.content
            # 콘텐츠가 리스트 형태인 경우 (Claude 모델 등에서 주로 발생)
            if isinstance(content, list) and len(content) > 0:
                message_chunk = content[0]
                # 텍스트 타입인 경우 처리
                if message_chunk["type"] == "text":
                    self.accumulated_text.append(message_chunk["text"])
                # 도구 사용 타입인 경우 처리
                elif message_chunk["type"] == "tool_use":
                    if "partial_json" in message_chunk:
                        tool_content = message_chunk["partial_json"]
                        formatted_tool = f"**도구 이름**: `{tool_content.get('name', '알 수 없음')}`\n\n**입력값**:\n```json\n{json.dumps(tool_content.get('args', {}), indent=2, ensure_ascii=False)}\n```\n"
                        self.accumulated_tool.append(formatted_tool)
                    else:
                        tool_call_chunks = message_content.tool_call_chunks
                        tool_call_chunk = tool_call_chunks[0]
                        formatted_tool = f"**도구 호출 정보**:\n\n```json\n{json.dumps(tool_call_chunk, indent=2, ensure_ascii=False)}\n```\n"
                        self.accumulated_tool.append(formatted_tool)
            # tool_calls 속성이 있는 경우 처리 (OpenAI 모델 등에서 주로 발생)
            elif (
                hasattr(message_content, "tool_calls")
                and message_content.tool_calls
                and len(message_content.tool_calls[0]["name"]) > 0
            ):
                # 🔍 원시 tool_calls 데이터 확인
                # 도구 호출 처리
                tool_call_info = message_content.tool_calls[0]
                tool_name = tool_call_info.get("name", "알 수 없음")
                tool_args = tool_call_info.get("arguments", {})
                
                # 마크다운 형식으로 도구 정보 포맷팅
                formatted_tool = f"**도구 이름**: `{tool_name}`\n\n**입력값**:\n```json\n{json.dumps(tool_args, indent=2, ensure_ascii=False)}\n```\n"
                self.accumulated_tool.append(formatted_tool)
                
                # 콘솔에 도구 호출 정보 출력 (디버깅용)
                print(f"도구 호출 감지: {tool_name}")
                print(f"원본 tool_call_info 전체: {tool_call_info}")
                print(f"tool_call_info.keys(): {list(tool_call_info.keys())}")
                
                # args 필드 직접 확인
                if 'args' in tool_call_info:
                    print(f"args 필드: {tool_call_info['args']}")
                    print(f"args 필드 타입: {type(tool_call_info['args'])}")
                    print(f"args 필드 내용: {tool_call_info['args']}")
                    print(f"args 필드 내용 타입: {type(tool_call_info['args'])}")
                    print(f"args 필드 내용 내용: {tool_call_info['args']}")
                    print(f"args 필드 내용 내용 타입: {type(tool_call_info['args'])}")
                
                # arguments 필드 확인
                if 'arguments' in tool_call_info:
                    print(f"get으로 파싱된 인자: {tool_args}")
                    print(f"파싱된 인자 타입: {type(tool_args)}")
                    
                if isinstance(tool_args, str):
                    try:
                        parsed_args = json.loads(tool_args)
                        print(f"JSON 파싱된 인자: {parsed_args}")
                    except Exception as e:
                        print(f"JSON 파싱 실패, 원본 문자열: {repr(tool_args)}, 오류: {e}")
            # 단순 문자열인 경우 처리
            elif isinstance(content, str):
                self.accumulated_text.append(content)
            # 유효하지 않은 도구 호출 정보가 있는 경우 처리
            elif (
                hasattr(message_content, "invalid_tool_calls")
                and message_content.invalid_tool_calls
            ):
                tool_call_info = message_content.invalid_tool_calls[0]
                formatted_tool = f"**유효하지 않은 도구 호출**:\n\n```json\n{json.dumps(tool_call_info, indent=2, ensure_ascii=False)}\n```\n"
                self.accumulated_tool.append(formatted_tool)
            # tool_call_chunks 속성이 있는 경우 처리
            elif (
                hasattr(message_content, "tool_call_chunks")
                and message_content.tool_call_chunks
            ):
                tool_call_chunk = message_content.tool_call_chunks[0]
                formatted_tool = f"**도구 호출**:\n\n```json\n{json.dumps(tool_call_chunk, indent=2, ensure_ascii=False)}\n```\n"
                self.accumulated_tool.append(formatted_tool)
            # additional_kwargs에 tool_calls가 있는 경우 처리 (다양한 모델 호환성 지원)
            elif (
                hasattr(message_content, "additional_kwargs")
                and "tool_calls" in message_content.additional_kwargs
            ):
                tool_call_info = message_content.additional_kwargs["tool_calls"][0]
                tool_name = tool_call_info.get("name", "알 수 없음")
                tool_args = tool_call_info.get("arguments", {})
                
                formatted_tool = f"**도구 이름**: `{tool_name}`\n\n**입력값**:\n```json\n{json.dumps(tool_args, indent=2, ensure_ascii=False)}\n```\n"
                self.accumulated_tool.append(formatted_tool)
                
                # 콘솔에 도구 호출 정보 출력 (디버깅용)
                print(f"도구 호출 감지(additional_kwargs): {tool_name}")
                print(f"원본 additional_kwargs 전체: {message_content.additional_kwargs}")
                print(f"tool_calls 배열: {message_content.additional_kwargs.get('tool_calls', [])}")
                
                if message_content.additional_kwargs.get('tool_calls'):
                    first_tool = message_content.additional_kwargs['tool_calls'][0]
                    print(f"첫 번째 tool_call 전체: {first_tool}")
                    print(f"tool_call keys: {list(first_tool.keys()) if isinstance(first_tool, dict) else 'Not dict'}")
                    
                    # function 필드 확인
                    if 'function' in first_tool:
                        func_info = first_tool['function']
                        print(f"function 필드: {func_info}")
                        if 'arguments' in func_info:
                            print(f"function.arguments: {func_info['arguments']}")
                            print(f"function.arguments 타입: {type(func_info['arguments'])}")
                
                print(f"get으로 파싱된 인자: {tool_args}")
                print(f"파싱된 인자 타입: {type(tool_args)}")
                
                if isinstance(tool_args, str):
                    try:
                        parsed_args = json.loads(tool_args)
                        print(f"JSON 파싱된 인자: {parsed_args}")
                    except Exception as e:
                        print(f"JSON 파싱 실패, 원본 문자열: {repr(tool_args)}, 오류: {e}")
        # 도구 메시지인 경우 처리 (도구의 응답)
        elif hasattr(message_content, 'content') and isinstance(message_content, ToolMessage):
            tool_content = message_content.content
            # 도구 응답 정보를 마크다운 형식으로 포맷팅
            formatted_tool = f"**도구 응답 결과**:\n\n```\n{tool_content}\n```\n"
            self.accumulated_tool.append(formatted_tool)
            
            # 콘솔에 도구 응답 정보 출력 (디버깅용)
            print(f"도구 응답 내용: {tool_content[:200]}..." if len(tool_content) > 200 else tool_content)
        # 일반 텍스트 응답 처리
        elif hasattr(message_content, 'content') and isinstance(message_content.content, str):
            self.accumulated_text.append(message_content.content)
        
        return None
    
    def get_results(self):
        final_text = "".join(self.accumulated_text)
        final_tool = "".join(self.accumulated_tool)
        
        print(f"최종 텍스트 길이: {len(final_text)}")
        print(f"최종 도구 정보 길이: {len(final_tool)}")
        
        return final_text, final_tool

# 질문 처리 함수
async def process_query(thread_id: str, query: str, timeout_seconds=60, recursion_limit=100):
    """
    사용자 질문을 처리하고 응답을 생성합니다.

    Args:
        thread_id: 대화 스레드 ID
        query: 사용자 질문 텍스트
        timeout_seconds: 응답 생성 제한 시간(초)
        recursion_limit: 재귀 호출 제한 횟수

    Returns:
        response: 에이전트 응답 객체
        final_text: 최종 텍스트 응답
        final_tool: 최종 도구 호출 정보
    """
    try:
        if thread_id in agents:
            streaming_response = StreamingResponse()
            
            try:                
                messages = [HumanMessage(content=query)]
                # 에이전트 처리 시작
                
                response = await asyncio.wait_for(
                    astream_graph(
                        agents[thread_id],
                        {"messages": messages},
                        callback=streaming_response.callback,
                        config=RunnableConfig(
                            recursion_limit=recursion_limit,
                            thread_id=thread_id,
                        ),
                    ),
                    timeout=timeout_seconds,
                )
            except asyncio.TimeoutError:
                error_msg = f"요청 시간이 {timeout_seconds}초를 초과했습니다. 나중에 다시 시도해주세요."
                return {"error": error_msg}, error_msg, ""

            print("StreamingResponse 완료")
            final_text, final_tool = streaming_response.get_results()
            
            return response, final_text, final_tool
        else:
            return {"error": "에이전트가 초기화되지 않았습니다."}, "에이전트가 초기화되지 않았습니다.", ""
    except Exception as e:
        import traceback
        error_msg = f"질문 처리 중 오류 발생: {str(e)}\n{traceback.format_exc()}"
        return {"error": error_msg}, error_msg, ""

# API 엔드포인트 정의
@app.get("/api/settings")
async def get_settings():
    """현재 설정 정보를 반환합니다."""
    print("[GET] /api/settings")
    global tool_count  # 전역 변수 사용 선언
    
    config = load_config_from_json()
    
    # MCP 클라이언트가 아직 초기화되지 않았으면 임시 클라이언트를 생성하여 도구 개수 확인
    if tool_count == 0:
        try:
            # 새로운 API 사용 방식 적용
            temp_client = MultiServerMCPClient(config)
            tools = await temp_client.get_tools()
            tool_count = len(tools)
        except Exception as e:
            print(f"도구 개수 확인 중 오류 발생: {str(e)}")
            # 오류 발생 시 설정 파일의 도구 개수를 사용
            tool_count = len(config)
    
    return SettingsResponse(
        tool_count=tool_count,
        available_models=list(OUTPUT_TOKEN_INFO.keys()),
        current_config=config
    )

@app.post("/api/settings")
async def update_settings(tool_config: ToolRequest):
    """설정을 업데이트하고 저장합니다."""
    print("[POST] /api/settings")
    global tool_count  # 전역 변수 사용 선언
    
    success = save_config_to_json(tool_config.tool_config)
    if not success:
        raise HTTPException(status_code=500, detail="설정 저장 중 오류가 발생했습니다.")
    
    # 설정이 업데이트되면 도구 개수도 업데이트
    try:
        # 새로운 API 사용 방식 적용
        temp_client = MultiServerMCPClient(tool_config.tool_config)
        tools = await temp_client.get_tools()
        tool_count = len(tools)
    except Exception as e:
        print(f"도구 개수 업데이트 중 오류 발생: {str(e)}")
        # 오류 발생 시 설정 파일의 도구 개수를 사용
        tool_count = len(tool_config.tool_config)
    
    return {
        "success": True, 
        "message": "설정이 업데이트되었습니다.", 
        "tool_count": tool_count
    }

@app.post("/api/threads")
async def create_thread():
    """새로운 대화 스레드를 생성합니다."""
    print("[POST] /api/threads")
    thread_id = random_uuid()
    await initialize_agent(thread_id)
    return ThreadResponse(thread_id=thread_id)

@app.get("/api/threads/{thread_id}")
async def get_thread(thread_id: str):
    """특정 스레드의 대화 기록을 반환합니다."""
    print("[GET] /api/threads/{thread_id}")
    if thread_id not in conversation_histories:
        raise HTTPException(status_code=404, detail="스레드를 찾을 수 없습니다.")
    return MessageResponse(
        messages=conversation_histories[thread_id],
        thread_id=thread_id
    )

@app.delete("/api/threads/{thread_id}")
async def delete_thread(thread_id: str):
    """스레드를 삭제합니다."""
    print("[DELETE] /api/threads/{thread_id}")
    if thread_id in mcp_clients:
        # 새 버전에서는 context manager 인터페이스를 사용하지 않으므로 직접 삭제만 함
        del mcp_clients[thread_id]
    
    if thread_id in agents:
        del agents[thread_id]
    
    if thread_id in conversation_histories:
        del conversation_histories[thread_id]
    
    return {"success": True, "message": "스레드가 삭제되었습니다."}

@app.post("/api/upload", response_model=FileUploadResponse)
async def upload_file(file: UploadFile = File(...)):
    """
    파일을 서버에 업로드합니다.
    """
    print(f"[POST] /api/upload - 파일 이름: {file.filename}, 크기: {file.size if hasattr(file, 'size') else '알 수 없음'}")
    try:
        # 고유 파일명 생성 (파일명 충돌 방지)
        file_extension = os.path.splitext(file.filename)[1] if file.filename else ""
        unique_filename = f"{uuid.uuid4()}{file_extension}"
        file_path = os.path.join(ABSOLUTE_UPLOAD_DIR, unique_filename)
        
        # 파일 저장
        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        
        # 파일 정보 반환
        file_size = os.path.getsize(file_path)
        print(f"파일 업로드 완료: {file_path}, 크기: {file_size}")
        
        # 응답 데이터 구성
        response_data = FileUploadResponse(
            file_path=file_path,  # 절대 경로로 반환
            file_name=file.filename,
            file_size=file_size
        )
        print(f"응답 데이터: {response_data}")
        return response_data
    except Exception as e:
        print(f"파일 업로드 중 오류 발생: {str(e)}")
        raise HTTPException(status_code=500, detail=f"파일 업로드 중 오류 발생: {str(e)}")
    finally:
        file.file.close()

@app.post("/api/threads/{thread_id}/query")
async def query_agent(thread_id: str, request: QueryRequest, background_tasks: BackgroundTasks):
    """
    에이전트에 질문을 전송하고 응답을 반환합니다.
    """
    print(f"[POST] /api/threads/{thread_id}/query: {request.query}")
    # 스레드 ID가 제공되지 않은 경우 새로 생성
    if not thread_id:
        thread_id = random_uuid()
    
    # 에이전트가 없거나 모델이 변경된 경우 재초기화
    need_init = (
        thread_id not in agents or 
        thread_id not in agent_models or 
        agent_models.get(thread_id) != request.model
    )
    
    if need_init:
        print(f"에이전트 초기화/재초기화 필요: thread_id={thread_id}, model={request.model}")
        # 에이전트 초기화
        success = await initialize_agent(
            thread_id, 
            model=request.model
        )
        
        if not success:
            raise HTTPException(status_code=500, detail="에이전트 초기화 중 오류가 발생했습니다.")
    
    # 첨부 파일 경로 추출
    attachment_path = None
    if "[첨부 파일: " in request.query:
        try:
            # 첨부 파일 경로 추출
            start_idx = request.query.find("[첨부 파일: ") + len("[첨부 파일: ")
            end_idx = request.query.find("]", start_idx)
            attachment_path = request.query[start_idx:end_idx]
            
            # 상대 경로인 경우 절대 경로로 변환
            if attachment_path.startswith("./uploads/") or attachment_path.startswith("/uploads/"):
                file_name = os.path.basename(attachment_path)
                attachment_path = os.path.join(ABSOLUTE_UPLOAD_DIR, file_name)
                print(f"첨부 파일 경로 변환: {attachment_path}")
            
            # 파일 존재 여부 확인
            if not os.path.exists(attachment_path):
                print(f"경고: 파일이 존재하지 않습니다: {attachment_path}")
                return {
                    "response": f"오류: 파일을 찾을 수 없습니다. 경로: {attachment_path}",
                    "thread_id": thread_id
                }
            else:
                print(f"파일 확인 완료: {attachment_path} (크기: {os.path.getsize(attachment_path)} bytes)")
        except Exception as e:
            print(f"첨부 파일 경로 추출 중 오류 발생: {str(e)}")
    
    try:
        # 대용량 파일이 포함된 경우 타임아웃 값 증가
        timeout_value = request.timeout_seconds
        if attachment_path and os.path.exists(attachment_path):
            file_size = os.path.getsize(attachment_path)
            # 파일 크기가 10MB를 초과하면 타임아웃 값 증가
            if file_size > 10 * 1024 * 1024:
                timeout_value = max(timeout_value, 300)  # 최소 300초
                print(f"대용량 파일 감지: 타임아웃 값을 {timeout_value}초로 증가")
        
        # 백그라운드 작업 시작 로깅
        print(f"대화 처리 시작: 타임아웃={timeout_value}초, 재귀 제한={request.recursion_limit}")
        
        # 질문 처리 - 멀티 에이전트 시스템 사용 여부에 따라 분기
        if request.use_multi_agent and MULTI_AGENT_AVAILABLE:
            try:
                # 멀티 에이전트 워크플로우 초기화 (필요시)
                if thread_id not in multi_agent_workflows:
                    # MCP 클라이언트 가져오기
                    mcp_client = mcp_clients.get(thread_id)
                    if not mcp_client:
                        # MCP 클라이언트가 없으면 기본 ReAct 에이전트로 폴백
                        raise RuntimeError("MCP 클라이언트가 초기화되지 않음")
                    
                    # LLM 모델 초기화
                    model_config = OUTPUT_TOKEN_INFO.get(request.model, OUTPUT_TOKEN_INFO["gpt-4o"])
                    llm_model = ChatOpenAI(
                        model=request.model,  # 선택한 모델 그대로 사용 (gpt-5, gpt-4o)
                        temperature=model_config["temperature"],
                        max_tokens=model_config["max_tokens"]
                    )
                    
                    multi_agent_workflows[thread_id] = MultiAgentWorkflow(
                        mcp_client=mcp_client,
                        model=llm_model
                    )
                    print(f"✨ [MULTI-AGENT] 멀티 에이전트 워크플로우 초기화 완료: {thread_id}")
                
                # 멀티 에이전트 워크플로우로 쿼리 처리
                workflow_result = await multi_agent_workflows[thread_id].execute(
                    user_query=request.query,
                    session_id=thread_id
                )
                
                final_text = workflow_result.get("final_insight", "멀티 에이전트 분석 완료")
                final_tool = ""  # 멀티 에이전트는 도구 정보를 별도로 제공하지 않음
                
                print(f"✨ [MULTI-AGENT] 멀티 에이전트 분석 완료: {len(final_text)} chars")
                
            except Exception as e:
                print(f"⚠️ [MULTI-AGENT] 멀티 에이전트 처리 실패, ReAct 에이전트로 폴백: {e}")
                # 기본 ReAct 에이전트로 폴백
                _, final_text, final_tool = await process_query(
                    thread_id, 
                    request.query,
                    timeout_seconds=timeout_value,
                    recursion_limit=request.recursion_limit
                )
        else:
            # 기본 ReAct 에이전트 사용
            print(f"🔧 [REACT] 기본 ReAct 에이전트 사용")
            _, final_text, final_tool = await process_query(
                thread_id, 
                request.query,
                timeout_seconds=timeout_value,
                recursion_limit=request.recursion_limit
            )
        
        print(f"final_text 길이: {len(final_text)}")
        if final_tool:
            print(f"final_tool 길이: {len(final_tool)}")
            print(f"final_tool 샘플: {final_tool[:200]}..." if len(final_tool) > 200 else final_tool)
        
        # 대화 기록에 추가
        conversation_histories[thread_id].append(
            Message(
                role="user", 
                content=request.query,
                attachment_path=attachment_path
            )
        )
        conversation_histories[thread_id].append(
            Message(
                role="assistant", 
                content=final_text
            )
        )
        
        # 도구 정보가 있으면 해당 정보 반환
        tool_info = None
        if final_tool:
            tool_info = final_tool
            print(f"도구 사용 정보 전송: 도구 정보 길이 {len(tool_info)}")
        
        return QueryResponse(
            response=final_text,
            tool_info=tool_info,
            thread_id=thread_id
        )
    except asyncio.TimeoutError:
        error_msg = f"요청 시간이 {timeout_value}초를 초과했습니다. 나중에 다시 시도해주세요."
        print(f"오류: 타임아웃 발생 - {error_msg}")
        
        # 타임아웃 발생 시 대화 기록에 오류 메시지 추가
        conversation_histories[thread_id].append(
            Message(
                role="user", 
                content=request.query,
                attachment_path=attachment_path
            )
        )
        conversation_histories[thread_id].append(
            Message(
                role="assistant", 
                content=error_msg
            )
        )
        
        return QueryResponse(
            response=error_msg,
            thread_id=thread_id
        )
    except Exception as e:
        import traceback
        error_msg = f"질문 처리 중 오류 발생: {str(e)}\n{traceback.format_exc()}"
        print(f"오류: {error_msg}")
        
        # 오류 발생 시 대화 기록에 오류 메시지 추가
        conversation_histories[thread_id].append(
            Message(
                role="user", 
                content=request.query,
                attachment_path=attachment_path
            )
        )
        conversation_histories[thread_id].append(
            Message(
                role="assistant", 
                content=f"오류가 발생했습니다: {str(e)}"
            )
        )
        
        return QueryResponse(
            response=f"오류가 발생했습니다: {str(e)}",
            thread_id=thread_id
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 