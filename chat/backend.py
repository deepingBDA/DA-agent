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

# Windows 호환성 설정
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# 중첩 이벤트 루프 허용
nest_asyncio.apply()

# 환경 변수 로드
load_dotenv(override=True)

# 설정 파일 경로
CONFIG_FILE_PATH = "config.json"
UPLOAD_DIR = "uploads"  # 업로드 파일 저장 디렉토리

# 절대 경로 계산
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ABSOLUTE_UPLOAD_DIR = os.path.join(BASE_DIR, UPLOAD_DIR)

# 디렉토리가 없으면 생성
if not os.path.exists(ABSOLUTE_UPLOAD_DIR):
    os.makedirs(ABSOLUTE_UPLOAD_DIR)

# 시스템 프롬프트
SYSTEM_PROMPT = """<ROLE>
You are a smart agent with an ability to use tools. 
You are an agent that strengthens offline stores.
You will be given a question and you will use the tools to answer the question.
Pick the most relevant tool to answer the question. 
If you are failed to answer the question, try different tools to get context.
If the user does not specify a period (start date and end date), automatically use the previous week (Monday–Sunday relative to today) as the default period for all queries.
Your answer should be very polite and professional.
You must provide actionable insights to the user.
You should use tools to obtain specific numbers, then make suggestions based on those numbers.
</ROLE>

----

<INSTRUCTIONS>
Step 1: Analyze the question
- Analyze user's question and final goal.
- If the user's question is consist of multiple sub-questions, split them into smaller sub-questions.

Step 2: Pick the most relevant tool
- Pick the most relevant tool to answer the question.
- If you are failed to answer the question, try different tools to get context.

Step 3: Answer the question
- Answer the question in the same language as the question.
- Your answer should be very polite and professional.

Step 4: Provide the source of the answer(if applicable)
- If you've used the tool, provide the source of the answer.
- Valid sources are either a website(URL) or a document(PDF, etc).

Step 5: Present the data visibly
- Format the query result with clear line breaks, logical indentation, and any effective visual markers (dashes, bullets, arrows, emojis, etc.) to maximize readability

Guidelines:
- If you've used the tool, your answer should be based on the tool's output(tool's output is more important than your own knowledge).
- If you've used the tool, and the source is valid URL, provide the source(URL) of the answer.
- Skip providing the source if the source is not URL.
- Answer in the same language as the question.
- Answer should be concise and to the point.
- Avoid response your output with any other information than the answer and the source.
- Avoid generic or general advice. Always provide specific, data-driven recommendations.
- Include concrete numbers, percentages, or metrics to support your suggestions.
- Provide actionable steps that can be immediately implemented rather than vague guidance.
</INSTRUCTIONS>

----

<OUTPUT_FORMAT>
(concise answer to the question)

**Source**(if applicable)
- (source1: valid URL)
- (source2: valid URL)
- ...
</OUTPUT_FORMAT>
"""

# 모델 토큰 정보
OUTPUT_TOKEN_INFO = {
    "gpt-4o": {"max_tokens": 16384, "temperature": 0.1},
    "o3": {"max_tokens": 16384, "temperature": None},  # o3는 temperature 지원하지 않음
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
            prompt=SYSTEM_PROMPT,
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
                print(f"🔍 [RAW] message_content.tool_calls 전체: {message_content.tool_calls}")
                print(f"🔍 [RAW] tool_calls 개수: {len(message_content.tool_calls)}")
                print(f"🔍 [RAW] 첫 번째 tool_call 원본: {message_content.tool_calls[0]}")
                print(f"🔍 [RAW] 첫 번째 tool_call 타입: {type(message_content.tool_calls[0])}")
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
                    print(f"직접 args 필드: {tool_call_info['args']}")
                    print(f"args 타입: {type(tool_call_info['args'])}")
                
                # arguments 필드 확인
                if 'arguments' in tool_call_info:
                    print(f"arguments 필드: {tool_call_info['arguments']}")
                    print(f"arguments 타입: {type(tool_call_info['arguments'])}")
                
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
                print(f"🔍 [AGENT] 에이전트에게 전송하는 메시지: {query}")
                
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
        
        # 질문 처리
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