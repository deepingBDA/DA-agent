"""
Multi-Agent API Server
======================

GPT-5 기반 멀티 에이전트 시스템을 활용한 API 서버입니다.
기존 backend.py의 단순한 ReAct 에이전트를 대체합니다.
"""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
import json
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import os

# 멀티 에이전트 시스템 import
from workflows.multi_agent_workflow import MultiAgentWorkflow
from agents.orchestrator import OrchestratorAgent
from agents.specialists.data_analyst import DataAnalystAgent
from agents.specialists.insight_generator import InsightGeneratorAgent
from agents.specialists.recommendation_engine import RecommendationAgent
from knowledge.schema_context import build_analysis_context

# MCP 관련 import
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_openai import ChatOpenAI

# 환경 변수 로드
load_dotenv(override=True)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Pydantic 모델들
# ============================================================================

class QueryRequest(BaseModel):
    query: str
    session_id: Optional[str] = None
    model: str = "gpt-5"
    timeout_seconds: int = 900  # 15분
    recursion_limit: int = 30
    use_multi_agent: bool = True

class QueryResponse(BaseModel):
    success: bool
    session_id: str
    response: str
    confidence_score: float
    processing_time: str
    agent_used: str
    completed_tasks: List[str]
    errors: List[str] = []

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    agents_active: int
    mcp_tools_count: int

class AgentStatusResponse(BaseModel):
    agent_id: str
    agent_type: str
    status: str
    capabilities_count: int
    current_load: int

# ============================================================================
# 글로벌 상태 관리
# ============================================================================

class MultiAgentSystem:
    """멀티 에이전트 시스템 관리자"""
    
    def __init__(self):
        self.workflows = {}  # 세션별 워크플로우
        self.mcp_client = None
        self.model = None
        self.agents = {}
        self.is_initialized = False
        
        # 에이전트 인스턴스들
        self.orchestrator = None
        self.data_analyst = None
        self.insight_generator = None
        self.recommendation_engine = None
    
    async def initialize(self, mcp_config: Dict[str, Any] = None):
        """시스템 초기화"""
        try:
            logger.info("멀티 에이전트 시스템 초기화 시작")
            
            # MCP 클라이언트 설정
            if mcp_config is None:
                mcp_config = self._load_default_mcp_config()
            
            self.mcp_client = MultiServerMCPClient(mcp_config)
            
            # LLM 모델 초기화
            self.model = ChatOpenAI(
                model="gpt-4o",  # gpt-5가 없으므로 gpt-4o 사용
                temperature=0.1,
                max_tokens=16384,
            )
            
            # 개별 에이전트들 초기화
            self.orchestrator = OrchestratorAgent()
            self.data_analyst = DataAnalystAgent(mcp_client=self.mcp_client)
            self.insight_generator = InsightGeneratorAgent()
            self.recommendation_engine = RecommendationAgent()
            
            # 에이전트 레지스트리
            self.agents = {
                "orchestrator": self.orchestrator,
                "data_analyst": self.data_analyst,
                "insight_generator": self.insight_generator,
                "recommendation_engine": self.recommendation_engine
            }
            
            self.is_initialized = True
            logger.info(f"멀티 에이전트 시스템 초기화 완료: {len(self.agents)}개 에이전트")
            
            return True
            
        except Exception as e:
            logger.error(f"시스템 초기화 실패: {e}")
            self.is_initialized = False
            return False
    
    def _load_default_mcp_config(self) -> Dict[str, Any]:
        """기본 MCP 설정 로드"""
        config_path = "config.json"
        
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            # 기본 설정
            return {
                "get_current_time": {
                    "command": "python",
                    "args": ["./mcp_server_time.py"],
                    "transport": "stdio"
                }
            }
    
    async def process_query(self, query: str, session_id: str, **kwargs) -> Dict[str, Any]:
        """쿼리 처리"""
        if not self.is_initialized:
            raise RuntimeError("시스템이 초기화되지 않았습니다.")
        
        start_time = datetime.now()
        
        try:
            # 세션별 워크플로우 생성 또는 재사용
            if session_id not in self.workflows:
                self.workflows[session_id] = MultiAgentWorkflow(
                    mcp_client=self.mcp_client,
                    model=self.model
                )
            
            workflow = self.workflows[session_id]
            
            # 워크플로우 실행
            result = await workflow.execute(query, session_id)
            
            # 처리 시간 계산
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return {
                "success": result.get("success", True),
                "session_id": session_id,
                "response": result.get("final_insight", "처리 완료"),
                "confidence_score": result.get("confidence_score", 0.5),
                "processing_time": f"{processing_time:.2f}초",
                "agent_used": "multi_agent_workflow",
                "completed_tasks": result.get("completed_tasks", []),
                "errors": result.get("errors", [])
            }
            
        except Exception as e:
            logger.error(f"쿼리 처리 오류: {e}")
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return {
                "success": False,
                "session_id": session_id,
                "response": f"처리 중 오류가 발생했습니다: {str(e)}",
                "confidence_score": 0.1,
                "processing_time": f"{processing_time:.2f}초",
                "agent_used": "error_handler",
                "completed_tasks": [],
                "errors": [str(e)]
            }
    
    async def get_system_health(self) -> Dict[str, Any]:
        """시스템 상태 확인"""
        agents_active = len([a for a in self.agents.values() if getattr(a, 'is_active', True)])
        
        mcp_tools_count = 0
        if self.mcp_client:
            try:
                tools = await self.mcp_client.get_tools()
                mcp_tools_count = len(tools)
            except:
                mcp_tools_count = 0
        
        return {
            "status": "healthy" if self.is_initialized else "initializing",
            "timestamp": datetime.now().isoformat(),
            "agents_active": agents_active,
            "mcp_tools_count": mcp_tools_count
        }
    
    async def get_agent_status(self) -> List[Dict[str, Any]]:
        """에이전트 상태 조회"""
        status_list = []
        
        for agent_id, agent in self.agents.items():
            try:
                health = await agent.health_check()
                status_list.append({
                    "agent_id": agent_id,
                    "agent_type": agent.agent_type.value,
                    "status": health.get("status", "unknown"),
                    "capabilities_count": len(agent.capabilities),
                    "current_load": health.get("current_load", 0)
                })
            except Exception as e:
                status_list.append({
                    "agent_id": agent_id,
                    "agent_type": "unknown",
                    "status": "error",
                    "capabilities_count": 0,
                    "current_load": -1
                })
        
        return status_list
    
    async def cleanup(self):
        """시스템 정리"""
        try:
            if self.mcp_client:
                # MCP 클라이언트는 자동으로 정리됨
                pass
            logger.info("멀티 에이전트 시스템 정리 완료")
        except Exception as e:
            logger.error(f"시스템 정리 오류: {e}")

# 글로벌 시스템 인스턴스
multi_agent_system = MultiAgentSystem()

# ============================================================================
# FastAPI 애플리케이션
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 생명주기 관리"""
    # 시작 시 초기화
    await multi_agent_system.initialize()
    yield
    # 종료 시 정리
    await multi_agent_system.cleanup()

app = FastAPI(
    title="Multi-Agent Retail Intelligence API",
    description="GPT-5 기반 멀티 에이전트 시스템을 활용한 소매 인텔리전스 API",
    version="1.0.0",
    lifespan=lifespan
)

# ============================================================================
# API 엔드포인트들
# ============================================================================

@app.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest):
    """
    사용자 쿼리를 멀티 에이전트 시스템으로 처리합니다.
    
    - **query**: 분석하고자 하는 질문
    - **session_id**: 세션 ID (선택사항, 자동 생성)
    - **model**: 사용할 모델 (기본: gpt-5)
    - **timeout_seconds**: 타임아웃 (기본: 900초)
    - **use_multi_agent**: 멀티 에이전트 시스템 사용 여부
    """
    try:
        session_id = request.session_id or f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        if not multi_agent_system.is_initialized:
            raise HTTPException(status_code=503, detail="시스템이 초기화되지 않았습니다.")
        
        result = await multi_agent_system.process_query(
            query=request.query,
            session_id=session_id,
            model=request.model,
            timeout_seconds=request.timeout_seconds,
            recursion_limit=request.recursion_limit,
            use_multi_agent=request.use_multi_agent
        )
        
        return QueryResponse(**result)
        
    except Exception as e:
        logger.error(f"API 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """시스템 상태 확인"""
    try:
        health_data = await multi_agent_system.get_system_health()
        return HealthResponse(**health_data)
    except Exception as e:
        logger.error(f"헬스 체크 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/agents/status", response_model=List[AgentStatusResponse])
async def get_agents_status():
    """모든 에이전트 상태 조회"""
    try:
        status_list = await multi_agent_system.get_agent_status()
        return [AgentStatusResponse(**status) for status in status_list]
    except Exception as e:
        logger.error(f"에이전트 상태 조회 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/agents/{agent_id}/capabilities")
async def get_agent_capabilities(agent_id: str):
    """특정 에이전트의 능력 조회"""
    try:
        if agent_id not in multi_agent_system.agents:
            raise HTTPException(status_code=404, detail="에이전트를 찾을 수 없습니다.")
        
        agent = multi_agent_system.agents[agent_id]
        capabilities = agent.get_capabilities()
        
        return {
            "agent_id": agent_id,
            "capabilities": [
                {
                    "name": cap.name,
                    "description": cap.description,
                    "input_requirements": cap.input_requirements,
                    "output_format": cap.output_format,
                    "confidence_threshold": cap.confidence_threshold
                }
                for cap in capabilities
            ]
        }
    except Exception as e:
        logger.error(f"에이전트 능력 조회 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/test/simple")
async def simple_test():
    """간단한 테스트 엔드포인트"""
    try:
        test_query = "이번 주 매장 성과가 어떤가요?"
        
        result = await multi_agent_system.process_query(
            query=test_query,
            session_id="test_session",
            model="gpt-4o"
        )
        
        return {
            "test_query": test_query,
            "result": result,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"테스트 오류: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/")
async def root():
    """루트 엔드포인트"""
    return {
        "message": "Multi-Agent Retail Intelligence API",
        "version": "1.0.0",
        "status": "running" if multi_agent_system.is_initialized else "initializing",
        "docs": "/docs",
        "health": "/health"
    }

# ============================================================================
# 실행 부분
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # 개발 서버 실행
    uvicorn.run(
        "multi_agent_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

# 사용 예시:
"""
# 서버 실행
python multi_agent_api.py

# API 호출 예시
curl -X POST "http://localhost:8000/query" \
     -H "Content-Type: application/json" \
     -d '{
       "query": "이번 주 방문객 수가 어떻게 되고 있나요? 개선 방안도 알려주세요.",
       "session_id": "demo_session",
       "model": "gpt-5"
     }'

# 헬스 체크
curl http://localhost:8000/health

# 에이전트 상태 확인
curl http://localhost:8000/agents/status

# 특정 에이전트 능력 조회
curl http://localhost:8000/agents/data_analyst/capabilities
"""