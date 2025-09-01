"""
Workflow Builder
================

LangGraph 기반 워크플로우를 동적으로 구성하는 빌더입니다.
"""

from typing import Dict, Any, List, Callable
from langgraph.graph import StateGraph, END
from .multi_agent_workflow import MultiAgentState


class WorkflowBuilder:
    """워크플로우 동적 구성 빌더"""
    
    def __init__(self):
        self.graph = StateGraph(MultiAgentState)
        self.agents = {}
        self.conditions = {}
    
    def add_agent_node(self, name: str, agent):
        """에이전트 노드 추가"""
        self.agents[name] = agent
        
        async def agent_wrapper(state: MultiAgentState) -> MultiAgentState:
            # 에이전트 실행 로직
            result = await agent.process_message(state["current_message"])
            
            # 상태 업데이트
            state["agent_responses"][name] = result.content
            state["current_agent"] = name
            state["step_count"] += 1
            
            return state
        
        self.graph.add_node(name, agent_wrapper)
        return self
    
    def add_conditional_edge(self, source: str, condition_func: Callable, mapping: Dict[str, str]):
        """조건부 엣지 추가"""
        self.conditions[source] = (condition_func, mapping)
        self.graph.add_conditional_edges(
            source,
            condition_func,
            mapping
        )
        return self
    
    def add_edge(self, source: str, target: str):
        """일반 엣지 추가"""
        self.graph.add_edge(source, target)
        return self
    
    def set_entry_point(self, entry: str):
        """진입점 설정"""
        self.graph.set_entry_point(entry)
        return self
    
    def build(self):
        """워크플로우 컴파일"""
        return self.graph.compile()
    
    def get_available_agents(self) -> List[str]:
        """사용 가능한 에이전트 목록"""
        return list(self.agents.keys())
    
    def validate_workflow(self) -> bool:
        """워크플로우 유효성 검증"""
        try:
            # 기본적인 검증 로직
            if not self.agents:
                return False
            
            # 모든 조건부 엣지의 타겟이 존재하는지 확인
            for source, (_, mapping) in self.conditions.items():
                for target in mapping.values():
                    if target not in self.agents and target != END:
                        return False
            
            return True
            
        except Exception:
            return False