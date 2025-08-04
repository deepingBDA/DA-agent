"""
MCP Agent Helper 도구
====================

에이전트가 매장 관리 및 조회를 위해 사용하는 전용 도구입니다.
개별 MCP 도구들과 분리하여 에이전트의 역할을 명확히 합니다.
"""

from fastmcp import FastMCP
from database_manager import get_all_sites

mcp = FastMCP("agent_helper")

@mcp.tool()
def get_available_sites() -> str:
    """
    에이전트가 사용 가능한 모든 매장 목록을 조회합니다.
    
    이 함수는 에이전트가 다음과 같은 상황에서 사용합니다:
    1. 사용자가 "모든 매장" 분석을 요청할 때
    2. 특정 매장명이 유효한지 확인할 때
    3. 사용자에게 매장 목록을 보여줄 때
    
    Returns:
        str: 포맷된 매장 목록 문자열
    """
    try:
        sites = get_all_sites()
        if not sites:
            return "사용 가능한 매장이 없습니다."
        
        result = "📋 **사용 가능한 매장 목록:**\n\n"
        for i, site in enumerate(sites, 1):
            result += f"{i}. {site}\n"
        result += f"\n총 {len(sites)}개 매장이 등록되어 있습니다."
        return result
    except Exception as e:
        return f"매장 목록 조회 실패: {e}"

@mcp.tool()
def validate_site(site: str) -> str:
    """
    특정 매장명이 유효한지 검증합니다.
    
    Args:
        site (str): 검증할 매장명
        
    Returns:
        str: 검증 결과 메시지
    """
    try:
        sites = get_all_sites()
        if site in sites:
            return f"✅ '{site}' 매장이 존재합니다."
        else:
            available_sites = ", ".join(sites[:5])  # 처음 5개만 표시
            more_text = f" 외 {len(sites)-5}개" if len(sites) > 5 else ""
            return f"❌ '{site}' 매장을 찾을 수 없습니다.\n사용 가능한 매장: {available_sites}{more_text}"
    except Exception as e:
        return f"매장 검증 실패: {e}"

if __name__ == "__main__":
    mcp.run()