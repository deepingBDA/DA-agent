"""
MCP Tools 공통 유틸리티 모듈
=============================

모든 MCP 도구에서 공통으로 사용하는 함수들을 중앙화하여 중복을 제거합니다.
각 도구는 단일 매장만 처리하고, 매장 목록 조회는 에이전트가 담당합니다.
"""

import tiktoken


# 모델별 최대 토큰 수 설정
MODEL_MAX_TOKENS = {
    "gpt-4o": 128000,
}

DEFAULT_MODEL = "gpt-4o"


def num_tokens_from_string(string: str, model: str = DEFAULT_MODEL) -> int:
    """문자열의 토큰 수를 계산합니다."""
    encoding = tiktoken.encoding_for_model(model)
    num_tokens = len(encoding.encode(string))
    return num_tokens


def is_token_limit_exceeded(text: str, model: str = DEFAULT_MODEL, reserved_tokens: int = 1000) -> bool:
    """텍스트가 토큰 제한을 초과하는지 확인합니다."""
    token_count = num_tokens_from_string(text, model)
    max_tokens = MODEL_MAX_TOKENS.get(model, 4096)  # 기본값 4096
    return token_count > (max_tokens - reserved_tokens)


def format_site_error(site: str, error: Exception) -> str:
    """매장별 오류 메시지를 일관된 형식으로 포맷팅합니다."""
    return f"❌ {site} 매장 오류: {error}"


def format_site_connection_error(site: str) -> str:
    """매장 연결 실패 메시지를 일관된 형식으로 포맷팅합니다."""
    return f"❌ {site} 매장 연결 실패"


def format_site_no_data(site: str) -> str:
    """매장 데이터 없음 메시지를 일관된 형식으로 포맷팅합니다."""
    return f"⚠️ {site}: 데이터가 없습니다."