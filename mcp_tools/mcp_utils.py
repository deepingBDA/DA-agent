"""
MCP Utils
=========

MCP 툴들에서 공통으로 사용하는 유틸리티 함수들
"""

import tiktoken
from typing import Dict

# 기본 모델 설정
DEFAULT_MODEL = "gpt-4o"

# 모델별 최대 토큰 수
MODEL_MAX_TOKENS: Dict[str, int] = {
    "gpt-4o": 128000,
    "gpt-4": 8192,
    "gpt-3.5-turbo": 4096,
}

def num_tokens_from_string(string: str, model: str = DEFAULT_MODEL) -> int:
    """문자열의 토큰 수를 계산합니다."""
    try:
        encoding = tiktoken.encoding_for_model(model)
        num_tokens = len(encoding.encode(string))
        return num_tokens
    except Exception as e:
        # 모델을 찾을 수 없는 경우 기본 인코딩 사용
        encoding = tiktoken.get_encoding("cl100k_base")
        num_tokens = len(encoding.encode(string))
        return num_tokens

def is_token_limit_exceeded(text: str, model: str = DEFAULT_MODEL, reserved_tokens: int = 1000) -> bool:
    """텍스트가 토큰 제한을 초과하는지 확인합니다."""
    token_count = num_tokens_from_string(text, model)
    max_tokens = MODEL_MAX_TOKENS.get(model, 4096)  # 기본값 4096
    return token_count > (max_tokens - reserved_tokens)