#!/usr/bin/env python3
"""
에이전트가 주장한 파라미터로 실제 테스트
에이전트 주장: start_date=2025-06-12, end_date=2025-07-12, target_shelves=["빵"], 
age_groups=["10대"], gender_labels=["여자"], exclude_dates=["2025-06-22"], 
exclude_shelves=["진열대 없음", "전자렌지"]
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp_shelf import get_shelf_analysis_flexible

def test_agent_claimed_params():
    """에이전트가 주장한 파라미터로 테스트"""
    print("🔍 에이전트가 주장한 파라미터로 테스트 시작")
    
    result = get_shelf_analysis_flexible(
        start_date="2025-06-12",
        end_date="2025-07-12", 
        target_shelves=["빵"],
        age_groups=["10대"],
        gender_labels=["여자"],
        exclude_dates=["2025-06-22"],
        exclude_shelves=["진열대 없음", "전자렌지"]
    )
    
    print("🔍 에이전트 주장 파라미터 결과:")
    print(result)
    print()

def test_default_params():
    """기본 파라미터(빈 args)로 테스트"""
    print("🔍 기본 파라미터(빈 args 상황)로 테스트 시작")
    
    result = get_shelf_analysis_flexible()
    
    print("🔍 기본 파라미터 결과:")
    print(result)
    print()

if __name__ == "__main__":
    print("=" * 60)
    print("에이전트 주장 vs 실제 백엔드 로그 비교 테스트")
    print("=" * 60)
    
    test_agent_claimed_params()
    print("-" * 60)
    test_default_params()
    
    print("=" * 60)
    print("결론: 위 두 결과가 같다면 에이전트가 거짓말한 것!")
    print("=" * 60)