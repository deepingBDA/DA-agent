#!/usr/bin/env python3
"""
진열대 분석 함수 테스트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mcp_shelf import get_shelf_analysis_flexible

def test_basic_analysis():
    """기본 분석 테스트"""
    print("=== 기본 분석 테스트 ===")
    try:
        result = get_shelf_analysis_flexible()
        print(f"✅ 기본 분석 성공: {len(result)}행")
        if result and len(result) > 0:
            print(f"첫 번째 결과: {result[0]}")
        return True
    except Exception as e:
        print(f"❌ 기본 분석 실패: {e}")
        return False

def test_specific_period():
    """특정 기간 분석 테스트"""
    print("\n=== 픽업 전만 분석 테스트 ===")
    try:
        result = get_shelf_analysis_flexible(period="before")
        print(f"✅ 픽업 전 분석 성공: {len(result)}행")
        if result and len(result) > 0:
            print(f"첫 번째 결과: {result[0]}")
        return True
    except Exception as e:
        print(f"❌ 픽업 전 분석 실패: {e}")
        return False

def test_age_gender_filter():
    """연령대/성별 필터 테스트"""
    print("\n=== 20대 여성 분석 테스트 ===")
    try:
        result = get_shelf_analysis_flexible(
            age_groups=['20대'],
            gender_labels=['여자']
        )
        print(f"✅ 20대 여성 분석 성공: {len(result)}행")
        if result and len(result) > 0:
            print(f"첫 번째 결과: {result[0]}")
        return True
    except Exception as e:
        print(f"❌ 20대 여성 분석 실패: {e}")
        return False

def test_exclude_shelves():
    """진열대 제외 테스트"""
    print("\n=== 진열대 제외 테스트 ===")
    try:
        result = get_shelf_analysis_flexible(
            exclude_shelves=['진열대없음', '면류3']
        )
        print(f"✅ 진열대 제외 분석 성공: {len(result)}행")
        if result and len(result) > 0:
            print(f"첫 번째 결과: {result[0]}")
        return True
    except Exception as e:
        print(f"❌ 진열대 제외 분석 실패: {e}")
        return False

def test_top_n():
    """상위 N개 테스트"""
    print("\n=== 상위 3개 테스트 ===")
    try:
        result = get_shelf_analysis_flexible(top_n=3)
        print(f"✅ 상위 3개 분석 성공: {len(result)}행")
        if result and len(result) > 0:
            print(f"첫 번째 결과: {result[0]}")
        return True
    except Exception as e:
        print(f"❌ 상위 3개 분석 실패: {e}")
        return False

def main():
    """모든 테스트 실행"""
    print("🚀 진열대 분석 함수 테스트 시작\n")
    
    tests = [
        test_basic_analysis,
        test_specific_period,
        test_age_gender_filter,
        test_exclude_shelves,
        test_top_n
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print(f"\n📊 테스트 결과: {passed}/{total} 통과")
    
    if passed == total:
        print("🎉 모든 테스트 통과!")
    else:
        print("⚠️  일부 테스트 실패")

if __name__ == "__main__":
    main() 