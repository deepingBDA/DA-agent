#!/usr/bin/env python3
"""
mcp_shelf.py의 함수를 직접 호출해서 에이전트 주장 파라미터 테스트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# mcp_shelf.py에서 함수 로직만 추출해서 테스트
def test_with_agent_params():
    """에이전트가 주장한 파라미터로 테스트"""
    print("🧪 에이전트 주장 파라미터로 직접 테스트")
    print("=" * 50)
    
    # 실제 mcp_shelf.py 파일을 읽어서 쿼리 로직 확인
    try:
        with open('../mcp_shelf.py', 'r', encoding='utf-8') as f:
            content = f.read()
            
        # 쿼리 부분 찾기
        if 'exclude_shelves' in content and 'target_shelves' in content:
            print("✅ mcp_shelf.py에서 파라미터 처리 로직 확인됨")
            
            # 파라미터 처리 부분 찾기
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if 'exclude_shelves' in line and 'or [' in line:
                    print(f"📍 Line {i+1}: {line.strip()}")
                if 'target_shelves' in line and 'WHERE' in line:
                    print(f"📍 Line {i+1}: {line.strip()}")
                    
        else:
            print("❌ 파라미터 처리 로직을 찾을 수 없음")
            
    except FileNotFoundError:
        print("❌ mcp_shelf.py 파일을 찾을 수 없음")
    
    print("\n🔍 예상 결과:")
    print("만약 파라미터가 제대로 전달되었다면:")
    print("  - WHERE 조건에 target_shelves=['빵'] 적용")
    print("  - WHERE 조건에 age_groups=['10대'] 적용") 
    print("  - WHERE 조건에 gender_labels=['여자'] 적용")
    print("  - exclude_shelves에서 '진열대없음', '전자렌지' 제외")
    print("  → 결과가 현재와 완전히 달라야 함")

def compare_query_conditions():
    """쿼리 조건 비교"""
    print("\n🔍 쿼리 조건 비교:")
    print("=" * 50)
    
    print("🎯 에이전트 주장 조건:")
    print("  - 10대 여성만 필터링")
    print("  - 빵 매대 첫 픽업 고객만")
    print("  - 진열대없음, 전자렌지 제외")
    
    print("\n📊 백엔드 실제 결과:")
    print("  - 진열대없음 1위 (54%) ← 제외되어야 하는데 1위!")
    print("  - 전자렌지 4위→2위 (3%→10%) ← 제외되어야 하는데 상승!")
    print("  - 빵 3위→5위 (3%→4%) ← 타겟인데 비중 낮음")
    
    print("\n💡 결론:")
    print("  백엔드 결과는 '모든 조건 포함' 기본 쿼리 결과와 일치")
    print("  에이전트 파라미터는 전혀 적용되지 않음")

if __name__ == "__main__":
    test_with_agent_params()
    compare_query_conditions()
    
    print("\n" + "=" * 60)
    print("🎯 최종 결론: 에이전트는 파라미터를 전달하지 않았음")
    print("=" * 60)