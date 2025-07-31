#!/usr/bin/env python3
"""
에이전트 주장 파라미터 vs 백엔드 실제 응답 비교
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 백엔드에서 실제로 받은 응답 (로그에서 추출)
backend_actual_response = [
    ["BEFORE",1,"진열대없음","54%"],
    ["BEFORE",2,"커피음료","4%"],
    ["BEFORE",3,"빵","3%"],
    ["BEFORE",4,"전자렌지","3%"],
    ["BEFORE",5,"도시락,김밥","3%"],
    ["AFTER",1,"진열대없음","44%"],
    ["AFTER",2,"전자렌지","10%"],
    ["AFTER",3,"일반아이스크림2","6%"],  # 추정
    ["AFTER",4,"커피음료","5%"],  # 추정
    ["AFTER",5,"빵","4%"]  # 추정
]

def analyze_backend_response():
    """백엔드에서 실제로 받은 응답 분석"""
    print("🔍 백엔드 실제 응답 분석:")
    print("=" * 50)
    
    before_items = [item for item in backend_actual_response if item[0] == "BEFORE"]
    after_items = [item for item in backend_actual_response if item[0] == "AFTER"]
    
    print("📊 BEFORE (픽업 전):")
    for item in before_items:
        print(f"  {item[1]}위: {item[2]} ({item[3]})")
    
    print("\n📊 AFTER (픽업 후):")
    for item in after_items:
        print(f"  {item[1]}위: {item[2]} ({item[3]})")
    
    return before_items, after_items

def create_test_parameters():
    """에이전트가 주장한 파라미터 생성"""
    return {
        "start_date": "2025-06-12",
        "end_date": "2025-07-12", 
        "target_shelves": ["빵"],
        "age_groups": ["10대"],
        "gender_labels": ["여자"],
        "exclude_dates": ["2025-06-22"],
        "exclude_shelves": ["진열대 없음", "전자렌지"]
    }

def analyze_contradictions():
    """에이전트 주장과 실제 결과의 모순점 분석"""
    print("\n❌ 에이전트 주장 vs 실제 결과 모순점:")
    print("=" * 50)
    
    params = create_test_parameters()
    
    print(f"🎯 에이전트 주장:")
    print(f"  - target_shelves: {params['target_shelves']}")
    print(f"  - age_groups: {params['age_groups']}")
    print(f"  - gender_labels: {params['gender_labels']}")
    print(f"  - exclude_shelves: {params['exclude_shelves']}")
    
    print(f"\n🔍 실제 결과에서 발견되는 모순:")
    print(f"  1. '진열대없음'이 1위 (54% → 44%) - exclude_shelves에 있는데 나타남")
    print(f"  2. '전자렌지'가 4위 → 2위 (3% → 10%) - exclude_shelves에 있는데 나타남")
    print(f"  3. '빵'이 3위 → 5위 (3% → 4%) - target_shelves인데 비중이 낮음")
    
    print(f"\n✅ 결론:")
    print(f"  - 에이전트가 exclude_shelves를 전달했다면 '진열대없음', '전자렌지'는 결과에 없어야 함")
    print(f"  - 실제로는 기본 파라미터(모든 조건 포함)로 실행된 것으로 판단")

def create_manual_test():
    """수동 테스트를 위한 파라미터 출력"""
    print(f"\n🧪 수동 테스트용 파라미터:")
    print("=" * 50)
    
    params = create_test_parameters()
    
    print("다음 파라미터로 직접 도구를 호출해서 결과를 비교해보세요:")
    print()
    print("```python")
    print("result = get_shelf_analysis_flexible(")
    for key, value in params.items():
        if isinstance(value, list):
            print(f'    {key}={value},')
        else:
            print(f'    {key}="{value}",')
    print(")")
    print("```")
    
    print(f"\n예상 결과:")
    print(f"  - '진열대없음', '전자렌지'가 결과에서 제외되어야 함")
    print(f"  - '빵'을 첫 픽업한 10대 여성만의 패턴이 나와야 함")
    print(f"  - 현재 백엔드 결과와 다른 결과가 나와야 함")

if __name__ == "__main__":
    print("🔍 에이전트 파라미터 주장 vs 백엔드 실제 응답 비교")
    print("=" * 60)
    
    analyze_backend_response()
    analyze_contradictions()
    create_manual_test()
    
    print("\n" + "=" * 60)
    print("💡 다음 단계: 위 파라미터로 실제 도구를 호출해서 결과 비교")
    print("=" * 60)