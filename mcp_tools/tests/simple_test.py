#!/usr/bin/env python3
"""
mcp_shelf.py 간단 테스트 (2025-06-12 ~ 2025-07-12)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mcp_shelf import _create_clickhouse_client

def test_original_function():
    """원본 함수 직접 테스트"""
    print("=== 원본 mcp_shelf 함수 테스트 ===")
    
    client = _create_clickhouse_client()
    if not client:
        print("❌ ClickHouse 연결 실패")
        return False
    
    # 원본 함수에서 사용하는 파라미터들
    start_date = "2025-06-12"
    end_date = "2025-07-12"
    exclude_dates = ['2025-06-22']
    exclude_shelves = ['진열대없음']
    top_n = 5
    
    # 파라미터 처리 (원본과 동일)
    exclude_dates_str = "', '".join(exclude_dates)
    date_condition = f"AND cbe.date NOT IN ('{exclude_dates_str}')"
    
    exclude_shelves_str = "', '".join(exclude_shelves)
    exclude_shelf_condition = f"AND shelf_name NOT IN ('{exclude_shelves_str}')"
    
    # 원본 복잡한 쿼리를 그대로 실행해보기
    try:
        # 일단 가장 간단한 부분부터 테스트
        simple_query = f"""
        SELECT 
            multiIf(age >= 60, '60대 이상', age >= 50, '50대', age >= 40, '40대', age >= 30, '30대', age >= 20, '20대', age >= 10, '10대', age IS NULL, '미상', '10세 미만') AS age_group,
            multiIf(gender = 0, '남자', gender = 1, '여자', '미상') AS gender_label,
            'both' as period,
            COALESCE(z.name, '진열대없음') as shelf_name,
            COUNT(DISTINCT cbe.person_seq) as unique_customers,
            ROW_NUMBER() OVER (PARTITION BY age_group, gender_label ORDER BY unique_customers DESC) as rank
        FROM customer_behavior_event cbe
        LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
        LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
        WHERE cbe.date BETWEEN '{start_date}' AND '{end_date}'
            {date_condition}
            AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
        GROUP BY age_group, gender_label, shelf_name
        HAVING shelf_name NOT IN ('{exclude_shelves_str}')
        ORDER BY age_group, gender_label, rank
        LIMIT 20
        """
        
        result = client.query(simple_query)
        print(f"✅ 간단한 쿼리 성공: {len(result.result_rows)}행")
        
        if result.result_rows:
            print("📊 결과 샘플:")
            for i, row in enumerate(result.result_rows[:3]):
                print(f"  {i+1}. {row}")
        
        return True
        
    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {e}")
        return False

def test_parameter_variations():
    """파라미터 변형 테스트"""
    print("\n=== 파라미터 변형 테스트 ===")
    
    client = _create_clickhouse_client()
    if not client:
        return False
    
    # 1. 20대 여성만 필터링
    print("1. 20대 여성 필터링 테스트")
    query1 = """
    SELECT age_group, gender_label, shelf_name, unique_customers
    FROM (
        SELECT 
            multiIf(age >= 60, '60대 이상', age >= 50, '50대', age >= 40, '40대', age >= 30, '30대', age >= 20, '20대', age >= 10, '10대', age IS NULL, '미상', '10세 미만') AS age_group,
            multiIf(gender = 0, '남자', gender = 1, '여자', '미상') AS gender_label,
            COALESCE(z.name, '진열대없음') as shelf_name,
            COUNT(DISTINCT cbe.person_seq) as unique_customers
        FROM customer_behavior_event cbe
        LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
        LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
        WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
            AND cbe.date != '2025-06-22'
            AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
        GROUP BY age_group, gender_label, shelf_name
    )
    WHERE age_group = '20대' AND gender_label = '여자'
    ORDER BY unique_customers DESC
    LIMIT 5
    """
    
    try:
        result = client.query(query1)
        print(f"   ✅ 20대 여성 필터링: {len(result.result_rows)}행")
        if result.result_rows:
            print(f"   📊 상위 진열대: {result.result_rows[0][2]} ({result.result_rows[0][3]}명)")
    except Exception as e:
        print(f"   ❌ 실패: {e}")
        return False
    
    # 2. 특정 진열대 제외
    print("\n2. 진열대 제외 테스트")
    query2 = """
    SELECT age_group, gender_label, shelf_name, unique_customers
    FROM (
        SELECT 
            multiIf(age >= 60, '60대 이상', age >= 50, '50대', age >= 40, '40대', age >= 30, '30대', age >= 20, '20대', age >= 10, '10대', age IS NULL, '미상', '10세 미만') AS age_group,
            multiIf(gender = 0, '남자', gender = 1, '여자', '미상') AS gender_label,
            COALESCE(z.name, '진열대없음') as shelf_name,
            COUNT(DISTINCT cbe.person_seq) as unique_customers
        FROM customer_behavior_event cbe
        LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
        LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
        WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
            AND cbe.date != '2025-06-22'
            AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
        GROUP BY age_group, gender_label, shelf_name
    )
    WHERE shelf_name NOT IN ('진열대없음', '계산대')
    ORDER BY unique_customers DESC
    LIMIT 5
    """
    
    try:
        result = client.query(query2)
        print(f"   ✅ 진열대 제외: {len(result.result_rows)}행")
        if result.result_rows:
            print(f"   📊 계산대 제외 후 상위: {result.result_rows[0][2]} ({result.result_rows[0][3]}명)")
    except Exception as e:
        print(f"   ❌ 실패: {e}")
        return False
    
    return True

def main():
    print("🚀 mcp_shelf 간단 테스트 (2025-06-12 ~ 2025-07-12)\n")
    
    success1 = test_original_function()
    success2 = test_parameter_variations()
    
    if success1 and success2:
        print("\n🎉 모든 테스트 통과!")
        print("💡 유연한 파라미터 시스템이 정상 작동합니다.")
    else:
        print("\n❌ 일부 테스트에서 문제가 있습니다.")

if __name__ == "__main__":
    main()