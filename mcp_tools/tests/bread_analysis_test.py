#!/usr/bin/env python3
"""
10대 여성 빵 픽업 전후 탐색 패턴 분석
2025-06-12 ~ 2025-07-12, 6월 22일 제외
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mcp_shelf import _create_clickhouse_client

def find_bread_shelves():
    """빵 관련 진열대명 찾기"""
    print("=== 빵 관련 진열대 찾기 ===")
    
    client = _create_clickhouse_client()
    if not client:
        return []
    
    query = """
    SELECT DISTINCT COALESCE(z.name, '진열대없음') as shelf_name
    FROM customer_behavior_event cbe
    LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
    LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
    WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
        AND cbe.date != '2025-06-22'
        AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
        AND z.name IS NOT NULL
        AND (z.name LIKE '%빵%' OR z.name LIKE '%베이커리%' OR z.name LIKE '%제과%')
    ORDER BY shelf_name
    """
    
    try:
        result = client.query(query)
        bread_shelves = [row[0] for row in result.result_rows]
        print(f"✅ 빵 관련 진열대 {len(bread_shelves)}개 발견:")
        for shelf in bread_shelves:
            print(f"   - {shelf}")
        return bread_shelves
    except Exception as e:
        print(f"❌ 빵 관련 진열대 검색 실패: {e}")
        return []

def analyze_teen_female_bread_pattern():
    """10대 여성 빵 픽업 전후 패턴 분석"""
    print("\n=== 10대 여성 빵 픽업 전후 탐색 패턴 ===")
    
    client = _create_clickhouse_client()
    if not client:
        return False
    
    # 간단한 접근: 10대 여성이 빵 관련 진열대를 본 패턴
    print("\n1. 10대 여성의 빵 관련 진열대 탐색 패턴")
    simple_query = """
    SELECT 
        '10대' as age_group, 
        '여자' as gender_label, 
        COALESCE(z.name, '진열대없음') AS shelf_name,
        COUNT(DISTINCT cbe.person_seq) as unique_customers
    FROM customer_behavior_event cbe
    LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
    LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
    WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
        AND cbe.date != '2025-06-22'
        AND cbe.event_type = 1  -- 시선 이벤트만
        AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
        AND cbe.age >= 10 AND cbe.age < 20  -- 10대
        AND cbe.gender = 1  -- 여자
        AND (z.name LIKE '%빵%' OR z.name LIKE '%베이커리%' OR z.name LIKE '%제과%')
    GROUP BY shelf_name
    ORDER BY unique_customers DESC
    LIMIT 10
    """
    
    try:
        result = client.query(simple_query)
        print(f"   ✅ 10대 여성 빵 관련 탐색: {len(result.result_rows)}행")
        if result.result_rows:
            for i, row in enumerate(result.result_rows):
                print(f"   {i+1}. {row[2]} - {row[3]}명")
        else:
            print("   📊 10대 여성의 빵 관련 진열대 탐색 데이터가 없습니다.")
    except Exception as e:
        print(f"   ❌ 분석 실패: {e}")
        return False
    
    # 2. 픽업 이벤트가 있는 10대 여성 찾기
    print("\n2. 픽업 이벤트가 있는 10대 여성")
    pickup_query = """
    SELECT 
        '10대' as age_group,
        '여자' as gender_label,
        COUNT(DISTINCT cbe.person_seq) as unique_customers,
        COUNT(*) as total_pickups
    FROM customer_behavior_event cbe
    WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
        AND cbe.date != '2025-06-22'
        AND cbe.event_type = 2  -- 픽업 이벤트
        AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
        AND cbe.age >= 10 AND cbe.age < 20  -- 10대
        AND cbe.gender = 1  -- 여자
    """
    
    try:
        result = client.query(pickup_query)
        print(f"   ✅ 픽업 이벤트 분석: {len(result.result_rows)}행")
        if result.result_rows:
            for row in result.result_rows:
                print(f"   📦 {row[0]} {row[1]}: {row[2]}명, 총 {row[3]}회 픽업")
        else:
            print("   📊 10대 여성의 픽업 이벤트가 없습니다.")
    except Exception as e:
        print(f"   ❌ 픽업 분석 실패: {e}")
        return False
    
    # 3. 10대 여성의 진열대 탐색 퍼센테이지 (상위 5개)
    print("\n3. 10대 여성 진열대 탐색 퍼센테이지 (상위 5개)")
    percentage_query = """
    WITH total_customers AS (
        SELECT COUNT(DISTINCT cbe.person_seq) as total_teen_females
        FROM customer_behavior_event cbe
        WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
            AND cbe.date != '2025-06-22'
            AND cbe.event_type = 1
            AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
            AND cbe.age >= 10 AND cbe.age < 20
            AND cbe.gender = 1
    ),
    shelf_visits AS (
        SELECT 
            COALESCE(z.name, '진열대없음') AS shelf_name,
            COUNT(DISTINCT cbe.person_seq) as unique_customers
        FROM customer_behavior_event cbe
        LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
        LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
        WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
            AND cbe.date != '2025-06-22'
            AND cbe.event_type = 1
            AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
            AND cbe.age >= 10 AND cbe.age < 20
            AND cbe.gender = 1
            AND z.name NOT IN ('계산대', '전자렌지', '진열대없음')
        GROUP BY shelf_name
    )
    SELECT 
        shelf_name,
        unique_customers,
        ROUND((unique_customers * 100.0 / total_teen_females), 1) as percentage
    FROM shelf_visits, total_customers
    ORDER BY unique_customers DESC
    LIMIT 5
    """
    
    try:
        result = client.query(percentage_query)
        print(f"   📊 상위 5개 진열대:")
        if result.result_rows:
            for i, row in enumerate(result.result_rows):
                print(f"   {i+1}. {row[0]} - {row[1]}명 ({row[2]}%)")
        else:
            print("   데이터가 없습니다.")
    except Exception as e:
        print(f"   ❌ 실패: {e}")
        return False
    
    # 4. 모든 연령대의 픽업 이벤트 확인 (픽업 이벤트가 실제로 있는지 확인)
    print("\n4. 전체 픽업 이벤트 현황 (연령대별)")
    all_pickup_query = """
    SELECT 
        multiIf(age >= 60, '60대 이상', age >= 50, '50대', age >= 40, '40대', age >= 30, '30대', age >= 20, '20대', age >= 10, '10대', age IS NULL, '미상', '10세 미만') AS age_group,
        multiIf(gender = 0, '남자', gender = 1, '여자', '미상') AS gender_label,
        COUNT(DISTINCT person_seq) as unique_customers,
        COUNT(*) as total_pickups
    FROM customer_behavior_event
    WHERE date BETWEEN '2025-06-12' AND '2025-07-12'
        AND date != '2025-06-22'
        AND event_type = 2  -- 픽업 이벤트
        AND (is_staff IS NULL OR is_staff != 1)
    GROUP BY age_group, gender_label
    ORDER BY total_pickups DESC
    LIMIT 10
    """
    
    try:
        result = client.query(all_pickup_query)
        print(f"   ✅ 전체 픽업 현황: {len(result.result_rows)}행")
        if result.result_rows:
            for row in result.result_rows:
                print(f"   📦 {row[0]} {row[1]}: {row[2]}명, {row[3]}회 픽업")
        else:
            print("   📊 픽업 이벤트가 전혀 없습니다.")
    except Exception as e:
        print(f"   ❌ 전체 픽업 분석 실패: {e}")
        return False
    
    return True

def main():
    print("📊 10대 여성 진열대 탐색 퍼센테이지")
    print("📅 기간: 2025-06-12 ~ 2025-07-12 (6월 22일 제외)\n")
    
    # 10대 여성 패턴 분석
    analyze_teen_female_bread_pattern()

if __name__ == "__main__":
    main()