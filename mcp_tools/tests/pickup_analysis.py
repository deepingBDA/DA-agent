#!/usr/bin/env python3
"""
픽업 직전/직후 탐색 패턴 분석
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mcp_shelf import _create_clickhouse_client

def check_event_types():
    """이벤트 타입 확인"""
    print("=== 이벤트 타입 확인 ===")
    
    client = _create_clickhouse_client()
    if not client:
        return False
    
    query = """
    SELECT 
        event_type,
        COUNT(*) as count,
        COUNT(DISTINCT person_seq) as unique_persons
    FROM customer_behavior_event
    WHERE date BETWEEN '2025-06-12' AND '2025-07-12'
        AND date != '2025-06-22'
        AND (is_staff IS NULL OR is_staff != 1)
        AND age >= 10 AND age < 20
        AND gender = 1
    GROUP BY event_type
    ORDER BY event_type
    """
    
    try:
        result = client.query(query)
        print("📊 10대 여성 이벤트 타입별 현황:")
        for row in result.result_rows:
            print(f"   event_type {row[0]}: {row[1]:,}회, {row[2]}명")
        return result.result_rows
    except Exception as e:
        print(f"❌ 실패: {e}")
        return False

def find_pickup_events():
    """픽업 이벤트 찾기 (모든 이벤트 타입 확인)"""
    print("\n=== 픽업 관련 이벤트 찾기 ===")
    
    client = _create_clickhouse_client()
    if not client:
        return False
    
    # 모든 이벤트 타입에서 픽업과 관련된 것 찾기
    query = """
    SELECT 
        event_type,
        COUNT(*) as total_events,
        COUNT(DISTINCT person_seq) as unique_persons,
        MIN(timestamp) as earliest,
        MAX(timestamp) as latest
    FROM customer_behavior_event
    WHERE date BETWEEN '2025-06-12' AND '2025-07-12'
        AND date != '2025-06-22'
        AND (is_staff IS NULL OR is_staff != 1)
    GROUP BY event_type
    ORDER BY total_events DESC
    """
    
    try:
        result = client.query(query)
        print("📊 전체 이벤트 타입 현황:")
        for row in result.result_rows:
            print(f"   event_type {row[0]}: {row[1]:,}회, {row[2]}명")
        return result.result_rows
    except Exception as e:
        print(f"❌ 실패: {e}")
        return False

def analyze_time_sequence():
    """시간 순서별 이벤트 분석 (10대 여성 샘플)"""
    print("\n=== 시간 순서별 이벤트 분석 (10대 여성 샘플 3명) ===")
    
    client = _create_clickhouse_client()
    if not client:
        return False
    
    query = """
    SELECT 
        person_seq,
        timestamp,
        event_type,
        COALESCE(z.name, '진열대없음') as shelf_name
    FROM customer_behavior_event cbe
    LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
    LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
    WHERE cbe.date BETWEEN '2025-06-12' AND '2025-07-12'
        AND cbe.date != '2025-06-22'
        AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
        AND cbe.age >= 10 AND cbe.age < 20
        AND cbe.gender = 1
        AND cbe.person_seq IN (
            SELECT DISTINCT person_seq 
            FROM customer_behavior_event 
            WHERE date BETWEEN '2025-06-12' AND '2025-07-12'
                AND age >= 10 AND age < 20 
                AND gender = 1
            LIMIT 3
        )
    ORDER BY person_seq, timestamp
    LIMIT 50
    """
    
    try:
        result = client.query(query)
        print("📊 시간 순서별 이벤트 (샘플):")
        current_person = None
        for row in result.result_rows:
            if current_person != row[0]:
                current_person = row[0]
                print(f"\n👤 person_seq: {row[0]}")
            print(f"   {row[1]} | event_type:{row[2]} | {row[3]}")
        return True
    except Exception as e:
        print(f"❌ 실패: {e}")
        return False

def main():
    print("🔍 픽업 직전/직후 탐색 패턴 분석을 위한 데이터 확인")
    print("📅 기간: 2025-06-12 ~ 2025-07-12 (6월 22일 제외)\n")
    
    # 1. 이벤트 타입 확인
    event_types = check_event_types()
    
    # 2. 전체 이벤트 현황
    find_pickup_events()
    
    # 3. 시간 순서 분석
    analyze_time_sequence()
    
    print("\n🎯 다음 단계: event_type을 확인한 후 픽업 직전/직후 분석 로직 구현")

if __name__ == "__main__":
    main()