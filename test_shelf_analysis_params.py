#!/usr/bin/env python3
"""
진열대 분석 테스트 코드
사용자 지정 파라미터로 get_shelf_analysis_flexible 함수 테스트

파라미터:
- 분석 기간: 2025년 6월 12일 ~ 2025년 7월 12일
- 제외 날짜: 2025년 6월 22일
- 첫 픽업 진열대: 빵
- 연령대: 10대
- 성별: 여성
- 제외 진열대: 진열대없음, 전자렌지
"""

import sys
import os
from typing import Dict, Any

# 현재 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# mcp_tools 경로를 추가하고 직접 모듈을 import
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mcp_tools'))

# 직접 mcp_shelf 모듈을 import
import mcp_shelf


def test_shelf_analysis_with_params():
    """사용자 지정 파라미터로 진열대 분석 테스트"""
    
    print("=" * 60)
    print("🧪 진열대 분석 테스트 시작")
    print("=" * 60)
    
    # 테스트 파라미터 설정
    test_params = {
        "start_date": "2025-06-12",
        "end_date": "2025-07-12", 
        "exclude_dates": ["2025-06-22"],
        "target_shelves": ["빵"],
        "age_groups": ["10대"],
        "gender_labels": ["여자"],
        "exclude_shelves": ["진열대없음", "전자렌지"],
        "top_n": 5,
        "period": "both"
    }
    
    print("📋 테스트 파라미터:")
    for key, value in test_params.items():
        print(f"  - {key}: {value}")
    print()
    
    try:
        print("🔄 get_shelf_analysis_flexible 함수 호출 중...")
        print("-" * 40)
        
        # 함수 호출
        result = mcp_shelf.get_shelf_analysis_flexible(**test_params)
        
        print("✅ 함수 호출 완료!")
        print("-" * 40)
        
        # 결과 출력
        print("📊 분석 결과:")
        print("-" * 40)
        
        if isinstance(result, dict) and "error" in result:
            print(f"❌ 오류 발생: {result['error']}")
            if "suggestion" in result:
                print(f"💡 제안사항: {result['suggestion']}")
        elif isinstance(result, list):
            print(f"📈 총 {len(result)}개의 결과:")
            for i, item in enumerate(result, 1):
                if isinstance(item, tuple) and len(item) >= 4:
                    period, rank, shelf_name, percentage = item[:4]
                    print(f"  {i:2d}. [{period:6s}] {rank}위: {shelf_name:15s} ({percentage:6.2f}%)")
                else:
                    print(f"  {i:2d}. {item}")
        else:
            print(f"🔍 결과 타입: {type(result)}")
            print(f"📄 결과 내용: {result}")
            
    except Exception as e:
        print(f"❌ 테스트 실행 중 오류 발생:")
        print(f"   오류 타입: {type(e).__name__}")
        print(f"   오류 메시지: {str(e)}")
        
        # 상세 오류 정보
        import traceback
        print("\n🔍 상세 오류 정보:")
        print("-" * 40)
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("🧪 테스트 완료")
    print("=" * 60)


def test_connection():
    """ClickHouse 연결 테스트"""
    print("🔌 ClickHouse 연결 테스트 중...")
    
    try:
        client = mcp_shelf._create_clickhouse_client()
        
        if client:
            print("✅ ClickHouse 연결 성공!")
            
            # 간단한 쿼리 테스트
            try:
                result = client.query("SELECT 1 as test").result_rows
                print(f"✅ 테스트 쿼리 성공: {result}")
            except Exception as query_error:
                print(f"⚠️  테스트 쿼리 실패: {query_error}")
                
        else:
            print("❌ ClickHouse 연결 실패")
            
    except Exception as e:
        print(f"❌ 연결 테스트 중 오류: {e}")


if __name__ == "__main__":
    print("🚀 진열대 분석 테스트 프로그램 시작")
    print()
    
    # 1. 연결 테스트
    test_connection()
    print()
    
    # 2. 메인 분석 테스트
    test_shelf_analysis_with_params()