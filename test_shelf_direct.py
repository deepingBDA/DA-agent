#!/usr/bin/env python3
"""
진열대 분석 직접 테스트 코드
FastMCP 데코레이터를 우회하여 함수 로직을 직접 실행

파라미터:
- 분석 기간: 2025년 6월 12일 ~ 2025년 7월 12일
- 제외 날짜: 2025년 6월 22일
- 첫 픽업 진열대: 빵
- 연령대: 10대
- 성별: 여성
- 제외 진열대: 진열대없음, 전자렌지
"""

import os
import sys
from typing import Dict, Any, List, Union, Optional
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

def _create_clickhouse_client(database="plusinsight"):
    """ClickHouse 클라이언트 생성 (mcp_shelf.py에서 복사)"""
    import clickhouse_connect
    
    # 환경변수 로드
    CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
    CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
    SSH_HOST = os.getenv("SSH_HOST")
    SSH_PORT = int(os.getenv("SSH_PORT", "22"))
    SSH_USERNAME = os.getenv("SSH_USERNAME")
    SSH_PASSWORD = os.getenv("SSH_PASSWORD")
    
    # SSH 터널링이 필요한 경우
    try:
        from sshtunnel import SSHTunnelForwarder
        SSH_AVAILABLE = True
    except ImportError:
        SSH_AVAILABLE = False
        
    if SSH_AVAILABLE and SSH_HOST:
        try:
            ssh_tunnel = SSHTunnelForwarder(
                (SSH_HOST, SSH_PORT),
                ssh_username=SSH_USERNAME,
                ssh_password=SSH_PASSWORD,
                remote_bind_address=(CLICKHOUSE_HOST, int(CLICKHOUSE_PORT)),
                local_bind_address=("localhost", 0),
                allow_agent=False,
                host_pkey_directories=[]
            )
            ssh_tunnel.start()
            local_port = ssh_tunnel.local_bind_port
            print(f"SSH 터널 생성: localhost:{local_port}")
            
            client = clickhouse_connect.get_client(
                host="localhost",
                port=local_port,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=database
            )
            print(f"ClickHouse 연결 성공: localhost:{local_port}, db={database}")
            return client
            
        except Exception as e:
            print(f"SSH 터널 연결 실패: {e}")
            return None
    else:
        # 직접 연결
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=int(CLICKHOUSE_PORT),
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=database
            )
            print(f"ClickHouse 직접 연결 성공: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}, db={database}")
            return client
        except Exception as e:
            print(f"ClickHouse 직접 연결 실패: {e}")
            return None


def direct_shelf_analysis(
    start_date: str = "2025-06-12",
    end_date: str = "2025-07-12",
    exclude_dates: List[str] = None,
    target_shelves: List[str] = None,
    exclude_shelves: List[str] = None,
    age_groups: List[str] = None,
    gender_labels: List[str] = None,
    top_n: int = 5,
    exclude_from_top: List[str] = None,
    period: str = "both"
):
    """
    진열대 분석 함수 (mcp_shelf.py의 get_shelf_analysis_flexible 로직을 직접 구현)
    FastMCP 데코레이터 없이 직접 실행 가능한 버전
    """
    # 🔍 디버깅: 실제 전달받은 파라미터 로깅
    print(f"🔍 [DEBUG] direct_shelf_analysis 호출됨")
    print(f"  start_date: {start_date}")
    print(f"  end_date: {end_date}")
    print(f"  target_shelves: {target_shelves}")
    print(f"  age_groups: {age_groups}")
    print(f"  gender_labels: {gender_labels}")
    print(f"  exclude_dates: {exclude_dates}")
    print(f"  top_n: {top_n}")
    
    client = _create_clickhouse_client()
    if not client:
        return {"error": "ClickHouse 연결 실패"}
    
    # 안전장치: 너무 넓은 범위 쿼리 방지
    if not target_shelves and not age_groups and not gender_labels:
        return {
            "error": "분석 범위가 너무 넓습니다. target_shelves, age_groups, gender_labels 중 최소 하나는 지정해야 합니다.",
            "suggestion": "예: target_shelves=['빵'], age_groups=['20대'], gender_labels=['여자']"
        }
    
    # 파라미터 처리
    exclude_dates = exclude_dates or ['2025-06-22']
    exclude_shelves = exclude_shelves or ['계산대']
    exclude_from_top = exclude_from_top or []
    
    # 날짜 조건
    exclude_dates_str = "', '".join(exclude_dates)
    date_condition = f"AND cbe.date NOT IN ('{exclude_dates_str}')"
    
    # 연령대 조건 (실제 age 컬럼 기반)
    age_condition = ""
    if age_groups:
        age_conditions = []
        for age_group in age_groups:
            if age_group == '10대':
                age_conditions.append("(cbe.age >= 10 AND cbe.age < 20)")
            elif age_group == '20대':
                age_conditions.append("(cbe.age >= 20 AND cbe.age < 30)")
            elif age_group == '30대':
                age_conditions.append("(cbe.age >= 30 AND cbe.age < 40)")
            elif age_group == '40대':
                age_conditions.append("(cbe.age >= 40 AND cbe.age < 50)")
            elif age_group == '50대':
                age_conditions.append("(cbe.age >= 50 AND cbe.age < 60)")
            elif age_group == '60대 이상':
                age_conditions.append("(cbe.age >= 60)")
            elif age_group == '미상':
                age_conditions.append("(cbe.age IS NULL)")
        if age_conditions:
            age_condition = f"AND ({' OR '.join(age_conditions)})"
    
    # 성별 조건 (실제 gender 컬럼 기반: 0=남자, 1=여자)
    gender_condition = ""
    if gender_labels:
        gender_conditions = []
        for gender_label in gender_labels:
            if gender_label == '남자':
                gender_conditions.append("cbe.gender = 0")
            elif gender_label == '여자':
                gender_conditions.append("cbe.gender = 1")
            elif gender_label == '미상':
                gender_conditions.append("cbe.gender IS NULL")
        if gender_conditions:
            gender_condition = f"AND ({' OR '.join(gender_conditions)})"
    
    # 진열대 필터 조건 (첫 픽업 진열대)
    target_shelf_condition = ""
    if target_shelves:
        target_shelves_str = "', '".join(target_shelves)
        target_shelf_condition = f"AND z.name IN ('{target_shelves_str}')"
    
    exclude_shelf_condition = ""
    if exclude_shelves:
        exclude_shelves_str = "', '".join(exclude_shelves)
        exclude_shelf_condition = f"AND COALESCE(NULLIF(shelf_name, ''), '진열대없음') NOT IN ('{exclude_shelves_str}')"
    
    exclude_from_top_condition = ""
    if exclude_from_top:
        exclude_from_top_str = "', '".join(exclude_from_top)
        exclude_from_top_condition = f"AND shelf_name NOT IN ('{exclude_from_top_str}')"
    
    # 기간 조건
    period_condition = ""
    if period == "before":
        period_condition = "AND period = 'before'"
    elif period == "after":
        period_condition = "AND period = 'after'"
    
    print("🔍 [DEBUG] 쿼리 조건들:")
    print(f"  date_condition: {date_condition}")
    print(f"  age_condition: {age_condition}")
    print(f"  gender_condition: {gender_condition}")
    print(f"  target_shelf_condition: {target_shelf_condition}")
    print(f"  exclude_shelf_condition: {exclude_shelf_condition}")
    
    # 간단한 테스트 쿼리로 먼저 확인
    test_query = f"""
    SELECT COUNT(*) as total_events
    FROM customer_behavior_event cbe
    LEFT JOIN customer_behavior_area cba ON cbe.customer_behavior_area_id = cba.id
    LEFT JOIN zone z ON cba.attention_target_zone_id = z.id
    WHERE cbe.date BETWEEN '{start_date}' AND '{end_date}'
        {date_condition}
        AND cbe.event_type = 1  -- 픽업
        AND (cbe.is_staff IS NULL OR cbe.is_staff != 1)
        AND z.name IS NOT NULL
        {age_condition}
        {gender_condition}
        {target_shelf_condition}
    """
    
    print("🔍 [DEBUG] 테스트 쿼리 실행 중...")
    try:
        test_result = client.query(test_query).result_rows
        print(f"✅ 조건에 맞는 총 이벤트 수: {test_result[0][0] if test_result else 0}")
        
        if not test_result or test_result[0][0] == 0:
            return {
                "error": "조건에 맞는 데이터가 없습니다.",
                "suggestion": "날짜 범위나 필터 조건을 확인해주세요.",
                "query_conditions": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "target_shelves": target_shelves,
                    "age_groups": age_groups,
                    "gender_labels": gender_labels
                }
            }
            
    except Exception as e:
        return {
            "error": f"테스트 쿼리 실행 실패: {str(e)}",
            "query": test_query
        }
    
    # 실제 분석은 복잡하므로 여기서는 테스트 결과만 반환
    return {
        "success": True,
        "message": "조건에 맞는 데이터가 존재합니다. 실제 분석을 진행할 수 있습니다.",
        "total_events": test_result[0][0],
        "parameters": {
            "start_date": start_date,
            "end_date": end_date,
            "exclude_dates": exclude_dates,
            "target_shelves": target_shelves,
            "age_groups": age_groups,
            "gender_labels": gender_labels,
            "exclude_shelves": exclude_shelves,
            "top_n": top_n,
            "period": period
        }
    }


def test_shelf_analysis_direct():
    """직접 구현한 함수로 진열대 분석 테스트"""
    
    print("=" * 60)
    print("🧪 진열대 분석 직접 테스트 시작")
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
        print("🔄 direct_shelf_analysis 함수 호출 중...")
        print("-" * 40)
        
        # 함수 호출
        result = direct_shelf_analysis(**test_params)
        
        print("✅ 함수 호출 완료!")
        print("-" * 40)
        
        # 결과 출력
        print("📊 분석 결과:")
        print("-" * 40)
        
        if isinstance(result, dict):
            if "error" in result:
                print(f"❌ 오류 발생: {result['error']}")
                if "suggestion" in result:
                    print(f"💡 제안사항: {result['suggestion']}")
                if "query_conditions" in result:
                    print(f"🔍 쿼리 조건: {result['query_conditions']}")
            elif "success" in result and result["success"]:
                print(f"✅ {result['message']}")
                print(f"📈 총 이벤트 수: {result['total_events']}")
                print(f"📋 사용된 파라미터: {result['parameters']}")
            else:
                print(f"🔍 결과: {result}")
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


if __name__ == "__main__":
    print("🚀 진열대 분석 직접 테스트 프로그램 시작")
    print()
    
    # 메인 분석 테스트
    test_shelf_analysis_direct()