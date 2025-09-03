#!/usr/bin/env python3
"""
Simple Database Schema Extractor
================================

ClickHouse 데이터베이스에서 테이블명과 컬럼명만 추출하여 JSON 파일로 저장합니다.
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import clickhouse_connect
from dotenv import load_dotenv

# 프로젝트 루트에서 환경변수 로드
load_dotenv()

# 이미 구현된 database_manager 사용
sys.path.append(str(Path(__file__).parent / 'mcp_tools'))
from database_manager import get_site_client, get_all_sites

def extract_database_schema(database_name: str) -> Dict[str, Any]:
    """데이터베이스의 테이블명과 컬럼명만 간단하게 추출"""
    print(f"📊 {database_name} 데이터베이스 스키마 추출 시작...")
    
    # 간단한 스키마 구조
    schema = {
        "database": database_name,
        "extracted_at": datetime.now().isoformat(),
        "tables": {}
    }
    
    # 데이터베이스 연결
    if database_name == "cu_base":
        from database_manager import _create_config_client
        client = _create_config_client()
        if not client:
            print(f"❌ {database_name} 연결 실패")
            return schema
    else:
        # plusinsight는 매장별 DB - 첫 번째 매장 사용
        sites = get_all_sites()
        if not sites:
            print("❌ 사용 가능한 매장이 없습니다")
            return schema
        
        print(f"📍 {sites[0]} 매장의 {database_name} DB를 사용하여 스키마 추출")
        client = get_site_client(sites[0], database_name)
        if not client:
            print(f"❌ {sites[0]} 매장의 {database_name} 연결 실패")
            return schema
    
    try:
        # 1. 테이블 목록 조회
        print("🔍 테이블 목록 조회 중...")
        if database_name == "cu_base":
            # cu_base는 cu_revenue_total만 필요
            tables_query = f"""
            SELECT name AS table_name
            FROM system.tables 
            WHERE database = '{database_name}'
            AND name = 'cu_revenue_total'
            ORDER BY table_name
            """
        else:
            # plusinsight는 모든 테이블
            tables_query = f"""
            SELECT name AS table_name
            FROM system.tables 
            WHERE database = '{database_name}'
            AND name NOT LIKE '.%'
            ORDER BY table_name
            """
        
        tables_result = client.query(tables_query)
        print(f"✅ {len(tables_result.result_rows)}개 테이블 발견")
        
        # 2. 각 테이블의 컬럼명만 조회
        print("🔍 컬럼 정보 조회 중...")
        if database_name == "cu_base":
            # cu_base는 cu_revenue_total만 조회
            columns_query = f"""
            SELECT 
                table,
                name AS column_name,
                type AS data_type
            FROM system.columns 
            WHERE database = '{database_name}'
            AND table = 'cu_revenue_total'
            ORDER BY table, position
            """
        else:
            # plusinsight는 모든 테이블
            columns_query = f"""
            SELECT 
                table,
                name AS column_name,
                type AS data_type
            FROM system.columns 
            WHERE database = '{database_name}'
            AND table NOT LIKE '.%'
            ORDER BY table, position
            """
        
        columns_result = client.query(columns_query)
        print(f"✅ {len(columns_result.result_rows)}개 컬럼 정보 수집")
        
        # 3. 테이블별로 컬럼 그룹화
        for row in tables_result.result_rows:
            table_name = row[0]
            schema["tables"][table_name] = []
        
        # 4. 컬럼 정보 추가 (테이블명 - 컬럼명 구조)
        for row in columns_result.result_rows:
            table_name = row[0]
            column_name = row[1]
            data_type = row[2]
            
            if table_name in schema["tables"]:
                schema["tables"][table_name].append({
                    "name": column_name,
                    "type": data_type
                })
        
        print(f"✅ {database_name} 스키마 추출 완료: {len(schema['tables'])}개 테이블")
        
    except Exception as e:
        print(f"❌ 스키마 추출 오류: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        client.close()
    
    return schema

def save_schema_to_json(schema: Dict[str, Any], output_dir: Path):
    """스키마를 JSON 파일로 저장"""
    database_name = schema["database"]
    output_file = output_dir / f"{database_name}_schema.json"
    
    print(f"💾 {output_file}에 저장 중...")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(schema, f, ensure_ascii=False, indent=2)
    
    print(f"✅ {output_file} 저장 완료 ({len(schema['tables'])}개 테이블)")
    
    # 메타정보 파일 업데이트
    metadata_file = output_dir / "schema_metadata.json"
    
    # 기존 메타데이터 로드 (있다면)
    metadata = {}
    if metadata_file.exists():
        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
    
    # 현재 데이터베이스 메타정보 업데이트
    metadata[database_name] = {
        "extracted_at": schema["extracted_at"],
        "table_count": len(schema["tables"]),
        "file_path": f"{database_name}_schema.json"
    }
    
    # 메타데이터 저장
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

def main():
    """메인 실행 함수"""
    print("🚀 간단한 데이터베이스 스키마 추출 시작")
    
    # 출력 디렉토리 생성
    output_dir = Path(__file__).parent / "chat" / "knowledge" / "schema"
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 출력 디렉토리: {output_dir}")
    
    # 추출할 데이터베이스 목록
    databases = ["plusinsight", "cu_base"]
    
    for database in databases:
        print(f"\n{'='*50}")
        print(f"📊 {database} 데이터베이스 처리 중...")
        print(f"{'='*50}")
        
        try:
            # 스키마 추출
            schema = extract_database_schema(database)
            
            if schema["tables"]:
                # JSON 파일로 저장
                save_schema_to_json(schema, output_dir)
            else:
                print(f"⚠️ {database} 데이터베이스에서 테이블을 찾을 수 없습니다.")
                
        except Exception as e:
            print(f"❌ {database} 처리 중 오류: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"\n🎉 스키마 추출 완료!")
    print(f"📁 결과 파일: {output_dir}")
    
    # 결과 요약
    metadata_file = output_dir / "schema_metadata.json"
    if metadata_file.exists():
        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        print("\n📊 추출 결과 요약:")
        for db_name, info in metadata.items():
            print(f"  - {db_name}: {info['table_count']}개 테이블 ({info['extracted_at'][:10]})")

if __name__ == "__main__":
    main()