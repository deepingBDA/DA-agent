"""
Schema Manager
==============

JSON 파일로 저장된 데이터베이스 스키마를 로드하고 관리합니다.
GPT-5 에이전트가 동적으로 데이터베이스 구조를 이해하고 새로운 툴을 제안할 수 있도록 지원합니다.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime


class SchemaManager:
    """데이터베이스 스키마 관리 클래스"""
    
    def __init__(self, schema_dir: Optional[Path] = None):
        """
        Args:
            schema_dir: 스키마 JSON 파일들이 저장된 디렉토리 경로
        """
        if schema_dir is None:
            schema_dir = Path(__file__).parent / "schema"
        
        self.schema_dir = Path(schema_dir)
        self.schemas: Dict[str, Any] = {}
        self.metadata: Dict[str, Any] = {}
        
        # 스키마 로드
        self._load_schemas()
    
    def _load_schemas(self):
        """JSON 파일에서 스키마 로드"""
        if not self.schema_dir.exists():
            print(f"⚠️ 스키마 디렉토리가 존재하지 않습니다: {self.schema_dir}")
            return
        
        # 메타데이터 로드
        metadata_file = self.schema_dir / "schema_metadata.json"
        if metadata_file.exists():
            with open(metadata_file, 'r', encoding='utf-8') as f:
                self.metadata = json.load(f)
        
        # 각 데이터베이스 스키마 로드
        for db_name in self.metadata.keys():
            schema_file = self.schema_dir / f"{db_name}_schema.json"
            if schema_file.exists():
                with open(schema_file, 'r', encoding='utf-8') as f:
                    self.schemas[db_name] = json.load(f)
                print(f"✅ {db_name} 스키마 로드 완료 ({len(self.schemas[db_name]['tables'])}개 테이블)")
            else:
                print(f"⚠️ {db_name} 스키마 파일을 찾을 수 없습니다: {schema_file}")
    
    def get_database_list(self) -> List[str]:
        """사용 가능한 데이터베이스 목록 반환"""
        return list(self.schemas.keys())
    
    def get_table_list(self, database: str) -> List[str]:
        """특정 데이터베이스의 테이블 목록 반환"""
        if database not in self.schemas:
            return []
        return list(self.schemas[database]['tables'].keys())
    
    def get_table_schema(self, database: str, table: str) -> Optional[Dict[str, Any]]:
        """특정 테이블의 상세 스키마 반환"""
        if database not in self.schemas:
            return None
        
        tables = self.schemas[database]['tables']
        return tables.get(table)
    
    def get_column_info(self, database: str, table: str, column: str) -> Optional[Dict[str, Any]]:
        """특정 컬럼의 상세 정보 반환"""
        table_schema = self.get_table_schema(database, table)
        if not table_schema:
            return None
        
        columns = table_schema.get('columns', {})
        return columns.get(column)
    
    def search_tables_by_keyword(self, keyword: str) -> List[Dict[str, Any]]:
        """키워드로 테이블 검색"""
        results = []
        keyword_lower = keyword.lower()
        
        for db_name, schema in self.schemas.items():
            for table_name, table_info in schema['tables'].items():
                # 테이블명이나 설명에서 키워드 검색
                if (keyword_lower in table_name.lower() or 
                    keyword_lower in table_info.get('description', '').lower()):
                    results.append({
                        'database': db_name,
                        'table': table_name,
                        'description': table_info.get('description', ''),
                        'columns': len(table_info.get('columns', {}))
                    })
        
        return results
    
    def search_columns_by_keyword(self, keyword: str) -> List[Dict[str, Any]]:
        """키워드로 컬럼 검색"""
        results = []
        keyword_lower = keyword.lower()
        
        for db_name, schema in self.schemas.items():
            for table_name, table_info in schema['tables'].items():
                columns = table_info.get('columns', {})
                for col_name, col_info in columns.items():
                    # 컬럼명이나 설명에서 키워드 검색
                    if (keyword_lower in col_name.lower() or 
                        keyword_lower in col_info.get('description', '').lower()):
                        results.append({
                            'database': db_name,
                            'table': table_name,
                            'column': col_name,
                            'type': col_info.get('type', ''),
                            'description': col_info.get('description', '')
                        })
        
        return results
    
    def generate_compact_schema_summary(self) -> str:
        """모든 테이블을 간단한 CREATE TABLE 형식으로 요약 생성 (GPT-5 컨텍스트용)"""
        summary_parts = []
        
        summary_parts.append("# 📊 Database Schema (All Tables)\n")
        
        for db_name, schema in self.schemas.items():
            table_count = len(schema['tables'])
            tables = schema['tables']
            
            summary_parts.append(f"## {db_name} Database ({table_count} tables)")
            summary_parts.append("")
            
            # 모든 테이블을 알파벳 순으로 정렬하여 표시
            sorted_tables = sorted(tables.items())
            
            for table_name, table_info in sorted_tables:
                # 컬럼 정보 수집
                columns = table_info.get('columns', {})
                column_defs = []
                
                for col_name, col_info in columns.items():
                    col_type = col_info.get('type', '').split('(')[0]  # 괄호 안 내용 제거
                    column_defs.append(f"{col_name} {col_type}")
                
                # CREATE TABLE 형식으로 한 줄에 표시
                if column_defs:
                    columns_str = ", ".join(column_defs)
                    summary_parts.append(f"CREATE TABLE {table_name} ({columns_str});")
                else:
                    summary_parts.append(f"CREATE TABLE {table_name} ();")
            
            summary_parts.append("\n" + "="*50 + "\n")
        
        return "\n".join(summary_parts)
    
    def generate_detailed_table_info(self, database: str, table: str) -> str:
        """특정 테이블의 상세 정보 생성"""
        table_schema = self.get_table_schema(database, table)
        if not table_schema:
            return f"❌ {database}.{table} 테이블을 찾을 수 없습니다."
        
        info_parts = []
        info_parts.append(f"# 📋 {database}.{table}")
        info_parts.append(f"**Description**: {table_schema.get('description', 'No description')}")
        info_parts.append(f"**Engine**: {table_schema.get('engine', 'Unknown')}")
        info_parts.append(f"**Rows**: {table_schema.get('total_rows', 0):,}")
        info_parts.append("")
        
        # 컬럼 정보
        columns = table_schema.get('columns', {})
        if columns:
            info_parts.append("## Columns")
            for col_name, col_info in columns.items():
                col_type = col_info.get('type', 'Unknown')
                description = col_info.get('description', '')
                nullable = "NULL" if col_info.get('nullable', False) else "NOT NULL"
                primary = " (PK)" if col_info.get('is_primary_key', False) else ""
                
                info_parts.append(f"- **{col_name}** `{col_type}` {nullable}{primary}")
                if description:
                    info_parts.append(f"  - {description}")
        
        return "\n".join(info_parts)
    
    def get_schema_statistics(self) -> Dict[str, Any]:
        """스키마 통계 정보 반환"""
        stats = {
            'databases': len(self.schemas),
            'total_tables': 0,
            'total_columns': 0,
            'by_database': {}
        }
        
        for db_name, schema in self.schemas.items():
            tables = schema['tables']
            table_count = len(tables)
            column_count = sum(len(table.get('columns', {})) for table in tables.values())
            
            stats['total_tables'] += table_count
            stats['total_columns'] += column_count
            
            stats['by_database'][db_name] = {
                'tables': table_count,
                'columns': column_count,
                'extracted_at': schema.get('extracted_at', 'Unknown')
            }
        
        return stats
    
    def reload_schemas(self):
        """스키마를 다시 로드"""
        self.schemas.clear()
        self.metadata.clear()
        self._load_schemas()
        print("🔄 스키마가 다시 로드되었습니다.")


# 전역 스키마 매니저 인스턴스
_schema_manager = None

def get_schema_manager() -> SchemaManager:
    """전역 스키마 매니저 인스턴스 반환"""
    global _schema_manager
    if _schema_manager is None:
        _schema_manager = SchemaManager()
    return _schema_manager


# 편의 함수들
def get_compact_schema_summary() -> str:
    """컴팩트한 스키마 요약 반환"""
    return get_schema_manager().generate_compact_schema_summary()

def search_tables(keyword: str) -> List[Dict[str, Any]]:
    """테이블 검색"""
    return get_schema_manager().search_tables_by_keyword(keyword)

def search_columns(keyword: str) -> List[Dict[str, Any]]:
    """컬럼 검색"""
    return get_schema_manager().search_columns_by_keyword(keyword)

def get_table_details(database: str, table: str) -> str:
    """테이블 상세 정보"""
    return get_schema_manager().generate_detailed_table_info(database, table)


if __name__ == "__main__":
    # 테스트
    manager = SchemaManager()
    
    print("📊 Schema Statistics:")
    stats = manager.get_schema_statistics()
    print(f"- Databases: {stats['databases']}")
    print(f"- Total Tables: {stats['total_tables']}")
    print(f"- Total Columns: {stats['total_columns']}")
    
    print("\n" + "="*50)
    print("📋 Compact Schema Summary:")
    print("="*50)
    summary = manager.generate_compact_schema_summary()
    print(summary[:2000] + "..." if len(summary) > 2000 else summary)