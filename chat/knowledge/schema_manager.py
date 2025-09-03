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
    
    def generate_business_context_schema(self) -> str:
        """새로운 배열 형식의 스키마로 CREATE TABLE 형태 생성 (GPT-5 컨텍스트용)"""
        summary_parts = []
        summary_parts.append("# 📊 Database Schema (All Tables)\n")
        
        for db_name, schema in self.schemas.items():
            table_count = len(schema['tables'])
            tables = schema['tables']
            
            summary_parts.append(f'## {db_name} Database ({table_count} tables)\n')
            
            # 모든 테이블을 알파벳 순으로 정렬하여 CREATE TABLE 형태로 표시
            sorted_tables = sorted(tables.items())
            
            for table_name, columns_list in sorted_tables:
                # 컬럼 정보 수집 (새로운 배열 형식)
                column_defs = []
                for col_info in columns_list:
                    col_name = col_info.get('name', '')
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
    
    def _generate_old_business_context_schema(self) -> str:
        """이전 버전의 하드코딩된 비즈니스 컨텍스트 (백업용)"""
        
        # 테이블별 비즈니스 설명과 컬럼 설명 매핑 (이전 버전)
        table_descriptions = {
            'plusinsight': {
                'line_in_out_individual': {
                    'description': '방문객 입출입 개별 기록',
                    'business_meaning': '매장 방문 패턴과 체류 시간 분석의 기초 데이터',
                    'columns': {
                        'person_seq': '개별 방문객 고유 ID',
                        'date': '방문 날짜', 
                        'timestamp': '정확한 입출입 시각',
                        'in_out': '입장(IN) 또는 퇴장(OUT)',
                        'is_staff': '직원 여부 (0: 고객, 1: 직원)',
                        'triggered_line_id': '감지된 라인 ID',
                        'id': '레코드 고유 ID',
                        'age': '추정 연령',
                        'gender': '성별 (0: 남성, 1: 여성)',
                        'group_id': '그룹 ID'
                    }
                },
                'customer_behavior_event': {
                    'description': '고객의 매장 내 행동 이벤트',
                    'business_meaning': '고객의 실제 관심도와 구매 의도를 파악하는 핵심 데이터',
                    'columns': {
                        'person_seq': '방문객 ID',
                        'event_type': '행동 유형 (1: 픽업, 2: 시선, 3: 체류)',
                        'timestamp': '행동 발생 시각',
                        'customer_behavior_area_id': '행동 발생 구역 ID',
                        'date': '이벤트 발생 날짜',
                        'age': '추정 연령',
                        'gender': '성별 (0: 남성, 1: 여성)',
                        'is_staff': '직원 여부 (0: 고객, 1: 직원)'
                    }
                },
                'zone': {
                    'description': '매장 내 구역 정보',
                    'business_meaning': '매장 레이아웃과 동선 최적화의 기준',
                    'columns': {
                        'id': '구역 고유 ID',
                        'name': '구역명 (예: 음료, 과자, 시식대)',
                        'coords': '구역의 물리적 좌표',
                        'uid': '유니크 ID',
                        'order': '구역 순서',
                        'start_date': '구역 설정 시작일',
                        'end_date': '구역 설정 종료일',
                        'keywords': '구역 키워드',
                        'color': '구역 표시 색상'
                    }
                },
                'sales_funnel': {
                    'description': '방문-노출-픽업 퍼널 분석',
                    'business_meaning': '상품의 매력도와 진열 효과성을 측정',
                    'columns': {
                        'shelf_name': '진열대명',
                        'date': '분석 날짜',
                        'visit': '방문 수',
                        'gaze1': '노출 수 (시선 집중)',
                        'pickup': '픽업 수 (실제 집어든 행동)',
                        'site': '매장 사이트',
                        'hour': '시간대',
                        'area_id': '구역 ID',
                        'shelf_id': '진열대 ID',
                        'gender': '성별',
                        'age_group': '연령 그룹',
                        'conversion_rate': '전환율'
                    }
                },
                'two_step_flow': {
                    'description': '고객 동선 패턴 (3단계 이동 경로)',
                    'business_meaning': '고객 세그먼트별 매장 내 이동 패턴과 선호도 분석',
                    'columns': {
                        'gender': '성별 (0: 남성, 1: 여성)',
                        'age_group': '연령대',
                        'zone1_id': '첫 번째 방문 구역',
                        'zone2_id': '두 번째 방문 구역',
                        'zone3_id': '세 번째 방문 구역',
                        'num_people': '해당 패턴을 보인 고객 수',
                        'date': '분석 날짜'
                    }
                },
                'detected_time': {
                    'description': 'AI가 감지한 고객 속성 정보',
                    'business_meaning': '인구통계학적 세분화 분석의 기초',
                    'columns': {
                        'person_seq': '고객 ID',
                        'age': '추정 연령',
                        'gender': '성별 (0: 남성, 1: 여성)',
                        'id': '레코드 고유 ID',
                        'date': '감지 날짜',
                        'timestamp': '감지 시각',
                        'person_first_detected': '최초 감지 시각',
                        'person_last_detected': '마지막 감지 시각',
                        'group_id': '그룹 ID',
                        'is_staff': '직원 여부'
                    }
                }
            },
            'cu_base': {
                'cu_revenue_total': {
                    'description': '편의점 매출 상세 데이터',
                    'business_meaning': '실제 매출과 고객 행동 데이터 간의 연관성 분석',
                    'columns': {
                        'store_nm': '매장명',
                        'tran_ymd': '거래 날짜',
                        'pos_no': 'POS 번호',
                        'tran_no': '거래 번호',
                        'small_nm': '소분류 상품명',
                        'sale_amt': '판매 금액',
                        'sale_qty': '판매 수량',
                        'store_cd': '매장 코드',
                        'item_cd': '상품 코드',
                        'large_nm': '대분류명',
                        'mid_nm': '중분류명',
                        'small_cd': '소분류 코드',
                        'unit_price': '단가',
                        'discount_amt': '할인 금액'
                    }
                }
            }
        }
        
        summary_parts = []
        summary_parts.append("# 📊 Database Schema with Business Context\n")
        
        for db_name, schema in self.schemas.items():
            table_count = len(schema['tables'])
            tables = schema['tables']
            
            summary_parts.append(f'## {db_name} Database ({table_count} tables)\n')
            
            # 알파벳 순으로 정렬된 테이블들
            sorted_tables = sorted(tables.items())
            
            # 상세 설명이 있는 핵심 테이블들과 기본 테이블들 분리
            detailed_tables = []
            basic_tables = []
            
            for table_name, table_info in sorted_tables:
                table_context = table_descriptions.get(db_name, {}).get(table_name, {})
                if table_context:  # 상세 설명이 있는 테이블
                    detailed_tables.append((table_name, table_info, table_context))
                else:  # 기본 테이블
                    basic_tables.append((table_name, table_info))
            
            # 핵심 테이블들 상세 표시
            if detailed_tables:
                summary_parts.append("### 📋 Core Business Tables")
                for table_name, table_info, table_context in detailed_tables:
                    description = table_context.get('description', f'{table_name} 테이블')
                    business_meaning = table_context.get('business_meaning', '')
                    column_descriptions = table_context.get('columns', {})
                    
                    summary_parts.append(f'**{table_name}**: {description}')
                    if business_meaning:
                        summary_parts.append(f'  - *비즈니스 의미*: {business_meaning}')
                    
                    # 실제 테이블의 모든 컬럼 표시 (간단히)
                    columns = table_info.get('columns', {})
                    detailed_cols = []
                    for col_name, col_info in columns.items():
                        col_type = col_info.get('type', '').split('(')[0]
                        col_desc = column_descriptions.get(col_name, '')
                        if col_desc:
                            detailed_cols.append(f"{col_name}({col_type}): {col_desc}")
                        else:
                            detailed_cols.append(f"{col_name}({col_type})")
                    
                    summary_parts.append(f'  - *컬럼*: {", ".join(detailed_cols[:6])}' + 
                                        (f', ... ({len(detailed_cols)-6} more)' if len(detailed_cols) > 6 else ''))
                    summary_parts.append('')
            
            # 기본 테이블들 간단 표시
            if basic_tables:
                summary_parts.append("### 📊 Other Tables")
                for table_name, table_info in basic_tables:
                    columns = table_info.get('columns', {})
                    col_list = []
                    for col_name, col_info in list(columns.items())[:4]:  # 처음 4개만
                        col_type = col_info.get('type', '').split('(')[0]
                        col_list.append(f"{col_name}({col_type})")
                    
                    col_summary = ", ".join(col_list)
                    if len(columns) > 4:
                        col_summary += f", ... ({len(columns)-4} more)"
                    
                    summary_parts.append(f'- **{table_name}**: {col_summary}')
                
                summary_parts.append('')
            
            summary_parts.append("="*50 + "\n")
        
        return "\n".join(summary_parts)
    
    def generate_compact_schema_summary(self) -> str:
        """비즈니스 컨텍스트 포함된 상세 스키마 반환"""
        return self.generate_business_context_schema()
    
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