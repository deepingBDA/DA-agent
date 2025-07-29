#!/usr/bin/env python3
"""
Visitor Diagnose Workflow 테스트 스크립트

Usage:
    python test_visitor_workflow.py
    python test_visitor_workflow.py --store 더미데이터점1 --start 2025-01-01 --end 2025-01-07
    python test_visitor_workflow.py --multi  # 모든 더미매장 비교
"""

import sys
import os
import datetime
import argparse

# 현재 디렉터리를 Python path에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_visitor_workflow():
    """방문객 진단 워크플로우 테스트"""
    
    parser = argparse.ArgumentParser(description="Test Visitor Diagnose Workflow")
    parser.add_argument("--store", default="더미데이터점", help="Store name for test")
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--multi", action="store_true", help="Test with all dummy stores for comparison")
    args = parser.parse_args()
    
    # 로컬 로그 디렉터리 생성
    os.makedirs("logs", exist_ok=True)
    os.makedirs("report", exist_ok=True)
    
    # 기간 기본값 계산(지난주)
    if not args.start or not args.end:
        today = datetime.date.today()
        this_mon = today - datetime.timedelta(days=today.weekday())
        start = this_mon - datetime.timedelta(days=7)
        end = start + datetime.timedelta(days=6)
        start_date = start.isoformat()
        end_date = end.isoformat()
    else:
        start_date = args.start
        end_date = args.end
    
    # 매장 선택
    if args.multi:
        store_names = ["더미데이터점", "더미데이터점1", "더미데이터점2", "더미데이터점3", "망우혜원점"]
        test_name = "Multi-Store Comparison"
    else:
        store_names = args.store
        test_name = f"Single Store ({args.store})"
    
    print(f"=== Visitor Diagnose Workflow Test ===")
    print(f"Test Type: {test_name}")
    print(f"Store(s): {store_names}")
    print(f"Period: {start_date} ~ {end_date}")
    print()
    
    try:
        # 워크플로우 직접 import (패키지 초기화 피하기)
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'mcp_tools'))
        from visitor_diagnose_workflow import VisitorDiagnoseWorkflow
        
        print("Creating workflow instance...")
        wf = VisitorDiagnoseWorkflow()
        
        print("Running workflow...")
        result = wf.run(
            user_prompt="Test run",
            store_name=store_names,  # 리스트 또는 문자열 모두 지원
            start_date=start_date,
            end_date=end_date,
        )
        
        print(f"✅ Workflow completed successfully!")
        print(f"Result: {result}")
        
        # 생성된 파일 확인
        excel_path = "report/점포진단표.xlsx"
        if os.path.exists(excel_path):
            file_size = os.path.getsize(excel_path)
            print(f"📊 Excel file created: {excel_path} ({file_size} bytes)")
            
            if args.multi:
                print("🔍 Multi-store comparison enabled - check for highlight colors!")
        else:
            print("⚠️  Excel file not found")
            
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure all dependencies are installed and modules are accessible")
        return False
        
    except Exception as e:
        print(f"❌ Workflow error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = test_visitor_workflow()
    sys.exit(0 if success else 1) 