from fastmcp import FastMCP
import clickhouse_connect
import os
from dotenv import load_dotenv
import tiktoken
import pandas as pd
import io
from datetime import datetime

from utils import create_transition_data
from map_config import item2zone

load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

model_name = "gpt-o4"
model_max_tokens = {
    "gpt-o4": 128000,
}

def num_tokens_from_string(string: str, model: str) -> int:
    encoding = tiktoken.encoding_for_model(model)
    num_tokens = len(encoding.encode(string))
    return num_tokens

def is_token_limit_exceeded(text: str, model: str, reserved_tokens: int = 1000) -> bool:
    token_count = num_tokens_from_string(text, model)
    max_tokens = model_max_tokens.get(model, 4096)  # 기본값 4096
    return token_count > (max_tokens - reserved_tokens)

mcp = FastMCP("report")

@mcp.tool()
def read_file(file_path: str, start_line: int = 0, end_line: int = 100) -> str:
    try:
        if file_path.endswith(('.xlsx', '.xls')):
            return f"이 파일은 Excel 형식입니다. read_excel 함수를 사용하세요."
        
        with open(file_path, "r") as f:
            lines = f.readlines()
            if is_token_limit_exceeded("".join(lines), model_name):
                left = start_line
                right = end_line
                while left < right:
                    mid = (left + right) // 2
                    if is_token_limit_exceeded("".join(lines[start_line:mid]), model_name):
                        right = mid
                    else:
                        left = mid + 1
                return f"전체 라인 {len(lines)} 중, 라인 {start_line} ~ {right}:\n" + "".join(lines[start_line:right])
            else:
                return f"전체 라인 {len(lines)}:\n" + "".join(lines)
    except Exception as e:
        return f"오류가 발생했습니다: {e}"

@mcp.tool()
def read_excel(file_path: str, sheet_name: str = None, max_rows: int = 1000) -> str:
    try:
        if not file_path.endswith(('.xlsx', '.xls')):
            return "이 파일은 Excel 형식이 아닙니다. read_file 함수를 사용하세요."
        
        # 시트 정보 가져오기
        xl = pd.ExcelFile(file_path)
        sheet_names = xl.sheet_names
        if not sheet_names:
            return "Excel 파일에 시트가 없습니다."
        
        # 시트 선택
        selected_sheet = sheet_name if sheet_name in sheet_names else sheet_names[0]
        
        # 시트 정보 메시지 구성
        if len(sheet_names) > 1:
            sheet_info = f"시트 목록: {', '.join(sheet_names)}\n\n선택된 시트({selected_sheet}) 내용:\n"
        else:
            sheet_info = f"시트: {selected_sheet}\n\n"
        
        # 이진 탐색을 통해 토큰 제한을 초과하지 않는 최대 행 수 찾기
        df = pd.read_excel(file_path, sheet_name=selected_sheet)
        total_rows = len(df)
        
        left = 1  # 최소 1행은 보여줘야 함
        right = min(total_rows, max_rows)
        
        # 전체 데이터가 토큰 제한 내에 있는지 먼저 확인
        temp_result = sheet_info + f"(전체 {total_rows}행)\n" + df.head(right).to_string()
        if not is_token_limit_exceeded(temp_result, model_name):
            return temp_result
        
        # 이진 탐색으로 표시할 최대 행 수 찾기
        best_rows = 1
        while left <= right:
            mid = (left + right) // 2
            current_df = df.head(mid)
            current_result = sheet_info + f"(전체 {total_rows}행 중 처음 {mid}행 표시)\n" + current_df.to_string()
            
            if is_token_limit_exceeded(current_result, model_name):
                right = mid - 1
            else:
                best_rows = mid
                left = mid + 1
        
        final_df = df.head(best_rows)
        result = sheet_info + f"(전체 {total_rows}행 중 처음 {best_rows}행 표시)\n" + final_df.to_string()
        return result
        
    except Exception as e:
        return f"Excel 파일 읽기 오류: {e}"

@mcp.tool()
def save_file(file_path: str, content: str):
    try:
        with open(file_path, "w") as f:
            f.write(content)
        return f"파일 {file_path}에 저장되었습니다."
    except Exception as e:
        return f"오류가 발생했습니다: {e}"

@mcp.tool()
def edit_file(file_path: str, content: str, start_line: int = 0, end_line: int = 100):
    try:
        with open(file_path, "r") as f:
            lines = f.readlines()
        
        # 내용을 수정하고 개행 문자 처리
        new_lines = [line + '\n' if not line.endswith('\n') else line for line in content.split('\n')]
        lines[start_line:end_line] = new_lines
        
        # 수정된 내용을 파일에 다시 저장
        with open(file_path, "w") as f:
            f.writelines(lines)
            
        return f"파일 {file_path}에 수정되었습니다."
    except Exception as e:
        return f"오류가 발생했습니다: {e}"
        
@mcp.tool()
def append_file(file_path: str, content: str):
    try:
        with open(file_path, "a") as f:
            f.write(content)
            return f"파일 {file_path}에 추가되었습니다."
    except Exception as e:
        return f"오류가 발생했습니다: {e}"

@mcp.tool()
def get_current_time() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@mcp.tool()
def sum(a: int, b: int) -> int:
    return a + b

@mcp.tool()
def subtract(a: int, b: int) -> int:
    return a - b

@mcp.tool()
def multiply(a: int, b: int) -> int:
    return a * b

@mcp.tool()
def divide(a: int, b: int) -> float:
    return a / b


if __name__ == "__main__":
    mcp.run()