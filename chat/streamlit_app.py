"""
멀티 에이전트 시스템을 위한 Streamlit 프론트엔드
Backend API를 호출하여 작동
"""

import streamlit as st
import requests
import json
import os
import uuid
from datetime import datetime

# 페이지 설정
st.set_page_config(
    page_title="🤖 멀티 에이전트 분석 시스템",
    page_icon="🤖",
    layout="wide"
)

# 백엔드 URL 설정
BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")

# 세션 상태 초기화
if "thread_id" not in st.session_state:
    st.session_state.thread_id = str(uuid.uuid4())
if "messages" not in st.session_state:
    st.session_state.messages = []

# 사이드바 설정
with st.sidebar:
    st.title("⚙️ 설정")
    
    # 모델 선택
    model = st.selectbox(
        "🤖 모델 선택",
        ["gpt-5", "gpt-4o"],
        index=0,
        help="분석에 사용할 AI 모델"
    )
    
    # 멀티 에이전트 사용 여부
    use_multi_agent = st.toggle(
        "🚀 멀티 에이전트 시스템 사용",
        value=False,
        help="고급 분석을 위한 멀티 에이전트 시스템 활성화 (기본: GPT-5 + MCP 툴 직접 사용)"
    )
    
    # 고급 설정
    with st.expander("🔧 고급 설정"):
        timeout_seconds = st.slider(
            "⏱️ 응답 생성 시간 제한 (초)",
            min_value=60,
            max_value=900,
            value=900,
            step=30,
            help="AI 응답 생성을 위한 최대 대기 시간"
        )
        
        recursion_limit = st.slider(
            "🔄 재귀 호출 제한 (횟수)",
            min_value=5,
            max_value=30,
            value=30,
            step=5,
            help="AI가 도구를 사용할 수 있는 최대 횟수"
        )
    
    # 세션 정보
    st.divider()
    st.caption(f"📋 세션 ID: {st.session_state.thread_id[:8]}...")
    
    if st.button("🔄 새 세션 시작"):
        st.session_state.thread_id = str(uuid.uuid4())
        st.session_state.messages = []
        st.rerun()

# 메인 페이지
st.title("🤖 멀티 에이전트 분석 시스템")

# 시스템 상태 표시
col1, col2, col3 = st.columns(3)
with col1:
    if use_multi_agent:
        st.success("🚀 멀티 에이전트 모드")
    else:
        st.info("🔧 GPT-5 + MCP 툴 직접 모드")

with col2:
    st.info(f"🤖 {model}")

with col3:
    st.info(f"⏱️ {timeout_seconds}초 제한")

st.divider()

# 채팅 메시지 표시
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "timestamp" in message:
            st.caption(f"🕒 {message['timestamp']}")

# 채팅 입력
if prompt := st.chat_input("분석하고 싶은 내용을 입력해주세요..."):
    # 사용자 메시지 추가
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.messages.append({
        "role": "user", 
        "content": prompt,
        "timestamp": timestamp
    })
    
    # 사용자 메시지 표시
    with st.chat_message("user"):
        st.markdown(prompt)
        st.caption(f"🕒 {timestamp}")
    
    # AI 응답 생성
    with st.chat_message("assistant"):
        with st.spinner("🤖 분석 중..." if use_multi_agent else "🔧 처리 중..."):
            try:
                # 백엔드 API 호출
                response = requests.post(
                    f"{BACKEND_URL}/api/threads/{st.session_state.thread_id}/query",
                    json={
                        "query": prompt,
                        "model": model,
                        "use_multi_agent": use_multi_agent,
                        "timeout_seconds": timeout_seconds,
                        "recursion_limit": recursion_limit
                    },
                    timeout=timeout_seconds + 30  # API 호출 타임아웃을 조금 더 길게
                )
                
                if response.status_code == 200:
                    result = response.json()
                    ai_response = result.get("response", "응답을 받지 못했습니다.")
                    
                    # AI 응답 표시
                    st.markdown(ai_response)
                    
                    # 도구 사용 정보 표시
                    if result.get("tool_info"):
                        with st.expander("🔧 사용된 도구 정보"):
                            st.text(result["tool_info"])
                    
                    # AI 응답 저장
                    response_timestamp = datetime.now().strftime("%H:%M:%S")
                    st.caption(f"🕒 {response_timestamp}")
                    
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": ai_response,
                        "timestamp": response_timestamp,
                        "tool_info": result.get("tool_info")
                    })
                    
                else:
                    error_msg = f"❌ 오류가 발생했습니다. (상태 코드: {response.status_code})"
                    try:
                        error_detail = response.json()
                        if "detail" in error_detail:
                            error_msg += f"\n\n{error_detail['detail']}"
                    except:
                        error_msg += f"\n\n{response.text}"
                    
                    st.error(error_msg)
                    
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": error_msg,
                        "timestamp": datetime.now().strftime("%H:%M:%S")
                    })
                
            except requests.exceptions.Timeout:
                error_msg = f"⏱️ 요청 시간이 초과되었습니다. ({timeout_seconds}초)"
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg,
                    "timestamp": datetime.now().strftime("%H:%M:%S")
                })
                
            except requests.exceptions.ConnectionError:
                error_msg = "🔌 백엔드 서버에 연결할 수 없습니다. 서버가 실행 중인지 확인해주세요."
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg,
                    "timestamp": datetime.now().strftime("%H:%M:%S")
                })
                
            except Exception as e:
                error_msg = f"❌ 예기치 못한 오류가 발생했습니다: {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg,
                    "timestamp": datetime.now().strftime("%H:%M:%S")
                })

# 푸터
st.divider()
st.markdown("""
<div style='text-align: center; color: gray; font-size: 0.8em;'>
    🤖 멀티 에이전트 분석 시스템 | Powered by GPT-5 & LangGraph
</div>
""", unsafe_allow_html=True)