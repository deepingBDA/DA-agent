import axios from 'axios'
import {
  QueryRequest,
  QueryResponse,
  ThreadResponse,
  SettingsResponse,
  ToolConfig,
  Message,
} from '../types'

// API 기본 URL 설정
const API_BASE_URL =
  import.meta.env.VITE_API_URL || 'http://192.168.49.157:8501'

// axios 인스턴스 생성
const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

// API 요청에 대한 인터페이스 정의
interface UpdateSettingsRequest {
  tool_config: ToolConfig
}

// 새로운 스레드 생성
export const createThread = async (): Promise<ThreadResponse> => {
  const response = await api.post<ThreadResponse>('/api/threads')
  return response.data
}

// 스레드 정보 가져오기
export const getThread = async (
  threadId: string,
): Promise<{ messages: Message[]; thread_id: string }> => {
  const response = await api.get(`/api/threads/${threadId}`)
  return response.data
}

// 스레드 삭제
export const deleteThread = async (
  threadId: string,
): Promise<{ success: boolean; message: string }> => {
  const response = await api.delete(`/api/threads/${threadId}`)
  return response.data
}

// 에이전트에 질의하기
export const queryAgent = async (
  threadId: string,
  params: QueryRequest,
): Promise<QueryResponse> => {
  const response = await api.post<QueryResponse>(
    `/api/threads/${threadId}/query`,
    params,
  )
  return response.data
}

// 설정 정보 가져오기
export const getSettings = async (): Promise<SettingsResponse> => {
  try {
    const url = `${API_BASE_URL}/api/settings`
    const response = await axios.get<SettingsResponse>(url)
    return {
      tool_count: response.data.tool_count,
      current_config: response.data.current_config,
      current_model: response.data.current_model,
      models: response.data.available_models || [],
    }
  } catch (error) {
    console.error('설정 조회 중 오류 발생:', error)
    throw error
  }
}

// 설정 업데이트하기
export const updateSettings = async (
  data: UpdateSettingsRequest,
): Promise<{ success: boolean; message: string; tool_count: number }> => {
  try {
    const response = await api.post('/api/settings', data)
    return {
      ...response.data,
      tool_count: response.data.tool_count || 0, // 항상 숫자 타입 반환
    }
  } catch (error) {
    console.error('설정 업데이트 중 오류 발생:', error)
    throw error
  }
}

// 대화 초기화 API
export const resetConversation = async (): Promise<any> => {
  try {
    // 스레드 ID를 얻고 삭제하는 대신 새로운 스레드 생성
    const response = await createThread()
    return response
  } catch (error) {
    console.error('대화 초기화 중 오류 발생:', error)
    throw error
  }
}

// 파일 업로드 함수
export const uploadFile = async (file: File): Promise<string> => {
  console.log('파일 업로드 API 호출 시작', file.name, file.size)
  try {
    // FormData 생성
    const formData = new FormData()
    formData.append('file', file)

    // axios 설정을 multipart/form-data로 변경하는 새 인스턴스 생성
    const uploadApi = axios.create({
      baseURL: API_BASE_URL,
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    })

    console.log(`파일 업로드 요청: ${API_BASE_URL}/api/upload`)
    // 실제 백엔드 API 호출
    const response = await uploadApi.post<{
      file_path: string
      file_name: string
      file_size: number
    }>('/api/upload', formData)
    console.log('파일 업로드 응답:', response.data)

    if (!response.data.file_path) {
      throw new Error('서버에서 파일 경로를 반환하지 않았습니다.')
    }

    return response.data.file_path
  } catch (error: any) {
    console.error('파일 업로드 중 오류 발생:', error)
    // 에러 세부 정보 출력
    if (axios.isAxiosError && axios.isAxiosError(error)) {
      console.error('요청 URL:', error.config?.url)
      console.error('요청 메서드:', error.config?.method)
      console.error('상태 코드:', error.response?.status)
      console.error('응답 데이터:', error.response?.data)
    }
    throw error
  }
}

// 메시지 전송 API 수정
export const sendMessage = async (
  message: string,
  attachmentPath?: string,
): Promise<any> => {
  try {
    // 스레드 ID가 없으면 새로 생성
    let threadId = localStorage.getItem('threadId')
    if (!threadId) {
      const thread = await createThread()
      threadId = thread.thread_id
      localStorage.setItem('threadId', threadId)
    }

    // 쿼리 내용 생성 (첨부 파일 경로 포함)
    let queryContent = message
    if (attachmentPath) {
      queryContent = `${message}\n[첨부 파일: ${attachmentPath}]`
    }

    // /api/chat 대신 /api/threads/{threadId}/query 엔드포인트 사용
    const response = await api.post(`/api/threads/${threadId}/query`, {
      query: queryContent,
      model: 'gpt-4o', // 기본 모델
      timeout_seconds: 120, // 기본 타임아웃
      recursion_limit: 100, // 기본 재귀 제한
    })

    return response.data
  } catch (error) {
    console.error('메시지 전송 중 오류 발생:', error)
    throw error
  }
}
