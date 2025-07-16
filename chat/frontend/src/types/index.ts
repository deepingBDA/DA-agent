// 메시지 타입
export interface Message {
  role: 'user' | 'assistant' | 'assistant_tool'
  content: string
  timestamp?: string
}

// 대화 스레드 타입
export interface Thread {
  id: string
  messages: Message[]
}

// 질의 요청 타입
export interface QueryRequest {
  query: string
  model?: string
  timeout_seconds?: number
  recursion_limit?: number
}

// 질의 응답 타입
export interface QueryResponse {
  response: string
  tool_calls?: string
  tool_info?: string
}

// 스레드 응답 타입
export interface ThreadResponse {
  thread_id: string
  created_at: string
}

// 설정 응답 타입
export interface SettingsResponse {
  tool_count: number
  current_config: ToolConfig
  current_model?: string
  models?: string[]
  available_models?: string[]
}

// 도구 설정 타입
export interface ToolConfig {
  [toolName: string]: {
    command?: string
    args?: string[]
    transport?: string
    url?: string
    [key: string]: any
  }
}

// 앱 설정 타입
export interface AppSettings {
  model: string
  timeout_seconds: number
  recursion_limit: number
}

// 컴포넌트 Props 타입 정의
export interface SystemInfo {
  toolCount: number
  currentModel: string
}

export interface SettingsState {
  models: string[]
  selectedModel: string
  timeoutSeconds: number
  recursionLimit: number
}
