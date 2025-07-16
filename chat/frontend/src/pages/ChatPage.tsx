import React, { useState, useEffect, useRef } from 'react'
import DeleteIcon from '@mui/icons-material/Delete'
import RefreshIcon from '@mui/icons-material/Refresh'
import SendIcon from '@mui/icons-material/Send'
import {
  Box,
  TextField,
  Button,
  CircularProgress,
  Paper,
  Typography,
  IconButton,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Container,
  Divider,
} from '@mui/material'
import { AxiosError } from 'axios'
import {
  createThread,
  getThread,
  deleteThread,
  queryAgent,
  getSettings,
} from '../api'
import ChatMessage from '../components/ChatMessage'
import { Message, AppSettings } from '../types'

const DEFAULT_SETTINGS: AppSettings = {
  model: 'gpt-4o',
  timeout_seconds: 120,
  recursion_limit: 100,
}

const ChatPage: React.FC = () => {
  const [threadId, setThreadId] = useState<string>('')
  const [messages, setMessages] = useState<Message[]>([])
  const [query, setQuery] = useState<string>('')
  const [isLoading, setIsLoading] = useState<boolean>(false)
  const [error, setError] = useState<string | null>(null)
  const [toolInfo, setToolInfo] = useState<string | null>(null)
  const [settings, setSettings] = useState<AppSettings>(DEFAULT_SETTINGS)
  const [availableModels, setAvailableModels] = useState<string[]>([
    'gpt-4o',
    'o4-mini',
    'o3',
  ])

  const messagesEndRef = useRef<HTMLDivElement>(null)

  // 페이지 로드 시 스레드 생성 및 설정 로드
  useEffect(() => {
    const initChat = async () => {
      try {
        // 설정 가져오기
        const settingsData = await getSettings()
        if (settingsData.models && settingsData.models.length > 0) {
          setAvailableModels(settingsData.models)
        }

        // 기존 스레드 ID가 있는지 로컬 스토리지 확인
        const savedThreadId = localStorage.getItem('threadId')

        if (savedThreadId) {
          try {
            // 기존 스레드의 메시지 기록 가져오기
            const threadData = await getThread(savedThreadId)
            setThreadId(savedThreadId)
            setMessages(threadData.messages)
          } catch (e) {
            console.error(
              '기존 스레드를 불러오는데 실패했습니다. 새 스레드를 생성합니다.',
              e,
            )
            // 기존 스레드 불러오기 실패 시 새 스레드 생성
            const newThread = await createThread()
            setThreadId(newThread.thread_id)
            localStorage.setItem('threadId', newThread.thread_id)
          }
        } else {
          // 스레드 ID가 없으면 새로 생성
          const newThread = await createThread()
          setThreadId(newThread.thread_id)
          localStorage.setItem('threadId', newThread.thread_id)
        }
      } catch (e) {
        console.error('초기화 중 오류 발생:', e)
        setError('초기화 중 오류가 발생했습니다. 페이지를 새로고침 해주세요.')
      }
    }

    // eslint-disable-next-line no-void
    void initChat()
  }, [])

  // 메시지 목록이 변경될 때마다 스크롤 아래로 이동
  useEffect(() => {
    if (messagesEndRef.current) {
      messagesEndRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [messages])

  // 메시지 전송 처리
  const handleSendMessage = async () => {
    if (!query.trim() || isLoading) return

    const userMessage: Message = {
      role: 'user',
      content: query,
    }

    setMessages((prev) => [...prev, userMessage])
    setQuery('')
    setIsLoading(true)
    setError(null)
    setToolInfo(null)

    try {
      const response = await queryAgent(threadId, {
        query: userMessage.content,
        model: settings.model,
        timeout_seconds: settings.timeout_seconds,
        recursion_limit: settings.recursion_limit,
      })

      const assistantMessage: Message = {
        role: 'assistant',
        content: response.response,
      }

      setMessages((prev) => [...prev, assistantMessage])

      if (response.tool_calls) {
        setToolInfo(response.tool_calls)
      }
    } catch (e) {
      console.error('메시지 전송 중 오류 발생:', e)

      if (e instanceof AxiosError) {
        setError(
          e.response?.data?.detail || '메시지 전송 중 오류가 발생했습니다.',
        )
      }
    } finally {
      setIsLoading(false)
    }
  }

  // 대화 내역 초기화
  const handleResetChat = async () => {
    // eslint-disable-next-line no-alert
    if (window.confirm('정말 대화 내역을 초기화하시겠습니까?')) {
      setIsLoading(true)
      try {
        await deleteThread(threadId)
        const newThread = await createThread()
        setThreadId(newThread.thread_id)
        setMessages([])
        localStorage.setItem('threadId', newThread.thread_id)
      } catch (e) {
        console.error('대화 초기화 중 오류 발생:', e)
        setError('대화 초기화 중 오류가 발생했습니다.')
      } finally {
        setIsLoading(false)
      }
    }
  }

  // 설정 변경 처리
  const handleSettingChange = (key: keyof AppSettings, value: any) => {
    setSettings((prev) => ({
      ...prev,
      [key]: value,
    }))
  }

  return (
    <Container
      maxWidth="lg"
      sx={{
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        pt: 2,
        pb: 2,
      }}
    >
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
        <Typography variant="h5">MCP 도구 활용 에이전트와 대화</Typography>
        <IconButton
          color="error"
          onClick={handleResetChat}
          disabled={isLoading || messages.length === 0}
          title="대화 초기화"
        >
          <DeleteIcon />
        </IconButton>
      </Box>

      {/* 설정 패널 */}
      <Paper elevation={1} sx={{ p: 2, mb: 2 }}>
        <Typography variant="subtitle1" gutterBottom>
          대화 설정
        </Typography>
        <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
          <FormControl size="small" sx={{ minWidth: 150 }}>
            <InputLabel id="model-select-label">모델</InputLabel>
            <Select
              labelId="model-select-label"
              id="model-select"
              value={settings.model}
              label="모델"
              onChange={(e) => handleSettingChange('model', e.target.value)}
            >
              {availableModels.map((model) => (
                <MenuItem key={model} value={model}>
                  {model}
                </MenuItem>
              ))}
            </Select>
          </FormControl>

          <FormControl size="small" sx={{ width: 150 }}>
            <InputLabel id="timeout-select-label">제한 시간(초)</InputLabel>
            <Select
              labelId="timeout-select-label"
              id="timeout-select"
              value={settings.timeout_seconds}
              label="제한 시간(초)"
              onChange={(e) =>
                handleSettingChange('timeout_seconds', e.target.value)
              }
            >
              <MenuItem value={60}>60초</MenuItem>
              <MenuItem value={120}>120초</MenuItem>
              <MenuItem value={180}>180초</MenuItem>
              <MenuItem value={240}>240초</MenuItem>
              <MenuItem value={300}>300초</MenuItem>
            </Select>
          </FormControl>

          <FormControl size="small" sx={{ width: 150 }}>
            <InputLabel id="recursion-select-label">재귀 제한</InputLabel>
            <Select
              labelId="recursion-select-label"
              id="recursion-select"
              value={settings.recursion_limit}
              label="재귀 제한"
              onChange={(e) =>
                handleSettingChange('recursion_limit', e.target.value)
              }
            >
              <MenuItem value={50}>50</MenuItem>
              <MenuItem value={100}>100</MenuItem>
              <MenuItem value={150}>150</MenuItem>
              <MenuItem value={200}>200</MenuItem>
            </Select>
          </FormControl>
        </Box>
      </Paper>

      <Divider />

      {/* 메시지 목록 */}
      <Paper
        elevation={0}
        sx={{
          flexGrow: 1,
          overflow: 'auto',
          p: 2,
          mb: 2,
          backgroundColor: 'background.default',
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        {messages.length === 0 ? (
          <Box
            sx={{
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'center',
              alignItems: 'center',
              height: '100%',
              color: 'text.secondary',
            }}
          >
            <Typography variant="h6">대화를 시작해보세요</Typography>
            <Typography variant="body2">질문이나 요청을 입력하세요</Typography>
          </Box>
        ) : (
          messages.map((message, index) => (
            <ChatMessage
              // eslint-disable-next-line react/no-array-index-key
              key={index}
              role={message.role as 'user' | 'assistant'}
              content={message.content}
              toolInfo={
                index === messages.length - 1 && message.role === 'assistant'
                  ? toolInfo || undefined
                  : undefined
              }
            />
          ))
        )}
        {error && (
          <Box
            sx={{
              backgroundColor: 'error.dark',
              color: 'white',
              p: 2,
              borderRadius: 1,
              mt: 1,
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
            }}
          >
            <Typography variant="body2">{error}</Typography>
            <IconButton
              size="small"
              color="inherit"
              onClick={() => setError(null)}
            >
              <RefreshIcon fontSize="small" />
            </IconButton>
          </Box>
        )}
        <div ref={messagesEndRef} />
      </Paper>

      {/* 메시지 입력 영역 */}
      <Paper elevation={3} sx={{ p: 2 }}>
        <Box
          component="form"
          sx={{ display: 'flex', gap: 1 }}
          onSubmit={async (e) => {
            e.preventDefault()
            await handleSendMessage()
          }}
        >
          <TextField
            fullWidth
            placeholder="질문이나 요청을 입력하세요..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            disabled={isLoading}
            variant="outlined"
            size="medium"
            autoFocus
          />
          <Button
            variant="contained"
            color="primary"
            endIcon={
              isLoading ? (
                <CircularProgress size={20} color="inherit" />
              ) : (
                <SendIcon />
              )
            }
            onClick={handleSendMessage}
            disabled={isLoading || !query.trim()}
            type="submit"
          >
            전송
          </Button>
        </Box>
      </Paper>
    </Container>
  )
}

export default ChatPage
