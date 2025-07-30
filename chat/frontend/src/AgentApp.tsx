import React, { useState, useEffect, useRef } from 'react'
import AttachFileIcon from '@mui/icons-material/AttachFile'
import CloseIcon from '@mui/icons-material/Close'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown'
import KeyboardArrowUpIcon from '@mui/icons-material/KeyboardArrowUp'
import MenuIcon from '@mui/icons-material/Menu'
import SendIcon from '@mui/icons-material/Send'
import SettingsIcon from '@mui/icons-material/Settings'
import {
  Box,
  Typography,
  TextField,
  Button,
  Paper,
  Container,
  Grid,
  Card,
  CardContent,
  Divider,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Slider,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  LinearProgress,
  IconButton,
  Collapse,
} from '@mui/material'
import ReactMarkdown from 'react-markdown'
import rehypeKatex from 'rehype-katex'
import remarkMath from 'remark-math'
import 'katex/dist/katex.min.css'
import {
  getSettings,
  updateSettings,
  resetConversation,
  sendMessage,
  uploadFile,
} from './api'
import { Message as MessageType, ToolConfig } from './types'

// 확장된 메시지 타입 정의
interface ExtendedMessageType extends MessageType {
  toolReferenceId?: number // 도구 참조 번호
  referencedToolId?: number | null // 참조된 도구 번호 - null 타입 허용
  attachmentPath?: string // 첨부 파일 경로
}

const AgentApp: React.FC = () => {
  const [messages, setMessages] = useState<ExtendedMessageType[]>([])
  const [userInput, setUserInput] = useState('')
  const [isProcessing, setIsProcessing] = useState(false)
  const [showSidebar, setShowSidebar] = useState(true)
  const [config, setConfig] = useState<ToolConfig>({
    get_current_time: {
      command: 'python',
      args: ['./mcp_server_time.py'],
      transport: 'stdio',
    },
  })
  const [newToolJson, setNewToolJson] = useState('')
  const [systemInfo, setSystemInfo] = useState({
    toolCount: 0,
    currentModel: 'gpt-4o',
  })
  const [settings, setSettings] = useState({
    models: ['gpt-4o', 'o3'],
    selectedModel: 'gpt-4o',
    timeoutSeconds: 300,
    recursionLimit: 100,
  })

  // 도구 사용 내역 확장/축소 상태를 관리하는 상태
  const [expandedTools, setExpandedTools] = useState<{
    [key: number]: boolean
  }>({})

  // 현재 도구 참조 번호를 관리하는 상태
  const [currentToolReferenceId, setCurrentToolReferenceId] = useState(1)

  // 첨부 파일 관련 상태
  const [attachment, setAttachment] = useState<File | null>(null)
  const [attachmentPath, setAttachmentPath] = useState<string>('')
  const fileInputRef = useRef<HTMLInputElement>(null)

  // 도구 사용 내역 토글 함수
  const handleToolToggle = (index: number) => {
    setExpandedTools((prev) => ({
      ...prev,
      [index]: !prev[index],
    }))
  }

  // 새 메시지가 추가될 때 자동으로 도구 메시지를 펼쳐두기
  useEffect(() => {
    const toolMessageIndexes = messages
      .map((message, index) => (message.role === 'assistant_tool' ? index : -1))
      .filter((index) => index !== -1)

    if (toolMessageIndexes.length > 0) {
      const newExpandedTools = { ...expandedTools }
      toolMessageIndexes.forEach((index) => {
        newExpandedTools[index] = true
      })
      setExpandedTools(newExpandedTools)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [messages])

  const loadSettings = async () => {
    try {
      setIsProcessing(true)
      // 백엔드 API에서 설정 정보 가져오기
      const settingsData = await getSettings()

      // 현재 서버 설정 적용
      setConfig(settingsData.current_config || {})

      // 시스템 정보 업데이트 - 백엔드에서 받은 정확한 tool_count 사용
      setSystemInfo({
        // 서버에서 제공하는 실제 도구 개수 사용
        toolCount: settingsData.tool_count || 0,
        currentModel: settingsData.current_model || settings.selectedModel,
      })

      if (settingsData.models && settingsData.models.length > 0) {
        setSettings((prev) => ({
          ...prev,
          models: settingsData.models as string[],
        }))
      }
    } catch (error) {
      console.error('설정 로드 중 오류 발생:', error)
    } finally {
      setIsProcessing(false)
    }
  }

  // 초기 설정 로드
  useEffect(() => {
    const initializeApp = async () => {
      await loadSettings()

      // localStorage에서 스레드 ID 확인
      const savedThreadId = localStorage.getItem('threadId')
      if (savedThreadId) {
        try {
          // 새로운 메시지를 가져오지 않고 빈 배열로 시작
          setMessages([])
        } catch (error) {
          console.error('기존 스레드 로드 중 오류 발생:', error)
          // 오류 발생 시 새 스레드 생성
          const response = await resetConversation()
          if (response && response.thread_id) {
            localStorage.setItem('threadId', response.thread_id)
          }
        }
      } else {
        // 스레드 ID가 없으면 새로 생성
        const response = await resetConversation()
        if (response && response.thread_id) {
          localStorage.setItem('threadId', response.thread_id)
        }
      }
    }

    // eslint-disable-next-line no-void
    void initializeApp()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    // 브라우저 리페인트 타이밍과 일치시켜 깜빡임 방지
    const timeoutId = setTimeout(() => {
      // 실제로는 아무 작업도 하지 않음 - 타이밍만 맞춤
    }, 0)

    return () => clearTimeout(timeoutId)
  }, [showSidebar])

  // 설정 변경 핸들러
  const handleSettingsChange = (name: string, value: any) => {
    setSettings((prev) => ({
      ...prev,
      [name]: value,
    }))
  }

  // 설정 적용 핸들러
  const handleApplySettings = async () => {
    try {
      setIsProcessing(true)

      // API를 통해 설정 업데이트
      const response = await updateSettings({
        tool_config: config,
      })

      // 응답에서 직접 tool_count를 받아온 경우 사용
      if (response && typeof response.tool_count === 'number') {
        setSystemInfo((prev) => ({
          ...prev,
          currentModel: settings.selectedModel,
          toolCount: response.tool_count,
        }))

        // 설정이 즉시 적용된 경우 추가 로드는 불필요
      } else {
        // 응답에 tool_count가 없는 경우 전체 설정 다시 로드
        await loadSettings()
      }

      setIsProcessing(false)
    } catch (error) {
      console.error('설정 적용 중 오류 발생:', error)
      setIsProcessing(false)
    }
  }

  // 대화 초기화 핸들러
  const handleResetConversation = async () => {
    try {
      setIsProcessing(true)
      const response = await resetConversation()
      // 새로운 스레드 ID를 localStorage에 저장
      if (response && response.thread_id) {
        localStorage.setItem('threadId', response.thread_id)
      }
      setMessages([])
      setCurrentToolReferenceId(1) // 도구 참조 ID 초기화
      setIsProcessing(false)
    } catch (error) {
      console.error('대화 초기화 중 오류 발생:', error)
      setIsProcessing(false)
    }
  }

  // 파일 업로드 핸들러
  const handleFileUpload = async (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    if (event.target.files && event.target.files.length > 0) {
      const file = event.target.files[0]
      setAttachment(file)
      setIsProcessing(true) // 업로드 중임을 표시

      try {
        console.log('파일 업로드 시작:', file.name)
        // uploadFile API 함수 호출하여 파일 업로드
        const path = await uploadFile(file)
        console.log('파일 업로드 완료:', path)
        setAttachmentPath(path)
      } catch (error) {
        console.error('파일 업로드 중 오류 발생:', error)
        // 에러 발생 시 상태 초기화
        setAttachment(null)
        if (fileInputRef.current) {
          fileInputRef.current.value = ''
        }
        // 사용자에게 알림 (오류 메시지를 대화에 표시)
        const errorMessage: ExtendedMessageType = {
          role: 'assistant',
          content: '파일 업로드 중 오류가 발생했습니다. 다시 시도해주세요.',
        }
        setMessages((prev) => [...prev, errorMessage])
      } finally {
        setIsProcessing(false) // 업로드 완료 후 상태 업데이트
      }
    }
  }

  // 첨부 파일 제거 핸들러
  const handleRemoveAttachment = () => {
    setAttachment(null)
    setAttachmentPath('')
    if (fileInputRef.current) {
      fileInputRef.current.value = ''
    }
  }

  // 첨부 파일 선택 버튼 클릭 핸들러
  const handleAttachmentButtonClick = () => {
    if (fileInputRef.current) {
      fileInputRef.current.click()
    }
  }

  // 사용자 메시지 전송 핸들러 수정
  const handleSendMessage = async () => {
    // 입력이 없고 첨부파일도 없으면 전송하지 않음
    if (!userInput.trim() && !attachment) return

    // 파일이 첨부되었지만 경로가 없는 경우 (업로드 중이거나 실패한 경우)
    if (attachment && !attachmentPath) {
      console.error('파일이 아직 업로드되지 않았습니다.')
      return
    }

    console.log('전송할 메시지:', userInput)
    if (attachmentPath) {
      console.log('첨부 파일 경로:', attachmentPath)
    }

    const userMessage: ExtendedMessageType = {
      role: 'user',
      content: userInput,
      attachmentPath, // 첨부 파일 경로 추가
    }

    setMessages((prev) => [...prev, userMessage])
    setUserInput('')
    setIsProcessing(true)

    try {
      // API 호출로 응답 받아오기 (첨부 파일 경로 전달)
      const response = await sendMessage(
        userInput, 
        attachmentPath,
        settings.selectedModel,
        settings.timeoutSeconds,
        settings.recursionLimit
      )

      // threadId가 없거나 변경된 경우 localStorage에서 최신 값 확인
      const currentThreadId = localStorage.getItem('threadId')
      if (
        currentThreadId &&
        response.thread_id &&
        currentThreadId !== response.thread_id
      ) {
        // 백엔드에서 새 스레드를 생성한 경우 업데이트
        localStorage.setItem('threadId', response.thread_id)
      }

      // 첨부 파일 상태 초기화
      setAttachment(null)
      setAttachmentPath('')
      if (fileInputRef.current) {
        fileInputRef.current.value = ''
      }

      // 백엔드에서 도구 사용 내역이 전달된 경우 표시
      let toolReferenceId: number | null = null

      if (response && response.tool_info) {
        // 현재 도구 참조 번호 사용
        toolReferenceId = currentToolReferenceId

        const toolMessage: ExtendedMessageType = {
          role: 'assistant_tool',
          content: response.tool_info,
          toolReferenceId,
        }

        // 도구 사용 내역 메시지 추가
        setMessages((prev) => [...prev, toolMessage])

        // 도구 메시지 인덱스 계산 및 자동으로 확장
        const newToolIndex = messages.length

        // 다음 상태 업데이트에서 도구 메시지 확장
        setTimeout(() => {
          setExpandedTools((prevExpanded) => ({
            ...prevExpanded,
            [newToolIndex]: true,
          }))
        }, 100)

        // 다음 도구 참조 번호로 증가
        setCurrentToolReferenceId((prevId) => prevId + 1)
      }

      if (response) {
        // 응답 메시지 추가
        const assistantMessage: ExtendedMessageType = {
          role: 'assistant',
          content: response.response || '응답을 받지 못했습니다.',
          referencedToolId: toolReferenceId, // 도구를 사용한 경우에만 참조 ID 설정
        }

        setMessages((prev) => [...prev, assistantMessage])
      }
    } catch (error) {
      console.error('메시지 처리 중 오류 발생:', error)
      // 오류 메시지 추가
      const errorMessage: ExtendedMessageType = {
        role: 'assistant',
        content: '오류가 발생했습니다. 다시 시도해주세요.',
      }
      setMessages((prev) => [...prev, errorMessage])
    } finally {
      setIsProcessing(false)
    }
  }

  // 도구 추가 핸들러
  const handleAddTool = () => {
    try {
      const parsedTool = JSON.parse(newToolJson)
      setConfig((prev) => ({ ...prev, ...parsedTool }))
      setNewToolJson('')

      // 도구 추가는 API 호출로 실제 반영되기 전까지는 systemInfo를 변경하지 않음
      // Apply Settings 클릭 시 백엔드에 반영되고 loadSettings()를 통해 정확한 개수를 가져옴
    } catch (error) {
      console.error('도구 추가 중 오류 발생:', error)
    }
  }

  // 도구 삭제 핸들러
  const handleDeleteTool = (toolName: string) => {
    const newConfig = { ...config }
    delete newConfig[toolName]
    setConfig(newConfig)

    // 도구 삭제는 API 호출로 실제 반영되기 전까지는 systemInfo를 변경하지 않음
    // Apply Settings 클릭 시 백엔드에 반영되고 loadSettings()를 통해 정확한 개수를 가져옴
  }

  return (
    <Container
      maxWidth={false}
      disableGutters
      sx={{
        m: 0,
        p: 2, // 전체 여백 추가
        height: '100vh',
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden',
        width: '100%',
        background: '#c4bee2', // 연보라색 단색 배경
        position: 'relative',
        boxSizing: 'border-box',
      }}
    >
      <Grid
        container
        spacing={0}
        sx={{
          height: 'calc(100vh - 32px)', // 전체 패딩 고려
          flexGrow: 1,
          width: '100%',
          position: 'relative',
          overflow: 'hidden',
          background: 'transparent',
        }}
      >
        {/* 사이드바 */}
        <Grid
          item
          xs={12}
          md={2.4}
          sx={{
            height: '100%',
            position: 'absolute',
            left: 0,
            top: 0,
            width: showSidebar ? '19%' : '0%',
            opacity: showSidebar ? 1 : 0,
            visibility: showSidebar ? 'visible' : 'hidden',
            transition: 'all 0.3s ease',
            zIndex: 10,
          }}
        >
          <Paper
            sx={{
              p: 2,
              display: 'flex',
              flexDirection: 'column',
              height: '100%',
              overflow: 'auto',
              borderRadius: 3, // 둥근 모서리 추가
              bgcolor: '#f0edf4', // 현재 색상과 흰색의 중간 톤
              boxShadow: '0 6px 20px rgba(0, 0, 0, 0.16)',
              borderRight: '1px solid #b8b8b8', // 연한 회색 구분선
            }}
          >
                          <Typography variant="h6" gutterBottom sx={{ color: '#7c3aed', fontWeight: 'bold' }}>
                🏪 CU Agentic AI
              </Typography>
            <Divider sx={{ 
              my: 2,
              borderColor: 'rgba(124, 58, 237, 0.2)',
            }} />

            {/* 시스템 설정 */}
            <Accordion defaultExpanded>
              <AccordionSummary expandIcon={<ExpandMoreIcon sx={{ color: '#7c3aed' }} />}>
                              <SettingsIcon sx={{ mr: 1, color: '#1cb454' }} />
              <Typography sx={{ color: '#7c3aed', fontWeight: 'bold' }}>⚙️ 시스템 설정</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <FormControl fullWidth size="small" sx={{ mb: 2 }}>
                  <InputLabel sx={{ color: '#7c3aed' }}>🤖 모델 선택</InputLabel>
                  <Select
                    value={settings.selectedModel}
                    label="🤖 모델 선택"
                    onChange={(e) =>
                      handleSettingsChange('selectedModel', e.target.value)
                    }
                    sx={{
                      '& .MuiOutlinedInput-notchedOutline': {
                        borderColor: '#1cb454',
                      },
                      '&:hover .MuiOutlinedInput-notchedOutline': {
                        borderColor: '#169244',
                      },
                      '&.Mui-focused .MuiOutlinedInput-notchedOutline': {
                        borderColor: '#1cb454',
                      }
                    }}
                  >
                    {settings.models.map((model) => (
                      <MenuItem key={model} value={model}>
                        {model}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>

                <Typography variant="body2" gutterBottom sx={{ color: '#7c3aed' }}>
                  ⏱️ 응답 생성 시간 제한 (초)
                </Typography>
                <Slider
                  value={settings.timeoutSeconds}
                  min={60}
                  max={300}
                  step={10}
                  valueLabelDisplay="auto"
                  onChange={(_, value) =>
                    handleSettingsChange('timeoutSeconds', value)
                  }
                  sx={{ 
                    mb: 2,
                    color: '#1cb454',
                    '& .MuiSlider-thumb': {
                      backgroundColor: '#1cb454',
                    },
                    '& .MuiSlider-track': {
                      backgroundColor: '#1cb454',
                    },
                    '& .MuiSlider-rail': {
                      backgroundColor: '#d1d5db',
                    }
                  }}
                />

                <Typography variant="body2" gutterBottom sx={{ color: '#7c3aed' }}>
                  ⏱️ 재귀 호출 제한 (횟수)
                </Typography>
                <Slider
                  value={settings.recursionLimit}
                  min={10}
                  max={200}
                  step={10}
                  valueLabelDisplay="auto"
                  onChange={(_, value) =>
                    handleSettingsChange('recursionLimit', value)
                  }
                  sx={{ 
                    mb: 2,
                    color: '#1cb454',
                    '& .MuiSlider-thumb': {
                      backgroundColor: '#1cb454',
                    },
                    '& .MuiSlider-track': {
                      backgroundColor: '#1cb454',
                    },
                    '& .MuiSlider-rail': {
                      backgroundColor: '#d1d5db',
                    }
                  }}
                />
              </AccordionDetails>
            </Accordion>

            {/* 도구 설정 */}
            <Accordion>
              <AccordionSummary expandIcon={<ExpandMoreIcon sx={{ color: '#7c3aed' }} />}>
                <Typography sx={{ color: '#7c3aed', fontWeight: 'bold' }}>🔧 도구 설정</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Typography variant="body2" gutterBottom sx={{ color: '#6b7280' }}>
                  JSON 형식으로 도구를 추가하세요
                </Typography>
                <TextField
                  fullWidth
                  multiline
                  rows={4}
                  variant="outlined"
                  value={newToolJson}
                  onChange={(e) => setNewToolJson(e.target.value)}
                  placeholder={
                    '{\n  "tool_name": {\n    "command": "python",\n    "args": ["script.py"],\n    "transport": "stdio"\n  }\n}'
                  }
                  sx={{ mb: 2 }}
                />
                <Button variant="contained" fullWidth onClick={handleAddTool}
                  sx={{
                    backgroundColor: '#1cb454',
                    '&:hover': {
                      backgroundColor: '#169244',
                    }
                  }}>
                  도구 추가
                </Button>

                <Typography variant="h6" sx={{ mt: 2, mb: 1, color: '#7c3aed' }}>
                  등록된 도구
                </Typography>
                {Object.keys(config).map((toolName) => (
                  <Box
                    key={toolName}
                    sx={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      mb: 1,
                    }}
                  >
                    <Typography variant="body2">{toolName}</Typography>
                    <Button
                      size="small"
                      variant="outlined"
                      color="error"
                      onClick={() => handleDeleteTool(toolName)}
                    >
                      삭제
                    </Button>
                  </Box>
                ))}
              </AccordionDetails>
            </Accordion>

            {/* 시스템 정보 */}
            <Accordion defaultExpanded>
              <AccordionSummary expandIcon={<ExpandMoreIcon sx={{ color: '#7c3aed' }} />}>
                <Typography sx={{ color: '#7c3aed', fontWeight: 'bold' }}>📊 시스템 정보</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Typography variant="body2" gutterBottom>
                  🛠️ MCP 도구 개수: {systemInfo.toolCount}
                </Typography>
                <Typography variant="body2" gutterBottom>
                  🧠 현재 모델: {systemInfo.currentModel}
                </Typography>
              </AccordionDetails>
            </Accordion>

            {/* 작업 버튼 */}
            <Box sx={{ mt: 2 }}>
              <Button
                variant="contained"
                fullWidth
                startIcon={<SettingsIcon />}
                onClick={handleApplySettings}
                sx={{ 
                  mb: 1,
                  backgroundColor: '#1cb454',
                  '&:hover': {
                    backgroundColor: '#169244',
                  }
                }}
                disabled={isProcessing}
              >
                설정 적용
              </Button>
              <Button
                variant="outlined"
                fullWidth
                onClick={handleResetConversation}
                disabled={isProcessing}
                sx={{
                  borderColor: '#1cb454',
                  color: '#1cb454',
                  '&:hover': {
                    borderColor: '#169244',
                    backgroundColor: 'rgba(28, 180, 84, 0.04)',
                  }
                }}
              >
                대화 초기화
              </Button>
            </Box>
          </Paper>
        </Grid>

        {/* 도구 사용 내역 */}
        <Grid
          item
          xs={12}
          md={2.4}
          sx={{
            height: '100%',
            position: 'absolute',
            left: showSidebar ? '19.7%' : '0%',
            top: 0,
            width: showSidebar ? '20%' : '23%',
            transition: 'all 0.3s ease',
            zIndex: 5,
          }}
        >
          <Paper
            sx={{
              p: 2,
              display: 'flex',
              flexDirection: 'column',
              height: '100%',
              overflow: 'hidden',
              borderRadius: 3, // 둥근 모서리 추가
              background: '#f0edf4', // 현재 색상과 흰색의 중간 톤
              boxShadow: '0 8px 24px rgba(0, 0, 0, 0.18)',
              borderRight: '1px solid #b8b8b8', // 연한 회색 구분선
              borderLeft: '1px solid #d0d0d0', // 매우 연한 회색 구분선
            }}
          >
            <Typography variant="h6" gutterBottom sx={{ color: '#7c3aed', fontWeight: 'bold' }}>
              🔧 도구 사용 내역
            </Typography>
            
            {messages.filter((msg) => msg.role === 'assistant_tool').length === 0 && (
              <Box
                sx={{
                  display: 'flex',
                  justifyContent: 'center',
                  alignItems: 'center',
                  height: '100px',
                  mt: 2,
                }}
              >
                <Typography variant="body2" color="text.secondary">
                  아직 사용된 도구가 없습니다
                </Typography>
              </Box>
            )}
            
            <Box sx={{ flex: 1, overflowY: 'auto' }}>
              {messages
                .filter((msg) => msg.role === 'assistant_tool')
                .map((message, toolIndex) => (
                  <Paper
                    key={`tool-${toolIndex}`}
                    sx={{
                      p: 2,
                      mb: 2,
                      background: '#e0dae8', // 연보라색 배경
                      borderRadius: '12px',
                      borderLeft: '4px solid #1cb454',
                      boxShadow: '0 4px 12px rgba(0, 0, 0, 0.12)',
                      overflow: 'hidden',
                      border: '1px solid #e5e7eb',
                    }}
                  >
                    <Box
                      sx={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                        mb: 1,
                        cursor: 'pointer',
                        '&:hover': { opacity: 0.9 },
                      }}
                      onClick={() => handleToolToggle(messages.indexOf(message))}
                    >
                      <Typography
                        variant="subtitle2"
                        sx={{
                          color: '#1cb454',
                          fontWeight: 'bold',
                          display: 'flex',
                          alignItems: 'center',
                        }}
                      >
                        도구 참조 #{toolIndex + 1}
                      </Typography>
                      <IconButton size="small" sx={{ color: '#1cb454' }}>
                        {expandedTools[messages.indexOf(message)] ? (
                          <KeyboardArrowUpIcon />
                        ) : (
                          <KeyboardArrowDownIcon />
                        )}
                      </IconButton>
                    </Box>

                    <Collapse in={expandedTools[messages.indexOf(message)]}>
                      <Box
                        sx={{
                          mt: 1,
                          background: '#f9fafb',
                          borderRadius: '6px',
                          p: 1.5,
                          maxHeight: 'calc(100vh - 300px)',
                          overflowY: 'auto',
                          border: '1px solid #e5e7eb',
                        }}
                      >
                        <div>
                          <ReactMarkdown
                            remarkPlugins={[remarkMath]}
                            rehypePlugins={[rehypeKatex]}
                          >
                            {message.content}
                          </ReactMarkdown>
                        </div>
                      </Box>
                    </Collapse>
                  </Paper>
                ))}
            </Box>
          </Paper>
        </Grid>

        {/* 메인 채팅 영역 */}
        <Grid
          item
          xs={12}
          md={showSidebar ? 7.2 : 10}
          sx={{
            height: '100%',
            position: 'absolute',
            right: 0,
            top: 0,
            width: showSidebar ? '59.6%' : '76.3%',
            left: showSidebar ? '40.4%' : '23.7%',
            transition: 'width 0.3s ease',
            willChange: 'width',
          }}
        >
          <Paper
            sx={{
              p: 2,
              display: 'flex',
              flexDirection: 'column',
              height: '100%',
              overflow: 'hidden',
              borderRadius: 3, // 둥근 모서리 추가
              transition: 'all 0.3s ease',
              width: '100%',
              background: '#f0edf4', // 현재 색상과 흰색의 중간 톤
              boxShadow: '0 10px 28px rgba(0, 0, 0, 0.20)',
              borderLeft: '1px solid #b8b8b8', // 연한 회색 구분선
              position: 'relative',
              '&::before': {
                content: '""',
                position: 'absolute',
                top: 0,
                left: 0,
                right: 0,
                height: '3px',
                background: '#1cb454', // 초록색 상단 바
              },
            }}
          >
            {/* 사이드바 토글 버튼 */}
            <Box sx={{ display: 'flex', justifyContent: 'flex-start', mb: 1 }}>
              <Button
                variant="outlined"
                onClick={() => setShowSidebar(!showSidebar)}
                startIcon={showSidebar ? <CloseIcon /> : <MenuIcon />}
                size="small"
                sx={{
                  background: 'rgba(28, 180, 84, 0.1)',
                  border: '1px solid rgba(28, 180, 84, 0.3)',
                  color: '#1cb454',
                  '&:hover': {
                    background: 'rgba(28, 180, 84, 0.2)',
                    border: '1px solid rgba(28, 180, 84, 0.5)',
                  },
                  backdropFilter: 'blur(10px)',
                }}
              >
                {showSidebar ? '사이드바 닫기' : '사이드바 열기'}
              </Button>
            </Box>

            <Divider sx={{ my: 2 }} />

            {/* 메시지 표시 영역 */}
            <Box
              sx={{
                flexGrow: 1,
                overflow: 'hidden',
                display: 'flex',
                flexDirection: 'column',
                height: '100%',
              }}
            >
              {/* 메인 채팅 메시지 */}
              <Box
                sx={{
                  flex: 1,
                  height: '100%',
                  overflowY: 'auto',
                  pl: 2,
                }}
              >
                {messages.filter((msg, idx) => {
                  return msg.role !== 'assistant_tool'
                })
                  .length === 0 && (
                  <Box
                    sx={{
                      display: 'flex',
                      justifyContent: 'center',
                      alignItems: 'center',
                      height: '100%',
                    }}
                  >
                    <Typography variant="body1" color="text.secondary">
                      대화를 시작하려면 메시지를 입력하세요
                    </Typography>
                  </Box>
                )}

                {messages
                  .filter((msg) => msg.role !== 'assistant_tool')
                  .map((message, chatIndex) => (
                    <Box
                      // eslint-disable-next-line react/no-array-index-key
                      key={`chat-${chatIndex}`}
                      sx={{
                        display: 'flex',
                        flexDirection: 'column',
                        alignItems:
                          message.role === 'user' ? 'flex-end' : 'flex-start',
                        mb: 2,
                        width: '100%',
                      }}
                    >
                      <Card
                        sx={{
                          maxWidth: '80%',
                          width: message.role === 'user' ? 'auto' : 'auto',
                          bgcolor:
                            message.role === 'user'
                              ? '#1cb454' // 사용자 메시지는 초록색
                              : '#ffffff', // AI 메시지는 흰색
                          borderRadius: '20px',
                          borderTopLeftRadius:
                            message.role === 'user' ? '20px' : '8px',
                          borderTopRightRadius:
                            message.role === 'user' ? '8px' : '20px',
                          boxShadow: message.role === 'user' 
                            ? '0 6px 16px rgba(28, 180, 84, 0.20)'
                            : '0 4px 12px rgba(0, 0, 0, 0.12)',
                          position: 'relative',
                          background: message.role === 'user'
                            ? '#1cb454' // 사용자 메시지는 초록색
                            : '#ffffff', // AI 메시지는 흰색
                          color: message.role === 'user' ? '#ffffff' : '#374151',
                          border: message.role === 'user' ? 'none' : '1px solid #e5e7eb',
                          '&::before': message.role !== 'user' ? {
                            content: '""',
                            position: 'absolute',
                            top: 0,
                            left: 0,
                            right: 0,
                            height: '2px',
                            background: '#7c3aed', // 보라색 상단 바
                            borderRadius: '20px 20px 0 0',
                          } : {},
                        }}
                      >
                        {/* 말풍선 가장자리의 도구 참조 번호 표시 제거 */}
                        <CardContent>
                          {message.role === 'assistant' ? (
                            <>
                              <Box sx={{ 
                                typography: 'body1',
                                whiteSpace: 'pre-wrap',
                                wordBreak: 'break-word',
                                fontFamily: 'monospace, "Noto Sans KR", sans-serif',
                                lineHeight: 1.6
                              }}>
                                <div>
                                  {(() => {
                                    // URL 추출
                                    const urlMatch = message.content.match(/\[웹에서 보기\]\(([^)]+)\)/)
                                    if (urlMatch) {
                                      const reportUrl = urlMatch[1]
                                      const cleanText = message.content.replace(/🔗\s*\[웹에서 보기\]\([^)]+\)/g, '웹에서 보기')
                                      const parts = cleanText.split('웹에서 보기')
                                      return (
                                        <div>
                                          {parts[0]}
                                          <a
                                            href={reportUrl}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            style={{ 
                                              color: '#1976d2', 
                                              textDecoration: 'underline', 
                                              fontWeight: 600,
                                              cursor: 'pointer',
                                              textShadow: '0 1px 2px rgba(25, 118, 210, 0.3)'
                                            }}
                                          >
                                            웹에서 보기
                                          </a>
                                          {parts[1]}
                                        </div>
                                      )
                                    }
                                    return message.content
                                  })()}
                                </div>
                              </Box>
                              {message.referencedToolId && (
                                <Box
                                  sx={{
                                    display: 'flex',
                                    justifyContent: 'flex-end',
                                    mt: 1.5,
                                    pt: 1,
                                    borderTop:
                                      '1px solid rgba(255, 255, 255, 0.1)',
                                  }}
                                >
                                  <Typography
                                    variant="caption"
                                    sx={{
                                      color: '#7c3aed', // 보라색 참조 텍스트
                                      fontWeight: 'bold',
                                    }}
                                  >
                                    [도구 참조 #{message.referencedToolId}]
                                  </Typography>
                                </Box>
                              )}
                            </>
                          ) : (
                            <>
                              <Box sx={{ 
                                typography: 'body1',
                                whiteSpace: 'pre-wrap',
                                wordBreak: 'break-word',
                                fontFamily: 'monospace, "Noto Sans KR", sans-serif',
                                lineHeight: 1.6
                              }}>
                                {(() => {
                                  // URL 추출
                                  const urlMatch = message.content.match(/\[웹에서 보기\]\(([^)]+)\)/)
                                  if (urlMatch) {
                                    const reportUrl = urlMatch[1]
                                    const cleanText = message.content.replace(/🔗\s*\[웹에서 보기\]\([^)]+\)/g, '웹에서 보기')
                                    const parts = cleanText.split('웹에서 보기')
                                    return (
                                      <div>
                                        {parts[0]}
                                        <a
                                          href={reportUrl}
                                          target="_blank"
                                          rel="noopener noreferrer"
                                          style={{ 
                                            color: '#1976d2', 
                                            textDecoration: 'underline', 
                                            fontWeight: 600,
                                            cursor: 'pointer'
                                          }}
                                        >
                                          웹에서 보기
                                        </a>
                                        {parts[1]}
                                      </div>
                                    )
                                  }
                                  return message.content
                                })()}
                              </Box>
                              {message.attachmentPath && (
                                <Box
                                  sx={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    mt: 1,
                                    pt: 1,
                                    borderTop:
                                      '1px solid rgba(255, 255, 255, 0.1)',
                                  }}
                                >
                                  <AttachFileIcon
                                    fontSize="small"
                                    sx={{ mr: 0.5, color: '#FFFFFF' }}
                                  />
                                  <Typography
                                    variant="caption"
                                    sx={{ color: '#FFFFFF' }}
                                  >
                                    {message.attachmentPath.split('/').pop()}{' '}
                                    {/* 파일명만 표시 */}
                                  </Typography>
                                </Box>
                              )}
                            </>
                          )}
                        </CardContent>
                      </Card>
                    </Box>
                  ))}
              </Box>

              {/* 채팅 입력 영역 */}
              <Box
                sx={{
                  p: 2,
                  borderTop: '1px solid #b8b8b8', // 연한 회색 구분선
                  background: '#f0edf4', // 현재 색상과 흰색의 중간 톤
                }}
              >
                {/* 첨부 파일 표시 영역 */}
                {attachment && (
                  <Box
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      bgcolor: 'rgba(28, 180, 84, 0.1)',
                      p: 1,
                      borderRadius: 1,
                      mb: 1,
                    }}
                  >
                    <AttachFileIcon sx={{ mr: 1, color: '#1cb454' }} />
                    <Typography variant="body2" sx={{ flexGrow: 1 }}>
                      {attachment.name}
                    </Typography>
                    <IconButton
                      size="small"
                      onClick={() => setAttachment(null)}
                    >
                      <CloseIcon />
                    </IconButton>
                  </Box>
                )}

                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <input
                    type="file"
                    ref={fileInputRef}
                    style={{ display: 'none' }}
                    onChange={handleFileUpload}
                    accept=".txt,.pdf,.doc,.docx,.xlsx,.csv"
                  />
                  <IconButton
                    onClick={handleAttachmentButtonClick}
                    disabled={isProcessing}
                    sx={{ color: '#1cb454' }}
                  >
                    <AttachFileIcon />
                  </IconButton>
                  <TextField
                    fullWidth
                    variant="outlined"
                    placeholder="안녕하세요! 무엇을 도와드릴까요?"
                    value={userInput}
                    onChange={(e) => setUserInput(e.target.value)}
                    onKeyDown={async (e) => {
                      if (e.key === 'Enter' && !e.shiftKey) {
                        e.preventDefault()
                        await handleSendMessage()
                      }
                    }}
                    disabled={isProcessing}
                    size="small"
                    sx={{
                      '& .MuiOutlinedInput-root': {
                        backgroundColor: '#f0edf4 !important', // 현재 색상과 흰색의 중간 톤
                        '& input': {
                          backgroundColor: 'transparent',
                        },
                        '& fieldset': {
                          borderColor: '#1cb454',
                        },
                        '&:hover fieldset': {
                          borderColor: '#169244',
                        },
                        '&.Mui-focused fieldset': {
                          borderColor: '#1cb454',
                        },
                        '&.Mui-focused': {
                          backgroundColor: '#f0edf4 !important',
                        },
                        '&:hover': {
                          backgroundColor: '#f0edf4 !important',
                        }
                      },
                      '& .MuiInputBase-input': {
                        backgroundColor: 'transparent !important',
                      }
                    }}
                  />
                  <Button
                    variant="contained"
                    onClick={handleSendMessage}
                    disabled={isProcessing || !userInput.trim()}
                    sx={{ 
                      minWidth: '60px',
                      backgroundColor: '#1cb454',
                      '&:hover': {
                        backgroundColor: '#169244',
                      }
                    }}
                  >
                    전송
                  </Button>
                </Box>
              </Box>
            </Box>

                          {isProcessing && (
                <Box
                  sx={{
                    position: 'absolute',
                    bottom: '70px',
                    left: 0,
                    right: 0,
                    px: 2,
                  }}
                >
                  <LinearProgress sx={{
                    '& .MuiLinearProgress-bar': {
                      backgroundColor: '#1cb454',
                    },
                    backgroundColor: '#e5e7eb',
                  }} />
                </Box>
              )}
          </Paper>
        </Grid>
      </Grid>
    </Container>
  )
}

export default AgentApp
