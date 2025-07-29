import React, { useState, useMemo } from 'react'
import {
  Person as PersonIcon,
  SmartToy as BotIcon,
  KeyboardArrowDown as CollapseIcon,
  KeyboardArrowUp as ExpandIcon,
} from '@mui/icons-material'
import { Paper, Typography, Box, IconButton, Collapse } from '@mui/material'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
// remark-linkify 패키지는 존재하지 않아 제거

interface ChatMessageProps {
  role: 'user' | 'assistant' | 'assistant_tool'
  content: string
  toolInfo?: string
}

const ChatMessage: React.FC<ChatMessageProps> = ({
  role,
  content,
  toolInfo,
}) => {
  const [expanded, setExpanded] = useState<boolean>(false)

  // 백틱(``` ) 코드블록으로 감싸진 메시지가 있으면 앞/뒤 래퍼를 제거해 링크가 활성화되도록 처리
  const cleanedContent = useMemo(() => {
    let text = content.trim()

    // 1) 코드블록 래퍼 제거 (문자열 어디에 있든 첫/마지막 ``` 페어 제거)
    if (text.startsWith('```')) {
      // 앞쪽 ```lang?\n 제거 (가능한 lang 지정 포함)
      text = text.replace(/^```[^\n]*\n/, '')
      // 뒤쪽 ``` 또는 ```\r?\n 제거
      text = text.replace(/\n```\s*$/, '')
    }

    // 2) 보고서 링크 자동 래핑 → Markdown 링크
    //  - '/reports/...' (상대경로)
    //  - 'http(s)://.../reports/...' (절대경로, @ 기호로 시작할 수도 있음)
    const absPattern = /(?<![\[(])@?(https?:\/\/[^\s)]+\/reports\/[^\s)]+\.html)/g
    const relPattern = /(?<![\[(])(\/reports\/[^\s)]+\.html)/g
    text = text.replace(absPattern, '[$1]($1)')
    text = text.replace(relPattern, '[$1]($1)')

    return text
  }, [content])

  // toolInfo가 있을 때 동일한 전처리 적용
  const cleanedToolInfo = useMemo(() => {
    if (!toolInfo) return undefined

    let text = toolInfo.trim()

    if (text.startsWith('```')) {
      text = text.replace(/^```[^\n]*\n/, '')
      text = text.replace(/\n```\s*$/, '')
    }

    const absPattern = /(?<![\[(])@?(https?:\/\/[^\s)]+\/reports\/[^\s)]+\.html)/g
    const relPattern = /(?<![\[(])(\/reports\/[^\s)]+\.html)/g
    text = text.replace(absPattern, '[$1]($1)')
    text = text.replace(relPattern, '[$1]($1)')

    return text
  }, [toolInfo])

  const handleExpandClick = () => {
    setExpanded(!expanded)
  }

  // react-markdown으로 마크다운 콘텐츠 렌더링

  return (
    <Box
      sx={{
        display: 'flex',
        marginY: 1,
        justifyContent: role === 'user' ? 'flex-end' : 'flex-start',
        maxWidth: '100%',
      }}
    >
      <Paper
        elevation={1}
        sx={{
          padding: 2,
          borderRadius: 2,
          maxWidth: '80%',
          backgroundColor:
            role === 'user' ? 'primary.dark' : 'background.paper',
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
          {role === 'user' ? (
            <PersonIcon color="inherit" sx={{ mr: 1 }} />
          ) : (
            <BotIcon color="primary" sx={{ mr: 1 }} />
          )}

          <Typography variant="subtitle2" color="text.secondary">
            {role === 'user' ? '사용자' : '에이전트'}
          </Typography>
        </Box>

        <Box
          sx={{
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
            fontFamily: 'monospace, "Noto Sans KR", sans-serif',
            lineHeight: 1.6,
          }}
        >
          {/* 마크다운 렌더링 – 링크·개행·GFM, 자동 링크화 지원 */}
          <ReactMarkdown
            remarkPlugins={[remarkGfm]}
            components={{ a: renderAnchor }}
          >
            {cleanedContent}
          </ReactMarkdown>
        </Box>

        {toolInfo && (
          <Box sx={{ mt: 1 }}>
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <Typography variant="caption" color="text.secondary">
                도구 호출 정보
              </Typography>
              <IconButton
                size="small"
                onClick={handleExpandClick}
                aria-expanded={expanded}
                aria-label="도구 호출 정보 보기"
                sx={{ ml: 1 }}
              >
                {expanded ? (
                  <CollapseIcon fontSize="small" />
                ) : (
                  <ExpandIcon fontSize="small" />
                )}
              </IconButton>
            </Box>

            <Collapse in={expanded} timeout="auto" unmountOnExit>
              <Box
                sx={{
                  mt: 1,
                  p: 2,
                  backgroundColor: 'grey.50',
                  borderRadius: 1,
                  fontFamily: 'monospace',
                  fontSize: '0.875rem',
                  whiteSpace: 'pre-wrap',
                  overflow: 'auto',
                }}
              >
                {/* toolInfo 도 마크다운으로 렌더링하여 링크·코드블록 지원 */}
                {cleanedToolInfo && (
                  <ReactMarkdown
                    remarkPlugins={[remarkGfm]}
                    components={{ a: renderAnchor }}
                  >
                    {cleanedToolInfo}
                  </ReactMarkdown>
                )}
              </Box>
            </Collapse>
          </Box>
        )}
      </Paper>
    </Box>
  )
}

// ===== 헬퍼: 공통 a 태그 렌더러 =====
const renderAnchor = ({ node, ...props }: any) => (
  <a
    {...props}
    target="_blank"
    rel="noopener noreferrer"
    style={{ color: '#1976d2', textDecoration: 'underline', fontWeight: 600 }}
  />
)

export default ChatMessage
