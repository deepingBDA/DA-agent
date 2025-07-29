import React, { useState } from 'react'
import {
  Person as PersonIcon,
  SmartToy as BotIcon,
  KeyboardArrowDown as CollapseIcon,
  KeyboardArrowUp as ExpandIcon,
  OpenInNew as OpenIcon,
  Assessment as ReportIcon,
} from '@mui/icons-material'
import { Paper, Typography, Box, IconButton, Collapse, Button, Chip } from '@mui/material'

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

  const handleExpandClick = () => {
    setExpanded(!expanded)
  }

  // URL을 인식하고 클릭 가능한 링크로 변환하는 함수
  const renderContentWithLinks = (text: string) => {
    const urlRegex = /(https?:\/\/[^\s]+)/g
    const reportUrlRegex = /(http:\/\/localhost:8000\/reports\/[^\s]+\.html)/g
    
    // HTML 보고서 URL을 특별히 처리
    const parts = text.split(reportUrlRegex)
    
    return parts.map((part, index) => {
      if (reportUrlRegex.test(part)) {
        const filename = part.split('/').pop()?.replace('.html', '') || '보고서'
        return (
          <Box key={index} sx={{ my: 1 }}>
            <Button
              variant="contained"
              color="primary"
              startIcon={<ReportIcon />}
              endIcon={<OpenIcon />}
              onClick={() => window.open(part, '_blank')}
              sx={{
                borderRadius: 2,
                textTransform: 'none',
                fontWeight: 600,
                px: 2,
                py: 1,
              }}
            >
              📊 {filename} 보고서 열기
            </Button>
            <Chip
              label="클릭하여 새 탭에서 보기"
              size="small"
              variant="outlined"
              sx={{ ml: 1, fontSize: '0.75rem' }}
            />
          </Box>
        )
      } else if (urlRegex.test(part)) {
        // 일반 URL 처리
        return (
          <Button
            key={index}
            variant="outlined"
            size="small"
            startIcon={<OpenIcon />}
            onClick={() => window.open(part, '_blank')}
            sx={{ mx: 0.5, textTransform: 'none' }}
          >
            링크 열기
          </Button>
        )
      } else {
        return part
      }
    })
  }

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

        <Box sx={{ 
          whiteSpace: 'pre-wrap', 
          wordBreak: 'break-word',
          fontFamily: 'monospace, "Noto Sans KR", sans-serif'
        }}>
          <Typography 
            variant="body1" 
            component="div"
            sx={{ 
              whiteSpace: 'pre-wrap',
              fontFamily: 'inherit',
              lineHeight: 1.6
            }}
          >
            {renderContentWithLinks(content)}
          </Typography>
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
                <Typography 
                  variant="body2"
                  component="pre"
                  sx={{ 
                    fontFamily: 'inherit',
                    whiteSpace: 'pre-wrap',
                    margin: 0
                  }}
                >
                  {toolInfo}
                </Typography>
              </Box>
            </Collapse>
          </Box>
        )}
      </Paper>
    </Box>
  )
}

export default ChatMessage
