import React, { useState } from 'react'
import {
  Person as PersonIcon,
  SmartToy as BotIcon,
  KeyboardArrowDown as CollapseIcon,
  KeyboardArrowUp as ExpandIcon,
} from '@mui/icons-material'
import { Paper, Typography, Box, IconButton, Collapse } from '@mui/material'

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

  // 기본 마크다운 렌더링 사용 (안정성 우선)
  const renderContentWithLinks = (text: string) => {
    return text
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
