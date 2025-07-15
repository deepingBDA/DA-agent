import React, { useState } from 'react'
import {
  Person as PersonIcon,
  SmartToy as BotIcon,
  KeyboardArrowDown as CollapseIcon,
  KeyboardArrowUp as ExpandIcon,
} from '@mui/icons-material'
import { Paper, Typography, Box, IconButton, Collapse } from '@mui/material'
import ReactMarkdown from 'react-markdown'

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

        <Box sx={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
          <ReactMarkdown>{content}</ReactMarkdown>
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
              <Paper
                variant="outlined"
                sx={{
                  mt: 1,
                  p: 1,
                  backgroundColor: 'rgba(0, 0, 0, 0.1)',
                  fontSize: '0.8rem',
                  overflowX: 'auto',
                }}
              >
                <pre
                  style={{
                    margin: 0,
                    whiteSpace: 'pre-wrap',
                    wordBreak: 'break-word',
                  }}
                >
                  {toolInfo}
                </pre>
              </Paper>
            </Collapse>
          </Box>
        )}
      </Paper>
    </Box>
  )
}

export default ChatMessage
