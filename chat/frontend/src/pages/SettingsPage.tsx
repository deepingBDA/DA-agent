import React, { useState, useEffect } from 'react'
import AddIcon from '@mui/icons-material/Add'
import DeleteIcon from '@mui/icons-material/Delete'
import SaveIcon from '@mui/icons-material/Save'
import {
  Container,
  Typography,
  Paper,
  TextField,
  Button,
  Box,
  Alert,
  CircularProgress,
  Divider,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Grid,
  Tooltip,
} from '@mui/material'
import { AxiosError } from 'axios'
import { getSettings, updateSettings } from '../api'

const SettingsPage: React.FC = () => {
  const [isLoading, setIsLoading] = useState<boolean>(true)
  const [success, setSuccess] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [toolCount, setToolCount] = useState<number>(0)
  const [currentConfig, setCurrentConfig] = useState<Record<string, any>>({})
  const [jsonConfig, setJsonConfig] = useState<string>('')
  const [newToolJson, setNewToolJson] = useState<string>('')

  // 페이지 로드 시 현재 설정 가져오기
  useEffect(() => {
    const loadSettings = async () => {
      try {
        setIsLoading(true)
        const settingsData = await getSettings()
        setToolCount(settingsData.tool_count)
        setCurrentConfig(settingsData.current_config)
        setJsonConfig(JSON.stringify(settingsData.current_config, null, 2))
      } catch (e) {
        console.error('설정 로드 중 오류 발생:', e)
        setError('설정을 불러오는 중 오류가 발생했습니다.')
      } finally {
        setIsLoading(false)
      }
    }

    // eslint-disable-next-line no-void
    void loadSettings()
  }, [])

  // 설정 저장 핸들러
  const handleSaveConfig = async () => {
    try {
      setIsLoading(true)
      setSuccess(null)
      setError(null)

      const parsedConfig = JSON.parse(jsonConfig)

      await updateSettings({ tool_config: parsedConfig })

      setSuccess('설정이 성공적으로 저장되었습니다.')
      setCurrentConfig(parsedConfig)
      setToolCount(Object.keys(parsedConfig).length)
    } catch (e) {
      console.error('설정 저장 중 오류 발생:', e)
      if (e instanceof SyntaxError) {
        setError(`JSON 형식 오류: ${e.message}`)
      } else if (e instanceof AxiosError) {
        setError(
          e.response?.data?.detail || '설정 저장 중 오류가 발생했습니다.',
        )
      } else {
        setError('설정 저장 중 오류가 발생했습니다.')
      }
    } finally {
      setIsLoading(false)
    }
  }

  // 도구 추가 핸들러
  const handleAddTool = () => {
    try {
      if (!newToolJson.trim()) {
        setError('추가할 도구 JSON을 입력해주세요.')
        return
      }

      // 유효한 JSON인지 확인
      const toolObject = JSON.parse(newToolJson)

      // 도구 객체의 유효성 확인
      if (
        typeof toolObject !== 'object' ||
        Array.isArray(toolObject) ||
        Object.keys(toolObject).length === 0
      ) {
        setError(
          '유효한, 도구 설정 JSON을 입력해주세요. 형식: { "도구명": { 설정 } }',
        )
        return
      }

      // 기존 설정과 새 도구 병합
      const updatedConfig = { ...currentConfig, ...toolObject }

      // 업데이트된 설정 반영
      setJsonConfig(JSON.stringify(updatedConfig, null, 2))
      setNewToolJson('')
      setSuccess(
        '도구가 추가되었습니다. 설정을 저장하려면 "저장" 버튼을 클릭하세요.',
      )
    } catch (e) {
      if (e instanceof SyntaxError) {
        setError(`JSON 형식 오류: ${e.message}`)
      } else {
        setError('도구 추가 중 오류가 발생했습니다.')
      }
    }
  }

  // 도구 삭제 핸들러
  const handleDeleteTool = (toolName: string) => {
    try {
      const updatedConfig = { ...currentConfig }
      delete updatedConfig[toolName]

      setJsonConfig(JSON.stringify(updatedConfig, null, 2))
      setSuccess(
        `"${toolName}" 도구가 제거되었습니다. 설정을 저장하려면 "저장" 버튼을 클릭하세요.`,
      )
    } catch (_e) {
      setError('도구 제거 중 오류가 발생했습니다.')
    }
  }

  return (
    <Container maxWidth="lg" sx={{ pt: 2, pb: 4 }}>
      <Box sx={{ mb: 4 }}>
        <Typography variant="h4" gutterBottom>
          MCP 도구 설정
        </Typography>
        <Typography variant="subtitle1" color="text.secondary">
          에이전트가 사용할 MCP 도구를 설정합니다.
        </Typography>
      </Box>

      {/* 상태 표시 영역 */}
      {isLoading && (
        <CircularProgress sx={{ display: 'block', mx: 'auto', my: 2 }} />
      )}

      {success && (
        <Alert
          severity="success"
          sx={{ mb: 2 }}
          onClose={() => setSuccess(null)}
        >
          {success}
        </Alert>
      )}

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}

      <Grid container spacing={3}>
        {/* 현재 등록된 도구 목록 */}
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 2, height: '100%' }}>
            <Typography variant="h6" gutterBottom>
              등록된 도구 목록 ({toolCount})
            </Typography>
            <Divider sx={{ mb: 2 }} />

            {Object.keys(currentConfig).length === 0 ? (
              <Typography
                variant="body2"
                color="text.secondary"
                sx={{ textAlign: 'center', py: 3 }}
              >
                등록된 도구가 없습니다.
              </Typography>
            ) : (
              <List dense>
                {Object.keys(currentConfig).map((toolName) => (
                  <ListItem key={toolName}>
                    <ListItemText
                      primary={toolName}
                      secondary={
                        currentConfig[toolName].command
                          ? `명령: ${currentConfig[toolName].command}`
                          : `URL: ${currentConfig[toolName].url || '정보 없음'}`
                      }
                    />
                    <ListItemSecondaryAction>
                      <Tooltip title="도구 삭제">
                        <IconButton
                          edge="end"
                          onClick={() => handleDeleteTool(toolName)}
                        >
                          <DeleteIcon />
                        </IconButton>
                      </Tooltip>
                    </ListItemSecondaryAction>
                  </ListItem>
                ))}
              </List>
            )}
          </Paper>
        </Grid>

        {/* 도구 추가 영역 */}
        <Grid item xs={12} md={8}>
          <Paper sx={{ p: 2, mb: 3 }}>
            <Typography variant="h6" gutterBottom>
              도구 추가
            </Typography>
            <Divider sx={{ mb: 2 }} />

            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              JSON 형식으로 새 도구를 추가하세요. 형식:{' '}
              {
                '{ "도구명": { "command": "명령어", "args": [], "transport": "stdio" } }'
              }
            </Typography>

            <TextField
              label="새 도구 JSON"
              multiline
              rows={4}
              fullWidth
              value={newToolJson}
              onChange={(e) => setNewToolJson(e.target.value)}
              margin="normal"
              variant="outlined"
              placeholder='{ "my_tool": { "command": "python", "args": ["./script.py"], "transport": "stdio" } }'
            />

            <Button
              variant="contained"
              startIcon={<AddIcon />}
              onClick={handleAddTool}
              sx={{ mt: 1 }}
            >
              도구 추가
            </Button>
          </Paper>

          {/* 전체 설정 JSON 편집 영역 */}
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              전체 설정 JSON
            </Typography>
            <Divider sx={{ mb: 2 }} />

            <TextField
              label="MCP 설정 JSON"
              multiline
              rows={10}
              fullWidth
              value={jsonConfig}
              onChange={(e) => setJsonConfig(e.target.value)}
              margin="normal"
              variant="outlined"
            />

            <Button
              variant="contained"
              color="primary"
              startIcon={<SaveIcon />}
              onClick={handleSaveConfig}
              disabled={isLoading}
              sx={{ mt: 1 }}
            >
              설정 저장
            </Button>
          </Paper>
        </Grid>
      </Grid>
    </Container>
  )
}

export default SettingsPage
