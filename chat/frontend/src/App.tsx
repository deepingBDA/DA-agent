import React from 'react'
import { ThemeProvider, createTheme, Box } from '@mui/material'
import CssBaseline from '@mui/material/CssBaseline'
import { Chart as ChartJS, registerables } from 'chart.js'
import * as zoomPlugin from 'chartjs-plugin-zoom'

// 컴포넌트 가져오기
import AgentApp from './AgentApp'

// 기본 다크 테마 생성
const theme = createTheme({
  typography: {
    fontFamily: 'pretendard-kr',
  },
})

ChartJS.register(...registerables, zoomPlugin.default)

export default function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box
        sx={{
          display: 'flex',
          height: '100vh',
          width: '100vw',
          overflow: 'hidden',
          m: 0,
          p: 0,
          maxWidth: '100vw',
        }}
      >
        <AgentApp />
      </Box>
    </ThemeProvider>
  )
}
