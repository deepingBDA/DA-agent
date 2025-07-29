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
  palette: {
    mode: 'light',
    background: {
      default: '#f3f0f8',
      paper: '#f6f4f9',
    },
    primary: {
      main: '#8B5CF6', // CU 보라색
      light: '#A78BFA',
      dark: '#6B46C1',
    },
    secondary: {
      main: '#00a86b', // CU 초록색
      light: '#10b981',
      dark: '#047857',
    },
  },
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        body: {
          background: 'linear-gradient(135deg, #f3f0f8 0%, #e8e3f0 100%) !important',
          backgroundAttachment: 'fixed !important',
        },
      },
    },
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
