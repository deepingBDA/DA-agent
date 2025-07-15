import React from 'react'
import ChatIcon from '@mui/icons-material/Chat'
import MenuIcon from '@mui/icons-material/Menu'
import SettingsIcon from '@mui/icons-material/Settings'
import { AppBar, Toolbar, Typography, IconButton, Box } from '@mui/material'
import { useNavigate, useLocation } from 'react-router-dom'

interface HeaderProps {
  onDrawerToggle: () => void
}

const Header: React.FC<HeaderProps> = ({ onDrawerToggle }) => {
  const navigate = useNavigate()
  const location = useLocation()

  return (
    <AppBar position="fixed">
      <Toolbar>
        <IconButton
          color="inherit"
          aria-label="open drawer"
          onClick={onDrawerToggle}
          edge="start"
          sx={{ mr: 2 }}
        >
          <MenuIcon />
        </IconButton>

        <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
          MCP 도구 활용 에이전트
        </Typography>

        <Box sx={{ display: 'flex' }}>
          {location.pathname !== '/' && (
            <IconButton
              color="inherit"
              onClick={() => navigate('/')}
              title="대화"
            >
              <ChatIcon />
            </IconButton>
          )}

          {location.pathname !== '/settings' && (
            <IconButton
              color="inherit"
              onClick={() => navigate('/settings')}
              title="설정"
            >
              <SettingsIcon />
            </IconButton>
          )}
        </Box>
      </Toolbar>
    </AppBar>
  )
}

export default Header
