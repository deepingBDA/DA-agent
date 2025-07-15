import type { UserConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default {
  plugins: [
    react({
      babel: {
        plugins: [
          'babel-plugin-macros',
          ['babel-plugin-styled-components', { ssr: false, displayName: true }],
        ],
      },
    }),
  ],
  publicDir: 'public',
  build: {
    outDir: 'build',
  },
  server: {
    port: 3000,
  },
  preview: {
    port: 3000,
  },
} satisfies UserConfig
