import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// In dev the API and WebSocket are proxied to the Spring Boot backend so the browser sees one origin.
const backend = process.env.VITE_BACKEND_URL ?? 'http://localhost:8080'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': { target: backend, changeOrigin: true },
      '/ws': { target: backend, ws: true, changeOrigin: true },
    },
  },
})
