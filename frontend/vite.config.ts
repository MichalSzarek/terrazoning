import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const apiProxyTarget = process.env.VITE_API_PROXY_TARGET ?? 'http://127.0.0.1:8000';

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) {
            return;
          }

          if (id.includes('maplibre-gl') || id.includes('react-map-gl')) {
            return 'map';
          }

          if (id.includes('@tanstack/react-query') || id.includes('axios')) {
            return 'data';
          }

          if (id.includes('react') || id.includes('zustand') || id.includes('lucide-react')) {
            return 'vendor';
          }
        },
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      // Proxy /api/* → backend at :8000 to avoid CORS in dev
      '/api': {
        target: apiProxyTarget,
        changeOrigin: true,
      },
    },
  },
});
