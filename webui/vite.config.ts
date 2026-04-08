import { defineConfig, loadEnv } from 'vite'
import http from 'node:http'
import path from 'path'
import react from '@vitejs/plugin-react-swc'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  // Use IPv4 loopback by default to avoid intermittent proxy socket hang-up
  // on Windows when "localhost" resolves to ::1 while backend binds IPv4 only.
  const apiProxyTarget = env.VITE_PROXY_TARGET || 'http://127.0.0.1:9621'
  // Health checks are frequent; disable keep-alive on these routes so that
  // stale upstream sockets (after backend reload/restart) are not reused.
  const healthProxyAgent = new http.Agent({ keepAlive: false })
  // Governance endpoints include remediation actions and can be impacted by
  // stale sockets after local backend restarts.
  const governanceProxyAgent = new http.Agent({ keepAlive: false })
  // Connect onboarding can take minutes (Graph + Lambda + reflection); avoid short defaults.
  const connectProxyAgent = new http.Agent({ keepAlive: false })
  // run.ps1 sets VITE_USE_VITE_PROXY=1 in process.env; loadEnv does not always see the shell, so merge here.
  const viteUseViteProxy =
    process.env.VITE_USE_VITE_PROXY ?? env.VITE_USE_VITE_PROXY ?? ''

  return {
    define: {
      'import.meta.env.VITE_USE_VITE_PROXY': JSON.stringify(viteUseViteProxy),
    },
    plugins: [react(), tailwindcss()],
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src')
      }
    },
    // base: import.meta.env.VITE_BASE_URL || '/webui/',
    base: './',
    build: {
      outDir: 'dist',
      emptyOutDir: true,
      chunkSizeWarningLimit: 1000,
      rollupOptions: {
        external: (id) => {
        // External modules that cause build issues
          const externalPatterns = [
            /^graphology-layout/,
            /^react-select/,
            /^@react-sigma/
          ];
          return externalPatterns.some(pattern => pattern.test(id));
        },
        output: {
        // Manual chunking strategy
          manualChunks: {
          // Group React-related libraries into one chunk
            'react-vendor': ['react', 'react-dom', 'react-router-dom'],
            // Group graph visualization libraries into one chunk
            'graph-vendor': ['sigma', 'graphology'],
            // Group UI component libraries into one chunk
            'ui-vendor': ['@radix-ui/react-dialog', '@radix-ui/react-popover', '@radix-ui/react-select', '@radix-ui/react-tabs'],
            // Group utility libraries into one chunk
            'utils-vendor': ['axios', 'i18next', 'zustand', 'clsx', 'tailwind-merge'],

            // Mermaid-related modules
            'mermaid-vendor': ['mermaid'],

            // Markdown-related modules
            'markdown-vendor': [
              'react-markdown',
              'rehype-react',
              'remark-gfm',
              'remark-math',
              'react-syntax-highlighter'
            ]
          },
          // Ensure consistent chunk naming format
          chunkFileNames: 'assets/[name]-[hash].js',
          // Entry file naming format
          entryFileNames: 'assets/[name]-[hash].js',
          // Asset file naming format
          assetFileNames: 'assets/[name]-[hash].[ext]'
        }
      }
    },
    server: {
      host: '0.0.0.0',
      port: 5173,
      origin: 'http://localhost:5173',
      proxy: {
        '/api': {
          target: apiProxyTarget,
          changeOrigin: true,
        },
        // Proxy API requests to the backend
        '/login': {
          target: apiProxyTarget,
          changeOrigin: true,
        },
        '/auth-status': {
          target: apiProxyTarget,
          changeOrigin: true,
        },
        '/healthz': {
          target: apiProxyTarget,
          changeOrigin: true,
          agent: healthProxyAgent,
          proxyTimeout: 5000,
          timeout: 5000,
        },
        '/health': {
          target: apiProxyTarget,
          changeOrigin: true,
          agent: healthProxyAgent,
          proxyTimeout: 5000,
          timeout: 5000,
        },
        '/query': {
          target: apiProxyTarget,
          changeOrigin: true,
        },
        '/graphs': {
          target: apiProxyTarget,
          changeOrigin: true,
        },
        '/graph': {
          target: apiProxyTarget,
          changeOrigin: true,
        },
        '/ontology': {
          target: apiProxyTarget,
          changeOrigin: true,
        },
        '/connect': {
          target: apiProxyTarget,
          changeOrigin: true,
          agent: connectProxyAgent,
          proxyTimeout: 300_000,
          timeout: 300_000,
        },
        '/governance': {
          target: apiProxyTarget,
          changeOrigin: true,
          agent: governanceProxyAgent,
          proxyTimeout: 15000,
          timeout: 15000,
        },
        '/dashboard': {
          target: apiProxyTarget,
          changeOrigin: true,
        },
        '/audit': {
          target: apiProxyTarget,
          changeOrigin: true,
        },
        '/docs': {
          target: apiProxyTarget,
          changeOrigin: true,
        },
        '/openapi.json': {
          target: apiProxyTarget,
          changeOrigin: true,
        }
      }
    }
  }
})
