import { defineConfig, devices } from '@playwright/test'

const backendUrl = process.env.PLAYWRIGHT_BACKEND_URL || 'http://127.0.0.1:9621'

export default defineConfig({
  testDir: './e2e',
  timeout: 30_000,
  fullyParallel: false,
  retries: 0,
  reporter: 'list',
  use: {
    baseURL: 'http://127.0.0.1:4173',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure'
  },
  webServer: [
    {
      command: 'python graphsuite_server.py',
      cwd: '../api',
      port: 9621,
      reuseExistingServer: true,
      timeout: 90_000,
      env: {
        PYTHONIOENCODING: 'utf-8'
      }
    },
    {
      command: 'npm run dev -- --host 127.0.0.1 --port 4173',
      port: 4173,
      reuseExistingServer: true,
      timeout: 60_000,
      env: {
        VITE_PROXY_TARGET: backendUrl,
        VITE_BACKEND_URL: backendUrl
      }
    }
  ],
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] }
    }
  ]
})
