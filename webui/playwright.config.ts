import { defineConfig, devices } from '@playwright/test'

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
      command: 'node ./e2e/mock-governance-api.mjs',
      port: 9622,
      reuseExistingServer: true,
      timeout: 30_000,
      env: {
        MOCK_API_PORT: '9622'
      }
    },
    {
      command: 'npm run dev -- --host 127.0.0.1 --port 4173',
      port: 4173,
      reuseExistingServer: true,
      timeout: 60_000,
      env: {
        VITE_PROXY_TARGET: 'http://127.0.0.1:9622',
        VITE_BACKEND_URL: 'http://127.0.0.1:9622'
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
