import { expect, test } from '@playwright/test'
import { createHmac } from 'node:crypto'

const createTestJwt = (tenantId: string, username: string): string => {
  const secret = process.env.PLAYWRIGHT_TOKEN_SECRET || 'lightrag-jwt-default-secret'
  const role = process.env.PLAYWRIGHT_TOKEN_ROLE || 'guest'
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url')
  const payload = Buffer.from(
    JSON.stringify({
      sub: username,
      role,
      metadata: { tenant_id: tenantId },
      exp: Math.floor(Date.now() / 1000) + 3600
    })
  ).toString('base64url')
  const signingInput = `${header}.${payload}`
  const signature = createHmac('sha256', secret).update(signingInput).digest('base64url')
  return `${signingInput}.${signature}`
}

test('dashboard and audit endpoints keep tenant boundary', async ({ page }) => {
  const backendBaseUrl = 'http://127.0.0.1:9622'
  const tenantId = 'tenant-a'
  const token = createTestJwt(tenantId, 'alice')
  await page.addInitScript(({ injectedToken }) => {
    localStorage.setItem('LIGHTRAG-API-TOKEN', injectedToken)
    localStorage.setItem(
      'settings-storage',
      JSON.stringify({
        state: {
          currentTab: 'dashboard',
          enableHealthCheck: false
        },
        version: 18
      })
    )
    sessionStorage.setItem('VERSION_CHECKED_FROM_LOGIN', 'true')
  }, { injectedToken: token })

  await page.goto('/#/app')

  const readiness = await page.request.get(`${backendBaseUrl}/dashboard/readiness`, {
    headers: { Authorization: `Bearer ${token}` }
  })
  expect(readiness.status()).toBe(200)

  const records = await page.request.get(`${backendBaseUrl}/audit/records?domain=all&q=&limit=20&offset=0`, {
    headers: { Authorization: `Bearer ${token}` }
  })
  expect(records.status()).toBe(200)
  const rows = ((await records.json()) as { rows?: Array<{ target?: string }> }).rows ?? []
  expect(rows.every((row) => String(row.target || '').includes(tenantId))).toBeTruthy()
})
