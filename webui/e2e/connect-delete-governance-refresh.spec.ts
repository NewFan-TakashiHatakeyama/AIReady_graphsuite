import { expect, test, type Page } from '@playwright/test'
import { createHmac } from 'node:crypto'

const CONNECT_E2E_TENANT = 'tenant-connect-e2e'
const MOCK_API_PORT = process.env.MOCK_API_PORT || '9622'
const MOCK_API_ORIGIN = `http://127.0.0.1:${MOCK_API_PORT}`

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

const setConnectSession = async (page: Page, token: string) => {
  await page.addInitScript(({ injectedToken }) => {
    localStorage.setItem('LIGHTRAG-API-TOKEN', injectedToken)
    localStorage.setItem(
      'settings-storage',
      JSON.stringify({
        state: {
          currentTab: 'connect-operations',
          enableHealthCheck: false
        },
        version: 18
      })
    )
    sessionStorage.setItem('VERSION_CHECKED_FROM_LOGIN', 'true')
  }, { injectedToken: token })
}

test.beforeEach(async ({ request }) => {
  const res = await request.post(`${MOCK_API_ORIGIN}/__e2e/reset-connect-delete`)
  expect(res.ok()).toBeTruthy()
})

test('Connect safe delete clears governance findings (mock API)', async ({ page, request }) => {
  test.setTimeout(60_000)
  const token = createTestJwt(CONNECT_E2E_TENANT, 'e2e-connect-user')
  await setConnectSession(page, token)

  let res = await request.get(`${MOCK_API_ORIGIN}/governance/findings?limit=300&offset=0`, {
    headers: { Authorization: `Bearer ${token}` }
  })
  expect(res.ok()).toBeTruthy()
  let payload = (await res.json()) as { rows?: unknown[] }
  expect((payload.rows ?? []).length).toBe(1)

  await page.goto('/#/app')
  await expect(page.getByRole('heading', { name: '接続' })).toBeVisible({ timeout: 25_000 })
  await page.locator('button.liquid-glass-tab').nth(1).click()
  await page.getByRole('button', { name: /M365/ }).first().click()
  await page.getByRole('button', { name: '削除' }).first().click()
  await page.getByRole('button', { name: '安全モードで削除' }).click()
  await expect(page.getByText('接続を削除しました')).toBeVisible({ timeout: 20_000 })

  res = await request.get(`${MOCK_API_ORIGIN}/governance/findings?limit=300&offset=0`, {
    headers: { Authorization: `Bearer ${token}` }
  })
  payload = (await res.json()) as { rows?: unknown[] }
  expect((payload.rows ?? []).length).toBe(0)

  await page.getByRole('button', { name: /Diagnosis|診断/ }).click()
  await page.getByRole('menuitem', { name: /ガバナンス|Governance/ }).click()
  await expect(page.getByRole('heading', { name: /ガバナンス|Governance/ })).toBeVisible({ timeout: 20_000 })
  const findingsTab = page.getByRole('button', { name: '検知結果', exact: true }).first()
  await expect(findingsTab).toBeEnabled({ timeout: 15_000 })
  await findingsTab.click()
  await expect(page.getByText('finding-connect-e2e-001')).toHaveCount(0)
})
