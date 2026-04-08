import { expect, test, type Page } from '@playwright/test'
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

const setSession = async (page: Page, tenantId: string, username: string) => {
  const token = createTestJwt(tenantId, username)
  await page.addInitScript(({ injectedToken }) => {
    localStorage.setItem('LIGHTRAG-API-TOKEN', injectedToken)
    localStorage.setItem(
      'settings-storage',
      JSON.stringify({
        state: {
          currentTab: 'governance-operations',
          enableHealthCheck: false
        },
        version: 18
      })
    )
    sessionStorage.setItem('VERSION_CHECKED_FROM_LOGIN', 'true')
  }, { injectedToken: token })
}

test.describe('Governance remediation approve (mock API, camelCase body)', () => {
  test('approve response without remediation_state still shows executed in dialog', async ({ page }) => {
    await setSession(page, 'tenant-a', 'e2e-approve-camel-user')

    const approveResponse = page.waitForResponse(
      (response) =>
        response.url().includes('/governance/findings/finding-tenant-a-approve-camel-e2e/remediation/approve') &&
        response.request().method() === 'POST' &&
        response.status() === 200
    )

    await page.goto('/#/app')
    await expect(page.getByRole('heading', { name: 'ガバナンス' })).toBeVisible()
    await page.getByRole('button', { name: '検知結果', exact: true }).first().click()
    await page.getByRole('combobox').first().click()
    await page.getByRole('option', { name: 'すべて' }).click()

    await page.getByRole('row', { name: /approve-camel-e2e\.docx/ }).click()
    const actionDialog = page.getByRole('dialog', { name: '次アクションパネル' })
    await expect(actionDialog).toBeVisible()

    await actionDialog.getByRole('button', { name: '承認' }).click()
    const ar = await approveResponse
    const body = (await ar.json()) as Record<string, unknown>
    expect(body.remediation_state).toBeUndefined()
    expect(body.remediationState).toBe('executed')

    await expect(page.getByRole('dialog', { name: '実行結果パネル' })).toBeVisible({ timeout: 15_000 })
    await expect(
      page.getByRole('dialog', { name: '実行結果パネル' }).getByText(/executed（実行済み）/)
    ).toBeVisible({ timeout: 15_000 })
  })
})
