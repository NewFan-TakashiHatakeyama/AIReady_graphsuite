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

test.describe('Governance remediation (mock API)', () => {
  test('completed finding shows executed remediation summary and stream-deferred rescore note', async ({ page }) => {
    await setSession(page, 'tenant-a', 'e2e-remediation-user')
    const remediationResponse = page.waitForResponse(
      (response) =>
        response.url().includes('/governance/findings/finding-tenant-a-remediation-e2e/remediation') &&
        response.request().method() === 'GET' &&
        response.status() === 200
    )

    await page.goto('/#/app')
    await expect(page.getByRole('heading', { name: 'ガバナンス' })).toBeVisible()
    await page.getByRole('button', { name: '検知結果', exact: true }).first().click()
    await page.getByRole('combobox').first().click()
    await page.getByRole('option', { name: 'すべて' }).click()

    await page.getByRole('row', { name: /remediation-e2e-target\.docx/ }).click()
    const res = await remediationResponse
    const detail = (await res.json()) as {
      remediation_state?: string
      result?: { post_verification?: { immediate_rescore?: boolean; deferred_to?: string; success?: boolean } }
    }
    expect(detail.remediation_state).toBe('executed')
    expect(detail.result?.post_verification?.immediate_rescore).toBe(false)
    expect(detail.result?.post_verification?.deferred_to).toBe('connect_filemetadata_stream')
    expect(detail.result?.post_verification?.success).toBe(true)

    await expect(page.getByRole('dialog', { name: '実行結果パネル' })).toBeVisible()
    await expect(page.getByRole('dialog', { name: '実行結果パネル' }).getByText(/executed（実行済み）/)).toBeVisible()
    await page.getByRole('button', { name: '証跡を見る' }).click()
    await expect(page.getByText('是正処理の適用結果: 削除 1')).toBeVisible()
    await expect(
      page.getByText(/接続のメタデータ更新後、DynamoDB Streams 経由の再計算で反映/)
    ).toBeVisible()
  })
})
