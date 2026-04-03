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

test.describe('Governance reason/evidence display (mock API)', () => {
  test('shows detection reasons, guard reasons, and evidence fields', async ({ page }) => {
    await setSession(page, 'tenant-a', 'e2e-reason-evidence-user')
    await page.goto('/#/app')
    await expect(page.getByRole('heading', { name: 'ガバナンス' })).toBeVisible()

    await page.getByRole('button', { name: '検知結果', exact: true }).first().click()
    await page.getByRole('combobox').first().click()
    await page.getByRole('option', { name: 'すべて' }).click()

    // 一覧は検知理由列のみ（ガード理由は証跡パネル内）
    await expect(page.getByRole('columnheader', { name: '検知理由' })).toBeVisible()

    const targetRow = page.getByRole('row', { name: /doc-tenant-a\.docx/ })
    await expect(targetRow).toBeVisible()
    await expect(targetRow.getByText('B: 外部直接共有事故')).toBeVisible()
    await expect(targetRow.getByText('C: 公開リンク事故')).toBeVisible()

    await targetRow.click()
    const dialog = page.getByRole('dialog', { name: '次アクションパネル' })
    await expect(dialog).toBeVisible()
    await page.getByRole('button', { name: '証跡を見る' }).click()

    await expect(dialog.getByText('検知理由')).toBeVisible()
    await expect(dialog.getByText('ガード理由')).toBeVisible()
    await expect(dialog.getByText(/G3-公開リンク/)).toBeVisible()
    await expect(dialog.getByText(/G3-外部直接共有/)).toBeVisible()
    await expect(dialog.getByText('stayhungry.stayfoolish.1990@gmail.com')).toBeVisible()
  })
})
