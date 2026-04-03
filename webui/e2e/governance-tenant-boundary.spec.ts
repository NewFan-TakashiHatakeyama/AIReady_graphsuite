import { expect, test, type BrowserContext, type Page } from '@playwright/test'
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
  return token
}

const verifyGovernanceFlow = async (page: Page, tenantId: string, token: string) => {
  await page.goto('/#/app')
  await expect(page.getByRole('heading', { name: 'ガバナンス' })).toBeVisible()
  const findingsTab = page.getByRole('button', { name: '検知結果', exact: true }).first()
  let findingsResponse: { url(): string; headers(): Record<string, string>; json(): Promise<any> } | null = null
  try {
    await expect(findingsTab).toHaveAttribute('aria-disabled', 'false', { timeout: 10_000 })
  } catch {
    const runScanButton = page.getByRole('button', { name: 'スコアリングを実行' }).first()
    if (await runScanButton.isVisible()) {
      const scanAccepted = page.waitForResponse(
        (response) => response.url().includes('/governance/scans/daily') && response.status() === 200,
        { timeout: 10_000 }
      )
      await runScanButton.click()
      await scanAccepted
    }
  }

  const isFindingsUnlocked = (await findingsTab.getAttribute('aria-disabled')) === 'false'
  if (isFindingsUnlocked) {
    const findingsResponsePromise = page.waitForResponse((response) =>
      response.url().includes('/governance/findings') && response.status() === 200
    )
    await findingsTab.click()
    findingsResponse = await findingsResponsePromise
  } else {
    const response = await page.request.get('/governance/findings?limit=300&offset=0', {
      headers: { Authorization: `Bearer ${token}` }
    })
    expect(response.status()).toBe(200)
    findingsResponse = {
      url: () => response.url(),
      headers: () => response.headers(),
      json: async () => await response.json()
    }
  }

  const response = findingsResponse
  const correlationId = response.headers()['x-correlation-id']
  expect(Boolean(correlationId && correlationId.trim())).toBeTruthy()

  const payload = (await response.json()) as { rows?: Array<{ tenant_id?: string; finding_id?: string }> }
  const rows = payload.rows ?? []
  expect(rows.every((row) => row.tenant_id === tenantId)).toBeTruthy()

  const backendOrigin = new URL(response.url()).origin
  return { rows, backendOrigin }
}

const createTenantContext = async (
  browserContextFactory: () => Promise<BrowserContext>,
  tenantId: string,
  username: string
) => {
  const context = await browserContextFactory()
  const page = await context.newPage()
  const token = await setSession(page, tenantId, username)
  return { context, page, token }
}

const assertTenantOverrideRejected = async (
  page: Page,
  token: string,
  backendOrigin: string
) => {
  const queryResponse = await page.request.get(
    `${backendOrigin}/governance/overview?tenant_id=tenant-hijack`,
    { headers: { Authorization: `Bearer ${token}` } }
  )
  const queryPayload = (await queryResponse.json()) as { detail?: string }
  const queryTamper = { status: queryResponse.status(), detail: queryPayload?.detail || '' }
  expect(queryTamper.status).toBe(400)
  expect(queryTamper.detail).toContain('tenant_id must not be provided')

  const bodyResponse = await page.request.post(`${backendOrigin}/governance/policies`, {
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    data: {
      tenant_id: 'tenant-hijack',
      policy_type: 'scope',
      name: 'tampered-policy',
      scope_type: 'folder',
      scope_value: '/tamper'
    }
  })
  const bodyPayload = (await bodyResponse.json()) as { detail?: string }
  const bodyTamper = { status: bodyResponse.status(), detail: bodyPayload?.detail || '' }
  expect(bodyTamper.status).toBe(400)
  expect(bodyTamper.detail).toContain('tenant_id must not be provided')
}

test.describe('Governance tenant boundary (JWT A/B)', () => {
  test('tenant A and B see isolated data through UI flow', async ({ browser }) => {
    const createContext = () => browser.newContext()

    const tenantA = await createTenantContext(createContext, 'tenant-a', 'alice')
    const tenantAResult = await verifyGovernanceFlow(tenantA.page, 'tenant-a', tenantA.token)
    await assertTenantOverrideRejected(tenantA.page, tenantA.token, tenantAResult.backendOrigin)
    const rowsA = tenantAResult.rows
    expect(rowsA.every((row) => !String(row.finding_id || '').includes('tenant-b'))).toBeTruthy()
    await tenantA.context.close()

    const tenantB = await createTenantContext(createContext, 'tenant-b', 'bob')
    const tenantBResult = await verifyGovernanceFlow(tenantB.page, 'tenant-b', tenantB.token)
    const rowsB = tenantBResult.rows
    expect(rowsB.every((row) => !String(row.finding_id || '').includes('tenant-a'))).toBeTruthy()
    await tenantB.context.close()
  })
})
