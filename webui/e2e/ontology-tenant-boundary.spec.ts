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

test('ontology endpoints are isolated per tenant and production gate is present', async ({ browser, page }) => {
  const backendBaseUrl = 'http://127.0.0.1:9622'
  const tokenA = createTestJwt('tenant-a', 'alice')
  const tokenB = createTestJwt('tenant-b', 'bob')

  const resA = await page.request.get(`${backendBaseUrl}/ontology/unified-metadata?limit=20&offset=0`, {
    headers: { Authorization: `Bearer ${tokenA}` }
  })
  const rowsA = ((await resA.json()) as { rows?: Array<{ item_id?: string }> }).rows ?? []
  expect(rowsA.every((row) => String(row.item_id || '').includes('tenant-a'))).toBeTruthy()

  const contextB = await browser.newContext()
  const pageB = await contextB.newPage()
  const resB = await pageB.request.get(`${backendBaseUrl}/ontology/unified-metadata?limit=20&offset=0`, {
    headers: { Authorization: `Bearer ${tokenB}` }
  })
  const rowsB = ((await resB.json()) as { rows?: Array<{ item_id?: string }> }).rows ?? []
  expect(rowsB.every((row) => String(row.item_id || '').includes('tenant-b'))).toBeTruthy()
  await contextB.close()

  const health = await page.request.get(`${backendBaseUrl}/health`)
  expect(health.status()).toBe(200)
  const healthJson = (await health.json()) as { production_gate?: { overall?: string } }
  expect(healthJson.production_gate?.overall).toBe('ok')
})
