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

test('audit data differs between tenant A/B', async ({ browser }) => {
  const backendBaseUrl = 'http://127.0.0.1:9622'
  const contextA = await browser.newContext()
  const pageA = await contextA.newPage()
  const tokenA = createTestJwt('tenant-a', 'alice')
  const resA = await pageA.request.get(`${backendBaseUrl}/audit/records?domain=all&q=&limit=20&offset=0`, {
    headers: { Authorization: `Bearer ${tokenA}` }
  })
  const rowsA = ((await resA.json()) as { rows?: Array<{ audit_id?: string }> }).rows ?? []

  const contextB = await browser.newContext()
  const pageB = await contextB.newPage()
  const tokenB = createTestJwt('tenant-b', 'bob')
  const resB = await pageB.request.get(`${backendBaseUrl}/audit/records?domain=all&q=&limit=20&offset=0`, {
    headers: { Authorization: `Bearer ${tokenB}` }
  })
  const rowsB = ((await resB.json()) as { rows?: Array<{ audit_id?: string }> }).rows ?? []

  expect(rowsA.every((row) => String(row.audit_id || '').includes('tenant-a'))).toBeTruthy()
  expect(rowsB.every((row) => String(row.audit_id || '').includes('tenant-b'))).toBeTruthy()

  await contextA.close()
  await contextB.close()
})
