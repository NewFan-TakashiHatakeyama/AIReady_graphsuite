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

test('ontology aws-connected flow endpoints complete', async ({ page }) => {
  const backendBaseUrl = 'http://127.0.0.1:9622'
  const tenantId = 'tenant-a'
  const token = createTestJwt(tenantId, 'alice')

  await page.goto('/#/app')

  const overview = await page.request.get(`${backendBaseUrl}/ontology/overview`, {
    headers: { Authorization: `Bearer ${token}` }
  })
  expect(overview.status()).toBe(200)

  const unified = await page.request.get(`${backendBaseUrl}/ontology/unified-metadata?limit=50&offset=0`, {
    headers: { Authorization: `Bearer ${token}` }
  })
  expect(unified.status()).toBe(200)
  const unifiedRows = ((await unified.json()) as { rows?: Array<{ item_id?: string }> }).rows ?? []
  expect(unifiedRows.length).toBeGreaterThan(0)
  const itemId = String(unifiedRows[0].item_id || '')

  const itemGraph = await page.request.get(
    `${backendBaseUrl}/ontology/graph/by-item?item_id=${encodeURIComponent(itemId)}&file_name=&max_depth=2&max_nodes=80`,
    { headers: { Authorization: `Bearer ${token}` } }
  )
  expect(itemGraph.status()).toBe(200)
  expect(((await itemGraph.json()) as { start_node_id?: string }).start_node_id).toContain(itemId)

  const candidates = await page.request.get(`${backendBaseUrl}/ontology/entity-candidates?limit=20&offset=0&status=pending`, {
    headers: { Authorization: `Bearer ${token}` }
  })
  expect(candidates.status()).toBe(200)
  const candidateRows = ((await candidates.json()) as { rows?: Array<{ candidate_id?: string; suggestions?: Array<{ entity_id?: string }> }> }).rows ?? []
  expect(candidateRows.length).toBeGreaterThan(0)
  const candidateId = String(candidateRows[0].candidate_id || '')
  const targetEntityId = String(candidateRows[0].suggestions?.[0]?.entity_id || '')
  const resolve = await page.request.post(
    `${backendBaseUrl}/ontology/entity-candidates/${encodeURIComponent(candidateId)}/resolve-existing?target_entity_id=${encodeURIComponent(targetEntityId)}`,
    { headers: { Authorization: `Bearer ${token}` } }
  )
  expect(resolve.status()).toBe(200)

  const audit = await page.request.get(`${backendBaseUrl}/ontology/audit?limit=20&offset=0`, {
    headers: { Authorization: `Bearer ${token}` }
  })
  expect(audit.status()).toBe(200)
})
