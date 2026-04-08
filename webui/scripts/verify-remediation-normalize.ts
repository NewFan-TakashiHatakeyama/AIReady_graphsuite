/**
 * 正規化の契約検証（Vitest 無し）。CI/ローカル: cd webui && npx --yes tsx scripts/verify-remediation-normalize.ts
 */
import { normalizeGovernanceRemediationDetailResponse } from '../src/api/governanceRemediationNormalize'
import { remediationDetailState } from '../src/features/governance/remediationPayloadFields'

function assert(cond: boolean, msg: string): void {
  if (!cond) throw new Error(`verify-remediation-normalize: ${msg}`)
}

const wrapped = normalizeGovernanceRemediationDetailResponse({
  result: {
    finding_id: 'f-envelope',
    tenant_id: 't1',
    remediation_state: 'approved',
  },
}) as Record<string, unknown>
assert(wrapped.finding_id === 'f-envelope', 'result envelope: finding_id')
assert(wrapped.remediation_state === 'approved', 'result envelope: remediation_state')

const flat = normalizeGovernanceRemediationDetailResponse({
  finding_id: 'f-flat',
  remediation_state: 'executed',
  result: { phase: 'apply', results: [] },
}) as Record<string, unknown>
assert(flat.finding_id === 'f-flat', 'flat detail: finding_id')
const inner = flat.result as Record<string, unknown>
assert(inner.phase === 'apply', 'flat detail: execution result.phase preserved')

const camelOnly = normalizeGovernanceRemediationDetailResponse({
  findingId: 'f-camel',
  tenantId: 't1',
  remediationState: 'executed',
  allowedActions: ['rollback'],
}) as Record<string, unknown>
assert(camelOnly.remediation_state === 'executed', 'camel approve body: remediation_state aliased')
assert(remediationDetailState(camelOnly as never) === 'executed', 'remediationDetailState reads camel')

console.log('verify-remediation-normalize: ok')
