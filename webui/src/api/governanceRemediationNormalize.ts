/**
 * Governance remediation API 応答のラップ解除・camelCase → snake 補完。
 * graphsuite から循環参照にならないよう型はここでは付けず、呼び出し側でキャストする。
 */

function remediationAliasKey(out: Record<string, unknown>, snake: string, camel: string): void {
  if (out[snake] === undefined && out[camel] !== undefined) {
    out[snake] = out[camel]
  }
}

function normalizeRemediationResultRow(row: unknown): Record<string, unknown> {
  if (!row || typeof row !== 'object' || Array.isArray(row)) {
    return row as Record<string, unknown>
  }
  const r = row as Record<string, unknown>
  const out: Record<string, unknown> = { ...r }
  remediationAliasKey(out, 'action_type', 'actionType')
  remediationAliasKey(out, 'permission_id', 'permissionId')
  remediationAliasKey(out, 'reason_code', 'reasonCode')
  remediationAliasKey(out, 'action_id', 'actionId')
  remediationAliasKey(out, 'rollback_data', 'rollbackData')
  remediationAliasKey(out, 'status', 'Status')
  remediationAliasKey(out, 'http_status', 'httpStatus')
  return out
}

function normalizeRemediationPlanAction(action: unknown): Record<string, unknown> {
  if (!action || typeof action !== 'object' || Array.isArray(action)) {
    return action as Record<string, unknown>
  }
  const r = action as Record<string, unknown>
  const out: Record<string, unknown> = { ...r }
  remediationAliasKey(out, 'action_type', 'actionType')
  remediationAliasKey(out, 'permission_ids', 'permissionIds')
  remediationAliasKey(out, 'action_id', 'actionId')
  remediationAliasKey(out, 'scope', 'Scope')
  if (out.executable === undefined && r.Executable !== undefined) {
    out.executable = r.Executable
  }
  return out
}

/** BFF / プロキシが多段ラップした場合に内側の是正詳細を取り出す */
function peelRemediationResponseEnvelope(data: unknown): unknown {
  if (Array.isArray(data) && data.length === 1) {
    return peelRemediationResponseEnvelope(data[0])
  }
  if (Array.isArray(data) && data.length > 1) {
    const withFinding = data.find(
      (item) =>
        item &&
        typeof item === 'object' &&
        !Array.isArray(item) &&
        (((item as Record<string, unknown>).finding_id !== undefined) ||
          ((item as Record<string, unknown>).findingId !== undefined))
    )
    if (withFinding) return peelRemediationResponseEnvelope(withFinding)
  }
  if (!data || typeof data !== 'object' || Array.isArray(data)) return data
  const r = data as Record<string, unknown>
  if (r.finding_id !== undefined || r.findingId !== undefined) return data

  const unwrapKeys = [
    'remediation_detail',
    'remediationDetail',
    'remediation',
    'data',
    'payload',
    'body',
    'detail',
    // 一部 BFF / ゲートウェイが詳細を result に包む（トップに finding_id が無いときのみ到達）
    'result',
    'Result',
  ] as const
  for (const k of unwrapKeys) {
    const inner = r[k]
    if (inner && typeof inner === 'object' && !Array.isArray(inner)) {
      const peeled = peelRemediationResponseEnvelope(inner)
      const pr = peeled as Record<string, unknown>
      if (pr.finding_id !== undefined || pr.findingId !== undefined) return peeled
    }
  }

  const bodyObj = r.body
  if (bodyObj && typeof bodyObj === 'object' && !Array.isArray(bodyObj)) {
    const peeled = peelRemediationResponseEnvelope(bodyObj)
    const pr = peeled as Record<string, unknown>
    if (pr.finding_id !== undefined || pr.findingId !== undefined) return peeled
  }

  const body = r.body
  if (typeof body === 'string' && body.trim().startsWith('{')) {
    try {
      const parsed = JSON.parse(body) as unknown
      return peelRemediationResponseEnvelope(parsed)
    } catch {
      /* ignore */
    }
  }
  return data
}

export function normalizeGovernanceRemediationDetailResponse(data: unknown): unknown {
  let peeled: unknown = peelRemediationResponseEnvelope(data)
  while (Array.isArray(peeled) && peeled.length === 1) {
    peeled = peeled[0]
  }
  if (!peeled || typeof peeled !== 'object' || Array.isArray(peeled)) {
    return peeled
  }
  const raw = peeled as Record<string, unknown>
  const out: Record<string, unknown> = { ...raw }

  remediationAliasKey(out, 'tenant_id', 'tenantId')
  remediationAliasKey(out, 'finding_id', 'findingId')
  remediationAliasKey(out, 'remediation_state', 'remediationState')
  remediationAliasKey(out, 'remediation_mode', 'remediationMode')
  remediationAliasKey(out, 'auto_execute_skipped', 'autoExecuteSkipped')
  remediationAliasKey(out, 'auto_execute_skip_reason', 'autoExecuteSkipReason')
  remediationAliasKey(out, 'actions', 'Actions')
  remediationAliasKey(out, 'allowed_actions', 'allowedActions')
  remediationAliasKey(out, 'last_error', 'lastError')
  remediationAliasKey(out, 'updated_at', 'updatedAt')
  remediationAliasKey(out, 'execution_id', 'executionId')
  remediationAliasKey(out, 'approved_by', 'approvedBy')
  remediationAliasKey(out, 'approved_at', 'approvedAt')
  remediationAliasKey(out, 'last_execution_id', 'lastExecutionId')
  remediationAliasKey(out, 'proposal_generated', 'proposalGenerated')
  remediationAliasKey(out, 'exception_type', 'exceptionType')
  remediationAliasKey(out, 'exception_review_due_at', 'exceptionReviewDueAt')
  remediationAliasKey(out, 'exception_approved_by', 'exceptionApprovedBy')
  remediationAliasKey(out, 'exception_ticket', 'exceptionTicket')
  remediationAliasKey(out, 'exception_scope_hash', 'exceptionScopeHash')
  remediationAliasKey(out, 'exception_registered', 'exceptionRegistered')
  remediationAliasKey(out, 'rollback_id', 'rollbackId')

  const resIn = raw.result ?? raw.remediationResult ?? raw.Result
  if (resIn && typeof resIn === 'object' && !Array.isArray(resIn)) {
    const rb: Record<string, unknown> = { ...(resIn as Record<string, unknown>) }
    remediationAliasKey(rb, 'results', 'Results')
    remediationAliasKey(rb, 'phase', 'Phase')
    remediationAliasKey(rb, 'post_verification', 'postVerification')
    remediationAliasKey(rb, 'advisory_steps_skipped', 'advisoryStepsSkipped')
    remediationAliasKey(rb, 'manual_required', 'manualRequired')
    const rows = rb.results
    if (Array.isArray(rows)) {
      rb.results = rows.map((row) => normalizeRemediationResultRow(row))
    }
    out.result = rb
  }

  if (Array.isArray(out.actions)) {
    out.actions = (out.actions as unknown[]).map((a) => normalizeRemediationPlanAction(a))
  }

  return out
}
