/**
 * Governance remediation API は snake_case を返すが、プロキシや将来の層で camelCase になる可能性があるため、
 * 計画・実行ログの参照はここを経由する。
 */
import type { GovernanceRemediationDetailResponse } from '@/api/graphsuite'

const asRecord = (value: unknown): Record<string, unknown> | null => {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return value as Record<string, unknown>
  }
  return null
}

/** 詳細オブジェクト上の remediation_state（snake / camel）を 1 か所で解決 */
export function remediationDetailState(
  detail: GovernanceRemediationDetailResponse | null | undefined
): string | undefined {
  if (!detail) return undefined
  const top = detail as unknown as Record<string, unknown>
  const raw = top.remediation_state ?? top.remediationState
  if (raw === null || raw === undefined) return undefined
  const s = String(raw).trim()
  return s || undefined
}

/** remediation_mode の snake / camel 解決 */
export function remediationDetailMode(
  detail: GovernanceRemediationDetailResponse | null | undefined
): string | undefined {
  if (!detail) return undefined
  const top = detail as unknown as Record<string, unknown>
  const raw = top.remediation_mode ?? top.remediationMode
  if (raw === null || raw === undefined) return undefined
  const s = String(raw).trim()
  return s || undefined
}

/** Lambda / プロキシ差で result ブロックのキーがずれる場合の吸収 */
export function remediationResultRecord(
  detail: GovernanceRemediationDetailResponse | null
): Record<string, unknown> | null {
  if (!detail) return null
  const top = detail as unknown as Record<string, unknown>
  return (
    asRecord(detail.result)
    ?? asRecord(top.remediation_result)
    ?? asRecord(top.remediationResult)
    ?? asRecord(top.Result)
  )
}

/** actions 配列の取り出し（PascalCase 等） */
export function remediationActionsList(
  detail: GovernanceRemediationDetailResponse | null
): Array<Record<string, unknown>> {
  if (!detail) return []
  const top = detail as unknown as Record<string, unknown>
  const raw = detail.actions ?? top.Actions
  if (!Array.isArray(raw)) return []
  return raw as Array<Record<string, unknown>>
}

export function remediationResultPhase(detail: GovernanceRemediationDetailResponse | null): string {
  const r = remediationResultRecord(detail)
  if (!r) return ''
  return String(r.phase ?? r.Phase ?? '').trim()
}

export function remediationResultPostVerification(
  detail: GovernanceRemediationDetailResponse | null
): unknown {
  const r = remediationResultRecord(detail)
  if (!r) return undefined
  return r.post_verification ?? r.postVerification
}

export const pickStr = (obj: Record<string, unknown>, snake: string, camel: string): string =>
  String(obj[snake] ?? obj[camel] ?? '').trim()

export const planActionType = (action: Record<string, unknown>): string =>
  pickStr(action, 'action_type', 'actionType').toLowerCase()

export const planPermissionIds = (action: Record<string, unknown>): string[] => {
  const snake = action.permission_ids
  const camel = action.permissionIds
  const arr = Array.isArray(snake) ? snake : Array.isArray(camel) ? camel : []
  return arr.map((id) => String(id ?? '').trim()).filter(Boolean)
}

export const resultRowActionType = (row: Record<string, unknown>): string =>
  pickStr(row, 'action_type', 'actionType').toLowerCase()

export const resultRowStatus = (row: Record<string, unknown>): string =>
  pickStr(row, 'status', 'Status').toLowerCase()

export const resultRowPermissionId = (row: Record<string, unknown>): string =>
  pickStr(row, 'permission_id', 'permissionId')

export function executionResultRows(
  detail: GovernanceRemediationDetailResponse | null
): Array<Record<string, unknown>> {
  const block = remediationResultRecord(detail)
  if (!block) return []
  const rows = block.results ?? block.Results
  return Array.isArray(rows) ? (rows as Array<Record<string, unknown>>) : []
}

export function remediationDetailPlanHasRemovePermissions(
  detail: GovernanceRemediationDetailResponse | null
): boolean {
  const actions = remediationActionsList(detail)
  return actions.some((a) => planActionType(a) === 'remove_permissions')
}

export function graphPermissionDeletionResultsComplete(
  detail: GovernanceRemediationDetailResponse | null
): boolean {
  const rows = executionResultRows(detail)
  const byActionType = rows.filter((row) => resultRowActionType(row) === 'remove_permissions')
  if (byActionType.length > 0) {
    return byActionType.every((row) => ['deleted', 'not_found'].includes(resultRowStatus(row)))
  }
  if (!remediationDetailPlanHasRemovePermissions(detail)) return false
  const permRows = rows.filter((row) => resultRowPermissionId(row) !== '')
  if (permRows.length === 0) return false
  return permRows.every((row) => ['deleted', 'not_found'].includes(resultRowStatus(row)))
}
