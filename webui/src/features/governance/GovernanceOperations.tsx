/* eslint-disable react/prop-types */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  DndContext,
  DragOverlay,
  PointerSensor,
  closestCenter,
  useSensor,
  useSensors,
  type DragEndEvent,
  type DragOverEvent,
  type DragStartEvent
} from '@dnd-kit/core'
import { SortableContext, arrayMove, useSortable, verticalListSortingStrategy } from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'
import Badge, { BadgeProps } from '@/components/ui/Badge'
import Button from '@/components/ui/Button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/Card'
import Checkbox from '@/components/ui/Checkbox'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/Dialog'
import Input from '@/components/ui/Input'
import { Label } from '@/components/ui/Label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/Select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/Table'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/Tooltip'
import FeatureOnboardingPanel from '@/features/common/FeatureOnboardingPanel'
import InitialScoringGateCard, {
  INITIAL_SCORING_GATE_DEFAULT_DESCRIPTION
} from '@/features/common/InitialScoringGateCard'
import HoverHelpLabel from '@/features/common/HoverHelpLabel'
import TablePageControls from '@/features/common/TablePageControls'
import { takeOperationDeepLinkForTab } from '@/features/common/operationDeepLink'
import { cn } from '@/lib/utils'
import {
  GovernanceExceptionRegistrationRequest,
  GovernanceRemediationDetailResponse,
  GovernanceFindingApiRow,
  GovernanceOverviewResponse,
  GovernancePoliciesResponse,
  GovernanceScanJobApiRow,
  GovernanceSuppressionApiRow,
  approveGovernanceFindingRemediation,
  createGovernancePolicy,
  getGovernanceFindingRemediation,
  getGovernanceFindings,
  getGovernanceOverview,
  getGovernancePolicies,
  getGovernanceScanJobs,
  getGovernanceSuppressions,
  markGovernanceFindingCompleted,
  registerGovernanceFindingException,
  rollbackGovernanceFindingRemediation,
  runGovernanceDailyScan,
  simulateGovernancePolicy,
  updateGovernancePolicy
} from '@/api/graphsuite'
import OperationConfirmDialog from './components/OperationConfirmDialog'
import RemediationWorkflowPanel from './components/RemediationWorkflowPanel'
import ReadinessBreakdownD3 from './ReadinessBreakdownD3'
import {
  FindingStatus,
  GovernanceFinding,
  GovernanceOverviewStats,
  GovernancePageKey,
  GovernancePolicy,
  GovernanceScanJob,
  GovernanceSuppression,
  RemediationState
} from './types'
import { toast } from 'sonner'

const GOVERNANCE_PAGES: Array<{ key: GovernancePageKey; label: string }> = [
  { key: 'overview', label: '概要' },
  { key: 'findings', label: '検知結果' },
  { key: 'suppression', label: '例外レジストリ' },
  { key: 'jobs', label: '実行ジョブ' },
  { key: 'policies', label: 'ポリシー設定' },
  { key: 'help', label: 'ヘルプ' }
]

const GOVERNANCE_LOCKED_UNTIL_SCAN_COMPLETE: GovernancePageKey[] = [
  'overview',
  'findings',
  'suppression'
]

const GOVERNANCE_PAGE_GUIDE: Record<GovernancePageKey, string> = {
  overview: 'リスク全体像を確認します。未対応件数と高リスク件数を優先して確認します。',
  findings: '検知結果を確認し、対応優先度を判断します。',
  suppression: '期限付き例外の状態を管理し、失効漏れを防ぎます。',
  jobs: 'スキャンジョブの実行状態と結果を確認します。',
  policies: '判定基準（閾値）を確認し、検知精度を調整します。',
  help: 'ガバナンスの操作手順と主要用語を確認します。'
}

const GOVERNANCE_GLOSSARY = [
  { term: 'ガバナンススコア', description: 'raw_residual_risk 由来の総合スコア（0..100）です。' },
  { term: 'Coverage / Confidence', description: 'スキャン網羅率と信頼度を分離表示する運用指標です。' },
  { term: 'workflow_status', description: '表示上の運用状態です。acknowledged でも評価は継続します。' },
  { term: 'exception_type', description: '例外運用の種別（temporary_accept など）です。' },
  { term: 'グローバルポリシー', description: '全社共通で適用する最低基準（共通ガードレール）です。' },
  { term: 'スコープ別ポリシー', description: '部門・サイト・ラベル条件で上書き適用する限定ルールです。' },
  { term: 'Finding', description: 'スキャン結果として要確認になった検知レコードです。' },
  { term: '例外レジストリ', description: '即時是正が難しい案件を期限付きで一時管理する台帳です。' },
  { term: '実行ジョブ', description: 'スキャン・再評価などの処理実行状況と結果の証跡です。' }
]

const GOVERNANCE_TAB_HELP: Record<GovernancePageKey, string> = {
  overview: 'ガバナンスの主要KPIを確認します。',
  findings: '検知結果と優先度を確認します。',
  suppression: '期限付き例外の状態を確認します。',
  jobs: 'スキャンジョブの実行状態を確認します。',
  policies: '判定ルールと閾値を確認します。',
  help: '使い方と用語を確認します。'
}

const badgeVariantByValue = (value: string): BadgeProps['variant'] => {
  if (value === 'failed') return 'destructive'
  if (value === 'open' || value === 'acknowledged' || value === 'running' || value === 'skipped') {
    return 'secondary'
  }
  return 'outline'
}

const badgeClassByValue = (value: string): string => {
  // Risk labels
  if (value === 'critical') return 'border-rose-400 bg-rose-100 text-rose-900'
  if (value === 'high') return 'border-red-300 bg-red-100 text-red-800'
  if (value === 'medium') return 'border-amber-300 bg-amber-100 text-amber-900'
  if (value === 'low') return 'border-emerald-300 bg-emerald-100 text-emerald-800'
  if (value === 'none') return 'border-slate-300 bg-slate-100 text-slate-800'

  // Finding / suppression statuses
  if (value === 'open' || value === 'new') return 'border-red-300 bg-red-100 text-red-800'
  if (value === 'acknowledged') return 'border-amber-300 bg-amber-100 text-amber-900'
  if (value === 'completed' || value === 'remediated') return 'border-sky-300 bg-sky-100 text-sky-900'
  if (value === 'closed' || value === 'success') return 'border-emerald-300 bg-emerald-100 text-emerald-800'
  if (value === 'ai_proposed' || value === 'pending_approval' || value === 'pending') {
    return 'border-amber-300 bg-amber-100 text-amber-900'
  }
  if (value === 'approved' || value === 'executed') return 'border-emerald-300 bg-emerald-100 text-emerald-800'
  if (value === 'rejected') return 'border-red-300 bg-red-100 text-red-800'

  // Job statuses
  if (value === 'running') return 'border-sky-300 bg-sky-100 text-sky-900'
  if (value === 'partial') return 'border-amber-300 bg-amber-100 text-amber-900'
  if (value === 'skipped') return 'border-slate-300 bg-slate-100 text-slate-800'
  if (value === 'failed') return 'border-red-300 bg-red-100 text-red-800'

  return ''
}

const normalizeFindingStatus = (raw?: string): FindingStatus => {
  const s = String(raw ?? 'open').trim().toLowerCase()
  if (s === 'remediated') return 'completed'
  const allowed: FindingStatus[] = ['new', 'open', 'completed', 'closed', 'acknowledged']
  if (allowed.includes(s as FindingStatus)) return s as FindingStatus
  return 'open'
}

const governanceFindingStatusLabel = (status: FindingStatus): string => {
  if (status === 'completed') return '完了'
  return status
}

const riskLabel = (score: number): string => {
  if (score >= 80) return 'critical'
  if (score >= 55) return 'high'
  if (score >= 30) return 'medium'
  return 'low'
}

const resolveFindingRiskLevel = (finding: GovernanceFinding): 'critical' | 'high' | 'medium' | 'low' => {
  const rawLevel = String(finding.riskLevel ?? '').trim().toLowerCase()
  if (rawLevel === 'critical' || rawLevel === 'high' || rawLevel === 'medium' || rawLevel === 'low') {
    return rawLevel
  }
  return riskLabel(finding.riskScore) as 'critical' | 'high' | 'medium' | 'low'
}

const formatRiskScoreDisplay = (score: number): string => {
  return score > 0 ? score.toFixed(1) : ''
}

const remediationStateText = (state: RemediationState): string => {
  if (state === 'ai_proposed' || state === 'pending_approval') return 'AI提案中'
  if (state === 'approved') return '承認済み'
  if (state === 'failed') return '失敗'
  if (state === 'manual_required') return '手動対応'
  return '実行済み'
}

const toDateTimeLocalValue = (date: Date): string => {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  const hours = String(date.getHours()).padStart(2, '0')
  const minutes = String(date.getMinutes()).padStart(2, '0')
  return `${year}-${month}-${day}T${hours}:${minutes}`
}

const buildDefaultExceptionReviewDueAt = (): string => {
  const now = new Date()
  const plus14Days = new Date(now.getTime() + 14 * 24 * 60 * 60 * 1000)
  return toDateTimeLocalValue(plus14Days)
}

const exceptionTypeLabel = (value?: string): string => {
  const normalized = String(value ?? 'none').toLowerCase()
  if (normalized === 'temporary_accept') return '期限付き許容'
  if (normalized === 'permanent_accept') return '恒久許容'
  if (normalized === 'compensating_control') return '代替統制あり'
  if (normalized === 'false_positive') return '誤検知'
  return 'なし'
}

const workflowStatusLabel = (value?: string): string => {
  const normalized = String(value ?? 'normal').toLowerCase()
  if (normalized === 'acknowledged') return 'acknowledged（例外運用中）'
  if (normalized === 'none') return 'none'
  return 'normal'
}

const resolveTargetKind = (row: GovernanceFindingApiRow): 'file' | 'folder' | 'unknown' => {
  const explicit = String(row.target_type ?? '').toLowerCase()
  if (explicit === 'file' || explicit === 'folder') return explicit

  const containerType = String(row.container_type ?? '').toLowerCase()
  if (containerType.includes('folder') || containerType.includes('directory')) return 'folder'
  if (containerType.includes('file') || containerType.includes('document')) return 'file'

  const metadata = row.source_metadata
  const metadataObj =
    typeof metadata === 'string'
      ? (() => {
        try {
          const parsed = JSON.parse(metadata)
          return typeof parsed === 'object' && parsed !== null ? (parsed as Record<string, unknown>) : {}
        } catch {
          return {}
        }
      })()
      : (metadata ?? {})

  if (metadataObj.is_folder === true) return 'folder'
  if (metadataObj.is_file === true) return 'file'

  const itemName = String(row.item_name ?? '')
  if (itemName.endsWith('/')) return 'folder'
  if (itemName.includes('.') && !itemName.endsWith('.')) return 'file'
  return 'unknown'
}

const targetKindLabel = (kind: 'file' | 'folder' | 'unknown'): string => {
  if (kind === 'file') return 'ファイル'
  if (kind === 'folder') return 'フォルダ'
  return '不明'
}

const aiProposalSummary = (finding: GovernanceFinding): string => {
  const vectors = finding.exposureVectors ?? []
  const hasPublicLink = vectors.includes('public_link')
  const hasExternalShare = vectors.includes('guest') || vectors.includes('external_domain')
  const hasOrganizationWide = vectors.includes('org_link') || vectors.includes('all_users')
  if (hasPublicLink || hasExternalShare) {
    return '公開リンクを「組織内のみ」または「特定ユーザーのみ」に制限し、外部共有を段階的に遮断する提案です。'
  }
  if (hasOrganizationWide) {
    return '組織全体に広がる共有を見直し、業務上必要な利用者のみへアクセスを限定する提案です。'
  }
  return '対象ファイルの共有設定を最小権限へ是正し、過剰共有リスクを低減する提案です。'
}

const CONTENT_CATEGORY_LABELS: Record<string, string> = {
  payroll: '給与情報',
  customer_list: '顧客リスト',
  customer_data: '顧客データ',
  hr_evaluation: '人事評価情報',
  executive_confidential: '経営機密',
  legal_contract: '法務契約情報',
  financial_statement_draft: '財務諸表ドラフト',
  personnel_evaluation: '人事評価情報',
  pii: '個人情報（PII）',
  secret: '機密情報（Secret）'
}

const SENSITIVITY_LEVEL_LABELS: Record<string, string> = {
  none: 'なし',
  low: '低',
  medium: '中',
  high: '高',
  critical: '重大'
}

const EXPOSURE_VECTOR_LABELS: Record<string, string> = {
  public_link: '公開リンク事故',
  guest_direct_share: '外部直接共有事故',
  external_email_direct_share: '外部直接共有事故（メール）',
  external_domain_share: '外部ドメイン共有事故',
  external_domain: '外部ドメイン共有',
  guest: 'ゲスト共有',
  all_users: '全社共有事故',
  org_link: '組織内リンク共有',
  org_link_view: '組織内閲覧リンク共有',
  org_link_edit: '組織内編集リンク共有',
  org_link_editable: '組織内編集リンク共有',
  excessive_permissions: '過剰権限リスク'
}

const documentSignalSummary = (
  finding: GovernanceFinding
): {
  categories: string[]
  sensitivityLevel: string
  piiText: string
  secretText: string
} => {
  const signals = finding.contentSignals
  const categories = (signals?.docCategories ?? []).filter(Boolean)
  const categoryLabels = categories.map((category) => {
    const key = String(category).trim().toLowerCase()
    return CONTENT_CATEGORY_LABELS[key] ?? key
  })
  const sensitivityKey = String(signals?.docSensitivityLevel ?? 'none').trim().toLowerCase()
  return {
    categories: categoryLabels,
    sensitivityLevel: SENSITIVITY_LEVEL_LABELS[sensitivityKey] ?? sensitivityKey,
    piiText: signals?.containsPii ? 'あり' : 'なし',
    secretText: signals?.containsSecret ? 'あり' : 'なし'
  }
}

const sharingVectorSummary = (finding: GovernanceFinding): string[] => {
  const vectors = (finding.exposureVectors ?? []).filter(Boolean)
  return vectors.map((vector) => {
    const key = String(vector).trim().toLowerCase()
    return EXPOSURE_VECTOR_LABELS[key] ?? key
  })
}

const impactSummary = (finding: GovernanceFinding, executed: boolean): string => {
  const vectors = finding.exposureVectors ?? []
  const hasPublicLink = vectors.includes('public_link')
  const hasExternalShare = vectors.includes('guest') || vectors.includes('external_domain')
  if (executed) {
    if (hasPublicLink || hasExternalShare) {
      return '公開範囲制限が反映済みで、外部共有リスクは低減済みです。'
    }
    return '最小権限化が反映済みで、過剰共有リスクは抑制済みです。'
  }

  if (hasPublicLink || hasExternalShare) {
    return '適用されると、公開リンク露出が低下し外部共有リスクの縮小が見込まれます。'
  }
  return '適用されると、共有先の過多が解消されリスクスコア低下が見込まれます。'
}

const suggestedAction = (finding: GovernanceFinding): string => {
  const remediationMode = String(finding.remediationMode ?? '').trim().toLowerCase()
  const vectors = (finding.exposureVectors ?? []).filter(Boolean).map((value) => String(value).trim().toLowerCase())
  if (['manual', 'owner_review', 'recommend_only'].includes(remediationMode)) return '手動レビュー結果を記録'
  if (vectors.includes('public_link')) return '公開リンクを無効化し、特定ユーザー共有へ切り替え'
  if (vectors.includes('guest')) return '外部ゲスト共有の業務妥当性を確認し、不要分を削除'
  if (vectors.length === 0) return '共有設定の変更は不要。監視継続/定期再評価を実施'
  return '共有先の最小権限化を適用し、再評価結果を確認'
}

const transitionReason = (finding: GovernanceFinding): string => {
  if (finding.status === 'new') return '初回検知で new として登録'
  if (finding.status === 'open') return '再評価でリスク閾値以上のため open を維持'
  if (finding.workflowStatus === 'acknowledged') return 'workflow_status=acknowledged として例外運用中（評価は継続）'
  if (finding.status === 'completed') return '是正が完了し、ライフサイクルは完了（completed）へ遷移'
  return 'リスク閾値未満または対象解消により closed へ遷移'
}

const executionResultRows = (
  detail: GovernanceRemediationDetailResponse | null
): Array<Record<string, unknown>> => {
  const rows = detail?.result?.results
  return Array.isArray(rows) ? (rows as Array<Record<string, unknown>>) : []
}

const remediationExecutionSummary = (detail: GovernanceRemediationDetailResponse | null): string => {
  if (String(detail?.result?.phase ?? '') === 'rollback') {
    const rows = executionResultRows(detail)
    if (!rows.length) return 'ロールバック結果はまだ記録されていません。'
    const restored = rows.filter((row) => String(row.status ?? '') === 'restored').length
    const manualRequired = rows.filter((row) => String(row.status ?? '') === 'manual_required').length
    const summaryParts = [
      restored > 0 ? `復元 ${restored}` : '',
      manualRequired > 0 ? `手動対応 ${manualRequired}` : ''
    ].filter(Boolean)
    if (!summaryParts.length) return 'ロールバック結果は記録されています。'
    return `ロールバック結果: ${summaryParts.join(' / ')}`
  }
  const rows = executionResultRows(detail)
  if (!rows.length) {
    return detail?.remediation_state === 'manual_required'
      ? '自動適用可能な是正アクションがなく、運用者による手動対応が必要です。'
      : '是正実行結果はまだ記録されていません。'
  }
  const countBy = (status: string) => rows.filter((row) => String(row.status ?? '') === status).length
  const deleted = countBy('deleted')
  const notFound = countBy('not_found')
  const manualRequired = countBy('manual_required')
  const skipped = countBy('skipped')

  const summaryParts = [
    deleted > 0 ? `削除 ${deleted}` : '',
    notFound > 0 ? `対象なし ${notFound}` : '',
    manualRequired > 0 ? `手動対応 ${manualRequired}` : '',
    skipped > 0 ? `スキップ ${skipped}` : ''
  ].filter(Boolean)

  if (!summaryParts.length) return '実行結果は記録されています。'
  return `是正処理の適用結果: ${summaryParts.join(' / ')}`
}

const remediationPostVerificationDeferredFootnote = (
  detail: GovernanceRemediationDetailResponse | null
): string | null => {
  const pv = detail?.result?.post_verification
  if (pv === undefined || pv === null) return null
  if (typeof pv === 'string') {
    if (pv === 'deferred_to_next_batch_scoring') {
      return '※ リスク再評価は次回のバッチスコアリングで反映されます。'
    }
    return null
  }
  if (typeof pv === 'object' && !Array.isArray(pv)) {
    const rec = pv as Record<string, unknown>
    if (String(rec.deferred_to ?? '').trim() === 'connect_filemetadata_stream') {
      return '※ リスク再評価は接続のメタデータ更新後、DynamoDB Streams 経由の再計算で反映されます。'
    }
  }
  return null
}

const remediationManualReasonCodes = (detail: GovernanceRemediationDetailResponse | null): string[] => {
  const rows = executionResultRows(detail)
  const reasonCodes = rows
    .filter((row) => String(row.status ?? '') === 'manual_required')
    .map((row) => String(row.reason ?? '').trim())
    .filter(Boolean)
  return Array.from(new Set(reasonCodes))
}

const remediationManualReasonText = (reasonCode: string): string => {
  const code = reasonCode.trim().toLowerCase()
  if (code === 'sensitivity_label_id_unresolved') {
    return '感度ラベルIDが解決できないため、手動でラベル適用が必要です。'
  }
  if (code === 'label_automation_disabled') {
    return 'コスト制御ポリシーにより、ラベル自動化が無効化されています。'
  }
  if (code === 'label_automation_outside_batch_window') {
    return 'コスト制御ポリシーにより、ラベル自動化のバッチ許可時間外です。'
  }
  if (code === 'label_automation_daily_limit_exceeded') {
    return 'コスト制御ポリシーにより、ラベル自動化の日次上限件数を超過しました。'
  }
  if (code === 'label_automation_quota_check_failed') {
    return 'コスト制御のクォータ確認に失敗したため、安全側で手動対応へ縮退しました。'
  }
  if (code === 'rollback_data_missing') {
    return '復元元データが不足しているため、自動ロールバックできませんでした。'
  }
  if (code === 'rollback_email_missing') {
    return '復元対象ユーザーのメール情報が不足しているため、自動ロールバックできませんでした。'
  }
  if (code === 'label_rollback_not_supported') {
    return 'ラベル適用の自動ロールバックは未対応のため、手動で戻し作業が必要です。'
  }
  return '運用ポリシーまたは実行環境の制約により、手動対応が必要です。'
}

const remediationManualChecklist = (
  detail: GovernanceRemediationDetailResponse | null,
  finding: GovernanceFinding
): string[] => {
  const actionTypes = new Set(
    (detail?.actions ?? [])
      .map((action) => String(action?.action_type ?? '').trim().toLowerCase())
      .filter(Boolean)
  )
  const checklist: string[] = []
  if (actionTypes.has('manual_review') || !actionTypes.size) {
    checklist.push('対象ファイルの共有先（内部/外部）と権限をレビューし、不要な共有を削除する。')
  }
  if (actionTypes.has('apply_sensitivity_label')) {
    checklist.push('M365 側で感度ラベルを適用し、必要に応じて外部共有ポリシーを制限する。')
  }
  if ((finding.exposureVectors ?? []).includes('guest')) {
    checklist.push('外部ゲスト共有が業務上不要であればアクセス権を剥奪する。')
  }
  if ((finding.exposureVectors ?? []).includes('public_link')) {
    checklist.push('公開リンクを無効化し、特定ユーザー共有へ切り替える。')
  }
  checklist.push('対応後に再スキャン/再評価し、リスクスコアと是正状態の更新を確認する。')
  return Array.from(new Set(checklist))
}

const normalizeRemediationStateForDisplay = (
  status: FindingStatus,
  workflowStatus: string,
  remediationStateRaw?: string | null
): RemediationState => {
  const normalizedRaw = String(remediationStateRaw ?? '').trim().toLowerCase()
  const normalizedStatus = String(status).trim().toLowerCase()
  const normalizedWorkflow = String(workflowStatus ?? '').trim().toLowerCase()

  // Finding lifecycle is the source of truth for terminal states.
  if (normalizedStatus === 'closed' || normalizedStatus === 'completed' || normalizedStatus === 'remediated') {
    return 'executed'
  }
  if (
    normalizedRaw === 'ai_proposed' ||
    normalizedRaw === 'pending_approval' ||
    normalizedRaw === 'approved' ||
    normalizedRaw === 'executed' ||
    normalizedRaw === 'failed' ||
    normalizedRaw === 'manual_required'
  ) {
    return normalizedRaw as RemediationState
  }
  if (normalizedWorkflow === 'acknowledged') return 'approved'
  return 'ai_proposed'
}

const decodeUtf8DisplayText = (value: string): string => {
  let current = String(value ?? '').trim()
  if (!current) return ''
  for (let i = 0; i < 3; i += 1) {
    try {
      const decoded = decodeURIComponent(current)
      if (decoded === current) break
      current = decoded
    } catch {
      break
    }
  }
  return current
}

const resolveDocumentName = (row: GovernanceFindingApiRow): string => {
  const itemName = decodeUtf8DisplayText(String(row.item_name ?? ''))
  if (itemName) return itemName

  const rawPath = decodeUtf8DisplayText(String(row.item_url ?? ''))
  if (!rawPath) return String(row.item_id ?? '-')
  const normalized = rawPath.replace(/\\/g, '/')
  const parts = normalized.split('/').filter(Boolean)
  return parts.length > 0 ? parts[parts.length - 1] : String(row.item_id ?? '-')
}

const toGovernanceFindingFromApi = (row: GovernanceFindingApiRow): GovernanceFinding => {
  const status = normalizeFindingStatus(row.status)
  const workflowStatus = row.workflow_status ?? (status === 'acknowledged' ? 'acknowledged' : 'normal')
  const remediationState: RemediationState = normalizeRemediationStateForDisplay(
    status as FindingStatus,
    workflowStatus,
    row.remediation_state ?? null
  )
  const approvalState = remediationState === 'ai_proposed' ? 'pending' : 'approved'
  const riskScore = typeof row.risk_score === 'number' ? row.risk_score : 0
  const displayPathRaw = row.item_url || [row.container_name, row.item_name].filter(Boolean).join('/') || row.item_id
  const displayPath = decodeUtf8DisplayText(String(displayPathRaw ?? ''))
  const documentName = resolveDocumentName(row)

  return {
    id: row.finding_id,
    planId: `plan-${row.finding_id.slice(0, 8)}`,
    remediationState,
    approvalState,
    riskScore,
    status,
    riskLevel: (row.risk_level === 'none' ? 'low' : row.risk_level) ?? (riskLabel(riskScore) as GovernanceFinding['riskLevel']),
    rawResidualRisk: typeof row.raw_residual_risk === 'number' ? row.raw_residual_risk : undefined,
    workflowStatus: workflowStatus as GovernanceFinding['workflowStatus'],
    exceptionType: (row.exception_type ?? 'none') as GovernanceFinding['exceptionType'],
    exceptionReviewDueAt: row.exception_review_due_at ?? row.suppress_until ?? null,
    ageFactor: typeof row.age_factor === 'number' ? row.age_factor : undefined,
    exceptionFactor: typeof row.exception_factor === 'number' ? row.exception_factor : undefined,
    assetCriticality: typeof row.asset_criticality === 'number' ? row.asset_criticality : undefined,
    scanConfidence: typeof row.scan_confidence === 'number' ? row.scan_confidence : undefined,
    aiReachability: typeof row.ai_reachability === 'number' ? row.ai_reachability : undefined,
    targetKind: resolveTargetKind(row),
    source: row.source || 'm365',
    itemPath: displayPath,
    matchedGuards: row.matched_guards ?? [],
    guardReasonCodes: row.guard_reason_codes ?? [],
    detectionReasons: row.detection_reasons ?? [],
    findingEvidence: row.finding_evidence ?? undefined,
    lastEvaluatedAt: row.last_evaluated_at ?? '-',
    tenantId: row.tenant_id,
    itemId: row.item_id,
    itemName: documentName,
    itemUrl: row.item_url,
    exposureVectors: row.exposure_vectors ?? [],
    contentSignals: {
      docSensitivityLevel: String(row.content_signals?.doc_sensitivity_level ?? 'none'),
      docCategories: Array.isArray(row.content_signals?.doc_categories)
        ? row.content_signals?.doc_categories ?? []
        : [],
      containsPii: Boolean(row.content_signals?.contains_pii ?? false),
      containsSecret: Boolean(row.content_signals?.contains_secret ?? false)
    },
    decision: String(row.decision ?? 'review'),
    effectivePolicyId: String(row.effective_policy_id ?? ''),
    effectivePolicyVersion: Number(row.effective_policy_version ?? 1),
    matchedPolicyIds: Array.isArray(row.matched_policy_ids) ? row.matched_policy_ids : [],
    decisionTrace: Array.isArray(row.decision_trace) ? row.decision_trace : [],
    reasonCodes: Array.isArray(row.reason_codes) ? row.reason_codes : [],
    remediationMode: String(row.remediation_mode ?? 'manual'),
    remediationAction: String(row.remediation_action ?? 'request_review'),
    decisionSource: String(row.decision_source ?? 'fallback'),
    expectedAudience: String(row.expected_audience ?? 'internal_need_to_know'),
    expectedDepartment: String(row.expected_department ?? 'unknown'),
    expectationGapReason: String(row.expectation_gap_reason ?? ''),
    expectationGapScore: typeof row.expectation_gap_score === 'number' ? row.expectation_gap_score : 0,
    sharingScope: row.sharing_scope,
    suppressUntil: row.suppress_until ?? null,
    acknowledgedReason: row.acknowledged_reason ?? null,
    remediationVersion: row.remediation_version,
    remediationLastError: row.remediation_last_error ?? null
  }
}

const toGovernanceScanJobFromApi = (row: GovernanceScanJobApiRow): GovernanceScanJob => ({
  jobId: row.job_id,
  jobType: 'batch_scoring',
  status:
    row.status === 'accepted' ? 'queued' :
      row.status === 'running' ? 'running' :
        row.status === 'partial' ? 'partial' :
          row.status === 'failed' ? 'failed' : 'success',
  startedAt: row.accepted_at ?? '-',
  endedAt: row.status === 'running' || row.status === 'partial' ? null : row.accepted_at ?? null,
  message:
    row.status === 'partial'
      ? '未処理が残っているため継続実行を予定'
      : row.source ? `source=${row.source}` : 'governance daily scan'
})

const GUARD_DICTIONARY: Record<string, { name: string; condition: string; relatedPolicy: string }> = {
  G1: {
    name: '基本共有レビュー',
    condition: '共有状態の定期点検対象として抽出',
    relatedPolicy: 'グローバルポリシー: 共有リンク既定値'
  },
  G2: {
    name: '過剰権限ガード',
    condition: '継承崩れ・過剰付与・全社共有の組み合わせを検知',
    relatedPolicy: 'グローバルポリシー: RiskScore閾値 / スコープ別ポリシー: 権限制限'
  },
  G3: {
    name: '公開リンクガード',
    condition: 'Anyoneリンクまたは公開リンクを検知',
    relatedPolicy: 'グローバルポリシー: 共有リンク既定値'
  }
}

// Terminology style guide:
// - "事故": already exposed incident pattern (external/public/company-wide exposure)
// - "リスク": latent risk pattern (misconfiguration, missing controls, drift)
const DETECTION_REASON_LABELS: Record<string, string> = {
  scenario_a_org_overshare: 'A: 社内過剰共有リスク',
  scenario_b_external_direct_share: 'B: 外部直接共有事故',
  scenario_c_public_link: 'C: 公開リンク事故'
}

const GUARD_REASON_LABELS: Record<string, string> = {
  g3_public_link: 'G3-公開リンク事故',
  g3_external_direct_share: 'G3-外部直接共有事故',
  g3_org_link_editable: 'G3-組織内編集リンクリスク',
  g7_no_label: 'G7-ラベル未付与リスク',
  g7_acl_drift: 'G7-権限ドリフトリスク'
}

const reasonPriorityWeight = (detectionReasons: string[] = []): number => {
  if (detectionReasons.includes('scenario_c_public_link')) return 3
  if (detectionReasons.includes('scenario_b_external_direct_share')) return 2
  if (detectionReasons.includes('scenario_a_org_overshare')) return 1
  return 0
}

const findingPriorityScore = (
  riskScore: number,
  matchedGuardsCount: number,
  status: FindingStatus,
  detectionReasons: string[] = []
): number => {
  const statusWeight =
    status === 'open' ? 1.4 :
      status === 'new' ? 1.2 :
        status === 'acknowledged' ? 0.9 :
          status === 'completed' ? 0.35 : 0.4
  return Number((riskScore * 10 * statusWeight + matchedGuardsCount * 1.5 + reasonPriorityWeight(detectionReasons) * 10).toFixed(1))
}

const relatedJobForFinding = (findingId: string, scanJobs: GovernanceScanJob[]): GovernanceScanJob => {
  if (scanJobs.length === 0) {
    return {
      jobId: 'job-unavailable',
      jobType: 'batch_scoring',
      status: 'skipped',
      startedAt: '-',
      endedAt: null,
      message: 'scan job unavailable'
    }
  }
  const numeric = Number(findingId.replace(/\D/g, ''))
  const base = Number.isNaN(numeric) ? 0 : numeric
  return scanJobs[base % scanJobs.length]
}

type GovernanceProtectionViewModel = {
  components: Array<{
    key: string
    label: string
    score: number
    details: Array<{
      label: string
      score: number
      note?: string
      evidence?: Array<{
        itemId: string
        itemName: string
        itemUrl: string
        status: string
        reason: string
        impactPoints: number
      }>
    }>
  }>
  governanceScore: number
  coverageScore: number
  confidenceLevel: 'High' | 'Medium' | 'Low'
  confidenceScore: number
  riskSummary: {
    governanceRaw: number
    exceptionDebt: number
    coveragePenalty: number
  }
  coverageBreakdown: Array<{ key: string; label: string; value: number }>
  scaleMax: number
}

const normalizeDisplayScore = (value: number): number => {
  if (!Number.isFinite(value)) return 0
  return value <= 1 ? value * 100 : value
}

const defaultGovernanceProtectionViewModel = (): GovernanceProtectionViewModel => {
  return {
    components: [
      {
        key: 'oversharing-control',
        label: '過剰共有の抑制',
        score: 0,
        details: []
      },
      {
        key: 'assurance',
        label: '運用・保証',
        score: 0,
        details: []
      }
    ],
    governanceScore: 0,
    coverageScore: 0,
    confidenceLevel: 'Low',
    confidenceScore: 0,
    riskSummary: {
      governanceRaw: 0,
      exceptionDebt: 0,
      coveragePenalty: 0
    },
    coverageBreakdown: [],
    scaleMax: 100
  }
}

const toGovernanceProtectionViewModel = (overview: GovernanceOverviewResponse | null): GovernanceProtectionViewModel => {
  if (!overview) return defaultGovernanceProtectionViewModel()
  const fallback = defaultGovernanceProtectionViewModel()
  const governanceScore = normalizeDisplayScore(Number(overview.governance_score ?? overview.protection_scores?.overall ?? 0))
  const oversharing = normalizeDisplayScore(Number(overview.subscores?.oversharing_control ?? overview.protection_scores?.oversharing_protection ?? 0))
  const assurance = normalizeDisplayScore(Number(overview.subscores?.assurance ?? 0))
  const coverageScore = Number(overview.coverage?.coverage_score ?? 0)
  const confidenceScore = Number(overview.confidence?.scan_confidence ?? 0)
  const computedConfidence: 'High' | 'Medium' | 'Low' =
    confidenceScore >= 0.8 && coverageScore >= 0.8
      ? 'High'
      : confidenceScore >= 0.55 && coverageScore >= 0.55
        ? 'Medium'
        : 'Low'
  const confidenceLevel = (overview.confidence?.level ?? computedConfidence) as 'High' | 'Medium' | 'Low'
  const riskSummary = {
    governanceRaw: Number(overview.risk_summary?.governance_raw ?? 0),
    exceptionDebt: Number(overview.risk_summary?.exception_debt ?? 0),
    coveragePenalty: Number(overview.risk_summary?.coverage_penalty ?? 0)
  }
  const breakdown = overview.subscores_breakdown
  const oversharingOrder = [
    'broad_audience_risk',
    'public_link_risk',
    'privilege_excess_risk',
    'external_boundary_risk',
    'discoverability_risk',
    'reshare_risk',
    'permission_outlier_risk'
  ]
  const assuranceOrder = ['aging_open_risk', 'exception_debt', 'coverage_score', 'rescan_freshness', 'scan_confidence']
  const legacyBreakdown = overview.protection_score_breakdown
  const fallbackOversharingRows = [
    {
      key: 'broad_audience_risk',
      label: '公開到達範囲リスク',
      score: Number(legacyBreakdown?.oversharing?.details?.everyone_public ?? 0),
      value: Number.isFinite(legacyBreakdown?.oversharing?.details?.everyone_public as number)
        ? 1 - Number(legacyBreakdown?.oversharing?.details?.everyone_public ?? 0) / 100
        : undefined
    },
    {
      key: 'public_link_risk',
      label: '公開リンクリスク',
      score: Number(legacyBreakdown?.oversharing?.details?.public_link_exposure ?? 0),
      value: Number.isFinite(legacyBreakdown?.oversharing?.details?.public_link_exposure as number)
        ? 1 - Number(legacyBreakdown?.oversharing?.details?.public_link_exposure ?? 0) / 100
        : undefined
    },
    {
      key: 'privilege_excess_risk',
      label: '過剰権限リスク',
      score: Number(legacyBreakdown?.oversharing?.details?.excessive_permission_remediation ?? 0),
      value: Number.isFinite(legacyBreakdown?.oversharing?.details?.excessive_permission_remediation as number)
        ? 1 - Number(legacyBreakdown?.oversharing?.details?.excessive_permission_remediation ?? 0) / 100
        : undefined
    }
  ]
  const fallbackAssuranceRows = [
    {
      key: 'exception_debt',
      label: '例外負債',
      score: Number((1 - Math.max(0, Math.min(1, Number(overview.risk_summary?.exception_debt ?? 0)))) * 100),
      value: Number(overview.risk_summary?.exception_debt ?? 0)
    },
    {
      key: 'coverage_score',
      label: 'カバレッジ',
      score: Number((overview.coverage?.coverage_score ?? 0) * 100),
      value: Number(overview.coverage?.coverage_score ?? 0)
    },
    {
      key: 'scan_confidence',
      label: 'スキャン信頼度',
      score: Number((overview.confidence?.scan_confidence ?? 0) * 100),
      value: Number(overview.confidence?.scan_confidence ?? 0)
    }
  ]
  const toDetails = (
    rows: Array<{ key?: string; label?: string; score?: number; value?: number }> | undefined,
    fallbackRows: Array<{ key?: string; label?: string; score?: number; value?: number }>,
    orderedKeys: string[]
  ): Array<{ label: string; score: number; note?: string }> =>
    (() => {
      type BreakdownRow = { key: string; label: string; score: number; value?: number }
      const byKey = new Map(
        (rows && rows.length > 0 ? rows : fallbackRows).map((row) => [
          String(row.key ?? ''),
          {
            key: String(row.key ?? ''),
            label: String(row.label ?? '-'),
            score: Number(row.score ?? 0),
            value: typeof row.value === 'number' ? row.value : undefined
          }
        ])
      )
      const ordered = orderedKeys.map((key) => byKey.get(key)).filter(Boolean) as BreakdownRow[]
      const rest = [...byKey.values()].filter((row) => !orderedKeys.includes(row.key))
      return [...ordered, ...rest].map((row) => ({
        label: row.label || row.key,
        score: row.score,
        note: typeof row.value === 'number' ? `生値=${row.value.toFixed(4)}` : undefined
      }))
    })()
  return {
    ...fallback,
    governanceScore,
    coverageScore,
    confidenceLevel,
    confidenceScore,
    riskSummary,
    components: [
      {
        key: 'oversharing-control',
        label: '過剰共有の抑制',
        score: oversharing,
        details: toDetails(breakdown?.oversharing_control, fallbackOversharingRows, oversharingOrder)
      },
      {
        key: 'assurance',
        label: '運用・保証',
        score: assurance,
        details: toDetails(breakdown?.assurance, fallbackAssuranceRows, assuranceOrder)
      }
    ],
    coverageBreakdown: [
      { key: 'inventory', label: 'コンテナ網羅率', value: Number(overview.coverage?.inventory_coverage ?? 0) },
      { key: 'content_scan', label: 'コンテンツスキャン率', value: Number(overview.coverage?.content_scan_coverage ?? 0) },
      { key: 'supported_format', label: '対応形式率', value: Number(overview.coverage?.supported_format_coverage ?? 0) },
      { key: 'fresh_scan', label: '再スキャン鮮度', value: Number(overview.coverage?.fresh_scan_coverage ?? 0) },
      { key: 'permission_detail', label: 'ACL詳細率', value: Number(overview.coverage?.permission_detail_coverage ?? 0) }
    ]
  }
}

const toGovernanceOverviewStatsFromApi = (row: GovernanceOverviewResponse): GovernanceOverviewStats => {
  const coverageScore = Number(row.coverage?.coverage_score ?? 0)
  const confidenceScore = Number(row.confidence?.scan_confidence ?? 0)
  const computedConfidence: 'High' | 'Medium' | 'Low' =
    confidenceScore >= 0.8 && coverageScore >= 0.8
      ? 'High'
      : confidenceScore >= 0.55 && coverageScore >= 0.55
        ? 'Medium'
        : 'Low'
  return {
    governanceScore: normalizeDisplayScore(Number(row.governance_score ?? row.protection_scores?.overall ?? 0)),
    coverageScore,
    confidenceLevel: (row.confidence?.level ?? computedConfidence) as 'High' | 'Medium' | 'Low',
    confidenceScore,
    oversharingControlScore: normalizeDisplayScore(Number(row.subscores?.oversharing_control ?? row.protection_scores?.oversharing_protection ?? 0)),
    assuranceScore: normalizeDisplayScore(Number(row.subscores?.assurance ?? 0)),
    governanceRaw: Number(row.risk_summary?.governance_raw ?? 0),
    exceptionDebt: Number(row.risk_summary?.exception_debt ?? 0),
    coveragePenalty: Number(row.risk_summary?.coverage_penalty ?? 0),
    inventoryCoverage: Number(row.coverage?.inventory_coverage ?? 0),
    contentScanCoverage: Number(row.coverage?.content_scan_coverage ?? 0),
    supportedFormatCoverage: Number(row.coverage?.supported_format_coverage ?? 0),
    freshScanCoverage: Number(row.coverage?.fresh_scan_coverage ?? 0),
    permissionDetailCoverage: Number(row.coverage?.permission_detail_coverage ?? 0),
    totalFindingsCount: row.counts?.total_findings ?? 0,
    activeFindingsCount: row.counts?.active_findings ?? 0,
    highRiskCount: row.high_risk_count,
    actionRequiredCount: row.action_required_count ?? row.high_risk_count,
    expiringSuppressions24h: row.expiring_suppressions_24h,
    lastBatchRunAt: row.last_batch_run_at ?? '-'
  }
}

const toGovernanceSuppressionFromApi = (row: GovernanceSuppressionApiRow, index: number) => ({
  findingId: row.finding_id,
  planId: `plan-${row.finding_id.slice(0, 8)}`,
  exceptionType: (row.exception_type ?? 'none') as GovernanceSuppression['exceptionType'],
  reasonCode: 'api-imported',
  requestedBy: row.acknowledged_by ?? 'system',
  requestedAt: row.exception_review_due_at ?? row.suppress_until ?? '-',
  status: (row.status ?? 'open') as FindingStatus,
  workflowStatus: (row.workflow_status ?? 'acknowledged') as GovernanceSuppression['workflowStatus'],
  exceptionReviewDueAt: row.exception_review_due_at ?? row.suppress_until ?? '-',
  rawResidualRisk: typeof row.raw_residual_risk === 'number' ? row.raw_residual_risk : undefined,
  acknowledgedBy: row.acknowledged_by ?? 'system',
  acknowledgedReason: row.acknowledged_reason ?? `Suppression ${index + 1}`
})

type MetricSeverity = 'normal' | 'warning' | 'critical'

const getMetricSeverity = (value: number, warningThreshold: number, criticalThreshold: number): MetricSeverity => {
  if (value >= criticalThreshold) return 'critical'
  if (value >= warningThreshold) return 'warning'
  return 'normal'
}

const metricColorClass = (severity: MetricSeverity): string => {
  if (severity === 'critical') return 'text-destructive'
  if (severity === 'warning') return 'text-amber-600'
  return 'text-emerald-600'
}

const metricCardClass = (severity: MetricSeverity): string => {
  if (severity === 'critical') return 'border-destructive/40 bg-destructive/5'
  if (severity === 'warning') return 'border-amber-500/40 bg-amber-500/5'
  return 'border-emerald-500/40 bg-emerald-500/5'
}

const metricBadgeLabel = (severity: MetricSeverity): string => {
  if (severity === 'critical') return '要対応'
  if (severity === 'warning') return '注意'
  return '安定'
}

const metricSizeClass = (value: number, largeThreshold: number, mediumThreshold: number): string => {
  if (value >= largeThreshold) return 'text-4xl'
  if (value >= mediumThreshold) return 'text-3xl'
  return 'text-2xl'
}

const paginateRows = <T,>(rows: T[], page: number, pageSize: number): T[] =>
  rows.slice((page - 1) * pageSize, page * pageSize)

const OverviewPage = ({
  onNavigate,
  overview,
  protectionViewModel
}: {
  onNavigate: (page: GovernancePageKey, focus?: string) => void
  overview: GovernanceOverviewStats
  protectionViewModel: GovernanceProtectionViewModel
}) => (
  <div className="space-y-4">
    {(() => {
      const actionRequiredSeverity = getMetricSeverity(overview.highRiskCount, 8, 12)
      const expiringSeverity = getMetricSeverity(overview.expiringSuppressions24h, 2, 4)
      return (
        <Card className="border-primary/20 bg-primary/5">
          <CardHeader className="pb-2">
            <CardTitle>ガバナンス リスクダッシュボード</CardTitle>
            <CardDescription>
              ガバナンススコア / Coverage / Confidence と v1.2 の3層サブスコアを表示します。
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-3 md:grid-cols-3">
            <button
              type="button"
              onClick={() => onNavigate('findings', 'finding:highest_priority')}
              className={cn(
                'group rounded-md border-2 p-3 text-left transition-all duration-150 hover:border-primary/60 hover:bg-primary/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60 active:scale-[0.98] active:brightness-95',
                metricCardClass(actionRequiredSeverity)
              )}
            >
              <div className="flex items-center justify-between gap-2">
                <p className="text-xs text-muted-foreground">
                  <HoverHelpLabel
                    label="高リスク検知（high / critical）"
                    helpText="risk_level が high / critical の検知件数です。"
                  />
                </p>
                <Badge variant={actionRequiredSeverity === 'critical' ? 'destructive' : actionRequiredSeverity === 'warning' ? 'secondary' : 'outline'}>
                  {metricBadgeLabel(actionRequiredSeverity)}
                </Badge>
              </div>
              <p className={cn('mt-1 font-semibold leading-none', metricColorClass(actionRequiredSeverity), metricSizeClass(overview.highRiskCount, 12, 8))}>
                {overview.highRiskCount} 件
              </p>
              <p className="mt-1 text-[11px] text-muted-foreground">
                有効 {overview.activeFindingsCount} 件 / 累積 {overview.totalFindingsCount} 件
              </p>
              <p className="mt-2 text-xs font-medium text-primary opacity-80 transition group-hover:opacity-100">
                詳細を見る →
              </p>
            </button>
            <button
              type="button"
              onClick={() => onNavigate('suppression', 'suppression:expiring')}
              className={cn(
                'group rounded-md border-2 p-3 text-left transition-all duration-150 hover:border-primary/60 hover:bg-primary/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60 active:scale-[0.98] active:brightness-95',
                metricCardClass(expiringSeverity)
              )}
            >
              <div className="flex items-center justify-between gap-2">
                <p className="text-xs text-muted-foreground">
                  <HoverHelpLabel
                    label="24時間以内にレビュー期限切れ"
                    helpText="exception_review_due_at が 24h 以内の件数です。"
                  />
                </p>
                <Badge variant={expiringSeverity === 'critical' ? 'destructive' : expiringSeverity === 'warning' ? 'secondary' : 'outline'}>
                  {metricBadgeLabel(expiringSeverity)}
                </Badge>
              </div>
              <p className={cn('mt-1 font-semibold leading-none', metricColorClass(expiringSeverity), metricSizeClass(overview.expiringSuppressions24h, 4, 2))}>
                {overview.expiringSuppressions24h} 件
              </p>
              <p className="mt-1 text-[11px] text-muted-foreground">クリックで期限切れ前の抑止を確認</p>
              <p className="mt-2 text-xs font-medium text-primary opacity-80 transition group-hover:opacity-100">
                詳細を見る →
              </p>
            </button>
            <button
              type="button"
              onClick={() => onNavigate('findings', 'finding:highest_priority')}
              className="group rounded-md border-2 bg-card p-3 text-left transition-all duration-150 hover:border-primary/60 hover:bg-primary/5 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60 active:scale-[0.98] active:brightness-95"
            >
              <p className="text-xs text-muted-foreground">
                <HoverHelpLabel
                  label="ガバナンススコア / Coverage / Confidence"
                  helpText="総合スコア（0..100）に対し Coverage と Confidence を併記します。"
                />
              </p>
              <p className="mt-1 text-sm font-medium">{overview.governanceScore.toFixed(1)}</p>
              <p className="mt-1 text-[11px] text-muted-foreground">
                Coverage {Math.round(overview.coverageScore * 100)}% / Confidence {overview.confidenceLevel}
              </p>
              <p className="mt-2 text-xs font-medium text-primary opacity-80 transition group-hover:opacity-100">
                詳細を見る →
              </p>
            </button>
          </CardContent>
        </Card>
      )
    })()}

    <ReadinessBreakdownD3
      overallScore={protectionViewModel.governanceScore}
      targetScore={90}
      components={protectionViewModel.components}
      weakestPosition="bottom"
      summaryCards={[
        {
          key: 'oversharing-control',
          label: '過剰共有の抑制',
          score: protectionViewModel.components.find((component) => component.key === 'oversharing-control')?.score ?? 0,
          target: 90,
          details: protectionViewModel.components.find((component) => component.key === 'oversharing-control')?.details ?? []
        },
        {
          key: 'assurance',
          label: '運用・保証',
          score: protectionViewModel.components.find((component) => component.key === 'assurance')?.score ?? 0,
          target: 90,
          details: protectionViewModel.components.find((component) => component.key === 'assurance')?.details ?? []
        }
      ]}
      title="ガバナンススコア（v1.2）"
      description="raw_residual_risk を基準に算出された 3層サブスコアを表示します。"
      rootLabel="ガバナンススコア"
      scoreLabel="総合スコア"
      targetLabel="目標スコア"
      valueUnit=""
      maxScaleValue={protectionViewModel.scaleMax}
      showComponentChart={false}
      showComposition={false}
      showPriorityList={false}
    />

    <Card>
      <CardHeader>
        <CardTitle>リスクサマリ</CardTitle>
      </CardHeader>
      <CardContent className="grid gap-3 md:grid-cols-3">
        {[
          { label: '実リスク度', value: protectionViewModel.riskSummary.governanceRaw },
          { label: '例外負債', value: protectionViewModel.riskSummary.exceptionDebt },
          { label: 'カバレッジ不足', value: protectionViewModel.riskSummary.coveragePenalty }
        ].map((item) => (
          <div key={item.label} className="rounded-md border p-3">
            <p className="text-xs text-muted-foreground">{item.label}</p>
            <p className="mt-1 text-lg font-semibold">{item.value.toFixed(2)}</p>
            <div className="mt-2 h-2 rounded-full bg-muted">
              <div className="h-2 rounded-full bg-primary" style={{ width: `${Math.min(100, Math.max(0, item.value * 100))}%` }} />
            </div>
          </div>
        ))}
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Coverage 内訳</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid gap-2 md:grid-cols-2">
          {protectionViewModel.coverageBreakdown.map((row) => (
            <div key={row.key} className="rounded-md border p-2">
              <div className="flex items-center justify-between">
                <span className="text-sm">{row.label}</span>
                <span className="text-sm font-medium">{Math.round(row.value * 100)}%</span>
              </div>
              <div className="mt-1 h-1.5 rounded-full bg-muted">
                <div className="h-1.5 rounded-full bg-emerald-500" style={{ width: `${Math.min(100, Math.max(0, row.value * 100))}%` }} />
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>

  </div>
)

const FindingsPage = ({
  deepLinkFocus,
  scanJobs
}: {
  deepLinkFocus?: string
  scanJobs: GovernanceScanJob[]
}) => {
  type FindingsRiskFilter = 'medium_or_higher' | 'all' | 'high_only' | 'low_or_none'
  type FindingsTargetFilter = 'all' | 'file' | 'folder' | 'unknown'

  const isMediumOrHigherRisk = (finding: GovernanceFinding): boolean => {
    const level = resolveFindingRiskLevel(finding)
    return level === 'medium' || level === 'high' || level === 'critical'
  }
  const isHighOrCriticalRisk = (finding: GovernanceFinding): boolean => {
    const level = resolveFindingRiskLevel(finding)
    return level === 'high' || level === 'critical'
  }

  const [loading, setLoading] = useState(false)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [apiFindings, setApiFindings] = useState<GovernanceFinding[]>([])
  const [riskFilter, setRiskFilter] = useState<FindingsRiskFilter>('medium_or_higher')
  const [remediationFilter, setRemediationFilter] = useState<RemediationState | 'all'>('all')
  const [targetFilter, setTargetFilter] = useState<FindingsTargetFilter>('all')
  const scopedFindings = apiFindings
  const filteredFindings = useMemo(
    () =>
      scopedFindings.filter((row) => {
        const byRisk =
          riskFilter === 'all'
            ? true
            : riskFilter === 'medium_or_higher'
              ? isMediumOrHigherRisk(row)
              : riskFilter === 'high_only'
                ? isHighOrCriticalRisk(row)
                : !isMediumOrHigherRisk(row)
        const byRemediation =
          remediationFilter === 'all' ? true : row.remediationState === remediationFilter
        const byTarget = targetFilter === 'all' ? true : (row.targetKind ?? 'unknown') === targetFilter
        return byRisk && byRemediation && byTarget
      }),
    [scopedFindings, riskFilter, remediationFilter, targetFilter]
  )
  const sortedFindings = useMemo(
    () =>
      [...filteredFindings].sort(
        (a, b) =>
          findingPriorityScore(b.riskScore, b.matchedGuards.length, b.status, b.detectionReasons ?? []) -
          findingPriorityScore(a.riskScore, a.matchedGuards.length, a.status, a.detectionReasons ?? [])
      ),
    [filteredFindings]
  )
  const [selectedFindingId, setSelectedFindingId] = useState(sortedFindings[0]?.id ?? '')
  const [flashRowId, setFlashRowId] = useState<string | null>(null)
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize, setPageSize] = useState(25)
  const [isActionDialogOpen, setIsActionDialogOpen] = useState(false)
  const [isEvidencePanelOpen, setIsEvidencePanelOpen] = useState(false)
  const [remediationDetail, setRemediationDetail] = useState<GovernanceRemediationDetailResponse | null>(null)
  const [remediationBusy, setRemediationBusy] = useState(false)
  const [exceptionType, setExceptionType] = useState<GovernanceExceptionRegistrationRequest['exception_type']>('temporary_accept')
  const [exceptionInputMode, setExceptionInputMode] = useState<'duration' | 'datetime'>('duration')
  const [exceptionDurationDays, setExceptionDurationDays] = useState('14')
  const [exceptionReviewDueAt, setExceptionReviewDueAt] = useState(buildDefaultExceptionReviewDueAt)
  const [exceptionReason, setExceptionReason] = useState('')
  const [exceptionTicket, setExceptionTicket] = useState('')
  const [actionPanelTab, setActionPanelTab] = useState<'remediation' | 'exception'>('remediation')
  const [isManualModeRollbackConfirmOpen, setIsManualModeRollbackConfirmOpen] = useState(false)
  const selectedRowRef = useRef<HTMLTableRowElement | null>(null)
  const loadFindings = useCallback(async () => {
    setLoading(true)
    setLoadError(null)
    try {
      const pageSize = 500
      let nextOffset: number | null = 0
      let totalCount = 0
      const rows: GovernanceFindingApiRow[] = []
      while (nextOffset !== null) {
        const response = await getGovernanceFindings(
          pageSize,
          nextOffset,
          'new,open,completed,remediated,closed,acknowledged',
          true,
          false
        )
        rows.push(...response.rows)
        totalCount = response.pagination?.total_count ?? rows.length
        const reportedNextOffset = response.pagination?.next_offset
        if (typeof reportedNextOffset === 'number') {
          nextOffset = reportedNextOffset
        } else {
          const computedNext: number = (nextOffset ?? 0) + response.rows.length
          nextOffset = computedNext < totalCount ? computedNext : null
        }
        if (rows.length >= totalCount) {
          nextOffset = null
        }
      }
      setApiFindings(rows.map(toGovernanceFindingFromApi))
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : '検知結果の取得に失敗しました。')
      setApiFindings([])
    } finally {
      setLoading(false)
    }
  }, [])
  useEffect(() => {
    void loadFindings()
  }, [loadFindings])
  useEffect(() => {
    if (!deepLinkFocus) return
    if (deepLinkFocus === 'finding:highest_priority' && sortedFindings[0]?.id) {
      setSelectedFindingId(sortedFindings[0].id)
      return
    }
    if (deepLinkFocus.startsWith('finding:id:')) {
      const targetId = deepLinkFocus.replace('finding:id:', '')
      if (sortedFindings.some((row) => row.id === targetId)) {
        setSelectedFindingId(targetId)
      }
    }
  }, [deepLinkFocus, sortedFindings])
  useEffect(() => {
    if (!deepLinkFocus) return
    selectedRowRef.current?.scrollIntoView({ behavior: 'smooth', block: 'center' })
    setFlashRowId(selectedFindingId)
    const timer = window.setTimeout(() => setFlashRowId((prev) => (prev === selectedFindingId ? null : prev)), 1600)
    return () => window.clearTimeout(timer)
  }, [selectedFindingId, deepLinkFocus])
  useEffect(() => {
    const selectedIndex = sortedFindings.findIndex((row) => row.id === selectedFindingId)
    if (selectedIndex >= 0) {
      setCurrentPage(Math.floor(selectedIndex / pageSize) + 1)
    }
  }, [selectedFindingId, sortedFindings, pageSize])
  useEffect(() => {
    setIsEvidencePanelOpen(false)
  }, [selectedFindingId])
  useEffect(() => {
    setExceptionType('temporary_accept')
    setExceptionInputMode('duration')
    setExceptionDurationDays('14')
    setExceptionReviewDueAt(buildDefaultExceptionReviewDueAt())
    setExceptionReason('')
    setExceptionTicket('')
    setActionPanelTab('remediation')
  }, [selectedFindingId])
  useEffect(() => {
    if (exceptionInputMode !== 'datetime') return
    if (exceptionReviewDueAt.trim()) return
    setExceptionReviewDueAt(buildDefaultExceptionReviewDueAt())
  }, [exceptionInputMode, exceptionReviewDueAt])
  useEffect(() => {
    if (!isActionDialogOpen || !selectedFindingId) return
    const loadRemediation = async () => {
      try {
        const detail = await getGovernanceFindingRemediation(selectedFindingId)
        setRemediationDetail(detail)
      } catch {
        setRemediationDetail(null)
      }
    }
    void loadRemediation()
  }, [isActionDialogOpen, selectedFindingId])
  useEffect(() => {
    setIsManualModeRollbackConfirmOpen(false)
  }, [selectedFindingId, isActionDialogOpen])
  useEffect(() => {
    setCurrentPage(1)
  }, [riskFilter, remediationFilter, targetFilter, pageSize])
  const totalRows = sortedFindings.length
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize))
  const pagedFindings = useMemo(() => paginateRows(sortedFindings, currentPage, pageSize), [sortedFindings, currentPage, pageSize])
  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(totalPages)
  }, [currentPage, totalPages])
  const selectedFinding = sortedFindings.find((row) => row.id === selectedFindingId) ?? sortedFindings[0]
  const selectedRiskLevel = selectedFinding ? resolveFindingRiskLevel(selectedFinding) : 'low'
  const isLowRiskFinding = selectedRiskLevel === 'low'
  const selectedJob = selectedFinding ? relatedJobForFinding(selectedFinding.id, scanJobs) : null
  const effectiveRemediationState = remediationDetail?.remediation_state ?? selectedFinding?.remediationState
  const normalizedRemediationState = (effectiveRemediationState ?? 'ai_proposed') as RemediationState
  const isExecutedFinding = normalizedRemediationState === 'executed'
  const isPostRemediationState =
    normalizedRemediationState === 'executed' ||
    normalizedRemediationState === 'manual_required' ||
    selectedFinding?.status === 'completed' ||
    selectedFinding?.status === 'remediated' ||
    selectedFinding?.status === 'closed'
  useEffect(() => {
    if (!isPostRemediationState) return
    if (actionPanelTab === 'exception') {
      setActionPanelTab('remediation')
    }
  }, [isPostRemediationState, actionPanelTab])
  useEffect(() => {
    if (!isLowRiskFinding) return
    if (actionPanelTab === 'exception') {
      setActionPanelTab('remediation')
    }
  }, [isLowRiskFinding, actionPanelTab])
  const isAiProposedState =
    normalizedRemediationState === 'ai_proposed' || normalizedRemediationState === 'pending_approval'
  const isApprovedState = normalizedRemediationState === 'approved'
  const isFailedState = normalizedRemediationState === 'failed'
  const effectiveRemediationMode = String(
    remediationDetail?.remediation_mode ?? selectedFinding?.remediationMode ?? ''
  ).trim().toLowerCase()
  const isManualOnlyRemediationMode = ['manual', 'owner_review', 'recommend_only'].includes(effectiveRemediationMode)
  const isRollbackEligibleState = normalizedRemediationState === 'executed'
    || normalizedRemediationState === 'manual_required'
    || normalizedRemediationState === 'failed'
  const jobDetailUrl = selectedFinding && selectedJob
    ? `http://localhost:5173/#/app?tab=governance&page=findings&findingId=${selectedFinding.id}&jobId=${selectedJob.jobId}`
    : '-'
  const canProposeRemediation =
    (isAiProposedState || isFailedState) && (remediationDetail?.allowed_actions?.includes('propose') ?? true)
  const canApproveRemediation =
    !isManualOnlyRemediationMode
    && isAiProposedState
    && (remediationDetail?.allowed_actions?.includes('approve') ?? false)
  const canExecuteRemediation =
    !isManualOnlyRemediationMode
    && isApprovedState
    && (remediationDetail?.allowed_actions?.includes('execute') ?? false)
  const canRollbackRemediation = isRollbackEligibleState && (remediationDetail?.allowed_actions?.includes('rollback') ?? false)
  const rollbackAllowedByServer = remediationDetail?.allowed_actions?.includes('rollback') ?? false
  const showRollbackWorkflowCta = rollbackAllowedByServer && canRollbackRemediation
  const executionRows = useMemo(() => executionResultRows(remediationDetail), [remediationDetail])
  const manualReasonCodes = useMemo(
    () => remediationManualReasonCodes(remediationDetail),
    [remediationDetail]
  )
  const manualChecklist = useMemo(
    () => (selectedFinding ? remediationManualChecklist(remediationDetail, selectedFinding) : []),
    [remediationDetail, selectedFinding]
  )

  const handleApproveRemediation = useCallback(async () => {
    if (!selectedFinding) return
    if (isManualOnlyRemediationMode) {
      toast('この検知は手動対応モードです。承認/自動実行ではなく手動対応を実施してください。')
      return
    }
    setRemediationBusy(true)
    try {
      const detail = await approveGovernanceFindingRemediation(selectedFinding.id)
      setRemediationDetail(detail)
      await loadFindings()
      if (detail.remediation_state === 'executed') {
        toast.success('是正提案を承認し、是正処理を実行しました。')
      } else if (detail.remediation_state === 'manual_required') {
        toast('是正提案を承認し、是正処理を実行しました。一部は手動対応が必要です。')
      } else {
        toast.success('是正提案を承認しました。')
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : '是正提案の承認に失敗しました。')
    } finally {
      setRemediationBusy(false)
    }
  }, [selectedFinding, loadFindings, isManualOnlyRemediationMode])

  const handleRollbackRemediation = useCallback(async () => {
    if (!selectedFinding) return
    setRemediationBusy(true)
    try {
      const detail = await rollbackGovernanceFindingRemediation(selectedFinding.id)
      setRemediationDetail(detail)
      await loadFindings()
      if (detail.remediation_state === 'manual_required') {
        toast('ロールバックを実行しました。一部は手動対応が必要です。')
      } else {
        toast.success('ロールバックを実行しました。')
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'ロールバックの実行に失敗しました。')
    } finally {
      setRemediationBusy(false)
    }
  }, [selectedFinding, loadFindings])

  const manualRollbackDryRunSummary = useMemo(() => {
    const n = Array.isArray(remediationDetail?.result?.results)
      ? remediationDetail.result.results.length
      : 0
    return `手動対応モード: 実行済み結果 ${n} 件を対象にロールバックを試みます`
  }, [remediationDetail])
  const manualRollbackPredictedImpact = useMemo(
    () =>
      selectedFinding
        ? `影響範囲: ${selectedFinding.source} / ${selectedFinding.itemPath}`
        : '-',
    [selectedFinding]
  )
  const manualRollbackCapabilityText = canRollbackRemediation ? '高（可逆）' : '低（不可逆）'

  const handleRegisterException = useCallback(async () => {
    if (!selectedFinding) return
    let normalizedDurationDays: number | undefined
    let normalizedReviewDueAt: string | undefined
    if (exceptionInputMode === 'duration') {
      const parsedDurationDays = Number.parseInt(exceptionDurationDays, 10)
      if (!Number.isFinite(parsedDurationDays) || parsedDurationDays <= 0) {
        toast.error('例外期間（日数）は1以上で指定してください。')
        return
      }
      normalizedDurationDays = parsedDurationDays
    } else {
      const rawDueAt = exceptionReviewDueAt.trim()
      if (!rawDueAt) {
        toast.error('レビュー期限（日時）を指定してください。')
        return
      }
      const parsedDueAt = new Date(rawDueAt)
      if (Number.isNaN(parsedDueAt.getTime())) {
        toast.error('レビュー期限（日時）の形式が不正です。')
        return
      }
      normalizedReviewDueAt = parsedDueAt.toISOString()
    }
    setRemediationBusy(true)
    try {
      const detail = await registerGovernanceFindingException(selectedFinding.id, {
        exception_type: exceptionType,
        duration_days: normalizedDurationDays,
        exception_review_due_at: normalizedReviewDueAt,
        reason: exceptionReason.trim() || undefined,
        exception_ticket: exceptionTicket.trim() || undefined,
        scope: {
          source: selectedFinding.source,
          item_path: selectedFinding.itemPath,
          matched_guards: selectedFinding.matchedGuards,
        },
      })
      setRemediationDetail(detail)
      await loadFindings()
      toast.success('例外レジストリに登録しました。')
    } catch (error) {
      toast.error(error instanceof Error ? error.message : '例外登録に失敗しました。')
    } finally {
      setRemediationBusy(false)
    }
  }, [
    selectedFinding,
    exceptionInputMode,
    exceptionDurationDays,
    exceptionReviewDueAt,
    exceptionType,
    exceptionReason,
    exceptionTicket,
    loadFindings,
  ])
  const handleMarkCompletedForLowRisk = useCallback(async () => {
    if (!selectedFinding) return
    if (selectedFinding.status === 'completed' || selectedFinding.status === 'closed' || selectedFinding.status === 'remediated') {
      toast('すでに完了済みです。')
      return
    }
    setRemediationBusy(true)
    try {
      const detail = await markGovernanceFindingCompleted(selectedFinding.id)
      setRemediationDetail(detail)
      await loadFindings()
      toast.success('low リスクとして完了に更新しました。')
    } catch (error) {
      toast.error(error instanceof Error ? error.message : '完了ステータス更新に失敗しました。')
    } finally {
      setRemediationBusy(false)
    }
  }, [selectedFinding, loadFindings])
  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle>検知結果</CardTitle>
          <CardDescription>過剰共有の検知結果一覧</CardDescription>
        </CardHeader>
        <CardContent>
          {loading && <p className="mb-3 text-sm text-muted-foreground">ガバナンス検知結果を取得中です...</p>}
          {loadError && (
            <p className="mb-3 text-sm text-rose-600">{loadError}</p>
          )}
          <div className="mb-3 grid gap-2 md:grid-cols-3">
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">リスク</span>
              <Select value={riskFilter} onValueChange={(value) => setRiskFilter(value as FindingsRiskFilter)}>
                <SelectTrigger className="h-8 w-48">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="medium_or_higher">medium以上（要対応）</SelectItem>
                  <SelectItem value="all">すべて</SelectItem>
                  <SelectItem value="high_only">high/criticalのみ</SelectItem>
                  <SelectItem value="low_or_none">low/none相当のみ</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">是正状態</span>
              <Select
                value={remediationFilter}
                onValueChange={(value) => setRemediationFilter(value as RemediationState | 'all')}
              >
                <SelectTrigger className="h-8 w-48">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">すべて</SelectItem>
                  <SelectItem value="ai_proposed">AI提案中</SelectItem>
                  <SelectItem value="approved">承認済み</SelectItem>
                  <SelectItem value="executed">実行済み</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">対象種別</span>
              <Select
                value={targetFilter}
                onValueChange={(value) => setTargetFilter(value as FindingsTargetFilter)}
              >
                <SelectTrigger className="h-8 w-48">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">すべて</SelectItem>
                  <SelectItem value="file">ファイル</SelectItem>
                  <SelectItem value="folder">フォルダ</SelectItem>
                  <SelectItem value="unknown">不明</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          <p className="mb-3 text-xs text-muted-foreground">
            表示中 {totalRows} 件 / 全体 {scopedFindings.length} 件
          </p>
          <TablePageControls
            totalRows={totalRows}
            currentPage={currentPage}
            pageSize={pageSize}
            onPageChange={setCurrentPage}
            onPageSizeChange={setPageSize}
          />
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead title="検知対象のドキュメント名とファイルパスです。">リソース</TableHead>
                <TableHead title="リスクスコア（高いほど優先対応）とレベルです。セルにホバーすると正規化実リスク（0–1）を表示します。">
                  リスク
                </TableHead>
                <TableHead title="ステータスです。">status</TableHead>
                <TableHead title="検知対象がファイルかフォルダかを示します。">対象種別</TableHead>
                <TableHead title="A/B/C/Dなどの検知理由です。">検知理由</TableHead>
                <TableHead title="ガードに紐づく理由コードです。">ガード理由</TableHead>
                <TableHead title="最終評価日時です。">最終評価日時</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {pagedFindings.map((row) => (
                <TableRow
                  key={row.id}
                  className={cn(
                    'cursor-pointer',
                    selectedFindingId === row.id && 'bg-primary/5',
                    row.workflowStatus === 'acknowledged' && 'bg-slate-100/70',
                    flashRowId === row.id && 'animate-pulse'
                  )}
                  onClick={() => {
                    setSelectedFindingId(row.id)
                    setIsEvidencePanelOpen(false)
                    setIsActionDialogOpen(true)
                  }}
                  ref={selectedFindingId === row.id ? selectedRowRef : null}
                >
                  <TableCell className="max-w-[360px]">
                    <div className="min-w-0">
                      <p className="truncate font-medium" title={row.itemName ?? row.itemId ?? row.id}>
                        {row.itemName ?? row.itemId ?? row.id}
                      </p>
                      <p className="truncate text-xs text-muted-foreground" title={row.itemPath}>
                        {row.itemPath}
                      </p>
                    </div>
                  </TableCell>
                  <TableCell>
                    <div
                      className="flex flex-wrap items-center gap-2"
                      title={
                        typeof row.rawResidualRisk === 'number'
                          ? `実リスク（正規化 0–1）: ${row.rawResidualRisk.toFixed(2)}`
                          : undefined
                      }
                    >
                      {formatRiskScoreDisplay(row.riskScore) && (
                        <span>{formatRiskScoreDisplay(row.riskScore)}</span>
                      )}
                      <Badge
                        variant={badgeVariantByValue(resolveFindingRiskLevel(row))}
                        className={badgeClassByValue(resolveFindingRiskLevel(row))}
                      >
                        {resolveFindingRiskLevel(row)}
                      </Badge>
                    </div>
                  </TableCell>
                  <TableCell>
                    <Badge variant={badgeVariantByValue(row.status)} className={badgeClassByValue(row.status)}>
                      {governanceFindingStatusLabel(row.status)}
                    </Badge>
                  </TableCell>
                  <TableCell>{targetKindLabel(row.targetKind ?? 'unknown')}</TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-1">
                      {(row.detectionReasons ?? []).length > 0
                        ? (row.detectionReasons ?? []).map((reason) => (
                          <Badge key={`${row.id}-dr-${reason}`} variant="outline">
                            {DETECTION_REASON_LABELS[reason] ?? reason}
                          </Badge>
                        ))
                        : <span className="text-xs text-muted-foreground">-</span>}
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-1">
                      {(row.guardReasonCodes ?? []).length > 0
                        ? (row.guardReasonCodes ?? []).map((reason) => (
                          <Badge key={`${row.id}-gr-${reason}`} variant="outline">
                            {GUARD_REASON_LABELS[reason] ?? reason}
                          </Badge>
                        ))
                        : <span className="text-xs text-muted-foreground">-</span>}
                    </div>
                  </TableCell>
                  <TableCell>{row.lastEvaluatedAt}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {selectedFinding && (
        <>
          <Dialog
            open={isActionDialogOpen}
            onOpenChange={(open) => {
              setIsActionDialogOpen(open)
              if (!open) setIsEvidencePanelOpen(false)
            }}
          >
            <DialogContent className="flex h-[100dvh] max-h-[100dvh] w-[96vw] max-w-[96vw] flex-col overflow-hidden sm:w-[94vw] sm:max-w-6xl">
              <DialogHeader>
                <DialogTitle>{isExecutedFinding ? '実行結果パネル' : '次アクションパネル'}</DialogTitle>
                <DialogDescription>
                  {isExecutedFinding
                    ? `${selectedFinding.itemName ?? selectedFinding.itemId ?? selectedFinding.id} の実行結果確認ポイント`
                    : `${selectedFinding.itemName ?? selectedFinding.itemId ?? selectedFinding.id} を判断するための要点`}
                </DialogDescription>
              </DialogHeader>
              <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-x-hidden text-sm">
                <div className="min-h-0 min-w-0 flex-1 space-y-3 overflow-y-auto overflow-x-hidden pr-1">
                  {isEvidencePanelOpen ? (
                    <div className="rounded-md border bg-muted/30 p-3 space-y-2">
                      <p className="text-xs text-muted-foreground">検知証跡: {selectedFinding.itemName ?? selectedFinding.itemId ?? '-'}</p>
                      <p className="text-xs text-muted-foreground">リスク判定と状態遷移の証跡情報です。</p>
                      {[
                        { key: 'ドキュメント名', value: selectedFinding.itemName ?? selectedFinding.itemId ?? '-' },
                        { key: 'ファイルパス', value: selectedFinding.itemPath },
                        { key: '是正状態', value: remediationStateText((effectiveRemediationState ?? selectedFinding.remediationState) as RemediationState) },
                        {
                          key: '優先度',
                          value: findingPriorityScore(
                            selectedFinding.riskScore,
                            selectedFinding.matchedGuards.length,
                            selectedFinding.status,
                            selectedFinding.detectionReasons ?? []
                          ).toString()
                        },
                        {
                          key: 'リスク',
                          value: (() => {
                            const level = resolveFindingRiskLevel(selectedFinding)
                            const raw =
                            selectedFinding.rawResidualRisk !== undefined
                              ? ` / 実リスク（0–1）: ${selectedFinding.rawResidualRisk.toFixed(2)}`
                              : ''
                            const scoreText = formatRiskScoreDisplay(selectedFinding.riskScore)
                            return scoreText ? `${scoreText} · ${level}${raw}` : `${level}${raw}`
                          })()
                        },
                        { key: '判定カテゴリ（ガード）', value: selectedFinding.matchedGuards.join(', ') || '-' },
                        {
                          key: '検知理由',
                          value: (selectedFinding.detectionReasons ?? [])
                            .map((reason) => DETECTION_REASON_LABELS[reason] ?? reason)
                            .join(', ') || '-'
                        },
                        {
                          key: 'ガード理由',
                          value: (selectedFinding.guardReasonCodes ?? [])
                            .map((reason) => GUARD_REASON_LABELS[reason] ?? reason)
                            .join(', ') || '-'
                        },
                        { key: '最終実行ID', value: remediationDetail?.last_execution_id ?? '-' },
                        { key: '最終ロールバックID', value: remediationDetail?.rollback_id ?? '-' },
                        { key: '承認者', value: remediationDetail?.approved_by ?? '-' },
                        { key: '承認日時', value: remediationDetail?.approved_at ?? '-' },
                        { key: '実行ジョブID', value: selectedJob?.jobId ?? '-' },
                        { key: '実行ジョブURL', value: jobDetailUrl },
                        { key: '状態遷移理由', value: transitionReason(selectedFinding) },
                        { key: '判定ソース', value: selectedFinding.decisionSource ?? 'fallback' },
                        { key: '想定閲覧範囲', value: selectedFinding.expectedAudience ?? 'internal_need_to_know' },
                        { key: '想定部署', value: selectedFinding.expectedDepartment ?? 'unknown' },
                        { key: 'Gap理由', value: selectedFinding.expectationGapReason || '-' },
                        {
                          key: 'Gapスコア',
                          value:
                          typeof selectedFinding.expectationGapScore === 'number'
                            ? selectedFinding.expectationGapScore.toFixed(2)
                            : '0.00'
                        },
                        { key: '最終評価日時', value: selectedFinding.lastEvaluatedAt }
                      ].map((item) => (
                        <div key={item.key} className="grid grid-cols-[130px_1fr] gap-2 text-sm">
                          <span className="font-medium text-foreground">{item.key}</span>
                          <span className="text-muted-foreground truncate" title={item.value}>{item.value}</span>
                        </div>
                      ))}
                      <div className="mt-3 rounded-md border bg-background p-3">
                        <p className="text-xs text-muted-foreground">是正適用サマリ</p>
                        <p className="mt-1">{remediationExecutionSummary(remediationDetail)}</p>
                        {(() => {
                          const footnote = remediationPostVerificationDeferredFootnote(remediationDetail)
                          return footnote ? (
                            <p className="mt-1 text-xs text-muted-foreground">{footnote}</p>
                          ) : null
                        })()}
                      </div>
                      {executionRows.length > 0 && (
                        <div className="mt-3 rounded-md border bg-background p-3">
                          <p className="text-xs text-muted-foreground">
                            {String(remediationDetail?.result?.phase ?? '') === 'rollback'
                              ? 'ロールバック結果'
                              : '是正アクション実行結果'}
                          </p>
                          <div className="mt-2 space-y-1 text-xs">
                            {executionRows.map((row, index) => (
                              <p key={`${row.action_id ?? 'action'}-${index}`}>
                                {String(row.action_type ?? '-')} / {String(row.status ?? '-')}
                                {row.permission_id ? ` (permission_id: ${String(row.permission_id)})` : ''}
                                {row.reason ? ` / reason: ${String(row.reason)}` : ''}
                              </p>
                            ))}
                          </div>
                        </div>
                      )}
                      {Array.isArray(selectedFinding.findingEvidence?.external_recipients) &&
                      selectedFinding.findingEvidence.external_recipients.length > 0 && (
                        <div className="mt-3 rounded-md border bg-background p-3">
                          <p className="text-xs text-muted-foreground">外部共有先メール</p>
                          <div className="mt-2 flex flex-wrap gap-1 text-xs">
                            {selectedFinding.findingEvidence.external_recipients.map((recipient) => (
                              <Badge key={`recipient-${recipient}`} variant="outline">
                                {recipient}
                              </Badge>
                            ))}
                          </div>
                        </div>
                      )}
                      {Array.isArray(selectedFinding.findingEvidence?.acl_drift_diff) &&
                      selectedFinding.findingEvidence.acl_drift_diff.length > 0 && (
                        <div className="mt-3 rounded-md border bg-background p-3">
                          <p className="text-xs text-muted-foreground">ACL差分</p>
                          <div className="mt-2 space-y-1 text-xs">
                            {selectedFinding.findingEvidence.acl_drift_diff.map((diff, index) => (
                              <p key={`acl-diff-${index}`}>
                                {String(diff.principal ?? '-')} / {String(diff.before ?? '-')} → {String(diff.after ?? '-')} / {String(diff.change ?? '-')}
                              </p>
                            ))}
                          </div>
                        </div>
                      )}
                      {remediationDetail?.last_error && (
                        <div className="mt-3 rounded-md border border-rose-200 bg-rose-50 p-3">
                          <p className="text-xs text-rose-700">最終エラー</p>
                          <p className="mt-1 text-xs text-rose-700">{remediationDetail.last_error}</p>
                        </div>
                      )}
                    </div>
                  ) : (
                    <>
                      {selectedFinding.matchedGuards.length > 0 && (
                        <div className="rounded-md border p-3">
                          <p className="text-xs text-muted-foreground">判定カテゴリ（ガード）詳細</p>
                          <div className="mt-2 flex flex-wrap gap-2">
                            {selectedFinding.matchedGuards.map((guard) => (
                              <Dialog key={`side-${selectedFinding.id}-${guard}`}>
                                <DialogTrigger asChild>
                                  <button
                                    type="button"
                                    className="rounded-md border border-primary/30 bg-primary/5 px-2 py-1 text-xs font-medium text-primary hover:bg-primary/10"
                                  >
                                    {guard} を確認
                                  </button>
                                </DialogTrigger>
                                <DialogContent className="sm:max-w-lg">
                                  <DialogHeader>
                                    <DialogTitle>{guard}: {GUARD_DICTIONARY[guard]?.name ?? 'ガード定義'}</DialogTitle>
                                    <DialogDescription>判定カテゴリ（ガード）の詳細定義</DialogDescription>
                                  </DialogHeader>
                                  <div className="space-y-2 text-sm">
                                    <p><span className="font-medium">主な検知条件:</span> {GUARD_DICTIONARY[guard]?.condition ?? '定義なし'}</p>
                                    <p><span className="font-medium">関連ポリシー:</span> {GUARD_DICTIONARY[guard]?.relatedPolicy ?? '未設定'}</p>
                                  </div>
                                </DialogContent>
                              </Dialog>
                            ))}
                          </div>
                        </div>
                      )}
                      <div className="rounded-md border p-3">
                        <p className="text-xs text-muted-foreground">{isExecutedFinding ? 'AI提案（適用済み）' : 'AI提案内容'}</p>
                        <p className="mt-1">{aiProposalSummary(selectedFinding)}</p>
                        <div className="mt-2 rounded-md border bg-muted/30 p-2 text-xs">
                          <p className="font-medium text-foreground">判定に使用した検知情報（リスク根拠）</p>
                          <p className="mt-1 text-[11px] text-muted-foreground">
                            ※ ここは検知根拠です。最終リスク判定はポリシー評価を加味して決定されます。
                          </p>
                          <div className="mt-2 space-y-2">
                            <div>
                              <p className="text-muted-foreground">文書側シグナル</p>
                              <div className="mt-1 flex flex-wrap gap-1">
                                {documentSignalSummary(selectedFinding).categories.length > 0 ? (
                                  documentSignalSummary(selectedFinding).categories.map((category) => (
                                    <Badge key={`${selectedFinding.id}-category-${category}`} variant="outline">
                                      {category}
                                    </Badge>
                                  ))
                                ) : (
                                  <span className="text-muted-foreground">カテゴリ: なし</span>
                                )}
                                <Badge variant="outline">機微レベル: {documentSignalSummary(selectedFinding).sensitivityLevel}</Badge>
                                <Badge variant="outline">PII: {documentSignalSummary(selectedFinding).piiText}</Badge>
                                <Badge variant="outline">Secret: {documentSignalSummary(selectedFinding).secretText}</Badge>
                              </div>
                            </div>
                            <div>
                              <p className="text-muted-foreground">共有側ベクトル</p>
                              <div className="mt-1 flex flex-wrap gap-1">
                                {sharingVectorSummary(selectedFinding).length > 0 ? (
                                  sharingVectorSummary(selectedFinding).map((vector) => (
                                    <Badge key={`${selectedFinding.id}-vector-${vector}`} variant="outline">
                                      {vector}
                                    </Badge>
                                  ))
                                ) : (
                                  <span className="text-muted-foreground">検知なし</span>
                                )}
                              </div>
                            </div>
                          </div>
                        </div>
                        {!isExecutedFinding && (
                          <p className="mt-1 text-xs text-muted-foreground">
                            次の判断: {suggestedAction(selectedFinding)}
                          </p>
                        )}
                      </div>
                      <div className="rounded-md border p-3">
                        <p className="text-xs text-muted-foreground">現在の状態</p>
                        <div className="mt-2 grid gap-2 text-xs md:grid-cols-2">
                          <div className="rounded-md border p-2">
                            <p className="text-muted-foreground">status</p>
                            <p className="mt-1 font-medium">{selectedFinding.status}</p>
                          </div>
                          <div className="rounded-md border p-2">
                            <p className="text-muted-foreground">workflow_status</p>
                            <p className="mt-1 font-medium">{workflowStatusLabel(selectedFinding.workflowStatus)}</p>
                          </div>
                          <div className="rounded-md border p-2">
                            <p className="text-muted-foreground">remediation_state</p>
                            <p className="mt-1 font-medium">{normalizedRemediationState}（{remediationStateText(normalizedRemediationState)}）</p>
                          </div>
                          <div className="rounded-md border p-2">
                            <p className="text-muted-foreground">exception_type</p>
                            <p className="mt-1 font-medium">{selectedFinding.exceptionType ?? 'none'}（{exceptionTypeLabel(selectedFinding.exceptionType)}）</p>
                          </div>
                        </div>
                      </div>
                      <div className="rounded-md border p-2">
                        <div className="inline-flex rounded-md border bg-muted p-1 text-xs">
                          <button
                            type="button"
                            className={cn(
                              'rounded-sm px-3 py-1',
                              actionPanelTab === 'remediation' ? 'bg-background font-medium text-foreground' : 'text-muted-foreground'
                            )}
                            onClick={() => setActionPanelTab('remediation')}
                          >
                            是正ワークフロー
                          </button>
                          {!isPostRemediationState && !isLowRiskFinding && (
                            <button
                              type="button"
                              className={cn(
                                'rounded-sm px-3 py-1',
                                actionPanelTab === 'exception' ? 'bg-background font-medium text-foreground' : 'text-muted-foreground'
                              )}
                              onClick={() => setActionPanelTab('exception')}
                            >
                              例外対応
                            </button>
                          )}
                        </div>
                      </div>
                      <div className={cn(actionPanelTab !== 'remediation' && 'hidden')}>
                        {isLowRiskFinding ? (
                          <div className="rounded-md border border-sky-200 bg-sky-50 p-3">
                            <p className="text-xs text-sky-700">low リスク（是正不要）</p>
                            <p className="mt-1 text-sky-900">
                              この検知は共有設定の是正処理を実行しません。必要に応じてステータスを完了にしてください。
                            </p>
                            <div className="mt-3">
                              <Button
                                size="sm"
                                onClick={() => void handleMarkCompletedForLowRisk()}
                                disabled={remediationBusy || isPostRemediationState}
                              >
                                完了にする
                              </Button>
                              {canRollbackRemediation && (
                                <Button
                                  size="sm"
                                  variant="outline"
                                  className="ml-2"
                                  onClick={() => void handleRollbackRemediation()}
                                  disabled={remediationBusy}
                                >
                                  ロールバックする
                                </Button>
                              )}
                            </div>
                          </div>
                        ) : !isManualOnlyRemediationMode ? (
                          <RemediationWorkflowPanel
                            selectedFinding={selectedFinding}
                            remediationDetail={remediationDetail}
                            showRollbackWorkflowCta={showRollbackWorkflowCta}
                            remediationBusy={remediationBusy}
                            canRollbackRemediation={canRollbackRemediation}
                            canProposeRemediation={canProposeRemediation}
                            canApproveRemediation={canApproveRemediation}
                            canExecuteRemediation={canExecuteRemediation}
                            impactSummaryText={impactSummary(selectedFinding, false)}
                            executionSummaryText={remediationExecutionSummary(remediationDetail)}
                            onRollback={() => void handleRollbackRemediation()}
                            onApprove={() => void handleApproveRemediation()}
                          />
                        ) : (
                          <div className="space-y-3">
                            <div className="rounded-md border border-amber-200 bg-amber-50 p-3">
                              <p className="text-xs text-amber-700">手動対応モード</p>
                              <p className="mt-1 text-amber-900">
                                この検知は手動対応専用です。Dry-run/承認ワークフローは表示しません。
                              </p>
                            </div>
                            {showRollbackWorkflowCta && (
                              <div className="flex flex-wrap gap-2">
                                <Button
                                  size="sm"
                                  variant="outline"
                                  onClick={() => setIsManualModeRollbackConfirmOpen(true)}
                                  disabled={remediationBusy || !canRollbackRemediation}
                                >
                                  ロールバック
                                </Button>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                      {!isPostRemediationState && !isLowRiskFinding && (
                        <div className={cn('rounded-md border p-3', actionPanelTab !== 'exception' && 'hidden')}>
                          <p className="text-xs text-muted-foreground">例外対応（例外レジストリ）</p>
                          <div className="mt-2 grid gap-2 md:grid-cols-2">
                            <div className="space-y-1">
                              <Label htmlFor="exception-type">例外種別</Label>
                              <select
                                id="exception-type"
                                className="h-8 w-full rounded-md border border-input bg-background px-2 text-sm"
                                value={exceptionType}
                                onChange={(event) =>
                                  setExceptionType(
                                  event.target.value as GovernanceExceptionRegistrationRequest['exception_type']
                                  )
                                }
                              >
                                <option value="temporary_accept">temporary_accept</option>
                                <option value="permanent_accept">permanent_accept</option>
                                <option value="compensating_control">compensating_control</option>
                                <option value="false_positive">false_positive</option>
                              </select>
                            </div>
                            <div className="space-y-1">
                              <Label htmlFor="exception-input-mode">期限指定方法</Label>
                              <select
                                id="exception-input-mode"
                                className="h-8 w-full rounded-md border border-input bg-background px-2 text-sm"
                                value={exceptionInputMode}
                                onChange={(event) => setExceptionInputMode(event.target.value as 'duration' | 'datetime')}
                              >
                                <option value="duration">日数指定</option>
                                <option value="datetime">日時直接指定</option>
                              </select>
                            </div>
                            <div className="space-y-1 md:col-span-2">
                              {exceptionInputMode === 'duration' ? (
                                <>
                                  <Label htmlFor="exception-duration">期間（日数）</Label>
                                  <Input
                                    id="exception-duration"
                                    type="number"
                                    min={1}
                                    value={exceptionDurationDays}
                                    onChange={(event) => setExceptionDurationDays(event.target.value)}
                                  />
                                </>
                              ) : (
                                <>
                                  <Label htmlFor="exception-due-at">レビュー期限（日時）</Label>
                                  <Input
                                    id="exception-due-at"
                                    type="datetime-local"
                                    value={exceptionReviewDueAt}
                                    onChange={(event) => setExceptionReviewDueAt(event.target.value)}
                                  />
                                </>
                              )}
                            </div>
                            <div className="space-y-1">
                              <Label htmlFor="exception-ticket">チケット番号（任意）</Label>
                              <Input
                                id="exception-ticket"
                                value={exceptionTicket}
                                onChange={(event) => setExceptionTicket(event.target.value)}
                                placeholder="INC-12345"
                              />
                            </div>
                            <div className="space-y-1">
                              <Label htmlFor="exception-reason">理由（任意）</Label>
                              <Input
                                id="exception-reason"
                                value={exceptionReason}
                                onChange={(event) => setExceptionReason(event.target.value)}
                                placeholder="業務都合のため期限付きで例外化"
                              />
                            </div>
                          </div>
                          <p className="mt-2 text-xs text-muted-foreground truncate" title={`${selectedFinding.source} / ${selectedFinding.itemPath}`}>
                            対象: {selectedFinding.source} / {selectedFinding.itemPath}
                          </p>
                          <div className="mt-2">
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => void handleRegisterException()}
                              disabled={remediationBusy}
                            >
                              例外登録
                            </Button>
                          </div>
                        </div>
                      )}
                      {!isLowRiskFinding && (normalizedRemediationState === 'manual_required' || isManualOnlyRemediationMode) && (
                        <div className="rounded-md border border-amber-200 bg-amber-50 p-3">
                          <p className="text-xs text-amber-700">手動対応が必要な理由</p>
                          <p className="mt-1 text-amber-900">
                            この検知では自動適用可能な是正が限定的なため、運用者による確認/対応が必要です。
                          </p>
                          {manualReasonCodes.length > 0 && (
                            <div className="mt-2 rounded-md border border-amber-300 bg-amber-100 p-2">
                              <p className="text-xs font-medium text-amber-900">手動対応へ縮退した理由コード（コスト制御含む）</p>
                              <div className="mt-1 space-y-1 text-xs text-amber-900">
                                {manualReasonCodes.map((code) => (
                                  <p key={code}>- {code}: {remediationManualReasonText(code)}</p>
                                ))}
                              </div>
                            </div>
                          )}
                          <div className="mt-2 space-y-1 text-xs text-amber-900">
                            {manualChecklist.map((item) => (
                              <p key={item}>- {item}</p>
                            ))}
                          </div>
                        </div>
                      )}
                      {isExecutedFinding && (
                        <div className="rounded-md border p-3">
                          <p className="text-xs text-muted-foreground">次アクション</p>
                          <p className="mt-1">
                            {canRollbackRemediation
                              ? isManualOnlyRemediationMode
                                ? '必要に応じて上の「ロールバック」から実行済み変更を復元できます'
                                : '必要に応じてロールバック可能（実行済み変更を復元）'
                              : '不要（この項目は是正実行済み）'}
                          </p>
                        </div>
                      )}
                      {isPostRemediationState && (
                        <div className="rounded-md border p-3">
                          <p className="text-xs text-muted-foreground">ロールバック余地</p>
                          <p className="mt-1">
                            {canRollbackRemediation
                              ? 'あり（是正前の状態へ復元可能）'
                              : selectedFinding.status === 'closed'
                                ? '不要'
                                : 'あり（抑止設定で一時回避可能）'}
                          </p>
                        </div>
                      )}
                    </>
                  )}
                </div>
                <div className="mt-3 flex shrink-0 justify-end">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setIsEvidencePanelOpen((prev) => !prev)}
                  >
                    {isEvidencePanelOpen ? '証跡を閉じる' : '証跡を見る'}
                  </Button>
                </div>
              </div>
            </DialogContent>
          </Dialog>
          <OperationConfirmDialog
            isOpen={isManualModeRollbackConfirmOpen}
            actionType="rollback"
            planId={selectedFinding.planId}
            scopeText={`${selectedFinding.source} / ${selectedFinding.itemPath}`}
            dryRunSummaryText={manualRollbackDryRunSummary}
            predictedImpactText={manualRollbackPredictedImpact}
            rollbackCapabilityText={manualRollbackCapabilityText}
            reason="手動対応モードで実行済みの是正をロールバックします"
            isConfirmDisabled={remediationBusy || !canRollbackRemediation}
            onClose={() => setIsManualModeRollbackConfirmOpen(false)}
            onConfirm={() => {
              setIsManualModeRollbackConfirmOpen(false)
              void handleRollbackRemediation()
            }}
          />
        </>
      )}
    </div>
  )
}

const GLOBAL_POLICY_UI_META: Record<string, {
  title: string
  description: string
  impact: string
  changeHint: string
  appliedInCurrentRuntime: boolean
}> = {
  risk_score_threshold: {
    title: 'リスクスコアしきい値',
    description: '再評価時に検知対象として扱う最小リスクスコアです。',
    impact: '影響: +18件（検知増加見込み）',
    changeHint: '下げると検知件数が増え、上げると減ります。',
    appliedInCurrentRuntime: true
  },
  permissions_count_threshold: {
    title: '過剰権限の検知しきい値',
    description: '権限数がこの値を超えると excessive_permissions を検知します。',
    impact: '影響: +6件（過剰権限検知が増加）',
    changeHint: '下げると過剰権限として扱う対象が増え、上げると減ります。',
    appliedInCurrentRuntime: true
  },
  policy_public_link_block_enabled: {
    title: '公開リンク遮断ポリシー',
    description: 'public_link（Anyoneリンク）をグローバルで block 判定するかを制御します。',
    impact: '影響: 公開リンク検知は即時 block 方針に統一',
    changeHint: 'true で公開リンクを常時 block、false でスコープ別ポリシーのみで判定します。',
    appliedInCurrentRuntime: true
  },
  policy_org_link_edit_review_enabled: {
    title: '組織リンク編集のレビュー必須ポリシー',
    description: 'org_link_edit を review 判定（approval）へ統一するグローバル設定です。',
    impact: '影響: 組織共有の編集権限がレビュー対象として増加',
    changeHint: 'true で org_link_edit を review、false で他ポリシーに委譲します。',
    appliedInCurrentRuntime: true
  },
  policy_external_specific_people_review_enabled: {
    title: '外部Specific People共有レビュー必須ポリシー',
    description: 'specific_people_external を approval + remove_permissions 方針で運用する設定です。',
    impact: '影響: 外部 specific_people の是正実行率が向上',
    changeHint: 'true で外部 specific_people を是正候補化、false で他ポリシーに委譲します。',
    appliedInCurrentRuntime: true
  },
  policy_external_domain_share_auto_remediation_enabled: {
    title: '外部ドメイン共有事故の是正ポリシー',
    description: 'external_domain_share を approval + remove_permissions 方針で運用する設定です。',
    impact: '影響: 外部ドメイン共有の是正実行率が向上',
    changeHint: 'true で自動是正候補化、false で他ポリシーに委譲します。',
    appliedInCurrentRuntime: true
  },
  policy_external_email_direct_share_auto_remediation_enabled: {
    title: '外部直接共有（メール）事故の是正ポリシー',
    description: 'external_email_direct_share を approval + remove_permissions 方針で運用する設定です。',
    impact: '影響: 外部メール共有の是正実行率が向上',
    changeHint: 'true で自動是正候補化、false で他ポリシーに委譲します。',
    appliedInCurrentRuntime: true
  },
  policy_guest_direct_share_auto_remediation_enabled: {
    title: '外部直接共有事故の是正ポリシー',
    description: 'guest_direct_share を approval + remove_permissions 方針で運用する設定です。',
    impact: '影響: 外部直接共有の是正実行率が向上',
    changeHint: 'true で自動是正候補化、false で他ポリシーに委譲します。',
    appliedInCurrentRuntime: true
  },
  policy_all_users_block_enabled: {
    title: '全社公開（all_users）遮断ポリシー',
    description: 'all_users ベクトルを block 判定するグローバル設定です。',
    impact: '影響: all_users 共有は高優先で block 対象化',
    changeHint: 'true で all_users を block、false で部門別運用へ委譲します。',
    appliedInCurrentRuntime: true
  },
  governance_finding_table_name: {
    title: '検知結果テーブル名',
    description: 'ガバナンスの Finding を保存/参照する DynamoDB テーブル名です。',
    impact: '影響: 軽微',
    changeHint: 'システム固定値（AIReadyGov-ExposureFinding）として運用し、ユーザー編集対象外です。',
    appliedInCurrentRuntime: false
  },
  batch_scoring_hour_utc: {
    title: 'バッチ再評価時刻（UTC）',
    description: '日次再評価の実行時刻設定値です。',
    impact: '影響: 軽微',
    changeHint: '現行はスケジューラ側の固定設定が優先されるため、この値単体では時刻変更されない場合があります。',
    appliedInCurrentRuntime: false
  },
  max_exposure_score: {
    title: '露出スコア上限',
    description: '露出スコア計算の上限値（将来拡張向け）です。',
    impact: '影響: 軽微',
    changeHint: '現行の oversharing-only 経路では直接反映されない設定です。',
    appliedInCurrentRuntime: false
  },
  max_file_size_bytes: {
    title: '解析対象ファイル最大サイズ（バイト）',
    description: 'コンテンツ解析の対象上限サイズです。',
    impact: '影響: 軽微',
    changeHint: '現行の oversharing-only 主経路では直接反映されない設定です。',
    appliedInCurrentRuntime: false
  },
  max_text_length: {
    title: '解析対象テキスト最大長',
    description: 'コンテンツ解析で扱う最大文字数です。',
    impact: '影響: 軽微',
    changeHint: '現行の oversharing-only 主経路では直接反映されない設定です。',
    appliedInCurrentRuntime: false
  },
  rescan_interval_days: {
    title: '再スキャン間隔（日）',
    description: '再評価サイクルを調整する設定値です。',
    impact: '影響: ジョブ負荷 +12% / 再検知速度向上',
    changeHint: '現行の定期ジョブ運用では直接反映されないため、将来適用予定の管理値です。',
    appliedInCurrentRuntime: false
  },
  onboarded_at: {
    title: 'オンボード日時',
    description: 'テナント初期化完了の監査用タイムスタンプです。',
    impact: '影響: 軽微',
    changeHint: '監査情報であり、検知判定には使われません。',
    appliedInCurrentRuntime: false
  },
  status: {
    title: 'テナント有効状態',
    description: 'オンボーディング状態を示す運用メタ情報です。',
    impact: '影響: 軽微',
    changeHint: '運用状態管理用であり、検知判定には使われません。',
    appliedInCurrentRuntime: false
  }
}

const globalPolicySuffix = (key: string): string => key.split('/').slice(-1)[0] || key

const isSuppressionExpiringSoon = (exceptionReviewDueAt: string): boolean => {
  const due = new Date(exceptionReviewDueAt)
  if (Number.isNaN(due.getTime())) return false
  const now = Date.now()
  const diffMs = due.getTime() - now
  return diffMs >= 0 && diffMs <= 24 * 60 * 60 * 1000
}

const SuppressionPage = ({
  deepLinkFocus,
  suppressions,
  loading,
  loadError
}: {
  deepLinkFocus?: string
  suppressions: GovernanceSuppression[]
  loading: boolean
  loadError: string | null
}) => {
  const highlightedRowRef = useRef<HTMLTableRowElement | null>(null)
  const [flashActive, setFlashActive] = useState(false)
  const [preset, setPreset] = useState<'all' | 'expiring24h'>('all')
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize, setPageSize] = useState(25)
  useEffect(() => {
    if (deepLinkFocus === 'suppression:expiring') {
      setPreset('expiring24h')
    }
  }, [deepLinkFocus])
  useEffect(() => {
    if (deepLinkFocus === 'suppression:expiring') {
      highlightedRowRef.current?.scrollIntoView({ behavior: 'smooth', block: 'center' })
      setFlashActive(true)
      const timer = window.setTimeout(() => setFlashActive(false), 1600)
      return () => window.clearTimeout(timer)
    }
  }, [deepLinkFocus])
  const filteredRows =
    preset === 'expiring24h'
      ? suppressions.filter((row) => isSuppressionExpiringSoon(row.exceptionReviewDueAt))
      : suppressions
  const totalRows = filteredRows.length
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize))
  const pagedRows = useMemo(() => paginateRows(filteredRows, currentPage, pageSize), [filteredRows, currentPage, pageSize])
  useEffect(() => {
    setCurrentPage(1)
  }, [preset, pageSize])
  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(totalPages)
  }, [currentPage, totalPages])
  return (
    <Card>
      <CardHeader>
        <CardTitle>例外レジストリ</CardTitle>
        <CardDescription>期限付き抑止の運用管理</CardDescription>
      </CardHeader>
      <CardContent>
        {loading && <p className="mb-3 text-sm text-muted-foreground">抑止レジストリを取得中です...</p>}
        {loadError && (
          <p className="mb-3 text-sm text-rose-600">{loadError}</p>
        )}
        <div className="mb-3 flex flex-wrap items-center gap-2">
          <Button size="sm" variant={preset === 'expiring24h' ? 'default' : 'outline'} onClick={() => setPreset('expiring24h')}>
            24h以内に失効
          </Button>
          <Button size="sm" variant={preset === 'all' ? 'default' : 'outline'} onClick={() => setPreset('all')}>
            すべて表示
          </Button>
        </div>
        <TablePageControls
          totalRows={totalRows}
          currentPage={currentPage}
          pageSize={pageSize}
          onPageChange={setCurrentPage}
          onPageSizeChange={setPageSize}
        />
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead title="抑止対象の検知IDです。">検知ID</TableHead>
              <TableHead title="抑止対象の現在状態です。">状態</TableHead>
              <TableHead title="ワークフロー状態です。">workflow</TableHead>
              <TableHead title="例外種別です。">例外種別</TableHead>
              <TableHead title="再レビュー期限です。">レビュー期限</TableHead>
              <TableHead title="実リスクです（0..1）。">実リスク</TableHead>
              <TableHead title="抑止を設定したユーザーです。">対応者</TableHead>
              <TableHead title="抑止理由です。">理由</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {pagedRows.map((row, idx) => {
              const shouldHighlight =
              deepLinkFocus === 'suppression:expiring' && idx === 0
              return (
                <TableRow
                  key={row.findingId}
                  ref={shouldHighlight ? highlightedRowRef : null}
                  className={cn(shouldHighlight && flashActive && 'animate-pulse')}
                >
                  <TableCell className={cn(shouldHighlight && 'bg-primary/10 font-medium')}>{row.findingId}</TableCell>
                  <TableCell>
                    <Badge variant={badgeVariantByValue(row.status)} className={badgeClassByValue(row.status)}>
                      {row.status}
                    </Badge>
                  </TableCell>
                  <TableCell>{row.workflowStatus ?? '-'}</TableCell>
                  <TableCell>{row.exceptionType}</TableCell>
                  <TableCell>{row.exceptionReviewDueAt}</TableCell>
                  <TableCell>{typeof row.rawResidualRisk === 'number' ? row.rawResidualRisk.toFixed(2) : '-'}</TableCell>
                  <TableCell>{row.acknowledgedBy}</TableCell>
                  <TableCell className="max-w-[520px] truncate">{row.acknowledgedReason}</TableCell>
                </TableRow>
              )})}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}

const JobsPage = ({
  rows
}: {
  rows: GovernanceScanJob[]
}) => {
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize, setPageSize] = useState(25)
  const totalRows = rows.length
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize))
  const pagedRows = useMemo(() => paginateRows(rows, currentPage, pageSize), [rows, currentPage, pageSize])
  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(totalPages)
  }, [currentPage, totalPages])

  return (
    <Card>
      <CardHeader>
        <CardTitle>実行ジョブ</CardTitle>
        <CardDescription>ガバナンス スキャンジョブの実行状態と結果を確認します。</CardDescription>
      </CardHeader>
      <CardContent>
        <TablePageControls
          totalRows={totalRows}
          currentPage={currentPage}
          pageSize={pageSize}
          onPageChange={setCurrentPage}
          onPageSizeChange={setPageSize}
        />
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>ジョブID</TableHead>
              <TableHead>種別</TableHead>
              <TableHead>状態</TableHead>
              <TableHead>開始</TableHead>
              <TableHead>終了</TableHead>
              <TableHead>詳細</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {pagedRows.map((row) => (
              <TableRow key={row.jobId}>
                <TableCell>{row.jobId}</TableCell>
                <TableCell>{row.jobType}</TableCell>
                <TableCell>
                  <Badge variant={badgeVariantByValue(row.status)} className={badgeClassByValue(row.status)}>
                    {row.status}
                  </Badge>
                </TableCell>
                <TableCell>{row.startedAt}</TableCell>
                <TableCell>{row.endedAt ?? '-'}</TableCell>
                <TableCell>{row.message}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}

const PoliciesPage = ({
  policies,
  loading,
  loadError,
  onCreatePolicy,
  onUpdatePolicy
}: {
  policies: GovernancePoliciesResponse | null
  loading: boolean
  loadError: string | null
  onCreatePolicy: (payload: Record<string, any>, dryRun?: boolean) => Promise<void>
  onUpdatePolicy: (policyId: string, payload: Record<string, any>, dryRun?: boolean) => Promise<Record<string, any>>
}) => {
  type PolicyLayer = 'global' | 'scope'
  type PolicyEditorLayer = PolicyLayer
  type GlobalPolicyRow = GovernancePolicy & {
    title: string
    rowType: 'config' | 'policy'
    status: 'enabled' | 'disabled'
    updatedBy: string
    updatedAt: string
    raw?: Record<string, any>
  }
  type ScopePolicyRow = {
    id: string
    name: string
    scopeSummary: string
    overrideSummary: string
    priority: number
    status: 'enabled' | 'disabled'
    appliesEstimate: number
    updatedAt: string
    raw: Record<string, any>
  }
  type EditableRule = {
    ui_id: string
    rule_id: string
    vector: string
    effect: string
    severity: string
    remediation_mode: string
    remediation_action: string
    reason_codes: string
    priority_value: number
  }
  const policyStatusBadgeClass = (status: string): string => {
    if (status === 'enabled' || status === '有効') return 'border-emerald-300 bg-emerald-100 text-emerald-800'
    if (status === 'disabled' || status === '無効') return 'border-slate-300 bg-slate-100 text-slate-800'
    if (status === '期限切れ') return 'border-amber-300 bg-amber-100 text-amber-900'
    if (status === '取り消し') return 'border-red-300 bg-red-100 text-red-800'
    return ''
  }
  const formatUtcFromNowOffset = (offsetMinutes: number): string => {
    const d = new Date(Date.now() - offsetMinutes * 60_000)
    const yyyy = d.getUTCFullYear()
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0')
    const dd = String(d.getUTCDate()).padStart(2, '0')
    const hh = String(d.getUTCHours()).padStart(2, '0')
    const mi = String(d.getUTCMinutes()).padStart(2, '0')
    const ss = String(d.getUTCSeconds()).padStart(2, '0')
    return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss} UTC`
  }

  const [activeLayer, setActiveLayer] = useState<PolicyLayer>('global')
  const [mode, setMode] = useState<'list' | 'create'>('list')
  const [globalPolicies, setGlobalPolicies] = useState<GlobalPolicyRow[]>([])
  const [scopePolicies, setScopePolicies] = useState<ScopePolicyRow[]>([])
  const scopeMode = policies?.scope_mode ?? { enabled: true, message: '' }
  const scopePoliciesEnabled = scopeMode.enabled !== false

  useEffect(() => {
    if (!scopePoliciesEnabled && activeLayer === 'scope') {
      setActiveLayer('global')
    }
  }, [scopePoliciesEnabled, activeLayer])

  useEffect(() => {
    if (!policies) return
    const globalSource = (policies.global_policies ?? []).length > 0
      ? Object.fromEntries(
        (policies.global_policies ?? []).map((row) => [String(row.name ?? ''), String(row.value ?? '')])
      )
      : (policies.global ?? {})
    const configRows: GlobalPolicyRow[] = Object.entries(globalSource)
      .filter(([key]) => {
        const meta = GLOBAL_POLICY_UI_META[globalPolicySuffix(key)]
        return Boolean(meta?.appliedInCurrentRuntime)
      })
      .map(([key, value], idx) => {
        const suffix = globalPolicySuffix(key)
        const meta = GLOBAL_POLICY_UI_META[suffix]
        return {
          title: meta?.title ?? suffix,
          key,
          value: String(value),
          rowType: 'config',
          description: meta?.description ?? 'API取得値',
          status: 'enabled',
          updatedBy: 'system',
          updatedAt: formatUtcFromNowOffset(idx + 1)
        }
      })
    const policyRows: GlobalPolicyRow[] = (policies.global_policy_rows ?? []).map((row, idx) => ({
      title: String(row.name ?? row.policy_id ?? `global-policy-${idx + 1}`),
      key: String(row.policy_id ?? `global-policy-${idx + 1}`),
      value: `rules=${Array.isArray(row.rules) ? row.rules.length : 0}`,
      rowType: 'policy',
      description: String(row.description ?? '組織全体へ適用されるデータ駆動ポリシーです。'),
      status: String(row.status ?? 'active') === 'active' ? 'enabled' : 'disabled',
      updatedBy: String(row.operator ?? row.updated_by ?? 'system'),
      updatedAt: String(row.updated_at ?? formatUtcFromNowOffset(idx + 1)),
      raw: row as Record<string, any>
    }))
    setGlobalPolicies([...policyRows, ...configRows])
    const scopeSource = scopePoliciesEnabled
      ? ((policies.scope_policies ?? []).length > 0 ? (policies.scope_policies ?? []) : (policies.scope ?? []))
      : []
    const scopeRows: ScopePolicyRow[] = scopeSource.map((row, idx) => ({
      id: String(row.policy_id ?? `scope-policy-api-${idx + 1}`),
      name: String(row.name ?? `scope-policy-${idx + 1}`),
      scopeSummary: `${row.scope_type ?? '-'}:${row.scope_value ?? '-'}`,
      overrideSummary: `status=${row.status ?? 'active'}`,
      priority: Number(row.priority ?? 100),
      status: String(row.status ?? 'active') === 'active' ? 'enabled' : 'disabled',
      appliesEstimate: Number(row.estimated_affected_count ?? 0),
      updatedAt: String(row.updated_at ?? formatUtcFromNowOffset(idx + 1)),
      raw: row
    }))
    setScopePolicies(scopeRows)
  }, [policies])

  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize, setPageSize] = useState(25)
  const [stepIndex, setStepIndex] = useState(0)
  const [draft, setDraft] = useState({
    layer: 'global' as PolicyEditorLayer,
    policyName: '',
    purpose: 'accuracy',
    domain: 'exposure',
    detectionTarget: 'sharing',
    evaluationMode: 'threshold',
    policyDescription: '',
    scopeDepartment: '',
    scopeSiteId: '',
    scopeDataLabels: 'Confidential-HR, HighlyConfidential-HR',
    allowedPrincipals: 'HR-members, HR-managers, Executive',
    denyAnyoneLink: true,
    denyExternalGuest: true,
    denyOrgLink: true,
    policyValueType: 'numeric',
    thresholdValue: '2.0',
    warningThreshold: '1.5',
    criticalThreshold: '3.5',
    expectedFindingDelta: '+8',
    expectedJobLoadImpact: 'medium',
    expectedApplyCount: '1200',
    rollOutScope: 'pilot',
    confirmNoHardcodedSecret: false,
    confirmApprovalOwner: false,
    confirmRollbackReady: false
  })
  const [editingGlobal, setEditingGlobal] = useState<GlobalPolicyRow | null>(null)
  const [editingScope, setEditingScope] = useState<ScopePolicyRow | null>(null)
  const [editValue, setEditValue] = useState('')
  const [editDescription, setEditDescription] = useState('')
  const [editStatus, setEditStatus] = useState<'enabled' | 'disabled'>('enabled')
  const [editPriority, setEditPriority] = useState('100')
  const [editAppliesEstimate, setEditAppliesEstimate] = useState('0')
  const [editName, setEditName] = useState('')
  const [editScopeDepartmentIds, setEditScopeDepartmentIds] = useState('')
  const [editScopeSiteIds, setEditScopeSiteIds] = useState('')
  const [editScopePrincipalGroupIds, setEditScopePrincipalGroupIds] = useState('')
  const [editScopePartnerAllowlist, setEditScopePartnerAllowlist] = useState('')
  const [editScopeCriticality, setEditScopeCriticality] = useState('medium')
  const [editScopeUseCases, setEditScopeUseCases] = useState('')
  const [editRules, setEditRules] = useState<EditableRule[]>([])
  const [editRolloutStage, setEditRolloutStage] = useState('active')
  const [editRolloutDryRun, setEditRolloutDryRun] = useState(false)
  const [editStepIndex, setEditStepIndex] = useState(0)
  const [editSimulationResult, setEditSimulationResult] = useState<Record<string, any> | null>(null)
  const [activeDragRuleId, setActiveDragRuleId] = useState<string | null>(null)
  const [overDragRuleId, setOverDragRuleId] = useState<string | null>(null)
  const [isSavingEdit, setIsSavingEdit] = useState(false)
  const [runScanAfterSave, setRunScanAfterSave] = useState(true)

  const POLICY_STEPS = [
    { key: 'basic', label: '基本情報' },
    { key: 'rule', label: '条件定義' },
    { key: 'threshold', label: '閾値・効果' },
    { key: 'impact', label: '影響見積もり' },
    { key: 'confirm', label: '確認・登録' }
  ] as const

  const updateDraft = <K extends keyof typeof draft>(key: K, value: (typeof draft)[K]) => {
    setDraft((prev) => ({ ...prev, [key]: value }))
  }

  const layerMeta: Record<PolicyLayer, { label: string; description: string }> = {
    global: {
      label: 'グローバルポリシー',
      description: '全社共通の判定基準を管理します。'
    },
    scope: {
      label: 'スコープ別ポリシー',
      description: '部門・サイト・ラベル等の条件で限定上書きします。'
    }
  }

  const activeRows = useMemo<Array<GlobalPolicyRow | ScopePolicyRow>>(() => {
    if (activeLayer === 'global') return globalPolicies
    return scopePolicies
  }, [activeLayer, globalPolicies, scopePolicies])
  const totalRows = activeRows.length
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize))
  const pagedGlobalRows = useMemo(
    () => paginateRows(globalPolicies, currentPage, pageSize),
    [globalPolicies, currentPage, pageSize]
  )
  const pagedScopeRows = useMemo(
    () => paginateRows(scopePolicies, currentPage, pageSize),
    [scopePolicies, currentPage, pageSize]
  )

  const canProceed = (): boolean => {
    if (stepIndex === 0) return Boolean(draft.policyName.trim() && draft.layer)
    if (stepIndex === 1) {
      if (draft.layer === 'scope') {
        return Boolean(
          draft.policyDescription.trim() &&
          (draft.scopeDepartment.trim() || draft.scopeSiteId.trim() || draft.scopeDataLabels.trim())
        )
      }
      return Boolean(draft.policyDescription.trim())
    }
    if (stepIndex === 2) {
      return Boolean(draft.thresholdValue.trim() && draft.warningThreshold.trim() && draft.criticalThreshold.trim())
    }
    if (stepIndex === 3) {
      return Boolean(draft.expectedFindingDelta.trim() && draft.expectedApplyCount.trim())
    }
    if (stepIndex === 4) {
      return draft.confirmNoHardcodedSecret && draft.confirmApprovalOwner && draft.confirmRollbackReady
    }
    return true
  }

  const resetPolicyDraft = (layer?: PolicyEditorLayer) => {
    setStepIndex(0)
    setDraft({
      layer: layer ?? 'global',
      policyName: '',
      purpose: 'accuracy',
      domain: 'exposure',
      detectionTarget: 'sharing',
      evaluationMode: 'threshold',
      policyDescription: '',
      scopeDepartment: '',
      scopeSiteId: '',
      scopeDataLabels: 'Confidential-HR, HighlyConfidential-HR',
      allowedPrincipals: 'HR-members, HR-managers, Executive',
      denyAnyoneLink: true,
      denyExternalGuest: true,
      denyOrgLink: true,
      policyValueType: 'numeric',
      thresholdValue: '2.0',
      warningThreshold: '1.5',
      criticalThreshold: '3.5',
      expectedFindingDelta: '+8',
      expectedJobLoadImpact: 'medium',
      expectedApplyCount: '1200',
      rollOutScope: 'pilot',
      confirmNoHardcodedSecret: false,
      confirmApprovalOwner: false,
      confirmRollbackReady: false
    })
  }

  const openCreateForm = () => {
    if (activeLayer === 'scope' && !scopePoliciesEnabled) {
      toast('PoCモードではスコープ別ポリシーは無効です', {
        description: scopeMode.message || 'グローバルポリシーのみで運用してください。'
      })
      setActiveLayer('global')
      resetPolicyDraft('global')
      return
    }
    resetPolicyDraft(activeLayer)
    setMode('create')
  }

  const openGlobalEdit = (row: GlobalPolicyRow) => {
    if (row.rowType === 'policy' && row.raw) {
      const globalAsScope: ScopePolicyRow = {
        id: String(row.raw.policy_id ?? row.key),
        name: String(row.raw.name ?? row.title),
        scopeSummary: 'organization',
        overrideSummary: `status=${row.raw.status ?? 'active'}`,
        priority: Number(row.raw.priority ?? 900),
        status: String(row.raw.status ?? 'active') === 'active' ? 'enabled' : 'disabled',
        appliesEstimate: Number(row.raw.estimated_affected_count ?? 0),
        updatedAt: String(row.raw.updated_at ?? row.updatedAt),
        raw: row.raw
      }
      openScopeEdit(globalAsScope)
      return
    }
    setEditingGlobal(row)
    setEditingScope(null)
    setEditValue(row.value)
    setEditDescription(row.description)
  }

  const openScopeEdit = (row: ScopePolicyRow) => {
    const scope = (row.raw.scope ?? {}) as Record<string, any>
    const rules = Array.isArray(row.raw.rules) ? row.raw.rules : []
    const rollout = (row.raw.rollout ?? {}) as Record<string, any>
    setEditingScope(row)
    setEditingGlobal(null)
    setEditName(row.name)
    setEditStatus(row.status)
    setEditPriority(String(row.priority))
    setEditAppliesEstimate(String(row.appliesEstimate))
    setEditScopeDepartmentIds(Array.isArray(scope.department_ids) ? scope.department_ids.join(', ') : '')
    setEditScopeSiteIds(Array.isArray(scope.site_ids) ? scope.site_ids.join(', ') : '')
    setEditScopePrincipalGroupIds(Array.isArray(scope.principal_group_ids) ? scope.principal_group_ids.join(', ') : '')
    setEditScopePartnerAllowlist(
      Array.isArray(scope.partner_domains_allowlist) ? scope.partner_domains_allowlist.join(', ') : ''
    )
    setEditScopeCriticality(String(scope.criticality ?? 'medium'))
    setEditScopeUseCases(Array.isArray(scope.use_cases) ? scope.use_cases.join(', ') : '')
    const normalizedRules: EditableRule[] = (rules.length > 0 ? rules : [row.raw]).map((rule: Record<string, any>, idx: number) =>
      makeEditableRule(
        {
          ui_id: String(rule.rule_id ?? `rule-${idx + 1}`),
          rule_id: String(rule.rule_id ?? `rule-${idx + 1}`),
          vector: String(rule.vector ?? row.raw.conditions?.vector ?? 'specific_people_external'),
          effect: String(rule.effect ?? row.raw.actions?.effect ?? 'review'),
          severity: String(rule.severity ?? row.raw.actions?.severity ?? 'high'),
          remediation_mode: String(rule.remediation_mode ?? row.raw.actions?.remediation_mode ?? 'approval'),
          remediation_action: String(rule.remediation_action ?? row.raw.actions?.remediation_action ?? 'request_review'),
          reason_codes: Array.isArray(rule.reason_codes) ? rule.reason_codes.join(', ') : '',
          priority_value: Number(
            rule?.conditions?.order_priority ??
            rule?.priority_value ??
            Math.max(1, 100 - idx)
          )
        },
        idx
      )
    )
    setEditRules(normalizedRules)
    setEditRolloutStage(String(rollout.stage ?? 'active'))
    setEditRolloutDryRun(Boolean(rollout.dry_run))
    setEditStepIndex(0)
    setEditSimulationResult(null)
  }

  const closeEditDialog = () => {
    if (isSavingEdit) return
    setEditingGlobal(null)
    setEditingScope(null)
    setEditRules([])
    setActiveDragRuleId(null)
    setOverDragRuleId(null)
    setEditStepIndex(0)
    setEditSimulationResult(null)
  }

  const splitCsv = (value: string): string[] =>
    value
      .split(',')
      .map((v) => v.trim())
      .filter(Boolean)

  const makeEditableRule = (seed?: Partial<EditableRule>, defaultIndex: number = 0): EditableRule => ({
    ui_id: seed?.ui_id ?? `rule-ui-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    rule_id: seed?.rule_id ?? `rule-${defaultIndex + 1}`,
    vector: seed?.vector ?? 'specific_people_external',
    effect: seed?.effect ?? 'review',
    severity: seed?.severity ?? 'high',
    remediation_mode: seed?.remediation_mode ?? 'approval',
    remediation_action: seed?.remediation_action ?? 'request_review',
    reason_codes: seed?.reason_codes ?? '',
    priority_value: Number.isFinite(seed?.priority_value) ? Number(seed?.priority_value) : Math.max(1, 100 - defaultIndex),
  })

  const dndSensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 4 } }))

  const updateEditRule = (index: number, patch: Partial<EditableRule>) => {
    setEditRules((prev) => prev.map((rule, idx) => (idx === index ? { ...rule, ...patch } : rule)))
  }

  const addEditRule = () => {
    setEditRules((prev) => [
      ...prev,
      makeEditableRule({}, prev.length),
    ])
  }

  const removeEditRule = (index: number) => {
    setEditRules((prev) => prev.filter((_rule, idx) => idx !== index))
  }

  const moveEditRule = (index: number, direction: -1 | 1) => {
    setEditRules((prev) => {
      const nextIndex = index + direction
      if (nextIndex < 0 || nextIndex >= prev.length) return prev
      const copied = [...prev]
      const [rule] = copied.splice(index, 1)
      copied.splice(nextIndex, 0, rule)
      return copied
    })
  }

  const onRuleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event
    setActiveDragRuleId(null)
    setOverDragRuleId(null)
    if (!over || active.id === over.id) return
    setEditRules((prev) => {
      const oldIndex = prev.findIndex((rule) => rule.ui_id === String(active.id))
      const newIndex = prev.findIndex((rule) => rule.ui_id === String(over.id))
      if (oldIndex < 0 || newIndex < 0) return prev
      const moved = arrayMove(prev, oldIndex, newIndex)
      return moved.map((rule, idx) => ({
        ...rule,
        priority_value: moved.length - idx
      }))
    })
  }

  const onRuleDragStart = (event: DragStartEvent) => {
    setActiveDragRuleId(String(event.active.id))
  }

  const onRuleDragOver = (event: DragOverEvent) => {
    setOverDragRuleId(event.over ? String(event.over.id) : null)
  }

  const onRuleDragCancel = () => {
    setActiveDragRuleId(null)
    setOverDragRuleId(null)
  }

  const activeDragRule = useMemo(
    () => editRules.find((rule) => rule.ui_id === activeDragRuleId) ?? null,
    [editRules, activeDragRuleId]
  )

  const buildScopeUpdatePayload = (): Record<string, any> | null => {
    if (!editingScope) return null
    const nextPriority = Number(editPriority)
    const nextEstimate = Number(editAppliesEstimate)
    return {
      ...editingScope.raw,
      policy_type: 'scope',
      name: editName.trim() || editingScope.name,
      status: editStatus === 'enabled' ? 'active' : 'inactive',
      priority: Number.isFinite(nextPriority) ? nextPriority : editingScope.priority,
      estimated_affected_count: Number.isFinite(nextEstimate) ? nextEstimate : editingScope.appliesEstimate,
      scope: {
        ...(editingScope.raw.scope ?? {}),
        scope_type: String((editingScope.raw.scope ?? {}).scope_type ?? editingScope.raw.scope_type ?? 'department'),
        department_ids: splitCsv(editScopeDepartmentIds),
        site_ids: splitCsv(editScopeSiteIds),
        principal_group_ids: splitCsv(editScopePrincipalGroupIds),
        partner_domains_allowlist: splitCsv(editScopePartnerAllowlist),
        criticality: editScopeCriticality,
        use_cases: splitCsv(editScopeUseCases)
      },
      rules: [
        ...editRules
          .filter((rule) => rule.vector.trim().length > 0)
          .map((rule, idx) => ({
            rule_id: rule.rule_id.trim() || `rule-${idx + 1}`,
            vector: rule.vector.trim(),
            effect: rule.effect.trim() || 'review',
            severity: rule.severity.trim() || 'high',
            remediation_mode: rule.remediation_mode.trim() || 'approval',
            remediation_action: rule.remediation_action.trim() || 'request_review',
            reason_codes: splitCsv(rule.reason_codes),
            conditions: {
              ...(typeof (editingScope.raw.rules?.[idx]?.conditions) === 'object'
                ? editingScope.raw.rules[idx].conditions
                : {}),
              rule_order: idx + 1,
              order_priority: Number.isFinite(rule.priority_value) ? rule.priority_value : editRules.length - idx
            }
          }))
      ],
      rollout: {
        stage: editRolloutStage,
        dry_run: editRolloutDryRun
      }
    }
  }

  const nowUtcText = (): string => {
    const now = new Date()
    const yyyy = now.getUTCFullYear()
    const mm = String(now.getUTCMonth() + 1).padStart(2, '0')
    const dd = String(now.getUTCDate()).padStart(2, '0')
    const hh = String(now.getUTCHours()).padStart(2, '0')
    const mi = String(now.getUTCMinutes()).padStart(2, '0')
    const ss = String(now.getUTCSeconds()).padStart(2, '0')
    return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss} UTC`
  }

  const createPolicy = async () => {
    if (!canProceed()) {
      toast('必須項目が未入力です', {
        description: '確認チェックを含む必須項目を入力してください。'
      })
      return
    }

    const operatedAt = nowUtcText()
    const layer = draft.layer
    if (layer === 'global') {
      const normalized = draft.policyName
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '') || 'custom_policy'
      const newPolicy: GlobalPolicyRow = {
        title: draft.policyName.trim(),
        key: `global-policy-${normalized}`,
        value: 'rules=0',
        rowType: 'policy',
        description: draft.policyDescription.trim(),
        status: 'enabled',
        updatedBy: 'current-user',
        updatedAt: operatedAt,
        raw: {}
      }
      setGlobalPolicies((prev) => [newPolicy, ...prev])
      const rules: Array<Record<string, any>> = []
      if (draft.denyAnyoneLink) {
        rules.push({
          rule_id: `deny-public-link-${Date.now()}`,
          vector: 'public_link',
          effect: 'block',
          severity: 'critical',
          remediation_mode: 'auto',
          remediation_action: 'remove_permissions',
          reason_codes: ['GLOBAL_PUBLIC_LINK_BLOCK']
        })
      }
      if (draft.denyOrgLink) {
        rules.push({
          rule_id: `review-org-link-edit-${Date.now()}`,
          vector: 'org_link_edit',
          effect: 'review',
          severity: 'high',
          remediation_mode: 'approval',
          remediation_action: 'request_review',
          reason_codes: ['GLOBAL_ORG_LINK_EDIT_REVIEW']
        })
      }
      if (draft.denyExternalGuest) {
        rules.push({
          rule_id: `review-specific-external-${Date.now()}`,
          vector: 'specific_people_external',
          effect: 'review',
          severity: 'high',
          remediation_mode: 'approval',
          remediation_action: 'request_review',
          reason_codes: ['GLOBAL_EXTERNAL_SPECIFIC_REVIEW']
        })
      }
      if (rules.length === 0) {
        rules.push({
          rule_id: `block-all-users-${Date.now()}`,
          vector: 'all_users',
          effect: 'block',
          severity: 'critical',
          remediation_mode: 'owner_review',
          remediation_action: 'request_site_owner_review',
          reason_codes: ['GLOBAL_ALL_USERS_BLOCK']
        })
      }
      void onCreatePolicy({
        policy_type: 'global',
        policy_id: newPolicy.key,
        name: draft.policyName.trim() || newPolicy.key,
        description: draft.policyDescription.trim(),
        layer: 'organization',
        scope_type: 'organization',
        scope_value: 'organization',
        status: 'active',
        priority: 900,
        scope: {
          scope_type: 'organization',
          department_ids: [],
          site_ids: [],
          principal_group_ids: [],
          partner_domains_allowlist: [],
          criticality: 'high',
          use_cases: []
        },
        rules,
        rollout: { stage: draft.rollOutScope, dry_run: draft.rollOutScope === 'dry_run' },
        run_scan_after_save: runScanAfterSave
      }, draft.rollOutScope === 'dry_run')
      setActiveLayer('global')
    } else if (layer === 'scope') {
      if (!scopePoliciesEnabled) {
        toast('PoCモードではスコープ別ポリシーは無効です', {
          description: scopeMode.message || 'グローバルポリシーのみで運用してください。'
        })
        setActiveLayer('global')
        return
      }
      const scopeParts = [
        draft.scopeDepartment.trim() ? `department=${draft.scopeDepartment.trim()}` : '',
        draft.scopeSiteId.trim() ? `siteId=${draft.scopeSiteId.trim()}` : '',
        draft.scopeDataLabels.trim() ? `label in [${draft.scopeDataLabels.trim()}]` : ''
      ].filter(Boolean)
      const denyParts = [
        draft.denyAnyoneLink ? 'Anyoneリンク' : '',
        draft.denyExternalGuest ? '外部ゲスト共有' : '',
        draft.denyOrgLink ? '全社リンク' : ''
      ].filter(Boolean)
      const newScope: ScopePolicyRow = {
        id: `scope-policy-${Date.now()}`,
        name: draft.policyName.trim(),
        scopeSummary: scopeParts.join(' AND '),
        overrideSummary: `許可: ${draft.allowedPrincipals.trim()} / 禁止: ${denyParts.join(', ')}`,
        priority: 80,
        status: 'enabled',
        appliesEstimate: Number(draft.expectedApplyCount) || 0,
        updatedAt: operatedAt,
        raw: {}
      }
      setScopePolicies((prev) => [newScope, ...prev])
      void onCreatePolicy({
        policy_type: 'scope',
        name: newScope.name,
        scope_type: 'expression',
        scope_value: newScope.scopeSummary,
        status: newScope.status === 'enabled' ? 'active' : 'inactive',
        priority: newScope.priority,
        estimated_affected_count: newScope.appliesEstimate,
        rollout: { stage: draft.rollOutScope, dry_run: draft.rollOutScope === 'dry_run' },
        run_scan_after_save: runScanAfterSave
      }, draft.rollOutScope === 'dry_run')
      setActiveLayer('scope')
    }

    setCurrentPage(1)
    setMode('list')
    toast('ポリシーを追加しました', {
      description: `${layerMeta[layer].label}に登録しました。`
    })
    resetPolicyDraft()
  }

  const saveEditedPolicy = async () => {
    if (!editingGlobal && !editingScope) return
    setIsSavingEdit(true)
    try {
      if (editingGlobal) {
        const payload = {
          policy_type: 'global',
          parameter_name: editingGlobal.key,
          parameter_value: editValue.trim(),
          description: editDescription.trim(),
          run_scan_after_save: runScanAfterSave
        }
        await onUpdatePolicy(editingGlobal.key, payload, false)
      } else if (editingScope) {
        const payload = buildScopeUpdatePayload()
        if (!payload) return
        payload.run_scan_after_save = runScanAfterSave
        await onUpdatePolicy(editingScope.id, payload, false)
      }
      closeEditDialog()
    } catch {
      // toast is handled by parent callback
    } finally {
      setIsSavingEdit(false)
    }
  }

  const simulateScopeUpdate = async () => {
    if (!editingScope) return
    const payload = buildScopeUpdatePayload()
    if (!payload) return
    setIsSavingEdit(true)
    try {
      const result = await onUpdatePolicy(editingScope.id, payload, true)
      setEditSimulationResult(result)
      toast.success('更新前シミュレーションを実行しました')
    } catch {
      // toast is handled by parent callback
    } finally {
      setIsSavingEdit(false)
    }
  }

  const SortableRuleEditorCard = ({
    rule,
    index,
    total,
    isOver,
  }: {
    rule: EditableRule
    index: number
    total: number
    isOver: boolean
  }) => {
    const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id: rule.ui_id })
    const style = {
      transform: CSS.Transform.toString(transform),
      transition,
    }
    const priorityRank = total - index
    return (
      <div
        ref={setNodeRef}
        style={style}
        className={cn(
          'rounded-md border p-3 space-y-3 bg-background',
          isDragging && 'opacity-60 shadow-lg border-primary/60',
          isOver && 'border-dashed border-primary ring-1 ring-primary/30'
        )}
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <button
              type="button"
              className="cursor-grab rounded border px-2 py-1 text-xs text-muted-foreground active:cursor-grabbing"
              title="ドラッグして並び替え"
              {...attributes}
              {...listeners}
            >
              DnD
            </button>
            <p className="text-sm font-semibold">Rule {index + 1}</p>
            <Badge variant="outline">順序優先 {priorityRank}</Badge>
          </div>
          <div className="flex items-center gap-2">
            <Button
              size="sm"
              variant="outline"
              onClick={() => moveEditRule(index, -1)}
              disabled={index === 0}
            >
              ↑
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={() => moveEditRule(index, 1)}
              disabled={index === total - 1}
            >
              ↓
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={() => removeEditRule(index)}
              disabled={total <= 1}
            >
              削除
            </Button>
          </div>
        </div>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          <div className="space-y-1">
            <Label>rule_id</Label>
            <Input
              value={rule.rule_id}
              onChange={(e) => updateEditRule(index, { rule_id: e.target.value })}
            />
          </div>
          <div className="space-y-1">
            <Label>vector</Label>
            <Input
              value={rule.vector}
              onChange={(e) => updateEditRule(index, { vector: e.target.value })}
            />
          </div>
          <div className="space-y-1">
            <Label>priority（数値）</Label>
            <Input
              type="number"
              value={String(rule.priority_value)}
              onChange={(e) => {
                const parsed = Number(e.target.value)
                updateEditRule(index, { priority_value: Number.isFinite(parsed) ? parsed : rule.priority_value })
              }}
            />
          </div>
          <div className="space-y-1">
            <Label>effect</Label>
            <Select
              value={rule.effect}
              onValueChange={(v) => updateEditRule(index, { effect: v })}
            >
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="allow">allow</SelectItem>
                <SelectItem value="warn">warn</SelectItem>
                <SelectItem value="review">review</SelectItem>
                <SelectItem value="block">block</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label>severity</Label>
            <Select
              value={rule.severity}
              onValueChange={(v) => updateEditRule(index, { severity: v })}
            >
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="low">low</SelectItem>
                <SelectItem value="medium">medium</SelectItem>
                <SelectItem value="high">high</SelectItem>
                <SelectItem value="critical">critical</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label>remediation_mode</Label>
            <Select
              value={rule.remediation_mode}
              onValueChange={(v) => updateEditRule(index, { remediation_mode: v })}
            >
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="auto">auto</SelectItem>
                <SelectItem value="approval">approval</SelectItem>
                <SelectItem value="owner_review">owner_review</SelectItem>
                <SelectItem value="manual">manual</SelectItem>
                <SelectItem value="recommend_only">recommend_only</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label>remediation_action</Label>
            <Input
              value={rule.remediation_action}
              onChange={(e) => updateEditRule(index, { remediation_action: e.target.value })}
            />
          </div>
          <div className="space-y-1 md:col-span-2">
            <Label>reason_codes（カンマ区切り）</Label>
            <Input
              value={rule.reason_codes}
              onChange={(e) => updateEditRule(index, { reason_codes: e.target.value })}
            />
          </div>
        </div>
      </div>
    )
  }
  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(totalPages)
  }, [currentPage, totalPages])
  useEffect(() => {
    setCurrentPage(1)
  }, [pageSize, activeLayer, mode])

  return mode === 'list' ? (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <CardTitle>ポリシーと閾値設定（3層管理）</CardTitle>
            <CardDescription>{layerMeta[activeLayer].description}</CardDescription>
          </div>
          <Button size="sm" onClick={openCreateForm}>新規ポリシー追加</Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {loading && <p className="text-sm text-muted-foreground">ポリシー設定を取得中です...</p>}
        {loadError && <p className="text-sm text-amber-700">{loadError}</p>}
        <div className="flex flex-wrap gap-2">
          {(['global', 'scope'] as const).map((layer) => (
            <Button
              key={layer}
              size="sm"
              variant={activeLayer === layer ? 'default' : 'outline'}
              disabled={layer === 'scope' && !scopePoliciesEnabled}
              onClick={() => {
                if (layer === 'scope' && !scopePoliciesEnabled) return
                setActiveLayer(layer)
              }}
            >
              {layerMeta[layer].label}
              <span className="ml-2 text-xs opacity-80">
                {layer === 'global'
                  ? globalPolicies.length
                  : scopePolicies.length}
              </span>
            </Button>
          ))}
        </div>
        {!scopePoliciesEnabled && (
          <div className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-900">
            {scopeMode.message || 'PoCモードではスコープ別ポリシーは無効です。グローバルポリシーのみが適用されます。'}
          </div>
        )}
        <div className="flex items-center gap-2 rounded-md border bg-muted/20 px-3 py-2 text-sm">
          <Checkbox checked={runScanAfterSave} onCheckedChange={(v) => setRunScanAfterSave(v === true)} />
          <span>保存後に再スコアリングを自動実行する</span>
          <span className="text-xs text-muted-foreground">（変更したポリシーを検知結果へ反映）</span>
        </div>

        <TablePageControls
          totalRows={totalRows}
          currentPage={currentPage}
          pageSize={pageSize}
          onPageChange={setCurrentPage}
          onPageSizeChange={setPageSize}
        />

        {activeLayer === 'global' && (
          <Table className="min-w-full table-fixed">
            <TableHeader>
              <TableRow>
                <TableHead title="ポリシーの表示名です。">タイトル</TableHead>
                <TableHead title="現在値です。">現在値</TableHead>
                <TableHead title="設定の意味です。">説明</TableHead>
                <TableHead title="設定変更時の想定影響です。">影響見込み</TableHead>
                <TableHead title="ルールの有効状態です。">状態</TableHead>
                <TableHead title="最終更新者です。">最終更新者</TableHead>
                <TableHead title="最終更新日時です。">最終更新日時</TableHead>
                <TableHead title="操作です。">操作</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {pagedGlobalRows.map((row) => (
                <TableRow key={row.key}>
                  <TableCell className="max-w-[360px] truncate" title={row.title}>{row.title}</TableCell>
                  <TableCell>{row.value}</TableCell>
                  <TableCell>{row.description}</TableCell>
                  <TableCell>
                    {(() => {
                      const meta = GLOBAL_POLICY_UI_META[globalPolicySuffix(row.key)]
                      const hint = meta?.changeHint ?? 'この値の変更影響は設定に依存します。'
                      return (
                        <div className="flex items-center gap-2">
                          <span>{meta?.impact ?? '影響: 軽微'}</span>
                          <span
                            className="cursor-help rounded border px-1 text-xs text-muted-foreground"
                            title={hint}
                            aria-label={hint}
                          >
                            ?
                          </span>
                        </div>
                      )
                    })()}
                  </TableCell>
                  <TableCell>
                    <Badge
                      variant="outline"
                      className={policyStatusBadgeClass(row.status)}
                    >
                      {row.status === 'enabled' ? '有効' : '無効'}
                    </Badge>
                  </TableCell>
                  <TableCell>{row.updatedBy}</TableCell>
                  <TableCell>{row.updatedAt}</TableCell>
                  <TableCell>
                    <Button size="sm" variant="outline" onClick={() => openGlobalEdit(row)}>
                      編集
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}

        {activeLayer === 'scope' && (
          scopePoliciesEnabled ? (
            <Table className="min-w-0 table-fixed">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[16%]" title="ルール名です。">ルール名</TableHead>
                  <TableHead className="w-[24%]" title="対象スコープ条件です。">スコープ条件</TableHead>
                  <TableHead className="w-[24%]" title="上書き内容です。">上書き内容</TableHead>
                  <TableHead className="w-[8%]" title="競合時の優先度です。">優先度</TableHead>
                  <TableHead className="w-[8%]" title="ルールの有効状態です。">状態</TableHead>
                  <TableHead className="w-[10%]" title="推定適用件数です。">適用件数（推定）</TableHead>
                  <TableHead className="w-[10%]" title="最終更新日時です。">最終更新日時</TableHead>
                  <TableHead className="w-[10%]" title="操作です。">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {pagedScopeRows.map((row) => (
                  <TableRow key={row.id}>
                    <TableCell className="truncate" title={row.name}>{row.name}</TableCell>
                    <TableCell className="truncate text-xs" title={row.scopeSummary}>{row.scopeSummary}</TableCell>
                    <TableCell className="truncate text-xs" title={row.overrideSummary}>{row.overrideSummary}</TableCell>
                    <TableCell>{row.priority}</TableCell>
                    <TableCell>
                      <Badge
                        variant="outline"
                        className={policyStatusBadgeClass(row.status)}
                      >
                        {row.status === 'enabled' ? '有効' : '無効'}
                      </Badge>
                    </TableCell>
                    <TableCell>{row.appliesEstimate.toLocaleString()} 件</TableCell>
                    <TableCell>{row.updatedAt}</TableCell>
                    <TableCell>
                      <Button size="sm" variant="outline" onClick={() => openScopeEdit(row)}>
                        編集
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <div className="rounded-md border border-dashed px-4 py-6 text-sm text-muted-foreground">
              {scopeMode.message || 'PoCモードではスコープ別ポリシーは無効です。'}
            </div>
          )
        )}

        <Dialog open={Boolean(editingGlobal || editingScope)} onOpenChange={(open) => !open && closeEditDialog()}>
          <DialogContent className="sm:max-w-xl">
            <DialogHeader>
              <DialogTitle>ポリシー編集</DialogTitle>
              <DialogDescription>
                {editingGlobal
                  ? 'グローバル設定値を更新します。'
                  : 'スコープ別ポリシーの状態・優先度・適用件数を更新します。'}
              </DialogDescription>
            </DialogHeader>
            {editingGlobal && (
              <div className="space-y-3">
                <div className="space-y-1">
                  <Label>キー</Label>
                  <Input value={editingGlobal.key} disabled />
                </div>
                <div className="space-y-1">
                  <Label>現在値</Label>
                  <Input value={editValue} onChange={(e) => setEditValue(e.target.value)} />
                </div>
                <div className="space-y-1">
                  <Label>説明</Label>
                  <Input value={editDescription} onChange={(e) => setEditDescription(e.target.value)} />
                </div>
              </div>
            )}
            {editingScope && (
              <div className="space-y-3">
                <div className="grid grid-cols-5 gap-2">
                  {['基本情報', 'スコープ', 'ルール', 'ロールアウト', '影響見積もり'].map((label, idx) => (
                    <button
                      key={label}
                      type="button"
                      className={cn(
                        'rounded-md border px-2 py-1 text-xs',
                        idx === editStepIndex ? 'border-primary/40 bg-primary/10' : 'text-muted-foreground'
                      )}
                      onClick={() => setEditStepIndex(idx)}
                    >
                      {idx + 1}. {label}
                    </button>
                  ))}
                </div>

                {editStepIndex === 0 && (
                  <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                    <div className="space-y-1">
                      <Label>ポリシーID</Label>
                      <Input value={editingScope.id} disabled />
                    </div>
                    <div className="space-y-1">
                      <Label>ポリシー名</Label>
                      <Input value={editName} onChange={(e) => setEditName(e.target.value)} />
                    </div>
                    <div className="space-y-1">
                      <Label>状態</Label>
                      <Select value={editStatus} onValueChange={(v) => setEditStatus(v as 'enabled' | 'disabled')}>
                        <SelectTrigger><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="enabled">有効</SelectItem>
                          <SelectItem value="disabled">無効</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-1">
                      <Label>優先度</Label>
                      <Input value={editPriority} onChange={(e) => setEditPriority(e.target.value)} />
                    </div>
                  </div>
                )}

                {editStepIndex === 1 && (
                  <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                    <div className="space-y-1">
                      <Label>部署ID（カンマ区切り）</Label>
                      <Input value={editScopeDepartmentIds} onChange={(e) => setEditScopeDepartmentIds(e.target.value)} />
                    </div>
                    <div className="space-y-1">
                      <Label>サイトID（カンマ区切り）</Label>
                      <Input value={editScopeSiteIds} onChange={(e) => setEditScopeSiteIds(e.target.value)} />
                    </div>
                    <div className="space-y-1">
                      <Label>主体グループID（カンマ区切り）</Label>
                      <Input
                        value={editScopePrincipalGroupIds}
                        onChange={(e) => setEditScopePrincipalGroupIds(e.target.value)}
                      />
                    </div>
                    <div className="space-y-1">
                      <Label>許可ドメイン（allowlist）</Label>
                      <Input
                        value={editScopePartnerAllowlist}
                        onChange={(e) => setEditScopePartnerAllowlist(e.target.value)}
                      />
                    </div>
                    <div className="space-y-1">
                      <Label>criticality</Label>
                      <Select value={editScopeCriticality} onValueChange={setEditScopeCriticality}>
                        <SelectTrigger><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="low">low</SelectItem>
                          <SelectItem value="medium">medium</SelectItem>
                          <SelectItem value="high">high</SelectItem>
                          <SelectItem value="critical">critical</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-1">
                      <Label>use_case（任意・カンマ区切り）</Label>
                      <Input value={editScopeUseCases} onChange={(e) => setEditScopeUseCases(e.target.value)} />
                    </div>
                  </div>
                )}

                {editStepIndex === 2 && (
                  <div className="space-y-3">
                    <div className="flex items-center justify-between">
                      <p className="text-sm text-muted-foreground">複数ルールを配列で編集できます（上にあるルールほど優先）。</p>
                      <div className="flex items-center gap-2">
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => {
                            setEditRules((prev) => [...prev].sort((a, b) => b.priority_value - a.priority_value))
                          }}
                        >
                          priority順に整列
                        </Button>
                        <Button size="sm" variant="outline" onClick={addEditRule}>
                          ルール追加
                        </Button>
                      </div>
                    </div>
                    {editRules.length === 0 && (
                      <div className="rounded-md border p-3 text-sm text-muted-foreground">
                        ルールがありません。`ルール追加` から作成してください。
                      </div>
                    )}
                    {activeDragRuleId && (
                      <div className="rounded-md border border-primary/40 bg-primary/5 p-2 text-xs text-primary">
                        ドロップ先のカードを強調表示しています。配置後、上から順に優先適用されます。
                      </div>
                    )}
                    <DndContext
                      sensors={dndSensors}
                      collisionDetection={closestCenter}
                      onDragStart={onRuleDragStart}
                      onDragOver={onRuleDragOver}
                      onDragCancel={onRuleDragCancel}
                      onDragEnd={onRuleDragEnd}
                    >
                      <SortableContext items={editRules.map((rule) => rule.ui_id)} strategy={verticalListSortingStrategy}>
                        <div className="space-y-3">
                          {editRules.map((rule, idx) => (
                            <SortableRuleEditorCard
                              key={rule.ui_id}
                              rule={rule}
                              index={idx}
                              total={editRules.length}
                              isOver={Boolean(overDragRuleId && overDragRuleId === rule.ui_id && activeDragRuleId !== rule.ui_id)}
                            />
                          ))}
                        </div>
                      </SortableContext>
                      <DragOverlay>
                        {activeDragRule ? (
                          <div className="rounded-md border border-primary/60 bg-background px-3 py-2 shadow-lg">
                            <div className="text-xs text-muted-foreground">ドラッグ中</div>
                            <div className="text-sm font-medium">{activeDragRule.rule_id || activeDragRule.vector}</div>
                          </div>
                        ) : null}
                      </DragOverlay>
                    </DndContext>
                  </div>
                )}

                {editStepIndex === 3 && (
                  <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
                    <div className="space-y-1">
                      <Label>rollout.stage</Label>
                      <Select value={editRolloutStage} onValueChange={setEditRolloutStage}>
                        <SelectTrigger><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="dry_run">dry_run</SelectItem>
                          <SelectItem value="pilot">pilot</SelectItem>
                          <SelectItem value="active">active</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-1">
                      <Label>適用件数（推定）</Label>
                      <Input value={editAppliesEstimate} onChange={(e) => setEditAppliesEstimate(e.target.value)} />
                    </div>
                    <label className="mt-7 flex items-center gap-2 text-sm">
                      <Checkbox checked={editRolloutDryRun} onCheckedChange={(v) => setEditRolloutDryRun(v === true)} />
                      rollout.dry_run
                    </label>
                  </div>
                )}

                {editStepIndex === 4 && (
                  <div className="space-y-3">
                    <div className="rounded-md border p-3 text-sm">
                      <p>更新前に dry_run シミュレーションを実行できます。</p>
                      <p className="text-muted-foreground">保存はこの後に実施してください。</p>
                    </div>
                    <div>
                      <Button variant="outline" onClick={() => void simulateScopeUpdate()} disabled={isSavingEdit}>
                        {isSavingEdit ? '実行中...' : '影響見積もりを実行'}
                      </Button>
                    </div>
                    {editSimulationResult && (
                      <div className="rounded-md border p-3 text-sm space-y-1">
                        <p>estimated_affected_items: {String(editSimulationResult.estimated_affected_items ?? '-')}</p>
                        <p>estimated_new_findings: {String(editSimulationResult.estimated_new_findings ?? '-')}</p>
                        <p>estimated_resolved_findings: {String(editSimulationResult.estimated_resolved_findings ?? '-')}</p>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}
            <div className="flex items-center justify-end gap-2">
              {editingScope && (
                <Button
                  variant="outline"
                  onClick={() => setEditStepIndex((prev) => Math.max(0, prev - 1))}
                  disabled={isSavingEdit || editStepIndex === 0}
                >
                  戻る
                </Button>
              )}
              {editingScope && (
                <Button
                  variant="outline"
                  onClick={() => setEditStepIndex((prev) => Math.min(4, prev + 1))}
                  disabled={isSavingEdit || editStepIndex >= 4}
                >
                  次へ
                </Button>
              )}
              <Button variant="outline" onClick={closeEditDialog} disabled={isSavingEdit}>キャンセル</Button>
              <Button onClick={() => void saveEditedPolicy()} disabled={isSavingEdit}>
                {isSavingEdit ? '保存中...' : '保存'}
              </Button>
            </div>
          </DialogContent>
        </Dialog>

      </CardContent>
    </Card>
  ) : (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between gap-2">
          <div>
            <CardTitle>ガバナンス ポリシー新規追加（3層）</CardTitle>
            <CardDescription>
              Global / Scope / Exception のいずれかのレイヤーに、判定ルールを追加します。
            </CardDescription>
          </div>
          <Button
            size="sm"
            variant="outline"
            onClick={() => {
              setMode('list')
              resetPolicyDraft()
            }}
          >
            設定一覧へ戻る
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-2 md:grid-cols-5">
          {POLICY_STEPS.map((step, idx) => (
            <button
              key={step.key}
              type="button"
              className={cn(
                'rounded-md border px-3 py-2 text-sm',
                idx === stepIndex ? 'bg-primary/10 border-primary/40 text-foreground' : 'text-muted-foreground'
              )}
              onClick={() => setStepIndex(idx)}
            >
              {idx + 1}. {step.label}
            </button>
          ))}
        </div>

        <div className="rounded-lg border p-4 space-y-4">
          <div>
            <p className="text-sm text-muted-foreground">現在ステップ</p>
            <p className="text-base font-semibold">{POLICY_STEPS[stepIndex].label}</p>
          </div>

          {stepIndex === 0 && (
            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label>レイヤー</Label>
                <Select
                  value={draft.layer}
                  onValueChange={(v) => {
                    const nextLayer = v as PolicyEditorLayer
                    updateDraft('layer', nextLayer)
                  }}
                >
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="global">Global（全社共通）</SelectItem>
                    {scopePoliciesEnabled && <SelectItem value="scope">Scope（限定上書き）</SelectItem>}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>目的</Label>
                <Select value={draft.purpose} onValueChange={(v) => updateDraft('purpose', v)}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="accuracy">検知精度の調整</SelectItem>
                    <SelectItem value="load">運用負荷の調整</SelectItem>
                    <SelectItem value="audit">監査要件への対応</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2 md:col-span-2">
                <Label>ポリシータイトル</Label>
                <Input value={draft.policyName} onChange={(e) => updateDraft('policyName', e.target.value)} />
              </div>
              <p className="md:col-span-2 text-xs text-muted-foreground">
                設定キーはバックエンドで管理されます（画面ではタイトルで管理）。
              </p>
            </div>
          )}

          {stepIndex === 1 && (
            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label>判定対象</Label>
                <Select value={draft.detectionTarget} onValueChange={(v) => updateDraft('detectionTarget', v)}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="sharing">共有状態</SelectItem>
                    <SelectItem value="content">コンテンツカバレッジ</SelectItem>
                    <SelectItem value="activity">利用状況</SelectItem>
                    <SelectItem value="ai">AIアクセス性</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>評価方式</Label>
                <Select value={draft.evaluationMode} onValueChange={(v) => updateDraft('evaluationMode', v)}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="threshold">閾値判定</SelectItem>
                    <SelectItem value="multiplier">係数（掛け算）</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {draft.layer === 'scope' && (
                <>
                  <div className="space-y-2">
                    <Label>部門条件</Label>
                    <Input value={draft.scopeDepartment} onChange={(e) => updateDraft('scopeDepartment', e.target.value)} placeholder="例: HR" />
                  </div>
                  <div className="space-y-2">
                    <Label>サイト条件</Label>
                    <Input value={draft.scopeSiteId} onChange={(e) => updateDraft('scopeSiteId', e.target.value)} placeholder="例: /sites/hr" />
                  </div>
                  <div className="space-y-2 md:col-span-2">
                    <Label>データラベル条件</Label>
                    <Input
                      value={draft.scopeDataLabels}
                      onChange={(e) => updateDraft('scopeDataLabels', e.target.value)}
                      placeholder="例: Confidential-HR, HighlyConfidential-HR"
                    />
                  </div>
                  <div className="space-y-2 md:col-span-2">
                    <Label>許可主体</Label>
                    <Input
                      value={draft.allowedPrincipals}
                      onChange={(e) => updateDraft('allowedPrincipals', e.target.value)}
                      placeholder="例: HR-members, HR-managers, Executive"
                    />
                  </div>
                </>
              )}

              <div className="space-y-2 md:col-span-2">
                <Label>説明</Label>
                <Input
                  value={draft.policyDescription}
                  onChange={(e) => updateDraft('policyDescription', e.target.value)}
                  placeholder="何を、どの基準で、どれくらい厳しく検知するか"
                />
              </div>
            </div>
          )}

          {stepIndex === 2 && (
            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label>現在値</Label>
                <Input value={draft.thresholdValue} onChange={(e) => updateDraft('thresholdValue', e.target.value)} />
              </div>
              <div className="space-y-2">
                <Label>警告しきい値</Label>
                <Input value={draft.warningThreshold} onChange={(e) => updateDraft('warningThreshold', e.target.value)} />
              </div>
              <div className="space-y-2">
                <Label>重大しきい値</Label>
                <Input value={draft.criticalThreshold} onChange={(e) => updateDraft('criticalThreshold', e.target.value)} />
              </div>
              <div className="space-y-2">
                <Label>値の型</Label>
                <Select value={draft.policyValueType} onValueChange={(v) => updateDraft('policyValueType', v)}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="numeric">数値</SelectItem>
                    <SelectItem value="enum">列挙値</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              {draft.layer === 'scope' && (
                <div className="space-y-2 md:col-span-2">
                  <Label>禁止共有条件</Label>
                  <div className="grid gap-2 md:grid-cols-3">
                    <label className="flex items-center gap-2 text-sm">
                      <Checkbox checked={draft.denyAnyoneLink} onCheckedChange={(v) => updateDraft('denyAnyoneLink', v === true)} />
                      Anyoneリンク禁止
                    </label>
                    <label className="flex items-center gap-2 text-sm">
                      <Checkbox checked={draft.denyExternalGuest} onCheckedChange={(v) => updateDraft('denyExternalGuest', v === true)} />
                      外部ゲスト共有禁止
                    </label>
                    <label className="flex items-center gap-2 text-sm">
                      <Checkbox checked={draft.denyOrgLink} onCheckedChange={(v) => updateDraft('denyOrgLink', v === true)} />
                      全社リンク禁止
                    </label>
                  </div>
                </div>
              )}
            </div>
          )}

          {stepIndex === 3 && (
            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label>想定検知件数差分</Label>
                <Input value={draft.expectedFindingDelta} onChange={(e) => updateDraft('expectedFindingDelta', e.target.value)} />
              </div>
              <div className="space-y-2">
                <Label>想定ジョブ負荷</Label>
                <Select value={draft.expectedJobLoadImpact} onValueChange={(v) => updateDraft('expectedJobLoadImpact', v)}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="low">低</SelectItem>
                    <SelectItem value="medium">中</SelectItem>
                    <SelectItem value="high">高</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>想定対象件数</Label>
                <Input value={draft.expectedApplyCount} onChange={(e) => updateDraft('expectedApplyCount', e.target.value)} />
              </div>
              <div className="space-y-2">
                <Label>ロールアウト範囲</Label>
                <Select value={draft.rollOutScope} onValueChange={(v) => updateDraft('rollOutScope', v)}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="dry_run">ドライラン（保存なし）</SelectItem>
                    <SelectItem value="pilot">パイロット</SelectItem>
                    <SelectItem value="active">本番有効化</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          )}

          {stepIndex === 4 && (
            <div className="space-y-3">
              <div className="rounded-md border p-3 text-sm">
                <p>レイヤー: {layerMeta[draft.layer].label}</p>
                <p>タイトル: {draft.policyName || '-'}</p>
                <p>設定キー: バックエンド側で採番・保持</p>
                <p>
                  優先順位: Scope {'>'} Global（同層競合は priority 順）
                </p>
              </div>
              <div className="flex items-center gap-2">
                <Checkbox checked={draft.confirmNoHardcodedSecret} onCheckedChange={(v) => updateDraft('confirmNoHardcodedSecret', v === true)} />
                <span className="text-sm">機密情報の直書きがないことを確認しました</span>
              </div>
              <div className="flex items-center gap-2">
                <Checkbox checked={draft.confirmApprovalOwner} onCheckedChange={(v) => updateDraft('confirmApprovalOwner', v === true)} />
                <span className="text-sm">承認責任者が明確であることを確認しました</span>
              </div>
              <div className="flex items-center gap-2">
                <Checkbox checked={draft.confirmRollbackReady} onCheckedChange={(v) => updateDraft('confirmRollbackReady', v === true)} />
                <span className="text-sm">ロールバック手順が準備済みであることを確認しました</span>
              </div>
            </div>
          )}
        </div>

        <div className="flex items-center justify-between">
          <Button variant="outline" onClick={() => setStepIndex((prev) => Math.max(0, prev - 1))} disabled={stepIndex === 0}>
            戻る
          </Button>
          <Badge variant="outline">Step {stepIndex + 1}/{POLICY_STEPS.length}</Badge>
          {stepIndex < POLICY_STEPS.length - 1 ? (
            <Button
              onClick={() => {
                if (!canProceed()) {
                  toast('必須項目が未入力です')
                  return
                }
                setStepIndex((prev) => Math.min(POLICY_STEPS.length - 1, prev + 1))
              }}
            >
              次へ
            </Button>
          ) : (
            <Button onClick={() => void createPolicy()}>登録</Button>
          )}
        </div>
      </CardContent>
    </Card>
  )
}

export default function GovernanceOperations() {
  type PolicyRescoreSnapshot = {
    totalFindings: number
    highRisk: number
    actionRequired: number
    governanceScore: number
    oversharingControl: number
    assurance: number
  }
  type PolicyRescoreSummary = {
    label: string
    jobId: string
    jobStatus: string
    capturedAt: string
    before: PolicyRescoreSnapshot
    after: PolicyRescoreSnapshot
    delta: PolicyRescoreSnapshot
    ruleDetails: Array<{
      ruleId: string
      vector: string
      reasonCodes: string[]
      reasonCodeDeltaMap: Record<string, number>
      beforeApplied: number
      afterApplied: number
      deltaApplied: number
      beforeVectorCount: number
      afterVectorCount: number
      deltaVectorCount: number
      beforeReasonCount: number
      afterReasonCount: number
      deltaReasonCount: number
    }>
  }
  const [page, setPage] = useState<GovernancePageKey>('overview')
  const [deepLinkFocus, setDeepLinkFocus] = useState<string | undefined>()
  const [overviewApi, setOverviewApi] = useState<GovernanceOverviewResponse | null>(null)
  const [overview, setOverview] = useState<GovernanceOverviewStats>({
    governanceScore: 0,
    coverageScore: 0,
    confidenceLevel: 'Low',
    confidenceScore: 0,
    oversharingControlScore: 0,
    assuranceScore: 0,
    governanceRaw: 0,
    exceptionDebt: 0,
    coveragePenalty: 0,
    inventoryCoverage: 0,
    contentScanCoverage: 0,
    supportedFormatCoverage: 0,
    freshScanCoverage: 0,
    permissionDetailCoverage: 0,
    totalFindingsCount: 0,
    activeFindingsCount: 0,
    highRiskCount: 0,
    actionRequiredCount: 0,
    expiringSuppressions24h: 0,
    lastBatchRunAt: '-'
  })
  const [scanJobs, setScanJobs] = useState<GovernanceScanJob[]>([])
  const [suppressions, setSuppressions] = useState<GovernanceSuppression[]>([])
  const [policies, setPolicies] = useState<GovernancePoliciesResponse | null>(null)
  const [suppressionsLoading, setSuppressionsLoading] = useState(false)
  const [suppressionsError, setSuppressionsError] = useState<string | null>(null)
  const [policiesLoading, setPoliciesLoading] = useState(false)
  const [policiesError, setPoliciesError] = useState<string | null>(null)
  const [policyRescoreSummary, setPolicyRescoreSummary] = useState<PolicyRescoreSummary | null>(null)
  const hasCompletedScan = useMemo(
    () =>
      Boolean(overviewApi?.initial_scan_gate_open) ||
      scanJobs.some((row) => row.jobType === 'batch_scoring' && row.status === 'success'),
    [overviewApi, scanJobs]
  )
  const latestBatchScanJob = useMemo(
    () => scanJobs.find((row) => row.jobType === 'batch_scoring') ?? null,
    [scanJobs]
  )
  const isScanInProgress = Boolean(
    latestBatchScanJob &&
    (latestBatchScanJob.status === 'queued' ||
      latestBatchScanJob.status === 'running' ||
      latestBatchScanJob.status === 'partial')
  )
  const isPreScanLocked = !hasCompletedScan
  const protectionViewModel = useMemo(
    () => toGovernanceProtectionViewModel(overviewApi),
    [overviewApi]
  )
  const toPolicyRescoreSnapshot = useCallback((source: GovernanceOverviewResponse): PolicyRescoreSnapshot => ({
    totalFindings: Number(source.counts?.total_findings ?? 0),
    highRisk: Number(source.high_risk_count ?? 0),
    actionRequired: Number(source.action_required_count ?? 0),
    governanceScore: Number(source.governance_score ?? 0),
    oversharingControl: Number(source.subscores?.oversharing_control ?? 0),
    assurance: Number(source.subscores?.assurance ?? 0),
  }), [])
  const formatDelta = (value: number): string => {
    if (value > 0) return `+${value}`
    return `${value}`
  }
  const loadAllGovernanceFindings = useCallback(async (): Promise<GovernanceFindingApiRow[]> => {
    const rows: GovernanceFindingApiRow[] = []
    let offset = 0
    const limit = 500
    for (let i = 0; i < 20; i += 1) {
      const response = await getGovernanceFindings(limit, offset, 'new,open,acknowledged,closed')
      rows.push(...response.rows)
      const nextOffset = response.pagination?.next_offset
      if (typeof nextOffset === 'number' && nextOffset >= 0) {
        offset = nextOffset
        continue
      }
      if (response.rows.length < limit || response.pagination?.scan_capped) {
        break
      }
      offset += limit
    }
    return rows
  }, [])
  const buildRuleDetails = useCallback((
    policyId: string,
    rules: Array<Record<string, any>>,
    beforeRows: GovernanceFindingApiRow[],
    afterRows: GovernanceFindingApiRow[]
  ): PolicyRescoreSummary['ruleDetails'] => {
    if (!policyId || !Array.isArray(rules) || rules.length === 0) return []
    const contains = (list: unknown, value: string): boolean =>
      Array.isArray(list) && list.some((v) => String(v) === value)
    return rules
      .map((rule, idx) => {
        const ruleId = String(rule.rule_id ?? `rule-${idx + 1}`)
        const vector = String(rule.vector ?? '').trim()
        const reasonCodes = Array.isArray(rule.reason_codes)
          ? rule.reason_codes.map((code) => String(code).trim()).filter(Boolean)
          : []
        if (!vector) return null
        const beforeApplied = beforeRows.filter((row) =>
          contains(row.matched_policy_ids, policyId) && contains(row.exposure_vectors, vector)
        ).length
        const afterApplied = afterRows.filter((row) =>
          contains(row.matched_policy_ids, policyId) && contains(row.exposure_vectors, vector)
        ).length
        const beforeVectorCount = beforeRows.filter((row) => contains(row.exposure_vectors, vector)).length
        const afterVectorCount = afterRows.filter((row) => contains(row.exposure_vectors, vector)).length
        const beforeReasonCount = beforeRows.filter((row) =>
          reasonCodes.some((code) => contains(row.reason_codes, code))
        ).length
        const afterReasonCount = afterRows.filter((row) =>
          reasonCodes.some((code) => contains(row.reason_codes, code))
        ).length
        const reasonCodeDeltaMap: Record<string, number> = {}
        for (const code of reasonCodes) {
          const beforeCodeCount = beforeRows.filter((row) => contains(row.reason_codes, code)).length
          const afterCodeCount = afterRows.filter((row) => contains(row.reason_codes, code)).length
          reasonCodeDeltaMap[code] = afterCodeCount - beforeCodeCount
        }
        return {
          ruleId,
          vector,
          reasonCodes,
          reasonCodeDeltaMap,
          beforeApplied,
          afterApplied,
          deltaApplied: afterApplied - beforeApplied,
          beforeVectorCount,
          afterVectorCount,
          deltaVectorCount: afterVectorCount - beforeVectorCount,
          beforeReasonCount,
          afterReasonCount,
          deltaReasonCount: afterReasonCount - beforeReasonCount,
        }
      })
      .filter((row): row is NonNullable<typeof row> => row !== null)
  }, [])
  const runPolicyRescoreAndCaptureDiff = useCallback(async ({
    policyLabel,
    policyId,
    rules,
  }: {
    policyLabel: string
    policyId: string
    rules: Array<Record<string, any>>
  }) => {
    const beforeOverview = await getGovernanceOverview()
    const beforeFindings = await loadAllGovernanceFindings()
    const trigger = await runGovernanceDailyScan()
    let terminalStatus = 'accepted'
    for (let i = 0; i < 24; i += 1) {
      const jobs = await getGovernanceScanJobs(200, 0)
      const target = jobs.rows.find((row) => String(row.job_id) === String(trigger.job_id))
      if (target?.status) {
        terminalStatus = String(target.status)
      }
      if (terminalStatus === 'success' || terminalStatus === 'failed') break
      await new Promise((resolve) => window.setTimeout(resolve, 3000))
    }
    const afterOverview = await getGovernanceOverview()
    const afterFindings = await loadAllGovernanceFindings()
    const before = toPolicyRescoreSnapshot(beforeOverview)
    const after = toPolicyRescoreSnapshot(afterOverview)
    setPolicyRescoreSummary({
      label: policyLabel,
      jobId: String(trigger.job_id ?? ''),
      jobStatus: terminalStatus,
      capturedAt: new Date().toISOString(),
      before,
      after,
      delta: {
        totalFindings: after.totalFindings - before.totalFindings,
        highRisk: after.highRisk - before.highRisk,
        actionRequired: after.actionRequired - before.actionRequired,
        governanceScore: Number((after.governanceScore - before.governanceScore).toFixed(1)),
        oversharingControl: Number((after.oversharingControl - before.oversharingControl).toFixed(1)),
        assurance: Number((after.assurance - before.assurance).toFixed(1)),
      },
      ruleDetails: buildRuleDetails(policyId, rules, beforeFindings, afterFindings),
    })
  }, [buildRuleDetails, loadAllGovernanceFindings, toPolicyRescoreSnapshot])

  const reloadGovernanceSupportData = useCallback(async () => {
    try {
      const [overviewResponse, suppressionsResponse, policiesResponse, scanJobsResponse] = await Promise.all([
        getGovernanceOverview(),
        getGovernanceSuppressions(300, 0, 0),
        getGovernancePolicies(),
        getGovernanceScanJobs(200, 0)
      ])
      setOverviewApi(overviewResponse)
      setOverview(toGovernanceOverviewStatsFromApi(overviewResponse))
      setSuppressions(suppressionsResponse.rows.map(toGovernanceSuppressionFromApi))
      setPolicies(policiesResponse)
      setScanJobs(scanJobsResponse.rows.map(toGovernanceScanJobFromApi))
      setSuppressionsError(null)
      setPoliciesError(null)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'ガバナンスデータ取得に失敗しました'
      setSuppressionsError(message)
      setPoliciesError(message)
    } finally {
      setSuppressionsLoading(false)
      setPoliciesLoading(false)
    }
  }, [])

  const openGovernancePage = (targetPage: GovernancePageKey, focus?: string) => {
    setPage(targetPage)
    setDeepLinkFocus(focus)
  }

  useEffect(() => {
    const deepLink = takeOperationDeepLinkForTab('governance-operations')
    if (!deepLink?.page) return
    if (GOVERNANCE_PAGES.some((item) => item.key === deepLink.page)) {
      setPage(deepLink.page as GovernancePageKey)
    }
    setDeepLinkFocus(deepLink.focus)
  }, [])

  useEffect(() => {
    setSuppressionsLoading(true)
    setPoliciesLoading(true)
    void reloadGovernanceSupportData()
  }, [reloadGovernanceSupportData, toPolicyRescoreSnapshot])

  useEffect(() => {
    if (!isPreScanLocked) return
    if (!GOVERNANCE_LOCKED_UNTIL_SCAN_COMPLETE.includes(page)) return
    setPage('jobs')
  }, [isPreScanLocked, page])

  useEffect(() => {
    if (!isScanInProgress) return
    const timer = window.setInterval(() => {
      void reloadGovernanceSupportData()
    }, 5000)
    return () => window.clearInterval(timer)
  }, [isScanInProgress, reloadGovernanceSupportData])

  const handleCreatePolicy = useCallback(async (payload: Record<string, any>, dryRun: boolean = false) => {
    try {
      const shouldRunScan = Boolean(payload.run_scan_after_save) && !dryRun
      const apiPayload = { ...payload }
      delete apiPayload.run_scan_after_save
      if (dryRun) {
        await simulateGovernancePolicy(apiPayload)
      } else {
        await createGovernancePolicy(apiPayload, false)
        if (shouldRunScan) {
          await runPolicyRescoreAndCaptureDiff({
            policyLabel: String(apiPayload.name ?? apiPayload.policy_id ?? 'policy-create'),
            policyId: String(apiPayload.policy_id ?? ''),
            rules: Array.isArray(apiPayload.rules) ? apiPayload.rules : []
          })
        }
      }
      await reloadGovernanceSupportData()
      toast.success(dryRun ? 'ポリシー影響見積もりを実行しました' : 'ポリシーを保存しました')
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'ポリシー保存に失敗しました')
    }
  }, [reloadGovernanceSupportData, runPolicyRescoreAndCaptureDiff])

  const handleUpdatePolicy = useCallback(async (
    policyId: string,
    payload: Record<string, any>,
    dryRun: boolean = false
  ) => {
    try {
      const shouldRunScan = Boolean(payload.run_scan_after_save) && !dryRun
      const apiPayload = { ...payload }
      delete apiPayload.run_scan_after_save
      const result = await updateGovernancePolicy(policyId, apiPayload, dryRun)
      if (!dryRun) {
        if (shouldRunScan) {
          await runPolicyRescoreAndCaptureDiff({
            policyLabel: String(apiPayload.name ?? apiPayload.policy_id ?? policyId),
            policyId: String(apiPayload.policy_id ?? policyId),
            rules: Array.isArray(apiPayload.rules) ? apiPayload.rules : []
          })
        }
        await reloadGovernanceSupportData()
        toast.success('ポリシーを更新しました')
      }
      return result
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'ポリシー更新に失敗しました')
      throw error
    }
  }, [reloadGovernanceSupportData, runPolicyRescoreAndCaptureDiff])

  const handleRunDailyScan = useCallback(async () => {
    try {
      const response = await runGovernanceDailyScan()
      if (response.status === 'disabled') {
        toast.message(
          response.message ??
            'Connect 手動同期はこの環境では利用できません。ダッシュボード表示のみ続行します。'
        )
      } else if (response.job_id) {
        toast.success(`スコアリングを起動しました: ${response.job_id}`)
      } else {
        toast.success(response.message ?? 'スコアリングを受け付けました')
      }
      await reloadGovernanceSupportData()
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'スコアリング起動に失敗しました')
    }
  }, [reloadGovernanceSupportData])

  return (
    <div className="h-full overflow-y-auto p-4 [scrollbar-gutter:stable]">
      <div className="mx-auto w-full max-w-[1400px] space-y-4 pb-6">
        <Card className="border-primary/20 bg-gradient-to-r from-primary/10 via-background to-background shadow-md">
          <CardContent className="flex flex-wrap items-center justify-between gap-3 p-4">
            <div>
              <h1 className="text-xl font-semibold">ガバナンス</h1>
              <p className="text-sm text-muted-foreground">
                過剰共有リスクを検知し、是正対応の進捗と監査証跡を一元管理します。
              </p>
            </div>
            <div className="flex items-center gap-2">
              <Button size="sm" onClick={() => setPage('policies')}>ポリシー設定</Button>
              <Button variant="outline" size="sm" onClick={() => void handleRunDailyScan()} disabled={isScanInProgress}>
                {isScanInProgress ? 'スコアリング実行中...' : 'スコアリングを実行'}
              </Button>
            </div>
          </CardContent>
        </Card>

        {isScanInProgress && (
          <Card className="border-sky-300 bg-sky-50/40">
            <CardContent className="flex items-center justify-between gap-3 p-3">
              <div className="text-sm text-sky-900">
                スコアリングを実行中です。ジョブ状態: {latestBatchScanJob?.status ?? 'running'}
              </div>
              <Badge variant="secondary" className="border-sky-300 bg-sky-100 text-sky-900">
                処理中
              </Badge>
            </CardContent>
          </Card>
        )}
        {page === 'policies' && policyRescoreSummary && (
          <Card className="border-emerald-300 bg-emerald-50/30">
            <CardHeader className="pb-2">
              <CardTitle className="text-base">ポリシー反映差分サマリー（前回比）</CardTitle>
              <CardDescription>
                {policyRescoreSummary.label} / job: {policyRescoreSummary.jobId} / status: {policyRescoreSummary.jobStatus}
              </CardDescription>
            </CardHeader>
            <CardContent className="grid grid-cols-1 gap-2 text-sm md:grid-cols-3">
              <div className="rounded border bg-background p-2">
                <p className="text-muted-foreground">総検知件数</p>
                <p className="font-medium">{policyRescoreSummary.before.totalFindings} → {policyRescoreSummary.after.totalFindings} ({formatDelta(policyRescoreSummary.delta.totalFindings)})</p>
              </div>
              <div className="rounded border bg-background p-2">
                <p className="text-muted-foreground">高リスク件数</p>
                <p className="font-medium">{policyRescoreSummary.before.highRisk} → {policyRescoreSummary.after.highRisk} ({formatDelta(policyRescoreSummary.delta.highRisk)})</p>
              </div>
              <div className="rounded border bg-background p-2">
                <p className="text-muted-foreground">要対応件数</p>
                <p className="font-medium">{policyRescoreSummary.before.actionRequired} → {policyRescoreSummary.after.actionRequired} ({formatDelta(policyRescoreSummary.delta.actionRequired)})</p>
              </div>
              <div className="rounded border bg-background p-2">
                <p className="text-muted-foreground">ガバナンススコア</p>
                <p className="font-medium">{policyRescoreSummary.before.governanceScore} → {policyRescoreSummary.after.governanceScore} ({formatDelta(policyRescoreSummary.delta.governanceScore)})</p>
              </div>
              <div className="rounded border bg-background p-2">
                <p className="text-muted-foreground">Oversharing Control</p>
                <p className="font-medium">{policyRescoreSummary.before.oversharingControl} → {policyRescoreSummary.after.oversharingControl} ({formatDelta(policyRescoreSummary.delta.oversharingControl)})</p>
              </div>
              <div className="rounded border bg-background p-2">
                <p className="text-muted-foreground">Assurance</p>
                <p className="font-medium">{policyRescoreSummary.before.assurance} → {policyRescoreSummary.after.assurance} ({formatDelta(policyRescoreSummary.delta.assurance)})</p>
              </div>
              <p className="md:col-span-3 text-xs text-muted-foreground">
                取得時刻: {new Date(policyRescoreSummary.capturedAt).toLocaleString()}
              </p>
              {policyRescoreSummary.ruleDetails.length > 0 && (
                <details className="md:col-span-3 rounded border bg-background p-2">
                  <summary className="cursor-pointer text-sm font-medium">
                    どのポリシー変更が効いたか（rule_id / vector 単位）
                  </summary>
                  <div className="mt-2 overflow-x-auto">
                    <Table className="min-w-full">
                      <TableHeader>
                        <TableRow>
                          <TableHead>rule_id</TableHead>
                          <TableHead>vector</TableHead>
                          <TableHead>適用件数（before→after）</TableHead>
                          <TableHead>適用差分</TableHead>
                          <TableHead>vector件数（before→after）</TableHead>
                          <TableHead>vector差分</TableHead>
                          <TableHead>reason_codes</TableHead>
                          <TableHead>reason件数（before→after）</TableHead>
                          <TableHead>reason差分</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {policyRescoreSummary.ruleDetails.map((row) => (
                          <TableRow key={`${row.ruleId}-${row.vector}`}>
                            <TableCell>{row.ruleId}</TableCell>
                            <TableCell>{row.vector}</TableCell>
                            <TableCell>{row.beforeApplied} → {row.afterApplied}</TableCell>
                            <TableCell>{formatDelta(row.deltaApplied)}</TableCell>
                            <TableCell>{row.beforeVectorCount} → {row.afterVectorCount}</TableCell>
                            <TableCell>{formatDelta(row.deltaVectorCount)}</TableCell>
                            <TableCell>
                              {row.reasonCodes.length > 0 ? (
                                <div className="space-y-1">
                                  {row.reasonCodes.map((code) => (
                                    <div key={`${row.ruleId}-${code}`} className="text-xs">
                                      {code}: {formatDelta(row.reasonCodeDeltaMap[code] ?? 0)}
                                    </div>
                                  ))}
                                </div>
                              ) : '-'}
                            </TableCell>
                            <TableCell>{row.beforeReasonCount} → {row.afterReasonCount}</TableCell>
                            <TableCell>{formatDelta(row.deltaReasonCount)}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </details>
              )}
            </CardContent>
          </Card>
        )}

        <div className="rounded-xl border bg-card p-2 shadow-sm">
          <TooltipProvider>
            <div className="flex flex-wrap gap-2">
              {GOVERNANCE_PAGES.filter((item) => item.key !== 'policies').map((item) => (
                <Tooltip key={item.key}>
                  <TooltipTrigger asChild>
                    {(() => {
                      const isLocked = isPreScanLocked && GOVERNANCE_LOCKED_UNTIL_SCAN_COMPLETE.includes(item.key)
                      return (
                        <button
                          type="button"
                          onClick={() => {
                            if (!isLocked) setPage(item.key)
                          }}
                          onKeyDown={(event) => {
                            if (isLocked) return
                            if (event.key === 'Enter' || event.key === ' ' || event.key === 'Space' || event.key === 'Spacebar') {
                              event.preventDefault()
                              setPage(item.key)
                            }
                          }}
                          aria-current={page === item.key ? 'page' : undefined}
                          aria-disabled={isLocked}
                          className={cn(
                            'liquid-glass-tab rounded-full border px-4 py-2 text-sm font-medium transition-all duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60',
                            isLocked && 'cursor-not-allowed opacity-45',
                            page === item.key
                              ? 'liquid-glass-tab-active shadow-sm'
                              : 'text-foreground'
                          )}
                        >
                          {item.label}
                        </button>
                      )
                    })()}
                  </TooltipTrigger>
                  <TooltipContent side="bottom">
                    {isPreScanLocked && GOVERNANCE_LOCKED_UNTIL_SCAN_COMPLETE.includes(item.key)
                      ? '初回スコアリング完了後に表示できます。'
                      : GOVERNANCE_TAB_HELP[item.key]}
                  </TooltipContent>
                </Tooltip>
              ))}
            </div>
          </TooltipProvider>
        </div>

        {isPreScanLocked && (
          <InitialScoringGateCard
            description={INITIAL_SCORING_GATE_DEFAULT_DESCRIPTION}
            latestBatchScanJob={
              latestBatchScanJob
                ? { status: latestBatchScanJob.status, startedAt: latestBatchScanJob.startedAt }
                : null
            }
            onRunScoring={handleRunDailyScan}
            isScanInProgress={isScanInProgress}
          />
        )}

        {page === 'overview' && !isPreScanLocked && (
          <OverviewPage onNavigate={openGovernancePage} overview={overview} protectionViewModel={protectionViewModel} />
        )}
        {page === 'findings' && !isPreScanLocked && <FindingsPage deepLinkFocus={deepLinkFocus} scanJobs={scanJobs} />}
        {page === 'suppression' && !isPreScanLocked && (
          <SuppressionPage
            deepLinkFocus={deepLinkFocus}
            suppressions={suppressions}
            loading={suppressionsLoading}
            loadError={suppressionsError}
          />
        )}
        {page === 'jobs' && <JobsPage rows={scanJobs} />}
        {page === 'policies' && (
          <PoliciesPage
            policies={policies}
            loading={policiesLoading}
            loadError={policiesError}
            onCreatePolicy={handleCreatePolicy}
            onUpdatePolicy={handleUpdatePolicy}
          />
        )}
        {page === 'help' && (
          <FeatureOnboardingPanel
            title="ガバナンス手順（3分で理解）"
            purpose="ポリシー定義からスキャン、検知対応、例外管理までの基本運用を短時間で把握できます。"
            currentPageLabel={GOVERNANCE_PAGES.find((item) => item.key === page)?.label ?? 'ヘルプ'}
            currentPageDescription={GOVERNANCE_PAGE_GUIDE[page]}
            steps={[
              '「ポリシー設定」で、グローバルポリシー（全社基準）とスコープ別ポリシー（部門・サイト別）を定義する。',
              'スキャン実行後、「検知結果」に格納されたFindingを優先度順に確認する。',
              '即時是正できないFindingのみ「例外レジストリ」に期限付き登録し、期限到達後に再評価する。'
            ]}
            glossary={GOVERNANCE_GLOSSARY}
          />
        )}
      </div>
    </div>
  )
}
