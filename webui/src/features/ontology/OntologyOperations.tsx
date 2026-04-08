import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { CalendarClock, Files, RefreshCw, UserCircle } from 'lucide-react'
import { toast } from 'sonner'
import Badge, { BadgeProps } from '@/components/ui/Badge'
import Button from '@/components/ui/Button'
import Checkbox from '@/components/ui/Checkbox'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/Card'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle
} from '@/components/ui/Dialog'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/Table'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/Tooltip'
import AuditWorkbench from '@/features/common/AuditWorkbench'
import FeatureOnboardingPanel from '@/features/common/FeatureOnboardingPanel'
import TablePageControls from '@/features/common/TablePageControls'
import { takeOperationDeepLinkForTab } from '@/features/common/operationDeepLink'
import InitialScoringGateCard, {
  ONTOLOGY_INITIAL_SCORING_GATE_DESCRIPTION
} from '@/features/common/InitialScoringGateCard'
import { useGovernanceBatchScoringGate } from '@/features/common/useGovernanceBatchScoringGate'
import { cn } from '@/lib/utils'
import { useGraphStore } from '@/stores/graph'
import { useSettingsStore } from '@/stores/settings'
import OntologyGraphView from './OntologyGraphView'
import {
  ApiHttpError,
  OntologyAuditApiRow,
  OntologyEntityCandidateRow,
  OntologyEntityMasterApiRow,
  OntologyOverviewResponse,
  OntologyUnifiedMetadataApiRow,
  getOntologyAuditLogs,
  getOntologyEntityCandidates,
  getOntologyOverview,
  getOntologyEntityMaster,
  getOntologyUnifiedMetadata,
  registerOntologyEntityCandidateAsNew,
  resolveOntologyEntityCandidateWithExisting,
  aiFillOntologyProfile,
  refreshOntologyGraphProjection,
  updateOntologyProfile
} from '@/api/graphsuite'
import {
  EntityMasterRecord,
  OntologyAuditRecord,
  OntologyOverviewStats,
  OntologyPageKey,
  OntologyPillarDocumentCounts,
  RemediationState,
  UnifiedMetadataRecord
} from './types'

const ONTOLOGY_PAGES: Array<{ key: OntologyPageKey; label: string }> = [
  { key: 'overview', label: '概要' },
  { key: 'entity-candidates', label: 'エンティティ解決' },
  { key: 'unified-metadata', label: 'オントロジーグラフ' },
  { key: 'entity-master', label: 'エンティティマスタ' },
  { key: 'audit', label: '監査ログ' },
  { key: 'help', label: 'ヘルプ' }
]

const ONTOLOGY_TOP_NAV_PAGES: Array<{ key: OntologyPageKey; label: string }> = [
  { key: 'overview', label: '概要' },
  { key: 'unified-metadata', label: 'オントロジーグラフ' },
  { key: 'entity-master', label: 'エンティティ管理(互換)' },
  { key: 'audit', label: '監査ログ' },
  { key: 'help', label: 'ヘルプ' }
]

const ONTOLOGY_CONTENT_LOCKED_WHEN_NO_SCAN: OntologyPageKey[] = [
  'overview',
  'unified-metadata',
  'entity-candidates',
  'entity-master',
  'relationship-graph',
  'lineage',
  'reconcile-jobs'
]

const isOntologyTabLockedWhileNoScan = (
  key: OntologyPageKey,
  isPreScanLocked: boolean,
  nounResolutionEnabled: boolean
): boolean => {
  if (!isPreScanLocked) return false
  if (key === 'help' || key === 'audit') return false
  if (key === 'entity-master' && !nounResolutionEnabled) return false
  return ONTOLOGY_CONTENT_LOCKED_WHEN_NO_SCAN.includes(key)
}

const ONTOLOGY_PAGE_GUIDE: Record<OntologyPageKey, string> = {
  overview: 'データ品質と整合性の全体状況を確認します。',
  'relationship-graph': 'ドキュメント・人・プロジェクトの関係と解決状態を可視化します。',
  'unified-metadata': 'ドキュメント単位で管理されたオントロジーグラフを確認します。',
  'entity-candidates': '文書から抽出された辞書未登録ワードを確認し、エンティティ統合を判断します。',
  'entity-master': '正規化済みエンティティマスタを確認し、近似推薦の参照元として利用します。',
  lineage: 'データの由来と変換の履歴を確認します。',
  'reconcile-jobs': '不整合の再計算・整備ジョブの状況を確認します。',
  audit: '運用操作の監査証跡を確認します。',
  help: 'オントロジーの操作手順と主要用語を確認します。'
}

const ONTOLOGY_GLOSSARY = [
  { term: 'Entity', description: '人名や部署名など、意味を持つ対象語です。' },
  { term: 'Lineage', description: 'データの入出力と変換の履歴です。' },
  { term: 'Reconcile', description: '不整合データを再計算して整える処理です。' }
]

const ONTOLOGY_TAB_HELP: Record<OntologyPageKey, string> = {
  overview: 'オントロジーの主要KPIを確認します。',
  'relationship-graph': '関係グラフで解決根拠と影響範囲を確認します。',
  'unified-metadata': 'ドキュメント単位のオントロジーグラフ一覧を確認します。',
  'entity-candidates': '辞書未登録の候補と推薦候補を確認します。',
  'entity-master': '正規化済みエンティティマスタ（推薦の参照元）を確認します。',
  lineage: 'データ系譜と変換履歴を確認します。',
  'reconcile-jobs': '再整合ジョブの状態を確認します。',
  audit: '監査証跡を確認します。',
  help: '使い方と用語を確認します。'
}

const badgeVariantByValue = (value: string): BadgeProps['variant'] => {
  if (value === 'failure' || value === 'failed' || value === 'stale') return 'destructive'
  if (value === 'running' || value === 'aging' || value === 'skipped') return 'secondary'
  return 'outline'
}

const entityTypeLabel = (value: string): string => {
  const map: Record<string, string> = {
    organization: '組織',
    person: '人物',
    project: 'プロジェクト',
    topic_category: 'トピック',
    system: 'システム',
    policy: 'ポリシー',
    document: '文書'
  }
  return map[value] ?? value
}

const statusLabel = (value: string): string => {
  const map: Record<string, string> = {
    active: '有効',
    merged: '統合済み',
    archived: 'アーカイブ',
    pending: '保留',
    resolved: '解決済み',
    rejected: '却下',
    success: '成功',
    failure: '失敗',
    skipped: 'スキップ',
    failed: '失敗',
    running: '実行中',
    queued: '待機中',
    stale: '要再整合',
    aging: '更新遅れ予備群'
  }
  return map[value] ?? value
}

const qualityLabel = (score: number): { label: '良' | '注意' | '要対応'; variant: BadgeProps['variant'] } => {
  if (score >= 0.75) return { label: '良', variant: 'outline' }
  if (score >= 0.4) return { label: '注意', variant: 'secondary' }
  return { label: '要対応', variant: 'destructive' }
}

const qualityScoreButtonClass = (score: number): string => {
  if (score >= 0.75) return 'border-emerald-300 bg-emerald-50 text-emerald-800 hover:bg-emerald-100'
  if (score >= 0.4) return 'border-amber-300 bg-amber-50 text-amber-800 hover:bg-amber-100'
  return 'border-rose-300 bg-rose-50 text-rose-800 hover:bg-rose-100'
}

const qualityAction = (score: number): string => {
  if (score < 0.4) return '名寄せ精度向上 + 重複解消 + 鮮度反映'
  if (score < 0.75) return '重複解消 + エンティティ名寄せ精査'
  return '維持監視'
}

const hasMeaningfulOwner = (owner: string): boolean => {
  const normalized = owner.trim().toLowerCase()
  return normalized.length > 0 && normalized !== 'unknown'
}

const isProjectIdLike = (project: string): boolean => {
  const normalized = project.trim()
  if (!normalized) return false
  if (/^b![A-Za-z0-9_-]{16,}$/.test(normalized)) return true
  if (/^[0-9a-f]{8}-[0-9a-f-]{27,}$/i.test(normalized)) return true
  if (/^ent-proj-[0-9a-z-]+$/i.test(normalized)) return true
  return false
}

const hasMeaningfulProject = (project: string): boolean => {
  const normalized = project.trim().toLowerCase()
  if (!normalized || normalized === 'general') return false
  return !isProjectIdLike(project)
}

const hasMeaningfulTopicCategories = (topics: string[]): boolean => {
  if (!Array.isArray(topics) || topics.length === 0) return false
  return topics.some((topic) => topic.trim().toLowerCase() !== 'general')
}

const INTENT_LABELS: Record<string, string> = {
  system_overview: 'システム概要',
  onboarding_setup: '初期設定 / 利用開始',
  operation_howto: '操作方法',
  troubleshooting: '障害対応',
  policy_compliance: '社内規程 / コンプライアンス',
  security_compliance: 'セキュリティ / コンプライアンス',
  contact_escalation: '問い合わせ / エスカレーション先',
  product_specification: '製品仕様',
  initial_setup: '初期設定',
  billing: '料金 / 請求',
  contract_terms: '契約 / 利用条件',
  escalation_contact: 'エスカレーション先'
}

const average = (values: number[]): number => {
  if (values.length === 0) return 0
  return values.reduce((sum, value) => sum + value, 0) / values.length
}

const toUiErrorMessage = (error: unknown, fallback: string): string => {
  if (error instanceof ApiHttpError) {
    const payload = error.data as Record<string, unknown> | undefined
    const detailField = payload?.detail
    if (typeof detailField === 'string' && detailField.trim()) {
      return detailField
    }
    if (detailField && typeof detailField === 'object') {
      const detailRecord = detailField as Record<string, unknown>
      const detail = typeof detailRecord.detail === 'string' ? detailRecord.detail : ''
      const code = typeof detailRecord.error_code === 'string' ? detailRecord.error_code : ''
      if (detail && code) return `${detail} (${code})`
      if (detail) return detail
      if (code) return code
    }
    if (typeof payload?.message === 'string' && payload.message.trim()) {
      return payload.message
    }
    return `${fallback} (${error.status})`
  }
  if (error instanceof Error && error.message.trim()) {
    return error.message
  }
  return fallback
}

const mapPillarDocumentCountsFromApi = (
  raw: OntologyOverviewResponse['pillar_document_counts']
): OntologyPillarDocumentCounts | undefined => {
  if (!raw) return undefined
  const by = raw.freshness?.by_status ?? {}
  return {
    denominator: Number(raw.denominator ?? 0),
    freshness: {
      byStatus: {
        active: Number(by.active ?? 0),
        aging: Number(by.aging ?? 0),
        stale: Number(by.stale ?? 0),
        other: Number(by.other ?? 0)
      },
      staleOrAging: Number(raw.freshness?.stale_or_aging ?? 0)
    },
    duplication: {
      inDuplicateGroup: Number(raw.duplication?.in_duplicate_group ?? 0),
      nonCanonicalDuplicateCopy: Number(raw.duplication?.non_canonical_duplicate_copy ?? 0),
      canonicalOrNoDuplicateGroup: Number(raw.duplication?.canonical_or_no_duplicate_group ?? 0)
    },
    stewardship: {
      meaningfulOwner: Number(raw.stewardship?.meaningful_owner ?? 0),
      meaningfulProject: Number(raw.stewardship?.meaningful_project ?? 0),
      meaningfulTopicCategories: Number(raw.stewardship?.meaningful_topic_categories ?? 0),
      allThree: Number(raw.stewardship?.all_three ?? 0)
    }
  }
}

const toOverviewStatsFromApi = (
  row: OntologyOverviewResponse,
  metadataRows: UnifiedMetadataRecord[]
): OntologyOverviewStats => {
  const total = metadataRows.length
  const aiProposedCount = metadataRows.filter((item) => item.remediationState === 'ai_proposed').length
  const approvedCount = metadataRows.filter((item) => item.remediationState === 'approved').length
  const executedCount = metadataRows.filter((item) => item.remediationState === 'executed').length
  const activeCount = metadataRows.filter((item) => item.freshnessStatus === 'active').length
  const agingCount = metadataRows.filter((item) => item.freshnessStatus === 'aging').length
  const staleCount = metadataRows.filter((item) => item.freshnessStatus === 'stale').length
  const staleOrAging = staleCount + agingCount
  const ontologyScores = metadataRows.map((item) => Number(item.ontologyScore ?? 0)).filter((value) => value > 0)
  const baseScores = metadataRows.map((item) => Number(item.baseOntologyScore ?? 0)).filter((value) => value > 0)
  const readinessScores = metadataRows.map((item) => Number(item.useCaseReadiness ?? 0)).filter((value) => value > 0)
  const freshnessValidityScores = metadataRows.map((item) => Number(item.freshnessValidity ?? 0)).filter((value) => value > 0)
  const canonicalityDuplicationScores = metadataRows
    .map((item) => Number(item.canonicalityDuplication ?? 0))
    .filter((value) => value > 0)
  const stewardshipFindabilityScores = metadataRows
    .map((item) => Number(item.stewardshipFindability ?? 0))
    .filter((value) => value > 0)
  const intentCoverageScores = metadataRows.map((item) => Number(item.intentCoverage ?? 0)).filter((value) => value > 0)
  const evidenceCoverageScores = metadataRows.map((item) => Number(item.evidenceCoverage ?? 0)).filter((value) => value > 0)
  const freshnessFitScores = metadataRows.map((item) => Number(item.freshnessFit ?? 0)).filter((value) => value > 0)
  const benchmarkLiteScores = metadataRows.map((item) => Number(item.benchmarkLite ?? 0)).filter((value) => value >= 0)

  const intentCounts: Record<string, number> = {}
  metadataRows.forEach((item) => {
    ;(item.intentTags ?? []).forEach((intent) => {
      intentCounts[intent] = (intentCounts[intent] ?? 0) + 1
    })
  })
  const intentBreakdown = Object.entries(intentCounts)
    .map(([intentId, count]) => ({
      intentId,
      label: INTENT_LABELS[intentId] ?? intentId,
      score: metadataRows.length > 0 ? count / metadataRows.length : 0
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, 8)

  const fallbackFreshness = Number(row.signal_scores?.freshness ?? 0)
  const fallbackDuplication = Number(row.signal_scores?.duplication ?? 0)
  const fallbackLocation = Number(row.signal_scores?.location ?? 0)
  const resolvedFreshnessValidity = Number(row.freshness_validity ?? (average(freshnessValidityScores) || fallbackFreshness))
  const resolvedCanonicalityDuplication = Number(
    row.canonicality_duplication ?? (average(canonicalityDuplicationScores) || fallbackDuplication)
  )
  const resolvedStewardshipFindability = Number(
    row.stewardship_findability ?? (average(stewardshipFindabilityScores) || fallbackLocation)
  )
  const resolvedIntentCoverage = Number(row.intent_coverage ?? average(intentCoverageScores))
  const resolvedEvidenceCoverage = Number(row.evidence_coverage ?? average(evidenceCoverageScores))
  const resolvedFreshnessFit = Number(row.freshness_fit ?? average(freshnessFitScores))
  const resolvedBenchmarkLite = Number(row.benchmark_lite ?? average(benchmarkLiteScores))
  const resolvedBaseOntologyScore = Number(
    row.base_ontology_score ??
      (average(baseScores) ||
        (0.35 * resolvedFreshnessValidity
          + 0.35 * resolvedCanonicalityDuplication
          + 0.3 * resolvedStewardshipFindability))
  )
  const resolvedUseCaseReadiness = Number(
    row.use_case_readiness ??
      (average(readinessScores) ||
        (0.45 * resolvedIntentCoverage
          + 0.3 * resolvedEvidenceCoverage
          + 0.15 * resolvedFreshnessFit
          + 0.1 * resolvedBenchmarkLite))
  )
  const resolvedOntologyScore = Number(
    row.ontology_score ?? (average(ontologyScores) || (0.7 * resolvedBaseOntologyScore + 0.3 * resolvedUseCaseReadiness))
  )

  return {
    aiProposedCount,
    approvedCount,
    executedCount,
    exceptionResolved24h: 0,
    unifiedMetadataTotal: total,
    activeCount: total > 0 ? activeCount : Math.max(0, row.stale_or_aging_documents),
    agingCount: total > 0 ? agingCount : row.stale_or_aging_documents,
    staleCount: total > 0 ? staleCount : 0,
    unresolvedCandidates: row.unresolved_candidates,
    entityResolverSuccessRate24h: row.signal_scores.location,
    lastReconcileRunAt: staleOrAging > 0 ? '-' : '-',
    projectedDocuments: Number(row.projection_metrics?.projected_documents ?? 0),
    containedInLinks: Number(row.projection_metrics?.contained_in_links ?? 0),
    mentionsLinks: Number(row.projection_metrics?.mentions_links ?? 0),
    similarityLinks: Number(row.projection_metrics?.similarity_links ?? 0),
    skippedSimilarityDocs: Number(row.projection_metrics?.skipped_similarity_docs ?? 0),
    textFallbackVectors: Number(row.projection_metrics?.text_fallback_vectors ?? 0),
    autoPromotedEntities: Number(row.projection_metrics?.auto_promoted_entities ?? 0),
    projectionPreset: (row.projection_metrics?.projection_preset as 'strict' | 'standard' | 'relaxed') ?? 'standard',
    projectionPresetSource:
      (row.projection_metrics?.projection_preset_source as 'request' | 'tenant_mapping' | 'default') ?? 'default',
    lastProjectionRefreshAt: String(row.projection_metrics?.last_refresh_at ?? '-'),
    documentAnalysisTotal: Number(row.document_analysis_metrics?.analysis_total_count ?? 0),
    documentAnalysisMatchedUnified: Number(row.document_analysis_metrics?.matched_unified_count ?? 0),
    documentAnalysisCoverageRatio: Number(row.document_analysis_metrics?.coverage_ratio ?? 0),
    documentAnalysisUnmatched: Number(row.document_analysis_metrics?.unmatched_analysis_count ?? 0),
    documentAnalysisTargetMode:
      (row.document_analysis_contract?.target_mode as 'eligible_only' | 'all_unified') ?? 'eligible_only',
    documentAnalysisQueryError: String(row.document_analysis_metrics?.query_error ?? ''),
    ontologyScore: resolvedOntologyScore,
    baseOntologyScore: resolvedBaseOntologyScore,
    useCaseReadiness: resolvedUseCaseReadiness,
    freshnessValidity: resolvedFreshnessValidity,
    canonicalityDuplication: resolvedCanonicalityDuplication,
    stewardshipFindability: resolvedStewardshipFindability,
    intentCoverage: resolvedIntentCoverage,
    evidenceCoverage: resolvedEvidenceCoverage,
    freshnessFit: resolvedFreshnessFit,
    benchmarkLite: resolvedBenchmarkLite,
    intentBreakdown:
      Array.isArray(row.intent_breakdown) && row.intent_breakdown.length > 0
        ? row.intent_breakdown.map((item) => ({
          intentId: String(item.intent_id),
          label: String(item.label ?? INTENT_LABELS[String(item.intent_id)] ?? item.intent_id),
          score: Number(item.score ?? 0)
        }))
        : intentBreakdown,
    pillarDocumentCounts: mapPillarDocumentCountsFromApi(row.pillar_document_counts),
    ontologyScoreMode: row.ontology_score_mode === 'legacy' ? 'legacy' : 'count_based'
  }
}

const resolveOntologyDocumentName = (row: OntologyUnifiedMetadataApiRow, fallback: string): string => {
  const title = String(row.title ?? '').trim()
  if (title) return title

  const itemName = String((row as Record<string, unknown>).item_name ?? '').trim()
  if (itemName) return itemName

  return fallback
}

const resolveOntologyFilePath = (row: OntologyUnifiedMetadataApiRow, fallbackName: string): string => {
  const candidateKeys = ['item_url', 'file_path', 'path', 'hierarchy_path'] as const
  for (const key of candidateKeys) {
    const value = String((row as Record<string, unknown>)[key] ?? '').trim()
    if (value) return value
  }
  return fallbackName
}

const toUnifiedMetadataFromApi = (row: OntologyUnifiedMetadataApiRow, index: number) => ({
  itemId: String(row.item_id ?? `item-${String(index + 1).padStart(3, '0')}`),
  documentName: resolveOntologyDocumentName(row, String(row.item_id ?? `item-${index + 1}`)),
  filePath: resolveOntologyFilePath(
    row,
    resolveOntologyDocumentName(row, String(row.item_id ?? `item-${index + 1}`))
  ),
  planId: String(row.plan_id ?? `plan-api-${index + 1}`),
  remediationState: ((): RemediationState => {
    const raw = (row as Record<string, unknown>).remediation_state
    const allowed: RemediationState[] = ['ai_proposed', 'pending_approval', 'approved', 'executed']
    return typeof raw === 'string' && (allowed as string[]).includes(raw) ? (raw as RemediationState) : 'executed'
  })(),
  title: resolveOntologyDocumentName(row, String(row.item_id ?? `item-${index + 1}`)),
  contentType: String(row.content_type ?? 'application/octet-stream'),
  source: String(row.source ?? 'aws'),
  freshnessStatus: (row.freshness_status ?? 'active') as 'active' | 'aging' | 'stale',
  aiEligible: Boolean(row.ai_eligible ?? true),
  contentQualityScore: Number(row.content_quality_score ?? 0.5),
  transformedAt: String(row.transformed_at ?? '-'),
  owner: String(row.owner ?? ''),
  project: String(row.project ?? ''),
  topicCategories: Array.isArray(row.topic_categories)
    ? row.topic_categories.map((value) => String(value))
    : [],
  topics: Array.isArray(row.topics) ? row.topics.map((value) => String(value)) : [],
  categoryHierarchy:
    row.category_hierarchy && typeof row.category_hierarchy === 'object'
      ? {
        large: String(row.category_hierarchy.large ?? ''),
        medium: String(row.category_hierarchy.medium ?? ''),
        small: String(row.category_hierarchy.small ?? ''),
        confidence: Number(row.category_hierarchy.confidence ?? 0),
        reasonCodes: Array.isArray(row.category_hierarchy.reason_codes)
          ? row.category_hierarchy.reason_codes.map((value: unknown) => String(value))
          : []
      }
      : undefined,
  canonicalDocId: String(row.canonical_doc_id ?? ''),
  ontologyScore: Number(row.ontology_score ?? 0),
  baseOntologyScore: Number(row.base_ontology_score ?? 0),
  useCaseReadiness: Number(row.use_case_readiness ?? 0),
  freshnessValidity: Number(row.freshness_validity ?? 0),
  canonicalityDuplication: Number(row.canonicality_duplication ?? 0),
  stewardshipFindability: Number(row.stewardship_findability ?? 0),
  intentCoverage: Number(row.intent_coverage ?? 0),
  evidenceCoverage: Number(row.evidence_coverage ?? 0),
  freshnessFit: Number(row.freshness_fit ?? 0),
  benchmarkLite: Number(row.benchmark_lite ?? 0),
  authorityLevel: String(row.authority_level ?? 'low'),
  intentTags: Array.isArray(row.intent_tags) ? row.intent_tags.map((value) => String(value)) : [],
  useCaseFitScores:
    row.use_case_fit_scores && typeof row.use_case_fit_scores === 'object'
      ? Object.fromEntries(
        Object.entries(row.use_case_fit_scores).map(([key, value]) => [key, Number(value ?? 0)])
      )
      : {},
  profileInferenceFallback:
    row.profile_inference_fallback === undefined ? undefined : Boolean(row.profile_inference_fallback),
  profileNeedsReview: row.profile_needs_review === undefined ? undefined : Boolean(row.profile_needs_review),
  profileLlmUsed: row.profile_llm_used === undefined ? undefined : Boolean(row.profile_llm_used),
  profileInferenceSource:
    row.profile_inference_source !== undefined && row.profile_inference_source !== null
      ? String(row.profile_inference_source)
      : undefined
})

const toEntityMasterFromApi = (row: OntologyEntityMasterApiRow, index: number) => ({
  entityId: String(row.entity_id ?? `ent-api-${index + 1}`),
  planId: `plan-api-${index + 1}`,
  remediationState: ((): RemediationState => {
    const raw = (row as Record<string, unknown>).remediation_state
    const allowed: RemediationState[] = ['ai_proposed', 'pending_approval', 'approved', 'executed']
    return typeof raw === 'string' && (allowed as string[]).includes(raw) ? (raw as RemediationState) : 'executed'
  })(),
  canonicalValue: String(row.canonical_value ?? row.canonical_name ?? `entity-${index + 1}`),
  entityType: String(row.entity_type ?? 'entity'),
  piiFlag: Boolean(row.pii_flag ?? false),
  confidence: Number(row.confidence ?? 0.8),
  spreadFactor: Number(row.spread_factor ?? 1),
  status: (row.status ?? 'active') as 'active' | 'merged' | 'archived',
  updatedAt: String(row.updated_at ?? '-')
})

const toOntologyAuditFromApi = (row: OntologyAuditApiRow, index: number) => ({
  auditId: `oa-api-${index + 1}`,
  operator: String(row.operator ?? 'system'),
  action: String(row.event ?? row.job_name ?? 'ontology.audit'),
  targetType: 'job' as const,
  targetId: String(row.job_name ?? row.source ?? '-'),
  timestamp: String(row.timestamp ?? '-'),
  correlationId: String(row.correlation_id ?? '-')
})

const paginateRows = <T,>(rows: T[], page: number, pageSize: number): T[] =>
  rows.slice((page - 1) * pageSize, page * pageSize)

// Unused legacy helpers removed (remediationStatusMessage, resolveLegacyPlanIdFromItemId)

const fetchAllOntologyUnifiedMetadata = async (
  onlyActiveConnectScopes: boolean = true
): Promise<{
  rows: OntologyUnifiedMetadataApiRow[]
  totalCount: number
}> => {
  const limit = 500
  let offset = 0
  let totalCount = 0
  const rows: OntologyUnifiedMetadataApiRow[] = []

  while (true) {
    const response = await getOntologyUnifiedMetadata(limit, offset, onlyActiveConnectScopes)
    if (totalCount === 0) {
      totalCount = response.pagination.total_count
    }
    rows.push(...response.rows)
    if (response.rows.length === 0 || rows.length >= response.pagination.total_count) {
      break
    }
    offset += response.rows.length
  }

  return { rows, totalCount }
}

const fetchAllOntologyEntityMaster = async (): Promise<{
  rows: OntologyEntityMasterApiRow[]
  totalCount: number
}> => {
  const limit = 500
  let offset = 0
  let totalCount = 0
  const rows: OntologyEntityMasterApiRow[] = []

  while (true) {
    const response = await getOntologyEntityMaster(limit, offset)
    if (totalCount === 0) {
      totalCount = response.pagination.total_count
    }
    rows.push(...response.rows)
    if (response.rows.length === 0 || rows.length >= response.pagination.total_count) {
      break
    }
    offset += response.rows.length
  }

  return { rows, totalCount }
}

const PillarHintLabel = ({ label, hint }: { label: string; hint: string }) => (
  <Tooltip>
    <TooltipTrigger asChild>
      <span
        tabIndex={0}
        className="cursor-help border-b border-dotted border-muted-foreground/60 text-foreground outline-none focus-visible:rounded-sm focus-visible:ring-2 focus-visible:ring-ring"
      >
        {label}
      </span>
    </TooltipTrigger>
    <TooltipContent side="top" className="max-w-[280px] text-xs leading-relaxed">
      {hint}
    </TooltipContent>
  </Tooltip>
)

const OverviewPage = ({
  onNavigate,
  overview,
  unresolvedDocumentProfileCount
}: {
  onNavigate: (page: OntologyPageKey, focus?: string) => void
  overview: OntologyOverviewStats
  unresolvedDocumentProfileCount: number
}) => {
  return (
    <div className="space-y-4">
      <Card className="border-primary/20 bg-primary/5">
        <CardHeader className="pb-2">
          <CardTitle>オントロジー ショートカット</CardTitle>
          <CardDescription>問題箇所へすぐ移動して、優先度の高い項目から対応できます。</CardDescription>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-3">
          <button
            type="button"
            onClick={() => onNavigate('entity-candidates', 'candidate:lowest_confidence')}
            className="group rounded-md border-2 border-amber-500/40 bg-amber-500/5 p-3 text-left transition-all duration-150 hover:border-primary/60 hover:bg-primary/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60 active:scale-[0.98] active:brightness-95"
          >
            <p className="text-xs text-muted-foreground">未解決ワード</p>
            <p className="mt-1 text-3xl font-semibold leading-none text-amber-700">{overview.unresolvedCandidates} 件</p>
            <p className="mt-1 text-[11px] text-muted-foreground">エンティティ解決で名寄せを実施</p>
            <p className="mt-2 text-xs font-medium text-primary opacity-80 transition group-hover:opacity-100">詳細を見る →</p>
          </button>
          <button
            type="button"
            onClick={() => onNavigate('unified-metadata', 'freshness:stale')}
            className="group rounded-md border-2 border-orange-500/40 bg-orange-500/5 p-3 text-left transition-all duration-150 hover:border-primary/60 hover:bg-primary/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60 active:scale-[0.98] active:brightness-95"
          >
            <p className="text-xs text-muted-foreground">鮮度更新遅れ</p>
            <p className="mt-1 text-3xl font-semibold leading-none text-orange-700">{overview.staleCount + overview.agingCount} 件</p>
            <p className="mt-1 text-[11px] text-muted-foreground">更新遅れ文書を優先確認</p>
            <p className="mt-2 text-xs font-medium text-primary opacity-80 transition group-hover:opacity-100">詳細を見る →</p>
          </button>
          <button
            type="button"
            onClick={() => onNavigate('unified-metadata', 'profile:missing')}
            className="group rounded-md border-2 border-sky-500/40 bg-sky-500/5 p-3 text-left transition-all duration-150 hover:border-primary/60 hover:bg-primary/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60 active:scale-[0.98] active:brightness-95"
          >
            <p className="text-xs text-muted-foreground">文書プロファイル未確定</p>
            <p className="mt-1 text-3xl font-semibold leading-none text-sky-700">{unresolvedDocumentProfileCount} 件</p>
            <p className="mt-1 text-[11px] text-muted-foreground">owner/project/topic の補完対象を確認</p>
            <p className="mt-2 text-xs font-medium text-primary opacity-80 transition group-hover:opacity-100">詳細を見る →</p>
          </button>
        </CardContent>
      </Card>

      {overview.pillarDocumentCounts && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle>情報整備の3つのチェック（件数）</CardTitle>
            <CardDescription className="space-y-1.5">
              <span className="block">
                鮮度・かぶり・管理情報の3点について、文書ごとの件数を並べています。加重スコアではなく、そのままの数です。
              </span>
              <span className="block text-muted-foreground">
                対象は運用中の文書 {overview.pillarDocumentCounts.denominator} 件です（論理削除は含みません）。
              </span>
            </CardDescription>
          </CardHeader>
          <TooltipProvider delayDuration={200}>
            <CardContent className="grid gap-4 md:grid-cols-3">
              <div className="rounded-md border bg-muted/30 p-3 text-sm">
                <p className="flex items-center gap-2 font-medium text-foreground">
                  <CalendarClock className="h-4 w-4 shrink-0 text-muted-foreground" aria-hidden />
                  鮮度と有効期限
                </p>
                <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
                  更新が追いついているか、見直しが必要かを件数で見ます。
                </p>
                <dl className="mt-3 space-y-1.5 text-muted-foreground">
                  <div className="flex justify-between gap-2">
                    <dt className="min-w-0 shrink">
                      <PillarHintLabel
                        label="問題なし"
                        hint="鮮度・有効期限の観点で、すぐ参照してよい状態として数えている文書です（システム上の active に相当）。"
                      />
                    </dt>
                    <dd className="tabular-nums text-foreground">{overview.pillarDocumentCounts.freshness.byStatus.active}</dd>
                  </div>
                  <div className="flex justify-between gap-2">
                    <dt className="min-w-0 shrink">
                      <PillarHintLabel
                        label="そろそろ要確認"
                        hint="まだ「要対応」ではないものの、更新が遅れ始めているなど注意が必要な状態です（aging）。優先的に目を通すとよい文書の件数です。"
                      />
                    </dt>
                    <dd className="tabular-nums text-foreground">{overview.pillarDocumentCounts.freshness.byStatus.aging}</dd>
                  </div>
                  <div className="flex justify-between gap-2">
                    <dt className="min-w-0 shrink">
                      <PillarHintLabel
                        label="更新・整合が必要"
                        hint="鮮度や整合の面で手当てや再計算が必要な状態です（stale）。放置しないほうがよい文書の件数です。"
                      />
                    </dt>
                    <dd className="tabular-nums text-foreground">{overview.pillarDocumentCounts.freshness.byStatus.stale}</dd>
                  </div>
                  <div className="flex justify-between gap-2">
                    <dt className="min-w-0 shrink">
                      <PillarHintLabel
                        label="その他"
                        hint="上記の区分に当てはまらない鮮度ステータスの文書です。"
                      />
                    </dt>
                    <dd className="tabular-nums text-foreground">{overview.pillarDocumentCounts.freshness.byStatus.other}</dd>
                  </div>
                  <div className="mt-2 flex justify-between gap-2 rounded-md bg-muted/50 px-2 py-2 text-foreground">
                    <dt className="min-w-0 font-medium">
                      <PillarHintLabel
                        label="まとめ：注意〜要対応の合計"
                        hint="「そろそろ要確認」と「更新・整合が必要」の件数を足したものです。0 に近いほど鮮度面は安心です。"
                      />
                    </dt>
                    <dd className="shrink-0 tabular-nums font-semibold">{overview.pillarDocumentCounts.freshness.staleOrAging}</dd>
                  </div>
                </dl>
              </div>
              <div className="rounded-md border bg-muted/30 p-3 text-sm">
                <p className="flex items-center gap-2 font-medium text-foreground">
                  <Files className="h-4 w-4 shrink-0 text-muted-foreground" aria-hidden />
                  重なり（重複）
                </p>
                <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
                  同じ内容の文書が複数あるとき、公式の1本が決まっているかを件数で見ます。
                </p>
                <dl className="mt-3 space-y-1.5 text-muted-foreground">
                  <div className="flex justify-between gap-2">
                    <dt className="min-w-0 shrink">
                      <PillarHintLabel
                        label="同じ内容としてひも付いた件数"
                        hint="システムが「中身が同じ」と判断し、ひとまとまりに関連付けている文書の件数です（重複グループに含まれる件数）。"
                      />
                    </dt>
                    <dd className="tabular-nums text-foreground">{overview.pillarDocumentCounts.duplication.inDuplicateGroup}</dd>
                  </div>
                  <div className="flex justify-between gap-2">
                    <dt className="min-w-0 shrink">
                      <PillarHintLabel
                        label="メイン以外の控え"
                        hint="同じ内容のなかで、公式の1本（メイン）ではない側として数えている文書です。整理・統合の候補になりやすい件数です。"
                      />
                    </dt>
                    <dd className="tabular-nums text-foreground">
                      {overview.pillarDocumentCounts.duplication.nonCanonicalDuplicateCopy}
                    </dd>
                  </div>
                  <div className="mt-2 flex justify-between gap-2 rounded-md bg-muted/50 px-2 py-2 text-foreground">
                    <dt className="min-w-0 font-medium">
                      <PillarHintLabel
                        label="まとめ：メイン確定またはかぶりなし"
                        hint="公式の1本が決まっている文書、またはそもそも同じ内容の別ファイルとみなされていない文書の件数です。母数に近いほど望ましいです。"
                      />
                    </dt>
                    <dd className="shrink-0 tabular-nums font-semibold">
                      {overview.pillarDocumentCounts.duplication.canonicalOrNoDuplicateGroup}
                    </dd>
                  </div>
                </dl>
              </div>
              <div className="rounded-md border bg-muted/30 p-3 text-sm">
                <p className="flex items-center gap-2 font-medium text-foreground">
                  <UserCircle className="h-4 w-4 shrink-0 text-muted-foreground" aria-hidden />
                  管理情報（所在）
                </p>
                <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
                  担当・案件・分類など、誰がどの文脈の文書かが分かる情報が入っているかを見ます。
                </p>
                <dl className="mt-3 space-y-1.5 text-muted-foreground">
                  <div className="flex justify-between gap-2">
                    <dt className="min-w-0 shrink">
                      <PillarHintLabel
                        label="所有者が入っている"
                        hint="意味のある所有者（オーナー）情報が登録されている文書の件数です。"
                      />
                    </dt>
                    <dd className="tabular-nums text-foreground">{overview.pillarDocumentCounts.stewardship.meaningfulOwner}</dd>
                  </div>
                  <div className="flex justify-between gap-2">
                    <dt className="min-w-0 shrink">
                      <PillarHintLabel
                        label="プロジェクトが入っている"
                        hint="プレースホルダではない、意味のあるプロジェクト紐づけがある文書の件数です。"
                      />
                    </dt>
                    <dd className="tabular-nums text-foreground">{overview.pillarDocumentCounts.stewardship.meaningfulProject}</dd>
                  </div>
                  <div className="flex justify-between gap-2">
                    <dt className="min-w-0 shrink">
                      <PillarHintLabel
                        label="トピックが入っている"
                        hint="分類・トピックとして有効な値が入っている文書の件数です。"
                      />
                    </dt>
                    <dd className="tabular-nums text-foreground">{overview.pillarDocumentCounts.stewardship.meaningfulTopicCategories}</dd>
                  </div>
                  <div className="mt-2 flex justify-between gap-2 rounded-md bg-muted/50 px-2 py-2 text-foreground">
                    <dt className="min-w-0 font-medium">
                      <PillarHintLabel
                        label="まとめ：3つそろっている"
                        hint="所有者・プロジェクト・トピックの3つが、いずれも有効な値でそろっている文書の件数です。母数に近いほど管理情報が整っています。"
                      />
                    </dt>
                    <dd className="shrink-0 tabular-nums font-semibold">{overview.pillarDocumentCounts.stewardship.allThree}</dd>
                  </div>
                </dl>
              </div>
            </CardContent>
          </TooltipProvider>
        </Card>
      )}

      <Card>
        <CardHeader className="pb-2">
          <CardTitle>DocumentAnalysis カバレッジ</CardTitle>
          <CardDescription>
            UnifiedMetadata に対して DocumentAnalysis が保存されている割合を表示します。
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-3 text-sm md:grid-cols-5">
          <div>対象モード: {overview.documentAnalysisTargetMode}</div>
          <div>分析レコード: {overview.documentAnalysisTotal}</div>
          <div>Unified一致: {overview.documentAnalysisMatchedUnified}</div>
          <div>一致率: {Math.round(overview.documentAnalysisCoverageRatio * 100)}%</div>
          <div>不一致分析件数: {overview.documentAnalysisUnmatched}</div>
        </CardContent>
        {overview.documentAnalysisQueryError && (
          <CardContent className="pt-0 text-xs text-amber-700">
            DocumentAnalysis 参照エラー: {overview.documentAnalysisQueryError}
          </CardContent>
        )}
      </Card>

    </div>
  )
}

const UnifiedMetadataPage = ({
  deepLinkFocus,
  deepLinkRevision = 0,
  metadataRows,
  totalRowsFromApi,
  onReloadData,
  onlyActiveConnectScopes,
  onOnlyActiveConnectScopesChange
}: {
  deepLinkFocus?: string
  deepLinkRevision?: number
  metadataRows: UnifiedMetadataRecord[]
  totalRowsFromApi: number
  onReloadData: () => Promise<void>
  onlyActiveConnectScopes: boolean
  onOnlyActiveConnectScopesChange: (next: boolean) => void
}) => {
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize, setPageSize] = useState(25)
  const [selectedItemId, setSelectedItemId] = useState(metadataRows[0]?.itemId ?? '')
  const [expandedItemId, setExpandedItemId] = useState<string | null>(null)
  const [detailDialog, setDetailDialog] = useState<'quality' | 'edit' | null>(null)
  const [editOwner, setEditOwner] = useState('')
  const [editProject, setEditProject] = useState('')
  const [editTopics, setEditTopics] = useState('')
  const [editCanonical, setEditCanonical] = useState('')
  const [isSaving, setIsSaving] = useState(false)
  const [useLlmOnAiFill, setUseLlmOnAiFill] = useState(true)
  const scopedMetadataRows = useMemo(() => metadataRows, [metadataRows])
  const totalRows = totalRowsFromApi > 0 ? totalRowsFromApi : scopedMetadataRows.length
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize))
  const pagedRows = useMemo(() => paginateRows(scopedMetadataRows, currentPage, pageSize), [scopedMetadataRows, currentPage, pageSize])
  const selectedMetadata = useMemo(
    () => scopedMetadataRows.find((row) => row.itemId === selectedItemId) ?? null,
    [scopedMetadataRows, selectedItemId]
  )
  const selectedDocumentQualityScore = Number(
    selectedMetadata?.contentQualityScore ?? 0
  )
  const selectedUseCaseFitScore = Number(
    selectedMetadata?.useCaseFitScores?.internal_qa ??
      Object.values(selectedMetadata?.useCaseFitScores ?? {})[0] ??
      0
  )

  const handleAiFill = useCallback(async () => {
    if (!selectedItemId) return
    setIsSaving(true)
    try {
      await aiFillOntologyProfile(selectedItemId, { use_llm: useLlmOnAiFill })
      await onReloadData()
      useGraphStore.getState().setGraphDataFetchAttempted(false)
    } catch (error) {
      console.error('AI fill failed:', error)
      window.alert(toUiErrorMessage(error, 'AI補完に失敗しました'))
    } finally {
      setIsSaving(false)
    }
  }, [selectedItemId, onReloadData, useLlmOnAiFill])

  const openEditDialog = useCallback(() => {
    if (!selectedMetadata) return
    setEditOwner(selectedMetadata.owner)
    setEditProject(selectedMetadata.project)
    setEditTopics(selectedMetadata.topicCategories.join(', '))
    setEditCanonical(selectedMetadata.canonicalDocId)
    setDetailDialog('edit')
  }, [selectedMetadata])

  const handleSaveEdit = useCallback(async () => {
    if (!selectedItemId) return
    setIsSaving(true)
    try {
      await updateOntologyProfile(selectedItemId, {
        owner: editOwner.trim() || undefined,
        project: editProject.trim() || undefined,
        topic_categories: editTopics.split(',').map((t) => t.trim()).filter(Boolean),
        canonical_doc_id: editCanonical.trim() || undefined,
      })
      await onReloadData()
      useGraphStore.getState().setGraphDataFetchAttempted(false)
      setDetailDialog(null)
    } catch (error) {
      console.error('Save failed:', error)
      window.alert(toUiErrorMessage(error, '保存に失敗しました'))
    } finally {
      setIsSaving(false)
    }
  }, [selectedItemId, editOwner, editProject, editTopics, editCanonical, onReloadData])

  const minQualityScore =
    scopedMetadataRows.length > 0
      ? Math.min(...scopedMetadataRows.map((row) => Number(row.contentQualityScore ?? 0)))
      : 0
  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(totalPages)
  }, [currentPage, totalPages])
  useEffect(() => {
    if (scopedMetadataRows.length === 0) return
    if (!scopedMetadataRows.some((row) => row.itemId === selectedItemId)) {
      setSelectedItemId(scopedMetadataRows[0].itemId)
    }
  }, [scopedMetadataRows, selectedItemId])
  useEffect(() => {
    setCurrentPage(1)
  }, [pageSize])
  useEffect(() => {
    if (!deepLinkFocus) return
    if (deepLinkFocus === 'quality:lowest_score') {
      const target = scopedMetadataRows.find(
        (row) => Number(row.contentQualityScore ?? 0) === minQualityScore
      )
      if (target) setSelectedItemId(target.itemId)
      setDetailDialog('quality')
      return
    }
    if (deepLinkFocus === 'freshness:stale') {
      const stale = scopedMetadataRows.find((row) => row.freshnessStatus === 'stale') ?? scopedMetadataRows[0]
      if (!stale) return
      setSelectedItemId(stale.itemId)
      setCurrentPage(Math.floor(scopedMetadataRows.findIndex((row) => row.itemId === stale.itemId) / pageSize) + 1)
    }
    if (deepLinkFocus === 'profile:missing') {
      const unresolved =
        scopedMetadataRows.find((row) => !row.owner || !row.project || row.topicCategories.length === 0) ??
        scopedMetadataRows[0]
      if (!unresolved) return
      setSelectedItemId(unresolved.itemId)
      setCurrentPage(
        Math.floor(scopedMetadataRows.findIndex((row) => row.itemId === unresolved.itemId) / pageSize) + 1
      )
    }
  }, [deepLinkFocus, deepLinkRevision, minQualityScore, pageSize, scopedMetadataRows])

  const handleMetadataRowClick = (itemId: string) => {
    if (expandedItemId === itemId) {
      setExpandedItemId(null)
      setSelectedItemId(itemId)
      return
    }
    setExpandedItemId(itemId)
    setSelectedItemId(itemId)
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <CardTitle>オントロジーグラフ（ドキュメント単位）</CardTitle>
            <CardDescription>各ドキュメントを1単位として管理するオントロジーグラフ一覧</CardDescription>
          </div>
          <div className="flex items-center gap-2">
            <Checkbox
              id="ontology-unified-active-scope-only"
              checked={onlyActiveConnectScopes}
              onCheckedChange={(v) => onOnlyActiveConnectScopesChange(v === true)}
            />
            <label htmlFor="ontology-unified-active-scope-only" className="text-sm text-muted-foreground cursor-pointer max-w-[28rem]">
              アクティブな接続スコープのみ表示（ガバナンス検知一覧と同じ item 範囲）
            </label>
          </div>
        </div>
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
              <TableHead>リソース</TableHead>
              <TableHead>正規ドキュメント</TableHead>
              <TableHead>データソース</TableHead>
              <TableHead>AI利用可否</TableHead>
              <TableHead>ドキュメント単体スコア</TableHead>
              <TableHead>権威性</TableHead>
              <TableHead>intentタグ</TableHead>
              <TableHead>変換日時</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {pagedRows.map((row) => {
              const rowQualityScore = row.contentQualityScore
              return (
                <Fragment key={row.itemId}>
                  <TableRow
                    className={cn(
                      'cursor-pointer',
                      selectedItemId === row.itemId && 'bg-primary/5',
                      expandedItemId === row.itemId && 'bg-primary/10'
                    )}
                    role="button"
                    tabIndex={0}
                    onClick={() => void handleMetadataRowClick(row.itemId)}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter' || event.key === ' ') {
                        event.preventDefault()
                        void handleMetadataRowClick(row.itemId)
                      }
                    }}
                  >
                    <TableCell className="max-w-[360px]">
                      <div className="min-w-0">
                        <p className="truncate font-medium" title={row.documentName}>{row.documentName}</p>
                        <p className="truncate text-xs text-muted-foreground" title={row.filePath}>{row.filePath}</p>
                      </div>
                    </TableCell>
                    <TableCell>
                      {!row.canonicalDocId ? '-' : row.canonicalDocId === row.itemId ? '○' : row.canonicalDocId}
                    </TableCell>
                    <TableCell>
                      {row.source}
                    </TableCell>
                    <TableCell>{row.aiEligible ? '可' : '不可'}</TableCell>
                    <TableCell>
                      <Button
                        size="sm"
                        variant="outline"
                        className={qualityScoreButtonClass(rowQualityScore)}
                        onClick={(event) => {
                          event.stopPropagation()
                          setSelectedItemId(row.itemId)
                          setDetailDialog('quality')
                        }}
                      >
                        {rowQualityScore.toFixed(2)}
                      </Button>
                    </TableCell>
                    <TableCell>{row.authorityLevel ?? '-'}</TableCell>
                    <TableCell className="max-w-[220px] truncate">
                      {(row.intentTags ?? []).length > 0 ? (row.intentTags ?? []).join(', ') : '-'}
                    </TableCell>
                    <TableCell>{row.transformedAt}</TableCell>
                  </TableRow>
                  {expandedItemId === row.itemId && (
                    <TableRow className="bg-muted/20">
                      <TableCell colSpan={8} className="p-3">
                        <div className="mb-2 text-xs text-muted-foreground">
                          選択したドキュメント（{row.itemId}）に関連するノードのみを表示します。
                        </div>
                        <OntologyGraphView embedded fixedQueryLabel={row.itemId} />
                      </TableCell>
                    </TableRow>
                  )}
                </Fragment>
              )
            })}
          </TableBody>
        </Table>
        {selectedMetadata && (
          <Dialog
            open={detailDialog !== null}
            onOpenChange={(open) => {
              if (!open) setDetailDialog(null)
            }}
          >
            <DialogContent className="flex h-screen max-h-screen sm:max-w-4xl flex-col overflow-hidden">
              {detailDialog === 'quality' && (
                <>
                  <DialogHeader>
                    <DialogTitle>オントロジースコア詳細（ドキュメント単体）（{selectedMetadata.itemId}）</DialogTitle>
                    <DialogDescription>
                      {selectedMetadata.title} / テナント集計値ではなく、このドキュメント単体の指標を表示します。
                    </DialogDescription>
                  </DialogHeader>
                  <div className="min-h-0 flex flex-1 flex-col text-sm">
                    <div className="min-h-0 flex-1 space-y-2 overflow-y-auto pr-1">
                      <div className="rounded-md border p-3">
                        <div className="text-xs text-muted-foreground">ドキュメント単体スコア</div>
                        <div className="mt-1 flex items-center gap-2 font-medium">
                          <span>{selectedDocumentQualityScore.toFixed(2)}</span>
                          <Badge variant={qualityLabel(selectedDocumentQualityScore).variant}>
                            {qualityLabel(selectedDocumentQualityScore).label}
                          </Badge>
                        </div>
                        <div className="mt-1 text-xs text-muted-foreground">
                          ContentQualityScore（freshness_score × uniqueness_score × relevance_score）
                        </div>
                      </div>
                      <div className="grid gap-2 md:grid-cols-3">
                        <div className="rounded-md border p-2">
                          <p className="text-xs text-muted-foreground">鮮度スコア（freshness_score）</p>
                          <p className="font-medium">{Number(selectedMetadata.freshnessFit ?? 0).toFixed(2)}</p>
                        </div>
                        <div className="rounded-md border p-2">
                          <p className="text-xs text-muted-foreground">一意性スコア（uniqueness_score）</p>
                          <p className="font-medium">{Number(selectedMetadata.canonicalityDuplication ?? 0).toFixed(2)}</p>
                        </div>
                        <div className="rounded-md border p-2">
                          <p className="text-xs text-muted-foreground">関連度スコア（relevance_score）</p>
                          <p className="font-medium">{Number(selectedMetadata.intentCoverage ?? 0).toFixed(2)}</p>
                        </div>
                      </div>
                      <div className="grid gap-2 md:grid-cols-2">
                        <div className="rounded-md border p-2">
                          <p className="text-xs text-muted-foreground">所在/所有者スコア（location_score）</p>
                          <p className="font-medium">{Number(selectedMetadata.stewardshipFindability ?? 0).toFixed(2)}</p>
                        </div>
                        <div className="rounded-md border p-2">
                          <p className="text-xs text-muted-foreground">ユースケース適合度（use_case_fit_scores.internal_qa）</p>
                          <p className="font-medium">{selectedUseCaseFitScore.toFixed(2)}</p>
                        </div>
                      </div>
                      <div className="rounded-md border p-3">
                        <div className="text-xs text-muted-foreground mb-2">プロファイル/根拠情報</div>
                        <div className="grid gap-1 text-xs">
                          <div><span className="text-muted-foreground">所有者:</span> {selectedMetadata.owner || '-'}</div>
                          <div><span className="text-muted-foreground">プロジェクト:</span> {selectedMetadata.project || '-'}</div>
                          <div><span className="text-muted-foreground">トピック:</span> {selectedMetadata.topicCategories.join(', ') || '-'}</div>
                          <div>
                            <span className="text-muted-foreground">カテゴリ（large/medium/small）:</span>{' '}
                            {selectedMetadata.categoryHierarchy
                              ? `${selectedMetadata.categoryHierarchy.large || '-'} / ${selectedMetadata.categoryHierarchy.medium || '-'} / ${selectedMetadata.categoryHierarchy.small || '-'}`
                              : '-'}
                          </div>
                          <div><span className="text-muted-foreground">正規ドキュメント:</span> {selectedMetadata.canonicalDocId || '-'}</div>
                          <div><span className="text-muted-foreground">権威性（authority_level）:</span> {selectedMetadata.authorityLevel || '-'}</div>
                          <div><span className="text-muted-foreground">intentタグ（intent_tags）:</span> {(selectedMetadata.intentTags ?? []).join(', ') || '-'}</div>
                          <div className="pt-2 border-t border-border/60 mt-2 space-y-1">
                            <div className="text-[11px] font-medium text-muted-foreground">プロファイル推論の根拠（DynamoDB）</div>
                            <div>
                              <span className="text-muted-foreground">LLM使用:</span>{' '}
                              {selectedMetadata.profileLlmUsed === undefined
                                ? '不明（古い行または未設定）'
                                : selectedMetadata.profileLlmUsed
                                  ? 'はい'
                                  : 'いいえ'}
                            </div>
                            <div>
                              <span className="text-muted-foreground">推論フォールバック:</span>{' '}
                              {selectedMetadata.profileInferenceFallback === undefined
                                ? '不明'
                                : selectedMetadata.profileInferenceFallback
                                  ? 'はい（Bedrock 未応答・設定要確認など）'
                                  : 'いいえ'}
                            </div>
                            <div>
                              <span className="text-muted-foreground">更新ソース:</span>{' '}
                              {selectedMetadata.profileInferenceSource === undefined
                                ? '不明'
                                : selectedMetadata.profileInferenceSource === 'ingest'
                                  ? '取り込み（schema transform）'
                                  : selectedMetadata.profileInferenceSource === 'ai_fill'
                                    ? 'AIで補完'
                                    : selectedMetadata.profileInferenceSource === 'manual_update'
                                      ? '手動編集'
                                      : selectedMetadata.profileInferenceSource}
                            </div>
                          </div>
                        </div>
                      </div>
                      <div className="rounded-md border p-2">
                        推奨アクション: {qualityAction(selectedDocumentQualityScore)}
                      </div>
                    </div>
                    <div className="mt-3 flex shrink-0 justify-end gap-2">
                      <label className="mr-auto inline-flex items-center gap-2 text-xs text-muted-foreground">
                        <input
                          type="checkbox"
                          checked={useLlmOnAiFill}
                          onChange={(event) => setUseLlmOnAiFill(event.target.checked)}
                          disabled={isSaving}
                        />
                        LLM提案を使用（要約・ディレクトリ構造・ファイル名を考慮）
                      </label>
                      <Button size="sm" variant="outline" disabled={isSaving} onClick={() => void handleAiFill()}>
                        {isSaving ? '処理中...' : 'AIで補完'}
                      </Button>
                      <Button size="sm" onClick={openEditDialog}>
                        編集
                      </Button>
                    </div>
                  </div>
                </>
              )}
              {detailDialog === 'edit' && (
                <>
                  <DialogHeader>
                    <DialogTitle>プロファイル編集（{selectedMetadata.itemId}）</DialogTitle>
                    <DialogDescription>{selectedMetadata.title}</DialogDescription>
                  </DialogHeader>
                  <div className="min-h-0 flex flex-1 flex-col text-sm">
                    <div className="min-h-0 flex-1 space-y-3 overflow-y-auto pr-1">
                      <div className="space-y-1">
                        <label className="text-xs text-muted-foreground">所有者</label>
                        <input className="w-full rounded border px-2 py-1 text-sm" value={editOwner} onChange={(e) => setEditOwner(e.target.value)} />
                      </div>
                      <div className="space-y-1">
                        <label className="text-xs text-muted-foreground">プロジェクト</label>
                        <input className="w-full rounded border px-2 py-1 text-sm" value={editProject} onChange={(e) => setEditProject(e.target.value)} />
                      </div>
                      <div className="space-y-1">
                        <label className="text-xs text-muted-foreground">トピック（カンマ区切り）</label>
                        <input className="w-full rounded border px-2 py-1 text-sm" value={editTopics} onChange={(e) => setEditTopics(e.target.value)} />
                      </div>
                      <div className="space-y-1">
                        <label className="text-xs text-muted-foreground">正規ドキュメント</label>
                        <input className="w-full rounded border px-2 py-1 text-sm" value={editCanonical} onChange={(e) => setEditCanonical(e.target.value)} />
                      </div>
                    </div>
                    <div className="mt-3 flex shrink-0 justify-end gap-2">
                      <Button size="sm" variant="outline" onClick={() => setDetailDialog('quality')}>
                        キャンセル
                      </Button>
                      <Button size="sm" disabled={isSaving} onClick={() => void handleSaveEdit()}>
                        {isSaving ? '保存中...' : '保存して品質再計算'}
                      </Button>
                    </div>
                  </div>
                </>
              )}
            </DialogContent>
          </Dialog>
        )}
      </CardContent>
    </Card>
  )
}

const EntityCandidatesPage = ({
  onActionComplete,
  deepLinkFocus,
  metadataRows
}: {
  onActionComplete: (message: string) => void
  deepLinkFocus?: string
  metadataRows: UnifiedMetadataRecord[]
}) => (
  <EntityCandidatesPageInner
    onActionComplete={onActionComplete}
    deepLinkFocus={deepLinkFocus}
    metadataRows={metadataRows}
  />
)

const EntityCandidatesPageInner = ({
  onActionComplete,
  deepLinkFocus,
  metadataRows
}: {
  onActionComplete: (message: string) => void
  deepLinkFocus?: string
  metadataRows: UnifiedMetadataRecord[]
}) => {
  const highlightedRowRef = useRef<HTMLTableRowElement | null>(null)
  const [flashActive, setFlashActive] = useState(false)
  const [loading, setLoading] = useState(false)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [rows, setRows] = useState<OntologyEntityCandidateRow[]>([])
  const [selectedEntityByCandidate, setSelectedEntityByCandidate] = useState<Record<string, string>>({})
  const [actioningCandidateId, setActioningCandidateId] = useState<string | null>(null)
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize, setPageSize] = useState(25)
  const metadataByItemId = useMemo(() => {
    return new Map(
      metadataRows.map((row) => [
        row.itemId,
        { documentName: row.documentName, filePath: row.filePath }
      ])
    )
  }, [metadataRows])
  const minConfidence = rows.length > 0 ? Math.min(...rows.map((item) => item.confidence)) : 1
  const totalRows = rows.length
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize))
  const pagedRows = useMemo(() => paginateRows(rows, currentPage, pageSize), [rows, currentPage, pageSize])

  const reloadCandidates = useCallback(async () => {
    setLoading(true)
    setLoadError(null)
    try {
      const response = await getOntologyEntityCandidates(200, 0, 'pending')
      setRows(response.rows)
      setSelectedEntityByCandidate((prev) => {
        const next: Record<string, string> = {}
        response.rows.forEach((row) => {
          const existing = prev[row.candidate_id]
          const defaultEntityId = row.suggestions[0]?.entity_id
          if (existing && row.suggestions.some((s) => s.entity_id === existing)) {
            next[row.candidate_id] = existing
            return
          }
          if (defaultEntityId) next[row.candidate_id] = defaultEntityId
        })
        return next
      })
    } catch (error) {
      setLoadError(toUiErrorMessage(error, '候補取得に失敗しました'))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void reloadCandidates()
  }, [reloadCandidates])
  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(totalPages)
  }, [currentPage, totalPages])
  useEffect(() => {
    setCurrentPage(1)
  }, [pageSize])
  useEffect(() => {
    if (deepLinkFocus === 'candidate:lowest_confidence') {
      const targetIndex = rows.findIndex((row) => row.confidence === minConfidence)
      if (targetIndex >= 0) setCurrentPage(Math.floor(targetIndex / pageSize) + 1)
      highlightedRowRef.current?.scrollIntoView({ behavior: 'smooth', block: 'center' })
      setFlashActive(true)
      const timer = window.setTimeout(() => setFlashActive(false), 1600)
      return () => window.clearTimeout(timer)
    }
  }, [deepLinkFocus, minConfidence, pageSize, rows])

  const resolveToExisting = async (candidate: OntologyEntityCandidateRow) => {
    const selectedEntityId = selectedEntityByCandidate[candidate.candidate_id] ?? candidate.suggestions[0]?.entity_id
    if (!selectedEntityId) return
    setActioningCandidateId(candidate.candidate_id)
    try {
      const result = await resolveOntologyEntityCandidateWithExisting(candidate.candidate_id, selectedEntityId)
      onActionComplete(
        `${candidate.surface_form} を正規名称「${result.canonical_name}」へ寄せて登録しました。`
      )
      await reloadCandidates()
    } catch (error) {
      setLoadError(toUiErrorMessage(error, '候補確定に失敗しました'))
    } finally {
      setActioningCandidateId(null)
    }
  }

  const registerAsNew = async (candidate: OntologyEntityCandidateRow) => {
    setActioningCandidateId(candidate.candidate_id)
    try {
      const result = await registerOntologyEntityCandidateAsNew(candidate.candidate_id)
      onActionComplete(
        `${candidate.surface_form} を新規エンティティ（正規名称: ${result.canonical_name}）として登録しました。`
      )
      await reloadCandidates()
    } catch (error) {
      setLoadError(toUiErrorMessage(error, '新規登録に失敗しました'))
    } finally {
      setActioningCandidateId(null)
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>エンティティ解決（辞書未登録）</CardTitle>
        <CardDescription>文書から抽出された未登録ワードと、辞書への統合候補の確認一覧</CardDescription>
      </CardHeader>
      <CardContent>
        {loading && <p className="mb-3 text-sm text-muted-foreground">候補エンティティを取得中です...</p>}
        {loadError && <p className="mb-3 text-sm text-rose-600">{loadError}</p>}
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
              <TableHead>抽出語ID</TableHead>
              <TableHead>抽出ワード</TableHead>
              <TableHead>種別</TableHead>
              <TableHead>抽出元</TableHead>
              <TableHead>信頼度</TableHead>
              <TableHead>機微情報</TableHead>
              <TableHead>リソース</TableHead>
              <TableHead>受信日時</TableHead>
              <TableHead>候補エンティティ（辞書）</TableHead>
              <TableHead>操作</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {pagedRows.map((row) => {
              const shouldHighlight =
              deepLinkFocus === 'candidate:lowest_confidence' && row.confidence === minConfidence
              const selectedEntityId = selectedEntityByCandidate[row.candidate_id]
              const isActioning = actioningCandidateId === row.candidate_id
              const metadata = metadataByItemId.get(row.item_id)
              const documentName = metadata?.documentName ?? row.item_id
              const filePath = metadata?.filePath ?? row.item_id
              return (
                <TableRow
                  key={row.candidate_id}
                  className={cn(shouldHighlight && 'bg-primary/5', shouldHighlight && flashActive && 'animate-pulse')}
                  ref={shouldHighlight ? highlightedRowRef : null}
                >
                  <TableCell>{row.candidate_id}</TableCell>
                  <TableCell>{row.surface_form}</TableCell>
                  <TableCell>{entityTypeLabel(row.entity_type)}</TableCell>
                  <TableCell>{row.extraction_source}</TableCell>
                  <TableCell>{row.confidence.toFixed(2)}</TableCell>
                  <TableCell>{row.pii_flag ? 'あり' : 'なし'}</TableCell>
                  <TableCell className="max-w-[360px]">
                    <div className="min-w-0">
                      <p className="truncate font-medium" title={documentName}>{documentName}</p>
                      <p className="truncate text-xs text-muted-foreground" title={filePath}>{filePath}</p>
                    </div>
                  </TableCell>
                  <TableCell>{row.received_at}</TableCell>
                  <TableCell className="min-w-[280px]">
                    {row.suggestions.length === 0 ? (
                      <span className="text-sm text-muted-foreground">候補なし（新規登録対象）</span>
                    ) : (
                      <select
                        className="h-8 w-full rounded-md border bg-background px-2 text-sm"
                        value={selectedEntityId ?? ''}
                        onChange={(event) =>
                          setSelectedEntityByCandidate((prev) => ({
                            ...prev,
                            [row.candidate_id]: event.target.value
                          }))
                        }
                      >
                        {row.suggestions.map((suggestion) => (
                          <option key={suggestion.entity_id} value={suggestion.entity_id}>
                            {suggestion.canonical_name}（一致度 {Math.round(suggestion.score * 100)}%）
                          </option>
                        ))}
                      </select>
                    )}
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-2">
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={isActioning || row.suggestions.length === 0}
                        onClick={() => void resolveToExisting(row)}
                      >
                        既存登録
                      </Button>
                      <Button
                        size="sm"
                        disabled={isActioning}
                        onClick={() => void registerAsNew(row)}
                      >
                        新規登録
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              )})}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}

const EntityMasterPage = ({
  entityRows,
  totalRowsFromApi
}: {
  entityRows: EntityMasterRecord[]
  totalRowsFromApi: number
}) => {
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize, setPageSize] = useState(25)
  const [selectedMergedEntityKey, setSelectedMergedEntityKey] = useState<string | null>(null)
  const aggregatedRows = useMemo(() => {
    type AggregatedRow = {
      aggregateKey: string
      representativeId: string
      canonicalValue: string
      entityType: string
      piiFlag: boolean
      spreadFactor: number
      status: 'active' | 'merged' | 'archived'
      updatedAt: string
      mergedEntities: Array<{
        entityId: string
        status: 'active' | 'merged' | 'archived'
        updatedAt: string
      }>
    }

    const groups = new Map<string, typeof entityRows>()
    for (const row of entityRows) {
      const key = `${row.entityType}::${row.canonicalValue.trim().toLowerCase()}`
      const items = groups.get(key) ?? []
      items.push(row)
      groups.set(key, items)
    }

    const rankByStatus = (status: 'active' | 'merged' | 'archived') => {
      if (status === 'active') return 3
      if (status === 'merged') return 2
      return 1
    }

    const resolved: AggregatedRow[] = []
    for (const [aggregateKey, rows] of groups.entries()) {
      const sorted = [...rows].sort((a, b) => {
        const rankDiff = rankByStatus(b.status) - rankByStatus(a.status)
        if (rankDiff !== 0) return rankDiff
        return b.updatedAt.localeCompare(a.updatedAt)
      })
      const representative = sorted[0]
      const updatedAt = [...rows]
        .map((row) => row.updatedAt)
        .sort((a, b) => b.localeCompare(a))[0]
      const status =
        rows.some((row) => row.status === 'active')
          ? 'active'
          : rows.some((row) => row.status === 'merged')
            ? 'merged'
            : 'archived'
      resolved.push({
        aggregateKey,
        representativeId: representative.entityId,
        canonicalValue: representative.canonicalValue,
        entityType: representative.entityType,
        piiFlag: rows.some((row) => row.piiFlag),
        spreadFactor: Math.max(...rows.map((row) => row.spreadFactor)),
        status,
        updatedAt,
        mergedEntities: [...rows]
          .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt))
          .map((row) => ({ entityId: row.entityId, status: row.status, updatedAt: row.updatedAt }))
      })
    }
    return resolved
  }, [entityRows])
  const sortedRows = useMemo(
    () =>
      [...aggregatedRows].sort((a, b) => {
        if (b.spreadFactor !== a.spreadFactor) return b.spreadFactor - a.spreadFactor
        return b.updatedAt.localeCompare(a.updatedAt)
      }),
    [aggregatedRows]
  )
  const totalRows = totalRowsFromApi > 0 ? totalRowsFromApi : sortedRows.length
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize))
  const pagedRows = useMemo(() => paginateRows(sortedRows, currentPage, pageSize), [sortedRows, currentPage, pageSize])
  const selectedMergedEntityRow = useMemo(
    () => sortedRows.find((row) => row.aggregateKey === selectedMergedEntityKey) ?? null,
    [selectedMergedEntityKey, sortedRows]
  )
  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(totalPages)
  }, [currentPage, totalPages])
  useEffect(() => {
    setCurrentPage(1)
  }, [pageSize])

  return (
    <Card>
      <CardHeader>
        <CardTitle>エンティティマスタ</CardTitle>
        <CardDescription>互換モード時のみ利用可能な noun 解決ビュー</CardDescription>
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
              <TableHead>エンティティID</TableHead>
              <TableHead>正規名称</TableHead>
              <TableHead>種別</TableHead>
              <TableHead>機微情報</TableHead>
              <TableHead>
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <span className="cursor-help underline decoration-dotted underline-offset-2">
                        拡散係数
                      </span>
                    </TooltipTrigger>
                    <TooltipContent side="top" className="max-w-[360px]">
                      <div className="space-y-1 text-xs leading-relaxed">
                        <p>
                          定義: 同一エンティティが文書・プロジェクト・組織をまたいで参照される広がりの指標です。
                        </p>
                        <p>使い方: 値が高いほど影響範囲が広いため、変更前レビューを優先します。</p>
                        <p>閾値目安: 1-2 ローカル / 3-5 中程度 / 6以上 重点管理。</p>
                      </div>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              </TableHead>
              <TableHead>状態</TableHead>
              <TableHead>更新日時</TableHead>
              <TableHead>統合エンティティ</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {pagedRows.map((row) => (
              <TableRow key={row.aggregateKey}>
                <TableCell>{row.representativeId}</TableCell>
                <TableCell>{row.canonicalValue}</TableCell>
                <TableCell>{entityTypeLabel(row.entityType)}</TableCell>
                <TableCell>{row.piiFlag ? 'あり' : 'なし'}</TableCell>
                <TableCell>{row.spreadFactor}</TableCell>
                <TableCell>
                  <Badge variant={badgeVariantByValue(row.status)}>{statusLabel(row.status)}</Badge>
                </TableCell>
                <TableCell>{row.updatedAt}</TableCell>
                <TableCell>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => setSelectedMergedEntityKey(row.aggregateKey)}
                  >
                    {row.mergedEntities.length}件を表示
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        <Dialog
          open={selectedMergedEntityRow !== null}
          onOpenChange={(open) => {
            if (!open) setSelectedMergedEntityKey(null)
          }}
        >
          <DialogContent className="sm:max-w-3xl">
            <DialogHeader>
              <DialogTitle>統合されたエンティティ一覧</DialogTitle>
              <DialogDescription>
                {selectedMergedEntityRow
                  ? `${selectedMergedEntityRow.canonicalValue}（${selectedMergedEntityRow.entityType}）に統合されたエンティティ`
                  : '統合エンティティ'}
              </DialogDescription>
            </DialogHeader>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>エンティティID</TableHead>
                  <TableHead>状態</TableHead>
                  <TableHead>更新日時</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(selectedMergedEntityRow?.mergedEntities ?? []).map((merged) => (
                  <TableRow key={merged.entityId}>
                    <TableCell>{merged.entityId}</TableCell>
                    <TableCell>
                      <Badge variant={badgeVariantByValue(merged.status)}>{statusLabel(merged.status)}</Badge>
                    </TableCell>
                    <TableCell>{merged.updatedAt}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </DialogContent>
        </Dialog>
      </CardContent>
    </Card>
  )
}

const AuditPage = ({ rows }: { rows: OntologyAuditRecord[] }) => (
  <AuditWorkbench
    title="オントロジー監査ログ 横断検索"
    description="オントロジーの証跡を横断検索し、CSV/PDFでエクスポートできます。"
    rows={rows.map((row) => ({
      auditId: row.auditId,
      operator: row.operator,
      action: row.action,
      target: `${row.targetType}:${row.targetId}`,
      timestamp: row.timestamp,
      correlationId: row.correlationId
    }))}
    columns={[
      { key: 'auditId', label: '監査ID' },
      { key: 'operator', label: '実行者' },
      { key: 'action', label: '操作内容' },
      { key: 'target', label: '対象' },
      { key: 'timestamp', label: '操作日時' },
      { key: 'correlationId', label: '相関ID' }
    ]}
    searchKeys={['auditId', 'operator', 'action', 'target', 'timestamp', 'correlationId']}
    queryParamKey="ontologyAuditQ"
  />
)

const OntologyOperations = () => {
  const [page, setPage] = useState<OntologyPageKey>('overview')
  const [entityMasterTab, setEntityMasterTab] = useState<'dictionary' | 'candidates'>('dictionary')
  const [candidateActionNotice, setCandidateActionNotice] = useState<string | null>(null)
  const [deepLinkFocus, setDeepLinkFocus] = useState<string | undefined>()
  const [deepLinkRevision, setDeepLinkRevision] = useState(0)
  const [overview, setOverview] = useState<OntologyOverviewStats>({
    aiProposedCount: 0,
    approvedCount: 0,
    executedCount: 0,
    exceptionResolved24h: 0,
    unifiedMetadataTotal: 0,
    activeCount: 0,
    agingCount: 0,
    staleCount: 0,
    unresolvedCandidates: 0,
    entityResolverSuccessRate24h: 0,
    lastReconcileRunAt: '-',
    projectedDocuments: 0,
    containedInLinks: 0,
    mentionsLinks: 0,
    similarityLinks: 0,
    skippedSimilarityDocs: 0,
    textFallbackVectors: 0,
    autoPromotedEntities: 0,
    projectionPreset: 'standard',
    projectionPresetSource: 'default',
    lastProjectionRefreshAt: '-',
    documentAnalysisTotal: 0,
    documentAnalysisMatchedUnified: 0,
    documentAnalysisCoverageRatio: 0,
    documentAnalysisUnmatched: 0,
    documentAnalysisTargetMode: 'eligible_only',
    documentAnalysisQueryError: '',
    ontologyScore: 0,
    baseOntologyScore: 0,
    useCaseReadiness: 0,
    freshnessValidity: 0,
    canonicalityDuplication: 0,
    stewardshipFindability: 0,
    intentCoverage: 0,
    evidenceCoverage: 0,
    freshnessFit: 0,
    benchmarkLite: 0,
    intentBreakdown: [],
    pillarDocumentCounts: undefined,
    ontologyScoreMode: 'count_based'
  })
  const [metadataRows, setMetadataRows] = useState<UnifiedMetadataRecord[]>([])
  const [metadataTotalRows, setMetadataTotalRows] = useState(0)
  const [entityRows, setEntityRows] = useState<EntityMasterRecord[]>([])
  const [entityMasterTotalRows, setEntityMasterTotalRows] = useState(0)
  const [auditRows, setAuditRows] = useState<OntologyAuditRecord[]>([])
  const [isRefreshingGraphProjection, setIsRefreshingGraphProjection] = useState(false)
  const [nounResolutionEnabled, setNounResolutionEnabled] = useState(false)
  const [unifiedScopeActiveOnly, setUnifiedScopeActiveOnly] = useState(true)

  const handlePostScoringProjectionRefresh = useCallback(async () => {
    try {
      const refreshResult = await refreshOntologyGraphProjection(true, 2000)
      useGraphStore.getState().setGraphDataFetchAttempted(false)
      toast.success(
        `スコアリング完了後にオントロジー投影を更新しました（documents=${refreshResult.projected_documents}）`
      )
    } catch (error) {
      const message = error instanceof Error ? error.message : '投影更新に失敗しました'
      toast.error(`スコアリング完了後の投影更新に失敗しました: ${message}`)
    }
  }, [])

  const scoringGate = useGovernanceBatchScoringGate({
    pollWhileRunning: true,
    onScanCompleted: handlePostScoringProjectionRefresh
  })
  const reloadScoringGate = scoringGate.reload
  const isPreScanLocked = !scoringGate.hasCompletedScan

  useEffect(() => {
    const deepLink = takeOperationDeepLinkForTab('ontology-operations')
    if (!deepLink?.page) return
    const nextPage = deepLink.page as OntologyPageKey
    if (nextPage === 'entity-candidates') {
      setPage('entity-master')
      setEntityMasterTab('candidates')
    } else if (nextPage === 'entity-master') {
      setPage('entity-master')
      setEntityMasterTab('dictionary')
    } else if (ONTOLOGY_PAGES.some((item) => item.key === nextPage)) {
      setPage(nextPage)
    }
    setDeepLinkFocus(deepLink.focus)
  }, [])

  useEffect(() => {
    if (!isPreScanLocked) return
    if (page === 'help' || page === 'audit') return
    if (!ONTOLOGY_CONTENT_LOCKED_WHEN_NO_SCAN.includes(page)) return
    setPage('help')
  }, [isPreScanLocked, page])

  const loadOntologyM3Data = useCallback(async () => {
    try {
      const [overviewResponse, metadataResponse, entityResponse, auditResponse] = await Promise.all([
        getOntologyOverview(),
        fetchAllOntologyUnifiedMetadata(unifiedScopeActiveOnly),
        fetchAllOntologyEntityMaster(),
        getOntologyAuditLogs(300, 0)
      ])
      const mappedMetadataRows = metadataResponse.rows.map(toUnifiedMetadataFromApi)
      setMetadataRows(mappedMetadataRows)
      setMetadataTotalRows(metadataResponse.totalCount)
      const nextOverview = toOverviewStatsFromApi(overviewResponse, mappedMetadataRows)
      setNounResolutionEnabled(Boolean(overviewResponse.noun_resolution_enabled))
      setOverview(nextOverview)
      setEntityRows(entityResponse.rows.map(toEntityMasterFromApi))
      setEntityMasterTotalRows(entityResponse.totalCount)
      setAuditRows(auditResponse.rows.map(toOntologyAuditFromApi))
      void reloadScoringGate()
    } catch (error) {
      const message =
        error instanceof Error ? error.message : 'オントロジーデータの取得に失敗しました'
      toast.error(message)
      setMetadataRows([])
      setMetadataTotalRows(0)
      setEntityRows([])
      setEntityMasterTotalRows(0)
      setAuditRows([])
    }
  }, [reloadScoringGate, unifiedScopeActiveOnly])

  useEffect(() => {
    void loadOntologyM3Data()
  }, [loadOntologyM3Data])

  const unresolvedDocumentProfileCount = useMemo(
    () =>
      metadataRows.filter(
        (item) =>
          !hasMeaningfulOwner(item.owner) ||
          !hasMeaningfulProject(item.project) ||
          !hasMeaningfulTopicCategories(item.topicCategories)
      ).length,
    [metadataRows]
  )
  const topNavPages = useMemo(
    () => ONTOLOGY_TOP_NAV_PAGES.filter((item) => item.key !== 'entity-master' || nounResolutionEnabled),
    [nounResolutionEnabled]
  )

  const handleNavigate = (targetPage: OntologyPageKey, focus?: string) => {
    setPage(targetPage)
    setDeepLinkFocus(focus)
    setDeepLinkRevision((value) => value + 1)
    if (targetPage === 'entity-candidates') {
      setPage('entity-master')
      setEntityMasterTab('candidates')
    } else if (targetPage === 'entity-master') {
      setEntityMasterTab('dictionary')
    }
  }

  const handleRefreshOntologyGraphFromHeader = useCallback(async (options?: { silent?: boolean }) => {
    if (isRefreshingGraphProjection) return
    setIsRefreshingGraphProjection(true)
    try {
      const refreshResult = await refreshOntologyGraphProjection(true, 2000)
      const graphState = useGraphStore.getState()
      graphState.setLabelsFetchAttempted(false)
      graphState.setGraphDataFetchAttempted(false)
      graphState.setLastSuccessfulQueryLabel('')
      graphState.incrementGraphDataVersion()
      setPage('unified-metadata')
      const settingsState = useSettingsStore.getState()
      settingsState.setQueryLabel('*')
      graphState.setGraphDataFetchAttempted(false)
      if (!options?.silent) {
        toast.success(
          `オントロジーグラフを更新しました（documents=${refreshResult.projected_documents}）`
        )
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'オントロジーグラフ更新に失敗しました'
      toast.error(message)
    } finally {
      setIsRefreshingGraphProjection(false)
    }
  }, [isRefreshingGraphProjection])

  useEffect(() => {
    if (isPreScanLocked || page !== 'unified-metadata') return
    const graphState = useGraphStore.getState()
    const currentLabel = useSettingsStore.getState().queryLabel
    if (currentLabel !== '*') {
      useSettingsStore.getState().setQueryLabel('*')
    }
    // Always re-trigger fetch when the graph tab becomes active.
    graphState.setGraphDataFetchAttempted(false)
    graphState.setLastSuccessfulQueryLabel('')
  }, [isPreScanLocked, page])

  return (
    <div className="h-full overflow-y-auto p-4">
      <div className="mx-auto w-full max-w-[1400px] space-y-4 pb-6">
        <Card className="border-primary/20 bg-gradient-to-r from-primary/10 via-background to-background shadow-md">
          <CardContent className="flex flex-wrap items-center justify-between gap-3 p-4">
            <div>
              <h1 className="text-xl font-semibold">オントロジー</h1>
              <p className="text-sm text-muted-foreground">
                データの意味づけと品質管理を一元管理します。
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => void scoringGate.runDailyScan()}
                disabled={scoringGate.isScanInProgress}
              >
                {scoringGate.isScanInProgress ? 'スコアリング実行中...' : 'スコアリングを実行'}
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={isRefreshingGraphProjection}
                onClick={() => {
                  void handleRefreshOntologyGraphFromHeader()
                }}
              >
                <RefreshCw className={`mr-1 h-4 w-4 ${isRefreshingGraphProjection ? 'animate-spin' : ''}`} />
                {isRefreshingGraphProjection ? 'グラフ更新中...' : 'オントロジーグラフを更新'}
              </Button>
              <Button
                size="sm"
                onClick={() => {
                  setPage('entity-master')
                  setEntityMasterTab('candidates')
                  setDeepLinkFocus('candidate:lowest_confidence')
                  setDeepLinkRevision((value) => value + 1)
                }}
                disabled={isPreScanLocked}
              >
                辞書未登録ワードを確認
              </Button>
            </div>
          </CardContent>
        </Card>

        <div className="liquid-glass-surface rounded-xl border bg-card/35 p-2 shadow-sm">
          <TooltipProvider>
            <div className="flex flex-wrap gap-2">
              {topNavPages.map((item) => {
                const isLocked = isOntologyTabLockedWhileNoScan(item.key, isPreScanLocked, nounResolutionEnabled)
                return (
                  <Tooltip key={item.key}>
                    <TooltipTrigger asChild>
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
                          page === item.key ? 'liquid-glass-tab-active shadow-sm' : 'text-foreground'
                        )}
                      >
                        {item.label}
                      </button>
                    </TooltipTrigger>
                    <TooltipContent side="bottom">
                      {isLocked ? '初回スコアリング完了後に表示できます。' : ONTOLOGY_TAB_HELP[item.key]}
                    </TooltipContent>
                  </Tooltip>
                )
              })}
            </div>
          </TooltipProvider>
        </div>

        {isPreScanLocked && (
          <>
            <InitialScoringGateCard
              description={ONTOLOGY_INITIAL_SCORING_GATE_DESCRIPTION}
              latestBatchScanJob={scoringGate.latestBatchScanJob}
              onRunScoring={scoringGate.runDailyScan}
              isScanInProgress={scoringGate.isScanInProgress}
            />
            {scoringGate.isScanInProgress && (
              <Card className="border-sky-300 bg-sky-50/40">
                <CardContent className="flex items-center justify-between gap-3 p-3">
                  <div className="text-sm text-sky-900">
                    スコアリングを実行中です。ジョブ状態: {scoringGate.latestBatchScanJob?.status ?? 'running'}
                  </div>
                  <Badge variant="secondary" className="border-sky-300 bg-sky-100 text-sky-900">
                    処理中
                  </Badge>
                </CardContent>
              </Card>
            )}
          </>
        )}

        {page === 'overview' && !isPreScanLocked && (
          <div className="space-y-4">
            <OverviewPage
              onNavigate={handleNavigate}
              overview={overview}
              unresolvedDocumentProfileCount={unresolvedDocumentProfileCount}
            />
          </div>
        )}
        {page === 'unified-metadata' && !isPreScanLocked && (
          <UnifiedMetadataPage
            deepLinkFocus={deepLinkFocus}
            deepLinkRevision={deepLinkRevision}
            metadataRows={metadataRows}
            totalRowsFromApi={metadataTotalRows}
            onReloadData={async () => { await loadOntologyM3Data() }}
            onlyActiveConnectScopes={unifiedScopeActiveOnly}
            onOnlyActiveConnectScopesChange={(next) => setUnifiedScopeActiveOnly(next)}
          />
        )}
        {(nounResolutionEnabled &&
          !isPreScanLocked &&
          (page === 'entity-master' || page === 'entity-candidates')) && (
          <div className="space-y-3">
            <div className="flex items-center gap-2">
              <Button
                size="sm"
                variant={entityMasterTab === 'dictionary' ? 'default' : 'outline'}
                onClick={() => setEntityMasterTab('dictionary')}
              >
                エンティティマスタ
              </Button>
              <Button
                size="sm"
                variant={entityMasterTab === 'candidates' ? 'default' : 'outline'}
                onClick={() => setEntityMasterTab('candidates')}
              >
                エンティティ解決
              </Button>
            </div>
            {candidateActionNotice && (
              <Card className="border-primary/30 bg-primary/5">
                <CardContent className="p-4 text-sm">
                  <span className="font-medium">候補を確定しました:</span>{' '}
                  {candidateActionNotice}
                </CardContent>
              </Card>
            )}
            {entityMasterTab === 'dictionary' ? (
              <EntityMasterPage entityRows={entityRows} totalRowsFromApi={entityMasterTotalRows} />
            ) : (
              <EntityCandidatesPage
                deepLinkFocus={deepLinkFocus}
                metadataRows={metadataRows}
                onActionComplete={(message) => setCandidateActionNotice(message)}
              />
            )}
          </div>
        )}
        {page === 'audit' && <AuditPage rows={auditRows} />}
        {page === 'help' && (
          <FeatureOnboardingPanel
            title="オントロジーガイド"
            purpose="データの意味統一と品質維持を継続的に実施するための画面です。"
            currentPageLabel={ONTOLOGY_PAGES.find((item) => item.key === page)?.label ?? 'ヘルプ'}
            currentPageDescription={ONTOLOGY_PAGE_GUIDE[page]}
            steps={[
              '「概要」で品質と鮮度の全体状況を確認する。',
              '「オントロジーグラフ」「エンティティ管理」で不整合や辞書未登録ワードを特定する。',
              '「監査ログ」で処理結果と変更履歴を確認する。'
            ]}
            glossary={ONTOLOGY_GLOSSARY}
          />
        )}
      </div>
    </div>
  )
}

export default OntologyOperations
