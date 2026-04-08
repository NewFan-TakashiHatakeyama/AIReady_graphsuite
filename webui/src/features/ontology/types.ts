export type OntologyPageKey =
  | 'overview'
  | 'relationship-graph'
  | 'unified-metadata'
  | 'entity-candidates'
  | 'entity-master'
  | 'lineage'
  | 'reconcile-jobs'
  | 'audit'
  | 'help'

export type RemediationState = 'ai_proposed' | 'pending_approval' | 'approved' | 'executed'

/** GET /ontology/overview の pillar_document_counts に対応（件数ベースの情報整備指標） */
export interface OntologyPillarDocumentCounts {
  denominator: number
  freshness: {
    byStatus: { active: number; aging: number; stale: number; other: number }
    staleOrAging: number
  }
  duplication: {
    inDuplicateGroup: number
    nonCanonicalDuplicateCopy: number
    canonicalOrNoDuplicateGroup: number
  }
  stewardship: {
    meaningfulOwner: number
    meaningfulProject: number
    meaningfulTopicCategories: number
    allThree: number
  }
}

export interface OntologyOverviewStats {
  aiProposedCount: number
  approvedCount: number
  executedCount: number
  exceptionResolved24h: number
  unifiedMetadataTotal: number
  activeCount: number
  agingCount: number
  staleCount: number
  unresolvedCandidates: number
  entityResolverSuccessRate24h: number
  lastReconcileRunAt: string
  projectedDocuments: number
  containedInLinks: number
  mentionsLinks: number
  similarityLinks: number
  skippedSimilarityDocs: number
  textFallbackVectors: number
  autoPromotedEntities: number
  projectionPreset: 'strict' | 'standard' | 'relaxed'
  projectionPresetSource: 'request' | 'tenant_mapping' | 'default'
  lastProjectionRefreshAt: string
  documentAnalysisTotal: number
  documentAnalysisMatchedUnified: number
  documentAnalysisCoverageRatio: number
  documentAnalysisUnmatched: number
  documentAnalysisTargetMode: 'eligible_only' | 'all_unified'
  documentAnalysisQueryError?: string
  ontologyScore?: number
  baseOntologyScore?: number
  useCaseReadiness?: number
  freshnessValidity?: number
  canonicalityDuplication?: number
  stewardshipFindability?: number
  intentCoverage?: number
  evidenceCoverage?: number
  freshnessFit?: number
  benchmarkLite?: number
  intentBreakdown?: Array<{ intentId: string; label: string; score: number }>
  /** 件数ベース（API pillar_document_counts）。未返却時は undefined */
  pillarDocumentCounts?: OntologyPillarDocumentCounts
  /** API の情報整備スコア算出方式 */
  ontologyScoreMode?: 'count_based' | 'legacy'
}

export interface UnifiedMetadataRecord {
  itemId: string
  documentName: string
  filePath: string
  planId: string
  remediationState: RemediationState
  title: string
  contentType: string
  source: string
  freshnessStatus: 'active' | 'aging' | 'stale'
  aiEligible: boolean
  contentQualityScore: number
  transformedAt: string
  owner: string
  project: string
  topicCategories: string[]
  topics?: string[]
  categoryHierarchy?: {
    large?: string
    medium?: string
    small?: string
    confidence?: number
    reasonCodes?: string[]
  }
  canonicalDocId: string
  ontologyScore?: number
  baseOntologyScore?: number
  useCaseReadiness?: number
  freshnessValidity?: number
  canonicalityDuplication?: number
  stewardshipFindability?: number
  intentCoverage?: number
  evidenceCoverage?: number
  freshnessFit?: number
  benchmarkLite?: number
  authorityLevel?: string
  intentTags?: string[]
  useCaseFitScores?: Record<string, number>
  /** DynamoDB extensions.profile_inference_meta / document_profile（取り込み・AI補完の根拠） */
  profileInferenceFallback?: boolean
  profileNeedsReview?: boolean
  profileLlmUsed?: boolean
  profileInferenceSource?: string
}

export interface EntityCandidateRecord {
  candidateId: string
  planId: string
  remediationState: RemediationState
  surfaceForm: string
  entityType: string
  extractionSource: string
  confidence: number
  piiFlag: boolean
  itemId: string
  receivedAt: string
}

export interface EntityMasterRecord {
  entityId: string
  planId: string
  remediationState: RemediationState
  canonicalValue: string
  entityType: string
  piiFlag: boolean
  confidence: number
  spreadFactor: number
  status: 'active' | 'merged' | 'archived'
  updatedAt: string
}

export interface LineageEventRecord {
  lineageId: string
  jobName: 'schemaTransform' | 'entityResolver' | 'governanceIntegration'
  eventType: 'START' | 'COMPLETE' | 'FAIL' | 'SKIP'
  status: 'success' | 'failure' | 'skipped'
  inputDataset: string
  outputDataset: string
  eventTime: string
}

export interface QualityRecord {
  planId: string
  itemId: string
  remediationState: RemediationState
  freshnessScore: number
  uniquenessScore: number
  relevanceScore: number
  baselineContentQualityScore: number
  contentQualityScore: number
  qualityDelta: number
  aiEligible: boolean
}

export interface ReconcileJobRecord {
  jobId: string
  mode: 'diff-check' | 'orphan-cleanup' | 're-match' | 'spread-factor' | 'quality-recalc'
  status: 'queued' | 'running' | 'success' | 'failed'
  startedAt: string
  endedAt: string | null
  updatedRecords: number
  errorSummary: string
}

export interface OntologyAuditRecord {
  auditId: string
  operator: string
  action: string
  targetType: 'unified' | 'entity' | 'lineage' | 'job'
  targetId: string
  timestamp: string
  correlationId: string
}
