export type FindingStatus =
  | 'new'
  | 'open'
  | 'in_progress'
  | 'completed'
  | 'closed'
  | 'acknowledged'
  | 'remediated'
export type GovernanceWorkflowStatus = 'acknowledged' | 'normal' | 'none'
export type RemediationState =
  | 'ai_proposed'
  | 'pending_approval'
  | 'approved'
  | 'executed'
  | 'failed'
  | 'manual_required'
export type ApprovalState = 'pending' | 'approved' | 'rejected'
export type GovernanceExceptionType =
  | 'none'
  | 'temporary_accept'
  | 'permanent_accept'
  | 'false_positive'
  | 'compensating_control'
  | 'business_required'
  | 'permission_override'
  | 'temporary_block'
  | 'manual_close'

export type GovernancePageKey =
  | 'overview'
  | 'findings'
  | 'suppression'
  | 'jobs'
  | 'policies'
  | 'help'

export interface GovernanceOverviewStats {
  governanceScore: number
  coverageScore: number
  confidenceLevel: 'High' | 'Medium' | 'Low'
  confidenceScore: number
  sensitiveProtectionScore?: number
  oversharingControlScore: number
  assuranceScore: number
  governanceRaw: number
  exceptionDebt: number
  coveragePenalty: number
  inventoryCoverage: number
  contentScanCoverage: number
  supportedFormatCoverage: number
  freshScanCoverage: number
  permissionDetailCoverage: number
  totalFindingsCount: number
  activeFindingsCount: number
  highRiskCount: number
  actionRequiredCount: number
  expiringSuppressions24h: number
  lastBatchRunAt: string
}

export type GovernanceRiskFactorKey =
  | 'exposure'
  | 'sensitivity'
  | 'activity'
  | 'ai-amplification'

export interface GovernanceRiskDetail {
  label: string
  multiplier: number
  matched: boolean
  note: string
  count: number
}

export interface GovernanceRiskFactor {
  key: GovernanceRiskFactorKey
  label: string
  description: string
  score: number
  maxScore: number
  count: number
  details: GovernanceRiskDetail[]
}

export interface GovernanceFinding {
  id: string
  planId: string
  remediationState: RemediationState
  approvalState: ApprovalState
  riskScore: number
  riskLevel?: 'critical' | 'high' | 'medium' | 'low'
  rawResidualRisk?: number
  status: FindingStatus
  workflowStatus?: GovernanceWorkflowStatus
  exceptionType?: GovernanceExceptionType
  exceptionReviewDueAt?: string | null
  ageFactor?: number
  exceptionFactor?: number
  assetCriticality?: number
  scanConfidence?: number
  aiReachability?: number
  targetKind?: 'file' | 'folder' | 'unknown'
  source: string
  itemPath: string
  matchedGuards: string[]
  guardReasonCodes?: string[]
  detectionReasons?: string[]
  findingEvidence?: {
    sharing_scope?: string
    permission_targets?: Array<{
      principal?: string
      role?: string
      is_external?: boolean
      scope?: string
    }>
    external_recipients?: string[]
    anonymous_links?: string[]
    org_edit_links?: string[]
    acl_drift_diff?: Array<{
      principal?: string
      before?: string
      after?: string
      change?: string
    }>
  }
  lastEvaluatedAt: string
  tenantId?: string
  itemId?: string
  itemName?: string
  itemUrl?: string
  exposureVectors?: string[]
  contentSignals?: {
    docSensitivityLevel?: string
    docCategories?: string[]
    containsPii?: boolean
    containsSecret?: boolean
  }
  decision?: string
  effectivePolicyId?: string
  effectivePolicyVersion?: number
  matchedPolicyIds?: string[]
  decisionTrace?: string[]
  reasonCodes?: string[]
  remediationMode?: string
  remediationAction?: string
  decisionSource?: string
  expectedAudience?: string
  expectedDepartment?: string
  expectationGapReason?: string
  expectationGapScore?: number
  sharingScope?: string
  suppressUntil?: string | null
  acknowledgedReason?: string | null
  remediationVersion?: number
  remediationLastError?: string | null
}

export interface GovernanceSuppression {
  findingId: string
  planId: string
  exceptionType: GovernanceExceptionType
  reasonCode: string
  requestedBy: string
  requestedAt: string
  status: FindingStatus
  workflowStatus?: GovernanceWorkflowStatus
  exceptionReviewDueAt: string
  rawResidualRisk?: number
  acknowledgedBy: string
  acknowledgedReason: string
}

export interface GovernanceScanJob {
  jobId: string
  jobType: 'detect_sensitivity' | 'batch_scoring'
  status: 'queued' | 'running' | 'partial' | 'skipped' | 'failed' | 'success'
  startedAt: string
  endedAt: string | null
  message: string
}

export interface GovernancePolicy {
  key: string
  value: string
  description: string
}

export interface GovernanceAuditRecord {
  id: string
  operatedAt: string
  operator: string
  action: string
  target: string
  correlationId: string
}
