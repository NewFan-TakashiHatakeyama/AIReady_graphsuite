export type RemediationDomain = 'governance' | 'ontology'

export type PlanStatus =
  | 'proposed'
  | 'under_review'
  | 'approved'
  | 'rejected'
  | 'dry_run_succeeded'
  | 'dry_run_failed'
  | 'executing'
  | 'completed'
  | 'rollback_required'
  | 'rolled_back'

export type PolicyDecision = 'allow' | 'deny' | 'would_allow'

export type WorkflowStatus =
  | 'proposed'
  | 'pending_approval'
  | 'on_hold'
  | 'waiting_reproposal'
  | 'manual_in_progress'
  | 'completed'

export type ExecutionStatus =
  | 'idle'
  | 'dry_run_succeeded'
  | 'dry_run_failed'
  | 'executing'
  | 'completed'
  | 'rollback_required'
  | 'rolled_back'

export type RejectDisposition = 'WAIT_REPROPOSAL' | 'CREATE_MANUAL_TASK' | 'SNOOZE_UNTIL_DUE'
export type RemediationScope = 'site' | 'folder' | 'policy'

export type OperationActionId =
  | 'approve'
  | 'reject'
  | 'dry-run'
  | 'execute'
  | 'rollback'
  | 'retry-step'
  | 'resume'
  | 'refresh-dry-run'
  | 'request-approval'
  | 'open-monitor'
  | 'sync-policy'

export interface DryRunSummary {
  targetCount: number
  predictedFailures: number
  changedFields: string[]
  impactedUsers?: number
  failedRiskLevel?: 'low' | 'medium' | 'high'
}

export type DiffLineType = 'context' | 'add' | 'remove'

export interface DiffInlineToken {
  text: string
  changed?: boolean
}

export interface DiffLine {
  oldLineNumber: number | null
  newLineNumber: number | null
  type: DiffLineType
  oldText: string
  newText: string
  oldTokens?: DiffInlineToken[]
  newTokens?: DiffInlineToken[]
}

export interface DiffFileBlock {
  filePath: string
  section: string
  lines: DiffLine[]
}

export interface JsonDiffNode {
  key: string
  before?: string
  after?: string
  type: 'added' | 'removed' | 'changed' | 'unchanged'
  children?: JsonDiffNode[]
}

export interface OntologyQualityDelta {
  freshnessBefore: number
  freshnessAfter: number
  uniquenessBefore: number
  uniquenessAfter: number
  relevanceBefore: number
  relevanceAfter: number
}

export interface OntologyCanonicalDecision {
  groupId: string
  selectedCanonicalId: string
  reasons: string[]
}

export interface OntologyDryRunEvidence {
  qualityDelta: OntologyQualityDelta
  canonicalSelectionReasons: OntologyCanonicalDecision[]
  lineageRelinkSummary?: {
    relinkedEdges: number
    affectedDocuments: number
    orphanRiskAfterRelink: number
  }
}

export interface RemediationOperationRecord {
  planId: string
  domain: RemediationDomain
  workflowStatus: WorkflowStatus
  executionStatus: ExecutionStatus
  status: PlanStatus
  tenantScope: string
  topPlaybook: string
  riskAssessment: {
    exposureScore: number
    sensitivityScore: number
    riskScore: number
  }
  approvalRequired: boolean
  rollbackAvailable: boolean
  policyDecision: PolicyDecision
  preferredScopeOrder: RemediationScope[]
  recommendedScope: RemediationScope
  dryRunSummary: DryRunSummary
  impactSummary?: {
    changedObjects: number
    predictedFailures: number
    impactedUsers: number
    confidenceScore: number
  }
  preview?: {
    what: string
    why: string
    boundedBy: string
  }
  blastRadius?: {
    users: number
    groups: number
    files: number
    externalLinks: number
  }
  actionChecklist?: Array<{
    id: 'dry_run_reviewed' | 'approved' | 'policy_allowed' | 'scope_confirmed'
    label: string
    done: boolean
    required: boolean
  }>
  allowedActions: OperationActionId[]
  lastRejectDisposition?: RejectDisposition
  nextActionAt?: string
  manualTask?: {
    id: string
    assignee: string
    dueAt: string
  }
  correlationId: string
  executionLogs: Array<{
    at: string
    level: 'info' | 'warn' | 'error'
    message: string
  }>
  executionId?: string
  dryRunDiff: {
    files: DiffFileBlock[]
    jsonTree: JsonDiffNode[]
  }
  governanceDetails?: {
    oversharingFindings: number
    externalLinksBefore: number
    externalLinksAfter: number
    sensitiveItems: number
  }
  ontologyDetails?: {
    entityMergeCandidates: number
    taxonomyChanges: number
    brokenReferenceRisk: 'low' | 'medium' | 'high'
    affectedConcepts: string[]
    proposalFocuses?: string[]
    qualityScoreBefore?: number
    qualityScoreAfterApproval?: number
    qualityScoreAfterExecution?: number
  }
  ontologyDryRunEvidence?: OntologyDryRunEvidence
}
