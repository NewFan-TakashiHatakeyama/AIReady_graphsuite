import axios, { AxiosError } from 'axios'
import { backendBaseUrl } from '@/lib/constants'
import { errorMessage } from '@/lib/utils'
import { navigationService } from '@/services/navigation'
import { useSettingsStore } from '@/stores/settings'

// Types
export type LightragNodeType = {
  id: string
  labels: string[]
  properties: Record<string, any>
}

export type LightragEdgeType = {
  id: string
  source: string
  target: string
  type: string
  properties: Record<string, any>
}

export type LightragGraphType = {
  nodes: LightragNodeType[]
  edges: LightragEdgeType[]
}

export type OntologyItemGraphResponse = LightragGraphType & {
  is_truncated: boolean
  start_node_id?: string | null
  matched_by?: 'item_id' | 'file_name' | 'fallback'
}

export type OntologyEntityCandidateSuggestion = {
  entity_id: string
  canonical_name: string
  score: number
  confidence: number
  pii_flag: boolean
  updated_at: string
}

export type OntologyEntityCandidateRow = {
  candidate_id: string
  surface_form: string
  entity_type: string
  extraction_source: string
  confidence: number
  pii_flag: boolean
  item_id: string
  received_at: string
  status: 'pending' | 'resolved' | 'rejected'
  suggestions: OntologyEntityCandidateSuggestion[]
}

export type OntologyEntityCandidatesResponse = {
  rows: OntologyEntityCandidateRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
  }
}

export type OntologyEntityCandidateResolveResponse = {
  status: 'resolved'
  candidate_id: string
  resolution_type: 'merge_existing' | 'register_new'
  entity_id: string
  canonical_name: string
  resolved_by: string
}

export type GovernanceFindingApiRow = {
  tenant_id: string
  finding_id: string
  source: string
  container_id?: string
  container_name?: string
  container_type?: string
  target_type?: 'file' | 'folder' | 'unknown' | string
  item_id: string
  item_name?: string
  item_url?: string
  source_metadata?: Record<string, unknown> | string | null
  risk_score?: number
  risk_level?: 'critical' | 'high' | 'medium' | 'low' | 'none'
  raw_residual_risk?: number
  exposure_score?: number
  sensitivity_score?: number
  sensitive_composite?: number
  activity_score?: number
  age_factor?: number
  exception_factor?: number
  asset_criticality?: number
  scan_confidence?: number
  ai_reachability?: number
  ai_amplification?: number
  exposure_vectors?: string[]
  content_signals?: {
    doc_sensitivity_level?: 'none' | 'low' | 'medium' | 'high' | 'critical' | string
    doc_categories?: string[]
    contains_pii?: boolean
    contains_secret?: boolean
    confidence?: number
    expected_audience?: string
    expected_department?: string
    expected_department_confidence?: number
    justification?: string
  }
  sharing_scope?: string
  permissions_summary?: string
  sensitivity_label?: string
  pii_detected?: boolean
  pii_types?: string[]
  pii_count?: number
  pii_density?: 'none' | 'low' | 'medium' | 'high'
  secrets_detected?: boolean
  secret_types?: string[]
  sensitivity_scan_at?: string | null
  status?: 'new' | 'open' | 'completed' | 'remediated' | 'acknowledged' | 'closed'
  workflow_status?: 'acknowledged' | 'normal' | 'none'
  exception_type?: string
  exception_review_due_at?: string | null
  matched_guards?: string[]
  guard_reason_codes?: string[]
  detection_reasons?: string[]
  finding_evidence?: {
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
  detected_at?: string
  last_evaluated_at?: string
  suppress_until?: string | null
  acknowledged_reason?: string | null
  acknowledged_by?: string | null
  acknowledged_at?: string | null
  remediation_state?: 'ai_proposed' | 'pending_approval' | 'approved' | 'executed' | 'failed' | 'manual_required'
  remediation_version?: number
  remediation_last_error?: string | null
  decision?: 'allow' | 'warn' | 'review' | 'block' | string
  effective_policy_id?: string
  effective_policy_version?: number
  matched_policy_ids?: string[]
  decision_trace?: string[]
  reason_codes?: string[]
  remediation_mode?: 'auto' | 'approval' | 'owner_review' | 'manual' | 'recommend_only' | string
  remediation_action?: string
  policy_hash?: string
  decision_source?: string
  expected_audience?: string
  expected_department?: string
  expectation_gap_reason?: string
  expectation_gap_score?: number
  document_summary?: string
  document_pii_summary?: {
    detected?: boolean
    types?: string[]
    count?: number
    density?: 'none' | 'low' | 'medium' | 'high'
    high_risk_detected?: boolean
  }
  document_secrets_summary?: {
    detected?: boolean
    types?: string[]
    count?: number
  }
  document_embedding_s3_key?: string
  document_analyzed_at?: string
}

export type GovernanceFindingsResponse = {
  rows: GovernanceFindingApiRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
    scan_capped?: boolean
    next_offset?: number | null
  }
}

export type GovernanceRemediationDetailResponse = {
  tenant_id: string
  finding_id: string
  remediation_state: 'ai_proposed' | 'pending_approval' | 'approved' | 'executed' | 'failed' | 'manual_required'
  remediation_mode?: string
  actions?: Array<Record<string, any>>
  allowed_actions?: string[]
  version?: number
  approved_by?: string
  approved_at?: string
  last_execution_id?: string
  last_error?: string
  result?: Record<string, any>
  updated_at?: string
  execution_id?: string
  proposal_generated?: boolean
  approved?: boolean
  replayed?: boolean
  exception_type?: string | null
  exception_review_due_at?: string | null
  exception_approved_by?: string | null
  exception_ticket?: string | null
  exception_scope_hash?: string | null
  exception_registered?: boolean
  rollback_id?: string
}

export type GovernanceExceptionRegistrationRequest = {
  exception_type: 'temporary_accept' | 'permanent_accept' | 'compensating_control' | 'false_positive' | 'business_required'
  duration_days?: number
  exception_review_due_at?: string
  reason?: string
  exception_ticket?: string
  scope?: Record<string, any>
}


export type GovernanceOverviewResponse = {
  governance_score?: number
  subscores?: {
    sensitive_protection?: number
    oversharing_control?: number
    assurance?: number
  }
  subscores_breakdown?: {
    sensitive_protection?: Array<{
      key?: string
      label?: string
      value?: number
      score?: number
    }>
    oversharing_control?: Array<{
      key?: string
      label?: string
      value?: number
      score?: number
    }>
    assurance?: Array<{
      key?: string
      label?: string
      value?: number
      score?: number
    }>
  }
  coverage?: {
    coverage_score?: number
    inventory_coverage?: number
    content_scan_coverage?: number
    supported_format_coverage?: number
    fresh_scan_coverage?: number
    permission_detail_coverage?: number
  }
  confidence?: {
    level?: 'High' | 'Medium' | 'Low'
    scan_confidence?: number
  }
  risk_summary?: {
    governance_raw?: number
    exception_debt?: number
    coverage_penalty?: number
  }
  high_risk_count: number
  action_required_count?: number
  expiring_suppressions_24h: number
  last_batch_run_at: string | null
  /** 初回バッチ相当のゲートを開いてよい（リアルタイム移行後は Connect 不可・Finding あり・成功ジョブのいずれか） */
  initial_scan_gate_open?: boolean
  protection_scores: {
    oversharing_protection: number
    sensitivity_protection: number
    overall: number
  }
  counts?: {
    total_findings?: number
    active_findings?: number
    acknowledged?: number
  }
  protection_score_breakdown?: {
    factors: {
      exposure: number
      sensitivity: number
      activity: number
      ai_amplification: number
    }
    oversharing: {
      metric: {
        intermediate_risk: number
        normalized_raw: number
        normalized_clamped: number
        score: number
      }
      score: number
      details: {
        everyone_public: number
        public_link_exposure: number
        excessive_permission_remediation: number
      }
    }
    sensitive: {
      metric: {
        intermediate_risk: number
        normalized_raw: number
        normalized_clamped: number
        score: number
      }
      score: number
      details: {
        pii_pci_exposure: number
        missing_label: number
        isolation_suppression_coverage: number
      }
    }
    detail_evidence?: {
      everyone_public?: {
        score: number
        total_count: number
        issue_count: number
        deducted_points: number
        evidence: Array<{
          item_id: string
          item_name: string
          item_url: string
          status: string
          reason: string
          impact_points: number
        }>
      }
      public_link_exposure?: {
        score: number
        total_count: number
        issue_count: number
        deducted_points: number
        evidence: Array<{
          item_id: string
          item_name: string
          item_url: string
          status: string
          reason: string
          impact_points: number
        }>
      }
      excessive_permission_remediation?: {
        score: number
        total_count: number
        issue_count: number
        deducted_points: number
        evidence: Array<{
          item_id: string
          item_name: string
          item_url: string
          status: string
          reason: string
          impact_points: number
        }>
      }
      pii_pci_exposure?: {
        score: number
        total_count: number
        issue_count: number
        deducted_points: number
        evidence: Array<{
          item_id: string
          item_name: string
          item_url: string
          status: string
          reason: string
          impact_points: number
        }>
      }
      missing_label?: {
        score: number
        total_count: number
        issue_count: number
        deducted_points: number
        evidence: Array<{
          item_id: string
          item_name: string
          item_url: string
          status: string
          reason: string
          impact_points: number
        }>
      }
      isolation_suppression_coverage?: {
        score: number
        total_count: number
        issue_count: number
        deducted_points: number
        evidence: Array<{
          item_id: string
          item_name: string
          item_url: string
          status: string
          reason: string
          impact_points: number
        }>
      }
    }
  }
}

export type GovernanceSuppressionApiRow = {
  finding_id: string
  status: 'new' | 'open' | 'completed' | 'remediated' | 'acknowledged' | 'closed'
  workflow_status?: 'acknowledged' | 'normal' | 'none'
  exception_type?: string
  exception_review_due_at?: string | null
  raw_residual_risk?: number
  suppress_until?: string | null
  acknowledged_by?: string | null
  acknowledged_reason?: string | null
}

export type GovernanceSuppressionsResponse = {
  rows: GovernanceSuppressionApiRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
  }
}

export type GovernancePoliciesResponse = {
  global?: Record<string, string>
  scope?: Array<{
    policy_id?: string
    name?: string
    scope_type?: string
    scope_value?: string
    status?: string
    priority?: number
    estimated_affected_count?: number
    updated_at?: string
    [key: string]: any
  }>
  global_policies?: Array<{ name?: string; value?: string }>
  global_policy_rows?: Array<{
    policy_id?: string
    name?: string
    layer?: string
    scope_type?: string
    status?: string
    priority?: number
    estimated_affected_count?: number
    updated_at?: string
    rollout?: { stage?: string; dry_run?: boolean }
    rules?: Array<Record<string, any>>
    scope?: Record<string, any>
    [key: string]: any
  }>
  scope_policies?: Array<{
    policy_id?: string
    name?: string
    scope_type?: string
    scope_value?: string
    status?: string
    priority?: number
    estimated_affected_count?: number
    updated_at?: string
    [key: string]: any
  }>
  policy_versions?: Array<{
    policy_id?: string
    version?: number
    updated_at?: string
  }>
  estimated_impacts?: Array<{
    policy_id?: string
    estimated_affected_count?: number
    [key: string]: any
  }>
  scope_mode?: {
    enabled?: boolean
    reason?: string
    message?: string
  }
}

export type GovernancePolicySimulationResponse = {
  estimated_affected_items: number
  estimated_new_findings: number
  estimated_resolved_findings: number
  sample_targets: Array<{
    finding_id?: string
    item_id?: string
    risk_level?: string
  }>
}

export type GovernanceScanTriggerResponse = {
  job_id?: string
  status: string
  accepted_at?: string
  lambda_name?: string
  mode?: string
  message?: string
  reason?: string
  tenant_id?: string
  detail?: string
}

export type GovernanceScanJobApiRow = {
  tenant_id?: string
  job_id: string
  status?: string
  accepted_at?: string
  operator?: string
  correlation_id?: string
  source?: string
  [key: string]: any
}

export type GovernanceScanJobsResponse = {
  rows: GovernanceScanJobApiRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
  }
}

export type GovernanceAuditApiRow = {
  tenant_id?: string
  event?: string
  message?: string
  timestamp?: string
  operator?: string
  target?: string
  correlation_id?: string
  source?: string
  [key: string]: any
}

export type GovernanceAuditResponse = {
  rows: GovernanceAuditApiRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
  }
}

export type ConnectOverviewResponse = {
  tenant_id: string
  delivery_status: 'healthy' | 'degraded' | 'failed'
  queue_backlog: number
  failed_jobs_24h: number
  next_subscription_renewal_at: string
  next_token_renewal_at: string
  next_delta_sync_at: string
}

export type ConnectSubscriptionApiRow = {
  id: string
  connection_id?: string
  connection_name?: string
  resource: string
  expiration_at: string
  client_state_verified: boolean
  status: 'initializing' | 'active' | 'expiring' | 'failed'
  resource_type?: 'drive' | 'message' | string
  target_type?: 'drive' | 'channel' | 'chat' | string
  is_placeholder?: boolean
  reflection_status?: 'pending' | 'ready'
  tenant_hint?: string
}

export type ConnectSubscriptionDeleteResponse = {
  tenant_id: string
  connection_id: string
  subscription_id: string
  delete_mode: 'safe' | 'force' | string
  status: string
  graph_unsubscribe_status: string
  deleted_at: string
}

export type ConnectSubscriptionsResponse = {
  rows: ConnectSubscriptionApiRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
  }
}

export type ConnectScopeApiRow = {
  id: string
  subscription_id: string
  tenant_id: string
  site: string
  drive: string
  excluded_path_count: number
  last_delta_sync_at: string
}

export type ConnectScopesResponse = {
  rows: ConnectScopeApiRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
  }
}

export type ConnectEventApiRow = {
  id: string
  scope_id: string
  received_at: string
  change_type: 'create' | 'update' | 'delete' | 'permission_change' | string
  resource: string
  idempotency_key: string
  status: 'queued' | 'processed' | 'duplicated' | 'failed' | 'deleted' | string
}

export type ConnectEventsResponse = {
  rows: ConnectEventApiRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
  }
  resolved_tenant_id?: string
}

export type ConnectJobApiRow = {
  id: string
  event_id?: string | null
  job_type: 'ingestion' | 'delta_sync' | 'governance_trigger' | string
  started_at: string
  status: 'queued' | 'running' | 'success' | 'retrying' | 'failed' | 'dead-lettered' | string
  last_message: string
  correlation_id?: string | null
  source?: string | null
}

export type ConnectJobsResponse = {
  rows: ConnectJobApiRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
  }
}

export type ConnectAuditApiRow = {
  id: string
  operated_at: string
  operator: string
  action: string
  target_type: 'subscription' | 'event' | 'job' | 'scope' | string
  target_id: string
  correlation_id?: string | null
  source?: string | null
}

export type ConnectAuditResponse = {
  rows: ConnectAuditApiRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
  }
}

export type ConnectSyncCheckApiResponse = {
  tenant_id: string
  status: string
  lambda_function_name: string
  status_code: number
  requested_by: string
  correlation_id: string
  requested_at: string
}

export type ConnectOnboardingRequest = {
  client_id?: string
  site_id?: string
  drive_id: string
  notification_url: string
  client_secret?: string
  client_state?: string
  connection_name?: string
  initialize_subscription?: boolean
  resource_type?: 'drive' | 'message'
  resource_path?: string
  change_type?: string
  target_type?: 'drive' | 'channel' | 'chat'
  team_id?: string
  channel_id?: string
  chat_id?: string
}

export type ConnectOnboardingResponse = {
  tenant_id: string
  status: string
  connection_id?: string | null
  bootstrap_status?: 'started' | 'succeeded' | 'failed' | 'initializing' | 'skipped' | null
  subscription_id?: string | null
  subscription_status?: string | null
  bootstrap_error?: string | null
  subscription_initialized: boolean
  subscription_init_status?: string | null
  subscription_reflection_ready?: boolean
  subscription_reflection_wait_ms?: number
  subscriptions: ConnectSubscriptionsResponse
}

export type ConnectOnboardingDefaultsResponse = {
  tenant_id: string
  client_id: string
  auth_method: string
  permission_profile: string
  notification_url: string
  client_secret_parameter: string
  client_state_parameter: string
}

export type ConnectSiteDiscoveryRequest = {
  azure_tenant_id?: string
  client_id?: string
  client_secret?: string
  site_url?: string
  site_id?: string
}

export type ConnectSiteDiscoveryResponse = {
  site_id: string
  drive_id: string
  site_name: string
  site_web_url: string
  suggested_connection_name: string
}

export type ConnectSiteOptionsRequest = {
  azure_tenant_id?: string
  client_id?: string
  client_secret?: string
  source_type?: 'sharepoint' | 'teams' | 'onedrive'
  query?: string
}

export type ConnectSiteOption = {
  site_id: string
  site_name: string
  site_web_url: string
  source_type: 'sharepoint' | 'teams' | 'onedrive'
}

export type ConnectSiteOptionsResponse = {
  rows: ConnectSiteOption[]
}

export type ConnectTeamChannelOptionsRequest = {
  azure_tenant_id?: string
  client_id?: string
  client_secret?: string
  team_query?: string
  channel_query?: string
  site_id?: string
  max_teams?: number
  max_channels_per_team?: number
}

export type ConnectTeamChannelOption = {
  team_id: string
  team_name: string
  team_mail?: string
  channel_id: string
  channel_name: string
  membership_type?: string
  files_drive_id: string
  files_folder_web_url?: string
  files_folder_name?: string
  site_id?: string
  site_web_url?: string
  source_type: 'teams'
  discovery_status?: 'ready' | 'files_folder_unavailable' | string
  error_message?: string
}

export type ConnectTeamChannelOptionsResponse = {
  rows: ConnectTeamChannelOption[]
  warnings?: string[]
  required_application_permissions_phase1?: string[]
  required_application_permissions_phase2?: string[]
}

export type AuditDomainFilter = 'all' | 'connect' | 'governance' | 'ontology'

export type AuditRecordApiRow = {
  domain: 'connect' | 'governance' | 'ontology'
  audit_id: string
  occurred_at: string
  operator: string
  action: string
  target: string
  correlation_id: string
}

export type AuditRecordsApiResponse = {
  rows: AuditRecordApiRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
  }
}

export type AuditExportAcceptedApiResponse = {
  job_id: string
  status: 'accepted'
}

export type AuditExportStatusApiResponse = {
  job_id: string
  status: 'accepted' | 'completed' | 'failed'
  format: 'csv' | 'pdf'
  download_url?: string | null
}

export type DashboardSignalApiRow = {
  key: string
  label: string
  score: number | null
  issues: number
  target: number
  sub_metrics?: DashboardSignalSubMetricApiRow[]
}

export type DashboardSignalSubMetricApiRow = {
  key: string
  label: string
  value: number | null
  unit: 'score' | 'count' | 'percent'
}

export type DashboardReadinessApiResponse = {
  readiness_score: number | null
  target_score: number
  signals: DashboardSignalApiRow[]
}

export type DashboardReadinessTrendPointApiRow = {
  label: string
  governance_oversharing: number
  governance_sensitive: number
  governance_assurance: number
  ontology_foundation: number
  ontology_usecase: number
}

export type DashboardReadinessTrendApiResponse = {
  rows: DashboardReadinessTrendPointApiRow[]
  target_score: number
  source: 'snapshot' | 'estimated' | 'insufficient_data'
}

export type DashboardRecommendedActionApiRow = {
  priority: 'P1' | 'P2' | 'P3'
  domain: string
  summary: string
}

export type DashboardRecommendedActionsApiResponse = {
  rows: DashboardRecommendedActionApiRow[]
}

export type OntologyOverviewResponse = {
  unified_document_count?: number
  active_unified_document_count?: number
  entity_candidate_count?: number
  unresolved_candidates: number
  stale_or_aging_documents: number
  owner_identified_documents?: number
  project_identified_documents?: number
  topic_categorized_documents?: number
  canonicalized_documents?: number
  noun_resolution_enabled?: boolean
  signal_scores: {
    freshness: number
    duplication: number
    location: number
    overall: number
  }
  projection_metrics?: {
    last_refresh_at?: string
    projected_documents?: number
    contained_in_links?: number
    mentions_links?: number
    similarity_links?: number
    skipped_similarity_docs?: number
    text_fallback_vectors?: number
    auto_promoted_entities?: number
    projection_preset?: 'strict' | 'standard' | 'relaxed'
    projection_preset_source?: 'request' | 'tenant_mapping' | 'default'
  }
  document_analysis_contract?: {
    target_mode?: 'eligible_only' | 'all_unified'
    description?: string
  }
  document_analysis_metrics?: {
    table_name?: string
    analysis_total_count?: number
    matched_unified_count?: number
    active_unified_total_count?: number
    coverage_ratio?: number
    unmatched_analysis_count?: number
    query_error?: string
  }
  ontology_score?: number
  base_ontology_score?: number
  use_case_readiness?: number
  freshness_validity?: number
  canonicality_duplication?: number
  stewardship_findability?: number
  intent_coverage?: number
  evidence_coverage?: number
  freshness_fit?: number
  benchmark_lite?: number
  intent_breakdown?: Array<{ intent_id: string; label?: string; score: number }>
}

export type OntologyProjectionRefreshResponse = {
  tenant_id: string
  clear_existing: boolean
  max_documents: number
  source_documents: number
  projected_documents: number
  projected_entities: number
  projected_links: number
  projected_lineage_links: number
  contained_in_links: number
  mentions_links: number
  similarity_links: number
  skipped_similarity_docs: number
  text_fallback_vectors: number
  auto_promoted_entities: number
  projection_preset: 'strict' | 'standard' | 'relaxed'
  projection_preset_source: 'request' | 'tenant_mapping' | 'default'
  projection_options: Record<string, string | number | boolean>
}

export type OntologyUserSettingsResponse = {
  projection_preset: 'strict' | 'standard' | 'relaxed'
  max_documents: number
  auto_refresh: boolean
  updated_at: string
  source: 'user_setting' | 'tenant_default'
}

export type OntologyUnifiedMetadataApiRow = {
  item_id?: string
  plan_id?: string
  title?: string
  source?: string
  freshness_status?: 'active' | 'aging' | 'stale'
  ai_eligible?: boolean
  transformed_at?: string
  content_quality_score?: number
  remediation_state?: 'ai_proposed' | 'pending_approval' | 'approved' | 'executed'
  owner?: string
  project?: string
  topic_categories?: string[]
  topics?: string[]
  category_hierarchy?: {
    large?: string
    medium?: string
    small?: string
    confidence?: number
    reason_codes?: string[]
  }
  canonical_doc_id?: string
  ontology_score?: number
  base_ontology_score?: number
  use_case_readiness?: number
  freshness_validity?: number
  canonicality_duplication?: number
  stewardship_findability?: number
  intent_coverage?: number
  evidence_coverage?: number
  freshness_fit?: number
  benchmark_lite?: number
  authority_level?: string
  intent_tags?: string[]
  use_case_fit_scores?: Record<string, number>
  profile_inference_fallback?: boolean
  profile_needs_review?: boolean
  profile_llm_used?: boolean
  profile_inference_source?: string
  [key: string]: any
}

export type OntologyUnifiedMetadataResponse = {
  rows: OntologyUnifiedMetadataApiRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
  }
}

export type OntologyEntityMasterApiRow = {
  entity_id?: string
  canonical_value?: string
  canonical_name?: string
  entity_type?: string
  pii_flag?: boolean
  spread_factor?: number
  status?: string
  updated_at?: string
  [key: string]: any
}

export type OntologyEntityMasterResponse = {
  rows: OntologyEntityMasterApiRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
  }
  source?: string
  noun_resolution_enabled?: boolean
}

export type OntologyAuditApiRow = {
  event?: string
  job_name?: string
  status?: string
  operator?: string
  correlation_id?: string
  timestamp?: string
  source?: string
  [key: string]: any
}

export type OntologyAuditResponse = {
  rows: OntologyAuditApiRow[]
  pagination: {
    limit: number
    offset: number
    total_count: number
  }
}

export type GraphsuiteStatus = {
  status: 'healthy'
  working_directory: string
  input_directory: string
  configuration: {
    llm_binding: string
    llm_binding_host: string
    llm_model: string
    embedding_binding: string
    embedding_binding_host: string
    embedding_model: string
    kv_storage: string
    doc_status_storage: string
    graph_storage: string
    vector_storage: string
    workspace?: string
    max_graph_nodes?: string
    enable_rerank?: boolean
    rerank_binding?: string | null
    rerank_model?: string | null
    rerank_binding_host?: string | null
    summary_language: string
    force_llm_summary_on_merge: boolean
    max_parallel_insert: number
    max_async: number
    embedding_func_max_async: number
    embedding_batch_num: number
    cosine_threshold: number
    min_rerank_score: number
    related_chunk_number: number
  }
  update_status?: Record<string, any>
  core_version?: string
  api_version?: string
  auth_mode?: 'enabled' | 'disabled'
  pipeline_busy: boolean
  keyed_locks?: {
    process_id: number
    cleanup_performed: {
      mp_cleaned: number
      async_cleaned: number
    }
    current_status: {
      total_mp_locks: number
      pending_mp_cleanup: number
      total_async_locks: number
      pending_async_cleanup: number
    }
  }
  webui_title?: string
  webui_description?: string
}


export type DocActionResponse = {
  status: 'success' | 'partial_success' | 'failure' | 'duplicated'
  message: string
  track_id?: string
}

export type AuthStatusResponse = {
  auth_configured: boolean
  access_token?: string
  token_type?: string
  auth_mode?: 'enabled' | 'disabled'
  message?: string
  core_version?: string
  api_version?: string
  webui_title?: string
  webui_description?: string
}

export type LoginResponse = {
  access_token: string
  token_type: string
  auth_mode?: 'enabled' | 'disabled'  // Authentication mode identifier
  message?: string                    // Optional message
  core_version?: string
  api_version?: string
  webui_title?: string
  webui_description?: string
}

export const InvalidApiKeyError = 'Invalid API Key'
export const RequireApiKeError = 'API Key required'

export type ApiErrorResponse = {
  code?: string
  message?: string
  correlation_id?: string
  relogin_required?: boolean
  detail?: unknown
}

export class ApiHttpError extends Error {
  status: number
  statusText: string
  url: string
  data: ApiErrorResponse | unknown
  correlationId?: string

  constructor(params: {
    status: number
    statusText: string
    url: string
    data: ApiErrorResponse | unknown
    correlationId?: string
  }) {
    const { status, statusText, url, data, correlationId } = params
    super(
      `${status} ${statusText}\n${JSON.stringify(data)}\n${url}${correlationId ? `\ncorrelation_id=${correlationId}` : ''}`
    )
    this.name = 'ApiHttpError'
    this.status = status
    this.statusText = statusText
    this.url = url
    this.data = data
    this.correlationId = correlationId
  }
}

const shouldHandleSessionAsExpired = (error: AxiosError): boolean => {
  if (error.response?.status !== 401) return false
  return false
}

const looksLikeHtmlPayload = (payload: unknown): boolean => {
  if (typeof payload !== 'string') return false
  const normalized = payload.trim().toLowerCase()
  return normalized.startsWith('<!doctype html') || normalized.startsWith('<html')
}

// Axios instance
const axiosInstance = axios.create({
  baseURL: backendBaseUrl,
  headers: {
    'Content-Type': 'application/json'
  }
})

const sleep = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms))

const isRetryableGetError = (error: unknown): boolean => {
  if (!axios.isAxiosError(error)) return false
  const status = error.response?.status
  if (typeof status === 'number' && status >= 500) return true
  const message = String(error.message ?? '').toLowerCase()
  return (
    message.includes('econnreset') ||
    message.includes('socket hang up') ||
    message.includes('network error') ||
    message.includes('timeout')
  )
}

const getWithRetry = async <T>(path: string, retries: number = 2, delayMs: number = 250): Promise<T> => {
  let lastError: unknown
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const response = await axiosInstance.get(path, { timeout: 8000 })
      return response.data as T
    } catch (error) {
      lastError = error
      if (attempt >= retries || !isRetryableGetError(error)) {
        throw error
      }
      await sleep(delayMs * (attempt + 1))
    }
  }
  throw lastError instanceof Error ? lastError : new Error('GET request failed')
}

// Interceptor: add api key and check authentication
axiosInstance.interceptors.request.use((config) => {
  const apiKey = useSettingsStore.getState().apiKey
  const token = localStorage.getItem('LIGHTRAG-API-TOKEN');

  // Always include token if it exists, regardless of path
  if (token) {
    config.headers['Authorization'] = `Bearer ${token}`
  }
  if (apiKey) {
    config.headers['X-API-Key'] = apiKey
  }
  return config
})

// Interceptor：hanle error
axiosInstance.interceptors.response.use(
  (response) => {
    if (looksLikeHtmlPayload(response.data)) {
      const url = response.config?.url ?? ''
      throw new Error(
        `API endpoint returned HTML instead of JSON. Check Vite proxy settings. path=${url}`
      )
    }
    return response
  },
  (error: AxiosError) => {
    if (error.response) {
      if (error.response?.status === 401) {
        // For login API, throw error directly
        if (error.config?.url?.includes('/login')) {
          throw error;
        }
        if (!shouldHandleSessionAsExpired(error)) {
          // For other APIs, navigate to login page
          navigationService.navigateToLogin();
          // return a reject Promise
          return Promise.reject(new Error('Authentication required'));
        }
      }
      const responseHeaders = error.response.headers ?? {}
      const correlationId = (responseHeaders['x-correlation-id'] || responseHeaders['X-Correlation-Id']) as string | undefined
      throw new ApiHttpError({
        status: error.response.status,
        statusText: error.response.statusText,
        data: error.response.data,
        url: error.config?.url ?? '',
        correlationId
      })
    }
    throw error
  }
)

// API methods
export const queryGraphs = async (
  label: string,
  maxDepth: number,
  maxNodes: number,
  source: 'default' | 'ontology' = 'ontology'
): Promise<LightragGraphType> => {
  const response = await axiosInstance.get(
    `/graphs?label=${encodeURIComponent(label)}&max_depth=${maxDepth}&max_nodes=${maxNodes}&source=${source}`
  )
  return response.data
}

export const getGraphLabels = async (source: 'default' | 'ontology' = 'ontology'): Promise<string[]> => {
  const response = await axiosInstance.get(`/graph/label/list?source=${source}`)
  return response.data
}

export const seedOntologyGraph = async (force: boolean = false): Promise<Record<string, any>> => {
  const response = await axiosInstance.post(`/ontology/seed?force=${force}`)
  return response.data
}

export const queryOntologyGraphByItem = async (
  itemId: string,
  fileName: string,
  maxDepth: number = 2,
  maxNodes: number = 80
): Promise<OntologyItemGraphResponse> => {
  const response = await axiosInstance.get(
    `/ontology/graph/by-item?item_id=${encodeURIComponent(itemId)}&file_name=${encodeURIComponent(fileName)}&max_depth=${maxDepth}&max_nodes=${maxNodes}`
  )
  return response.data
}

export const getOntologyEntityCandidates = async (
  limit: number = 100,
  offset: number = 0,
  status: 'pending' | 'resolved' | 'rejected' = 'pending'
): Promise<OntologyEntityCandidatesResponse> => {
  const response = await axiosInstance.get(
    `/ontology/entity-candidates?limit=${limit}&offset=${offset}&status=${encodeURIComponent(status)}`
  )
  return response.data
}

export const getGovernanceFindings = async (
  limit: number = 200,
  offset: number = 0,
  statuses: string = 'new,open,acknowledged,closed',
  _includeDocumentAnalysis: boolean = false,
  actionRequiredOnly: boolean = false
): Promise<GovernanceFindingsResponse> => {
  // Hard-cut compatibility: include_document_analysis contract is retired.
  void _includeDocumentAnalysis
  const response = await axiosInstance.get(
    `/governance/findings?limit=${limit}&offset=${offset}&statuses=${encodeURIComponent(statuses)}&action_required_only=${actionRequiredOnly}`
  )
  return response.data
}

export const getGovernanceFindingRemediation = async (
  findingId: string
): Promise<GovernanceRemediationDetailResponse> => {
  const response = await axiosInstance.get(
    `/governance/findings/${encodeURIComponent(findingId)}/remediation`
  )
  return response.data
}

export const proposeGovernanceFindingRemediation = async (
  findingId: string,
  force: boolean = false
): Promise<GovernanceRemediationDetailResponse> => {
  const response = await axiosInstance.post(
    `/governance/findings/${encodeURIComponent(findingId)}/remediation/propose?force=${force}`
  )
  return response.data
}

export const approveGovernanceFindingRemediation = async (
  findingId: string
): Promise<GovernanceRemediationDetailResponse> => {
  const response = await axiosInstance.post(
    `/governance/findings/${encodeURIComponent(findingId)}/remediation/approve`
  )
  return response.data
}

export const executeGovernanceFindingRemediation = async (
  findingId: string
): Promise<GovernanceRemediationDetailResponse> => {
  const response = await axiosInstance.post(
    `/governance/findings/${encodeURIComponent(findingId)}/remediation/execute`
  )
  return response.data
}

export const rollbackGovernanceFindingRemediation = async (
  findingId: string
): Promise<GovernanceRemediationDetailResponse> => {
  const response = await axiosInstance.post(
    `/governance/findings/${encodeURIComponent(findingId)}/remediation/rollback`
  )
  return response.data
}

export const markGovernanceFindingCompleted = async (
  findingId: string
): Promise<GovernanceRemediationDetailResponse> => {
  const response = await axiosInstance.post(
    `/governance/findings/${encodeURIComponent(findingId)}/complete`
  )
  return response.data
}

export const registerGovernanceFindingException = async (
  findingId: string,
  payload: GovernanceExceptionRegistrationRequest
): Promise<GovernanceRemediationDetailResponse> => {
  const response = await axiosInstance.post(
    `/governance/findings/${encodeURIComponent(findingId)}/remediation/exception`,
    payload
  )
  return response.data
}


export const getGovernanceOverview = async (): Promise<GovernanceOverviewResponse> => {
  return await getWithRetry<GovernanceOverviewResponse>('/governance/overview', 2, 300)
}

export const getGovernanceSuppressions = async (
  limit: number = 200,
  offset: number = 0,
  expiringWithinHours: number = 0
): Promise<GovernanceSuppressionsResponse> => {
  return await getWithRetry<GovernanceSuppressionsResponse>(
    `/governance/suppressions?limit=${limit}&offset=${offset}&expiring_within_hours=${expiringWithinHours}`,
    2,
    300
  )
}

export const getGovernancePolicies = async (): Promise<GovernancePoliciesResponse> => {
  return await getWithRetry<GovernancePoliciesResponse>('/governance/policies', 2, 300)
}

export const createGovernancePolicy = async (
  payload: Record<string, any>,
  dryRun: boolean = false
): Promise<Record<string, any>> => {
  const response = await axiosInstance.post(`/governance/policies?dry_run=${dryRun}`, payload)
  return response.data
}

export const updateGovernancePolicy = async (
  policyId: string,
  payload: Record<string, any>,
  dryRun: boolean = false
): Promise<Record<string, any>> => {
  const response = await axiosInstance.put(
    `/governance/policies/${encodeURIComponent(policyId)}?dry_run=${dryRun}`,
    payload
  )
  return response.data
}

export const simulateGovernancePolicy = async (
  payload: Record<string, any>
): Promise<GovernancePolicySimulationResponse> => {
  const response = await axiosInstance.post('/governance/policies/simulate', payload)
  return response.data
}

export const runGovernanceDailyScan = async (): Promise<GovernanceScanTriggerResponse> => {
  const response = await axiosInstance.post('/governance/scans/daily')
  return response.data
}

export const getGovernanceScanJobs = async (
  limit: number = 100,
  offset: number = 0
): Promise<GovernanceScanJobsResponse> => {
  return await getWithRetry<GovernanceScanJobsResponse>(
    `/governance/scan-jobs?limit=${limit}&offset=${offset}`,
    2,
    300
  )
}

export const getGovernanceAuditLogs = async (
  limit: number = 100,
  offset: number = 0
): Promise<GovernanceAuditResponse> => {
  const response = await axiosInstance.get(`/governance/audit?limit=${limit}&offset=${offset}`)
  return response.data
}

export const getOntologyOverview = async (): Promise<OntologyOverviewResponse> => {
  const response = await axiosInstance.get('/ontology/overview')
  return response.data
}

export const getOntologyUserSettings = async (): Promise<OntologyUserSettingsResponse> => {
  const response = await axiosInstance.get('/ontology/user-settings')
  return response.data
}

export const updateOntologyUserProjectionPreset = async (
  preset: 'strict' | 'standard' | 'relaxed'
): Promise<OntologyUserSettingsResponse> => {
  const response = await axiosInstance.put(`/ontology/user-settings/projection-preset?preset=${preset}`)
  return response.data
}

export const updateOntologyUserSettings = async (params: {
  projectionPreset?: 'strict' | 'standard' | 'relaxed'
  maxDocuments?: number
  autoRefresh?: boolean
}): Promise<OntologyUserSettingsResponse> => {
  const query = new URLSearchParams()
  if (params.projectionPreset) query.set('projection_preset', params.projectionPreset)
  if (typeof params.maxDocuments === 'number') query.set('max_documents', String(params.maxDocuments))
  if (typeof params.autoRefresh === 'boolean') query.set('auto_refresh', String(params.autoRefresh))
  const response = await axiosInstance.put(`/ontology/user-settings?${query.toString()}`)
  return response.data
}

export const refreshOntologyGraphProjection = async (
  clearExisting: boolean = true,
  maxDocuments: number = 2000,
  preset?: 'strict' | 'standard' | 'relaxed'
): Promise<OntologyProjectionRefreshResponse> => {
  const presetParam = preset ? `&preset=${preset}` : ''
  const response = await axiosInstance.post(
    `/ontology/graph/projection/refresh?clear_existing=${clearExisting}&max_documents=${maxDocuments}${presetParam}`
  )
  return response.data
}

export const getOntologyUnifiedMetadata = async (
  limit: number = 200,
  offset: number = 0
): Promise<OntologyUnifiedMetadataResponse> => {
  const response = await axiosInstance.get(`/ontology/unified-metadata?limit=${limit}&offset=${offset}`)
  return response.data
}

export const aiFillOntologyProfile = async (
  itemId: string,
  payload?: { use_llm?: boolean }
): Promise<{
  item_id: string
  profile: {
    owner: string
    project: string
    topics?: string[]
    topic_categories: string[]
    category_hierarchy?: Record<string, unknown>
    canonical_doc_id: string
    llm_used?: boolean
  }
  quality: Record<string, number>
}> => {
  const response = await axiosInstance.post(`/ontology/unified-metadata/${encodeURIComponent(itemId)}/ai-fill`, payload ?? {})
  return response.data
}

export const updateOntologyProfile = async (
  itemId: string,
  payload: { owner?: string; project?: string; topic_categories?: string[]; canonical_doc_id?: string }
): Promise<{ item_id: string; profile: { owner: string; project: string; topic_categories: string[]; canonical_doc_id: string }; quality: Record<string, number> }> => {
  const response = await axiosInstance.put(`/ontology/unified-metadata/${encodeURIComponent(itemId)}`, payload)
  return response.data
}

export const getOntologyEntityMaster = async (
  limit: number = 200,
  offset: number = 0
): Promise<OntologyEntityMasterResponse> => {
  const response = await axiosInstance.get(`/ontology/entity-master?limit=${limit}&offset=${offset}`)
  return response.data
}

export const getOntologyAuditLogs = async (
  limit: number = 200,
  offset: number = 0
): Promise<OntologyAuditResponse> => {
  const response = await axiosInstance.get(`/ontology/audit?limit=${limit}&offset=${offset}`)
  return response.data
}

export const getConnectOverview = async (): Promise<ConnectOverviewResponse> => {
  const response = await axiosInstance.get('/connect/overview')
  return response.data
}

export const getConnectSubscriptions = async (): Promise<ConnectSubscriptionsResponse> => {
  const response = await axiosInstance.get('/connect/subscriptions')
  return response.data
}

export const deleteConnectSubscription = async (
  subscriptionId: string,
  connectionId: string = '',
  deleteMode: 'safe' | 'force' = 'safe'
): Promise<ConnectSubscriptionDeleteResponse> => {
  const encodedSubscriptionId = encodeURIComponent(subscriptionId)
  const encodedConnectionId = encodeURIComponent(connectionId)
  const encodedDeleteMode = encodeURIComponent(deleteMode)
  const response = await axiosInstance.delete(
    `/connect/subscriptions/${encodedSubscriptionId}?connection_id=${encodedConnectionId}&delete_mode=${encodedDeleteMode}`
  )
  return response.data
}

export const getConnectScopes = async (
  subscriptionId: string = ''
): Promise<ConnectScopesResponse> => {
  const encodedSubscriptionId = encodeURIComponent(subscriptionId)
  const response = await axiosInstance.get(`/connect/scopes?subscription_id=${encodedSubscriptionId}`)
  return response.data
}

export const getConnectEvents = async (
  scopeId: string = '',
  status: string = '',
  limit: number = 100,
  offset: number = 0
): Promise<ConnectEventsResponse> => {
  const encodedScopeId = encodeURIComponent(scopeId)
  const encodedStatus = encodeURIComponent(status)
  const response = await axiosInstance.get(
    `/connect/events?scope_id=${encodedScopeId}&status=${encodedStatus}&limit=${limit}&offset=${offset}`
  )
  return response.data
}

export const getConnectJobs = async (
  eventId: string = '',
  status: string = '',
  limit: number = 100,
  offset: number = 0
): Promise<ConnectJobsResponse> => {
  const encodedEventId = encodeURIComponent(eventId)
  const encodedStatus = encodeURIComponent(status)
  const response = await axiosInstance.get(
    `/connect/jobs?event_id=${encodedEventId}&status=${encodedStatus}&limit=${limit}&offset=${offset}`
  )
  return response.data
}

export const getConnectAuditLogs = async (
  q: string = '',
  limit: number = 100,
  offset: number = 0
): Promise<ConnectAuditResponse> => {
  const encodedQuery = encodeURIComponent(q)
  const response = await axiosInstance.get(`/connect/audit?q=${encodedQuery}&limit=${limit}&offset=${offset}`)
  return response.data
}

export const runConnectSyncCheck = async (
  requestedBy?: string
): Promise<ConnectSyncCheckApiResponse> => {
  const query = requestedBy ? `?requested_by=${encodeURIComponent(requestedBy)}` : ''
  const response = await axiosInstance.post(
    `/connect/sync/check${query}`
  )
  return response.data
}

export const createConnectOnboarding = async (
  payload: ConnectOnboardingRequest
): Promise<ConnectOnboardingResponse> => {
  const response = await axiosInstance.post('/connect/onboarding', payload)
  return response.data
}

export const getConnectOnboardingDefaults = async (): Promise<ConnectOnboardingDefaultsResponse> => {
  const response = await axiosInstance.get('/connect/onboarding/defaults')
  return response.data
}

export const resolveConnectSiteDiscovery = async (
  payload: ConnectSiteDiscoveryRequest
): Promise<ConnectSiteDiscoveryResponse> => {
  const response = await axiosInstance.post('/connect/onboarding/site-discovery', payload)
  return response.data
}

export const getConnectSiteOptions = async (
  payload: ConnectSiteOptionsRequest
): Promise<ConnectSiteOptionsResponse> => {
  const response = await axiosInstance.post('/connect/onboarding/site-options', payload)
  return response.data
}

export const getConnectTeamChannelOptions = async (
  payload: ConnectTeamChannelOptionsRequest
): Promise<ConnectTeamChannelOptionsResponse> => {
  const response = await axiosInstance.post('/connect/onboarding/team-channel-options', payload)
  return response.data
}

export const getAuditRecords = async (
  domain: AuditDomainFilter = 'all',
  q: string = '',
  limit: number = 200,
  offset: number = 0
): Promise<AuditRecordsApiResponse> => {
  const response = await axiosInstance.get(
    `/audit/records?domain=${encodeURIComponent(domain)}&q=${encodeURIComponent(q)}&limit=${limit}&offset=${offset}`
  )
  return response.data
}

export const createAuditExport = async (
  domain: AuditDomainFilter = 'all',
  q: string = '',
  format: 'csv' | 'pdf' = 'csv'
): Promise<AuditExportAcceptedApiResponse> => {
  const response = await axiosInstance.post('/audit/exports', { domain, q, format })
  return response.data
}

export const getAuditExportStatus = async (jobId: string): Promise<AuditExportStatusApiResponse> => {
  const response = await axiosInstance.get(`/audit/exports/${encodeURIComponent(jobId)}`)
  return response.data
}

export const getDashboardReadiness = async (): Promise<DashboardReadinessApiResponse> => {
  const response = await axiosInstance.get('/dashboard/readiness')
  return response.data
}

export const getDashboardReadinessTrend = async (): Promise<DashboardReadinessTrendApiResponse> => {
  const response = await axiosInstance.get('/dashboard/readiness/trend')
  return response.data
}

export const getDashboardRecommendedActions = async (): Promise<DashboardRecommendedActionsApiResponse> => {
  const response = await axiosInstance.get('/dashboard/recommended-actions')
  return response.data
}

export const resolveOntologyEntityCandidateWithExisting = async (
  candidateId: string,
  targetEntityId: string
): Promise<OntologyEntityCandidateResolveResponse> => {
  const response = await axiosInstance.post(
    `/ontology/entity-candidates/${encodeURIComponent(candidateId)}/resolve-existing?target_entity_id=${encodeURIComponent(targetEntityId)}`
  )
  return response.data
}

export const registerOntologyEntityCandidateAsNew = async (
  candidateId: string
): Promise<OntologyEntityCandidateResolveResponse> => {
  const response = await axiosInstance.post(
    `/ontology/entity-candidates/${encodeURIComponent(candidateId)}/register-new`
  )
  return response.data
}

export const checkHealth = async (): Promise<
  GraphsuiteStatus | { status: 'error'; message: string }
> => {
  try {
    return await getWithRetry<GraphsuiteStatus>('/health', 1, 200)
  } catch (error) {
    try {
      await getWithRetry<{ status: 'healthy'; service?: string }>('/healthz', 1, 200)
      return {
        status: 'healthy',
        working_directory: '-',
        input_directory: '-',
        configuration: {
          llm_binding: '-',
          llm_binding_host: '-',
          llm_model: '-',
          embedding_binding: '-',
          embedding_binding_host: '-',
          embedding_model: '-',
          kv_storage: '-',
          doc_status_storage: '-',
          graph_storage: '-',
          vector_storage: '-',
          summary_language: '-',
          force_llm_summary_on_merge: false,
          max_parallel_insert: 0,
          max_async: 0,
          embedding_func_max_async: 0,
          embedding_batch_num: 0,
          cosine_threshold: 0,
          min_rerank_score: 0,
          related_chunk_number: 0
        },
        pipeline_busy: false,
        keyed_locks: {
          process_id: 0,
          cleanup_performed: {
            mp_cleaned: 0,
            async_cleaned: 0
          },
          current_status: {
            total_mp_locks: 0,
            pending_mp_cleanup: 0,
            total_async_locks: 0,
            pending_async_cleanup: 0
          }
        }
      }
    } catch {
      // fall through
    }
    return {
      status: 'error',
      message: errorMessage(error)
    }
  }
}

export const getAuthStatus = async (): Promise<AuthStatusResponse> => {
  try {
    // Add a timeout to the request to prevent hanging
    const response = await axiosInstance.get('/auth-status', {
      timeout: 5000, // 5 second timeout
      headers: {
        'Accept': 'application/json' // Explicitly request JSON
      }
    });

    // Check if response is HTML (which indicates a redirect or wrong endpoint)
    const contentType = response.headers['content-type'] || '';
    if (contentType.includes('text/html')) {
      console.warn('Received HTML response instead of JSON for auth-status endpoint');
      return {
        auth_configured: true,
        auth_mode: 'enabled'
      };
    }

    // Strict validation of the response data
    if (response.data &&
        typeof response.data === 'object' &&
        'auth_configured' in response.data &&
        typeof response.data.auth_configured === 'boolean') {

      // For unconfigured auth, ensure we have an access token
      if (!response.data.auth_configured) {
        if (response.data.access_token && typeof response.data.access_token === 'string') {
          return response.data;
        } else {
          console.warn('Auth not configured but no valid access token provided');
        }
      } else {
        // For configured auth, just return the data
        return response.data;
      }
    }

    // If response data is invalid but we got a response, log it
    console.warn('Received invalid auth status response:', response.data);

    // Default to auth configured if response is invalid
    return {
      auth_configured: true,
      auth_mode: 'enabled'
    };
  } catch (error) {
    // If the request fails, assume authentication is configured
    console.error('Failed to get auth status:', errorMessage(error));
    return {
      auth_configured: true,
      auth_mode: 'enabled'
    };
  }
}

export const loginToServer = async (username: string, password: string): Promise<LoginResponse> => {
  const formData = new FormData();
  formData.append('username', username);
  formData.append('password', password);

  const response = await axiosInstance.post('/login', formData, {
    headers: {
      'Content-Type': 'multipart/form-data'
    }
  });

  return response.data;
}

/**
 * Updates an entity's properties in the knowledge graph
 * @param entityName The name of the entity to update
 * @param updatedData Dictionary containing updated attributes
 * @param allowRename Whether to allow renaming the entity (default: false)
 * @returns Promise with the updated entity information
 */
export const updateEntity = async (
  entityName: string,
  updatedData: Record<string, any>,
  allowRename: boolean = false
): Promise<DocActionResponse> => {
  const response = await axiosInstance.post('/graph/entity/edit', {
    entity_name: entityName,
    updated_data: updatedData,
    allow_rename: allowRename
  })
  return response.data
}

/**
 * Updates a relation's properties in the knowledge graph
 * @param sourceEntity The source entity name
 * @param targetEntity The target entity name
 * @param updatedData Dictionary containing updated attributes
 * @returns Promise with the updated relation information
 */
export const updateRelation = async (
  sourceEntity: string,
  targetEntity: string,
  updatedData: Record<string, any>
): Promise<DocActionResponse> => {
  const response = await axiosInstance.post('/graph/relation/edit', {
    source_id: sourceEntity,
    target_id: targetEntity,
    updated_data: updatedData
  })
  return response.data
}

/**
 * Checks if an entity name already exists in the knowledge graph
 * @param entityName The entity name to check
 * @returns Promise with boolean indicating if the entity exists
 */
export const checkEntityNameExists = async (entityName: string): Promise<boolean> => {
  try {
    const response = await axiosInstance.get(`/graph/entity/exists?name=${encodeURIComponent(entityName)}`)
    return response.data.exists
  } catch (error) {
    console.error('Error checking entity name:', error)
    return false
  }
}

