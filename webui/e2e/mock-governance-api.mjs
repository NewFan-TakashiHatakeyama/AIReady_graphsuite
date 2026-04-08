import http from 'node:http'
import { URL } from 'node:url'

const PORT = Number(process.env.MOCK_API_PORT || 9621)
const exportJobs = new Map()

/** Playwright: Connect 削除 → Governance 一覧の連動を検証する専用テナント */
const CONNECT_E2E_TENANT = 'tenant-connect-e2e'

const initialConnectE2eSubscription = () => ({
  id: 'sub-e2e-connect',
  connection_id: 'conn-e2e-connect',
  connection_name: 'E2E Test Connection',
  resource: 'https://graph.microsoft.com/v1.0/drives/drive-e2e/root',
  expiration_at: '2027-01-01T00:00:00Z',
  client_state_verified: true,
  status: 'active',
  resource_type: 'drive',
  target_type: 'drive'
})

let connectE2eSubscriptions = [initialConnectE2eSubscription()]
let connectE2eFindingVisible = true

const resetConnectDeleteE2eState = () => {
  connectE2eSubscriptions = [initialConnectE2eSubscription()]
  connectE2eFindingVisible = true
}

const json = (res, statusCode, payload, headers = {}) => {
  res.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'access-control-allow-origin': '*',
    ...headers
  })
  res.end(JSON.stringify(payload))
}

const readBody = async (req) =>
  await new Promise((resolve) => {
    let data = ''
    req.on('data', (chunk) => {
      data += String(chunk || '')
    })
    req.on('end', () => resolve(data))
  })

const decodeTokenPayload = (authHeader) => {
  const token = String(authHeader || '').replace(/^Bearer\s+/i, '').trim()
  if (!token) return {}
  const parts = token.split('.')
  if (parts.length < 2) return {}
  try {
    const payload = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'))
    return payload
  } catch {
    return {}
  }
}

const resolveTenantId = (payload) =>
  String(
    payload.tenant_id ||
      payload['custom:tenant_id'] ||
      (payload.metadata && payload.metadata.tenant_id) ||
      ''
  ).trim()

const ensureNoTenantOverride = async (req, urlObj) => {
  if (urlObj.searchParams.has('tenant_id')) {
    return {
      ok: false,
      statusCode: 400,
      payload: { detail: 'tenant_id must not be provided in request query.' }
    }
  }
  if (req.method === 'POST') {
    const raw = await readBody(req)
    if (raw) {
      try {
        const body = JSON.parse(raw)
        if (body && typeof body === 'object' && 'tenant_id' in body) {
          return {
            ok: false,
            statusCode: 400,
            payload: { detail: 'tenant_id must not be provided in request body.' }
          }
        }
      } catch {
        // Ignore non-JSON payloads for this mock.
      }
    }
  }
  return { ok: true }
}

/** Aligns with api/services/dashboard_audit_service.py when Connect + pipeline data are absent. */
const buildEmptyTenantData = (normalized) => {
  const suffix = normalized.replace(/[^a-zA-Z0-9_-]/g, '-')
  return {
    overview: {
      high_risk_count: 0,
      action_required_count: 0,
      expiring_suppressions_24h: 0,
      last_batch_run_at: '',
      governance_score: 100,
      subscores: {
        sensitive_protection: 100,
        oversharing_control: 100,
        assurance: 100
      },
      subscores_breakdown: {
        sensitive_protection: [],
        oversharing_control: [],
        assurance: []
      },
      coverage: {
        coverage_score: 1,
        inventory_coverage: 1,
        content_scan_coverage: 1,
        supported_format_coverage: 1,
        fresh_scan_coverage: 1,
        permission_detail_coverage: 1
      },
      confidence: { level: 'High', scan_confidence: 1 },
      risk_summary: { governance_raw: 0, exception_debt: 0, coverage_penalty: 0 },
      protection_scores: {
        oversharing_protection: 100,
        sensitivity_protection: 100,
        overall: 100
      },
      counts: { total_findings: 0, active_findings: 0, acknowledged: 0 }
    },
    findings: {
      rows: [],
      pagination: { limit: 300, offset: 0, total_count: 0 }
    },
    suppressions: {
      rows: [],
      pagination: { limit: 300, offset: 0, total_count: 0 }
    },
    policies: { global: {}, scope: [] },
    scanJobs: {
      rows: [],
      pagination: { limit: 200, offset: 0, total_count: 0 }
    },
    audit: {
      rows: [],
      pagination: { limit: 200, offset: 0, total_count: 0 }
    },
    dashboard: {
      readiness: {
        readiness_score: null,
        target_score: 90,
        signals: [
          {
            key: 'governance_sensitive',
            label: '機微情報の保護',
            score: null,
            issues: 0,
            target: 90,
            sub_metrics: []
          },
          {
            key: 'governance_oversharing',
            label: '過剰共有の抑制',
            score: null,
            issues: 0,
            target: 90,
            sub_metrics: []
          },
          {
            key: 'governance_assurance',
            label: '運用・保証',
            score: null,
            issues: 0,
            target: 90,
            sub_metrics: []
          },
          {
            key: 'ontology_foundation',
            label: '情報整備スコア',
            score: null,
            issues: 0,
            target: 90,
            sub_metrics: []
          },
          {
            key: 'ontology_usecase',
            label: 'ユースケース解決力',
            score: null,
            issues: 0,
            target: 90,
            sub_metrics: []
          }
        ]
      },
      trend: { rows: [], target_score: 90, source: 'insufficient_data' },
      kpis: { pending: 0, executing: 0, overdue: 0, rollback_required: 0 },
      cards: {
        rows: [],
        pagination: { limit: 100, offset: 0, total_count: 0 }
      },
      plan: {
        exists: false,
        operation: { plan_id: `plan-${suffix}-001` }
      }
    },
    ontology: {
      overview: {
        unified_document_count: 0,
        entity_candidate_count: 0,
        unresolved_candidates: 0,
        stale_or_aging_documents: 0,
        high_spread_entities: 0,
        ontology_score: 0,
        base_ontology_score: 0,
        use_case_readiness: 0,
        signal_scores: { freshness: 0, duplication: 0, location: 0, overall: 0 }
      },
      unified: {
        rows: [],
        pagination: { limit: 200, offset: 0, total_count: 0 }
      },
      entityMaster: {
        rows: [],
        pagination: { limit: 200, offset: 0, total_count: 0 }
      },
      audit: {
        rows: [],
        pagination: { limit: 200, offset: 0, total_count: 0 }
      },
      candidates: {
        rows: [],
        pagination: { limit: 100, offset: 0, total_count: 0 }
      }
    }
  }
}

const createTenantData = (tenantId) => {
  const normalized = tenantId || 'tenant-unknown'
  const useReadinessDemo =
    normalized === 'tenant-a' ||
    normalized === 'tenant-b' ||
    String(process.env.MOCK_READINESS_DEMO || '').trim() === '1'
  if (!useReadinessDemo) {
    return buildEmptyTenantData(normalized)
  }
  const suffix = normalized.replace(/[^a-zA-Z0-9_-]/g, '-')
  const gs = normalized === 'tenant-a' ? 74.1 : 92.3
  const go = normalized === 'tenant-a' ? 81.2 : 90.5
  const ga = normalized === 'tenant-a' ? 69.8 : 88.4
  const of = normalized === 'tenant-a' ? 35.0 : 62.0
  const ou = normalized === 'tenant-a' ? 0.0 : 45.0
  const readinessAvg = Math.round(((gs + go + ga + of + ou) / 5) * 10) / 10
  return {
    overview: {
      high_risk_count: normalized === 'tenant-a' ? 4 : 1,
      action_required_count: normalized === 'tenant-a' ? 6 : 2,
      expiring_suppressions_24h: normalized === 'tenant-a' ? 2 : 0,
      last_batch_run_at: '2026-03-10T08:30:00Z',
      governance_score: normalized === 'tenant-a' ? 78.4 : 91.2,
      subscores: {
        sensitive_protection: normalized === 'tenant-a' ? 74.1 : 92.3,
        oversharing_control: normalized === 'tenant-a' ? 81.2 : 90.5,
        assurance: normalized === 'tenant-a' ? 69.8 : 88.4
      },
      subscores_breakdown: {
        sensitive_protection: [
          { key: 'sensitive_exposure_residual', label: '機微情報露出リスク', value: 0.26, score: 74.0 },
          { key: 'label_coverage', label: 'ラベル付与率', value: 0.15, score: 85.0 }
        ],
        oversharing_control: [
          { key: 'broad_audience_risk', label: '公開到達範囲リスク', value: 0.19, score: 81.0 },
          { key: 'public_link_risk', label: '公開リンクリスク', value: 0.11, score: 89.0 },
          { key: 'discoverability_risk', label: '発見可能性リスク', value: 0.12, score: 88.0 }
        ],
        assurance: [
          { key: 'coverage_score', label: 'カバレッジ', value: normalized === 'tenant-a' ? 0.54 : 0.86, score: normalized === 'tenant-a' ? 54.0 : 86.0 },
          { key: 'scan_confidence', label: 'スキャン信頼度', value: normalized === 'tenant-a' ? 0.62 : 0.9, score: normalized === 'tenant-a' ? 62.0 : 90.0 }
        ]
      },
      coverage: {
        coverage_score: normalized === 'tenant-a' ? 0.54 : 0.86,
        inventory_coverage: normalized === 'tenant-a' ? 0.66 : 0.92,
        content_scan_coverage: normalized === 'tenant-a' ? 0.51 : 0.88,
        supported_format_coverage: normalized === 'tenant-a' ? 0.73 : 0.91,
        fresh_scan_coverage: normalized === 'tenant-a' ? 0.47 : 0.82,
        permission_detail_coverage: normalized === 'tenant-a' ? 0.58 : 0.79
      },
      confidence: {
        level: normalized === 'tenant-a' ? 'Medium' : 'High',
        scan_confidence: normalized === 'tenant-a' ? 0.62 : 0.9
      },
      risk_summary: {
        governance_raw: normalized === 'tenant-a' ? 0.42 : 0.11,
        exception_debt: normalized === 'tenant-a' ? 0.28 : 0.06,
        coverage_penalty: normalized === 'tenant-a' ? 0.46 : 0.14
      },
      protection_scores: {
        oversharing_protection: 78.5,
        sensitivity_protection: 82.0,
        overall: 80.2
      },
      counts: {
        total_findings: normalized === 'tenant-a' ? 12 : 5,
        acknowledged: normalized === 'tenant-a' ? 3 : 1
      }
    },
    findings: {
      rows: [
        {
          tenant_id: normalized,
          finding_id: `finding-${suffix}-001`,
          source: 'm365',
          item_id: `item-${suffix}-001`,
          item_name: `doc-${suffix}.docx`,
          item_url: `/${normalized}/doc-${suffix}.docx`,
          risk_score: normalized === 'tenant-a' ? 7.2 : 4.1,
          raw_residual_risk: normalized === 'tenant-a' ? 0.72 : 0.41,
          risk_level: normalized === 'tenant-a' ? 'high' : 'medium',
          status: 'open',
          workflow_status: 'acknowledged',
          exception_type: 'temporary_accept',
          exception_review_due_at: '2026-03-11T00:00:00Z',
          sensitive_composite: normalized === 'tenant-a' ? 0.66 : 0.28,
          age_factor: 0.45,
          exception_factor: 0.65,
          asset_criticality: 1.25,
          scan_confidence: 0.9,
          ai_reachability: 0.8,
          matched_guards: ['G3', 'G6'],
          guard_reason_codes: ['g3_public_link', 'g3_external_direct_share'],
          detection_reasons: ['scenario_b_external_direct_share', 'scenario_c_public_link'],
          finding_evidence: {
            external_recipients: ['stayhungry.stayfoolish.1990@gmail.com'],
            acl_drift_diff: [],
            anonymous_links: ['https://example.invalid/anon'],
            org_edit_links: [],
            permission_targets: []
          },
          decision: 'review',
          effective_policy_id: 'pol-org-001',
          effective_policy_version: 1,
          matched_policy_ids: ['pol-org-001'],
          decision_trace: ['pol-org-001:review'],
          reason_codes: ['g3_public_link'],
          remediation_mode: 'approval',
          remediation_action: 'remove_permissions',
          last_evaluated_at: '2026-03-10T08:40:00Z'
        },
        ...(normalized === 'tenant-a'
          ? [
              {
                tenant_id: normalized,
                finding_id: 'finding-tenant-a-remediation-e2e',
                source: 'm365',
                item_id: 'item-tenant-a-remediation-e2e',
                item_name: 'remediation-e2e-target.docx',
                item_url: `/${normalized}/remediation-e2e-target.docx`,
                risk_score: 55.0,
                raw_residual_risk: 0.55,
                risk_level: 'high',
                status: 'completed',
                workflow_status: 'normal',
                exception_type: 'none',
                remediation_state: 'executed',
                sensitive_composite: 0.3,
                age_factor: 0.4,
                exception_factor: 1.0,
                asset_criticality: 1.0,
                scan_confidence: 0.85,
                ai_reachability: 0.7,
                matched_guards: ['G2'],
                guard_reason_codes: ['g7_no_label'],
                detection_reasons: ['scenario_label_missing'],
                finding_evidence: {
                  external_recipients: [],
                  acl_drift_diff: [],
                  anonymous_links: [],
                  org_edit_links: [],
                  permission_targets: []
                },
                decision: 'block',
                effective_policy_id: 'pol-org-001',
                effective_policy_version: 1,
                matched_policy_ids: ['pol-org-001'],
                decision_trace: ['pol-org-001:block'],
                reason_codes: ['all_users_broad_share'],
                remediation_mode: 'owner_review',
                remediation_action: 'notify_site_owner',
                last_evaluated_at: '2026-03-10T09:00:00Z'
              },
              {
                tenant_id: normalized,
                finding_id: 'finding-tenant-a-approve-camel-e2e',
                source: 'm365',
                item_id: 'item-tenant-a-approve-camel-e2e',
                item_name: 'approve-camel-e2e.docx',
                item_url: `/${normalized}/approve-camel-e2e.docx`,
                risk_score: 48.0,
                raw_residual_risk: 0.48,
                risk_level: 'high',
                status: 'open',
                workflow_status: 'normal',
                exception_type: 'none',
                remediation_state: 'pending_approval',
                sensitive_composite: 0.4,
                age_factor: 0.4,
                exception_factor: 1.0,
                asset_criticality: 1.0,
                scan_confidence: 0.85,
                ai_reachability: 0.7,
                matched_guards: ['G2'],
                guard_reason_codes: ['g7_no_label'],
                detection_reasons: ['scenario_label_missing'],
                finding_evidence: {
                  external_recipients: [],
                  acl_drift_diff: [],
                  anonymous_links: [],
                  org_edit_links: [],
                  permission_targets: []
                },
                decision: 'review',
                effective_policy_id: 'pol-org-001',
                effective_policy_version: 1,
                matched_policy_ids: ['pol-org-001'],
                decision_trace: ['pol-org-001:review'],
                reason_codes: ['all_users_broad_share'],
                remediation_mode: 'approval',
                remediation_action: 'remove_permissions',
                last_evaluated_at: '2026-03-10T09:15:00Z'
              }
            ]
          : [])
      ],
      pagination: {
        limit: 300,
        offset: 0,
        total_count: normalized === 'tenant-a' ? 3 : 1
      }
    },
    suppressions: {
      rows: [
        {
          finding_id: `finding-${suffix}-001`,
          status: 'open',
          workflow_status: 'acknowledged',
          exception_type: 'temporary_accept',
          exception_review_due_at: '2026-03-11T00:00:00Z',
          raw_residual_risk: normalized === 'tenant-a' ? 0.72 : 0.41,
          suppress_until: '2026-03-11T00:00:00Z',
          acknowledged_by: 'operator-a',
          acknowledged_reason: `suppression-${suffix}`
        }
      ],
      pagination: { limit: 300, offset: 0, total_count: 1 }
    },
    policies: {
      global: {
        '/aiready/governance/risk_score_threshold': '2.0'
      },
      scope: [],
      global_policies: [{ name: '/aiready/governance/risk_score_threshold', value: '2.0' }],
      global_policy_rows: [
        {
          policy_id: 'global-policy-public-link',
          name: '公開リンク遮断ポリシー',
          layer: 'organization',
          scope_type: 'organization',
          status: 'active',
          priority: 900,
          rules: [
            {
              rule_id: 'deny-public-link',
              vector: 'public_link',
              effect: 'block',
              severity: 'critical',
              remediation_mode: 'auto',
              remediation_action: 'remove_permissions'
            }
          ],
          scope: { scope_type: 'organization' },
          rollout: { stage: 'active', dry_run: false }
        }
      ],
      scope_policies: [],
      policy_versions: [],
      estimated_impacts: []
    },
    scanJobs: {
      rows: [
        {
          tenant_id: normalized,
          job_id: `job-${suffix}-001`,
          status: 'success',
          accepted_at: '2026-03-10T08:30:00Z',
          completed_at: '2026-03-10T08:31:10Z',
          correlation_id: `corr-${suffix}-scan`
        }
      ],
      pagination: { limit: 200, offset: 0, total_count: 1 }
    },
    audit: {
      rows: [
        {
          tenant_id: normalized,
          event: 'governance.findings.read',
          timestamp: '2026-03-10T08:42:00Z',
          operator: 'e2e-user',
          target: `tenant:${normalized}`,
          correlation_id: `corr-${suffix}-audit`
        }
      ],
      pagination: { limit: 200, offset: 0, total_count: 1 }
    },
    dashboard: {
      readiness: {
        readiness_score: readinessAvg,
        target_score: 90,
        signals: [
          {
            key: 'governance_sensitive',
            label: '機微情報の保護',
            score: gs,
            issues: normalized === 'tenant-a' ? 6 : 2,
            target: 90,
            sub_metrics: [
              { key: 'sensitive_protection', label: '機微情報保護スコア', value: gs, unit: 'score' },
              {
                key: 'action_required_count',
                label: '対応要件数',
                value: normalized === 'tenant-a' ? 6 : 2,
                unit: 'count'
              },
              {
                key: 'total_findings',
                label: '検知総数',
                value: normalized === 'tenant-a' ? 12 : 5,
                unit: 'count'
              }
            ]
          },
          {
            key: 'governance_oversharing',
            label: '過剰共有の抑制',
            score: go,
            issues: normalized === 'tenant-a' ? 4 : 1,
            target: 90,
            sub_metrics: [
              { key: 'oversharing_control', label: '過剰共有保護スコア', value: go, unit: 'score' },
              {
                key: 'high_risk_findings',
                label: '高リスク検知件数',
                value: normalized === 'tenant-a' ? 4 : 1,
                unit: 'count'
              },
              {
                key: 'expiring_suppressions_24h',
                label: '24h内期限切れ抑止',
                value: normalized === 'tenant-a' ? 2 : 0,
                unit: 'count'
              }
            ]
          },
          {
            key: 'governance_assurance',
            label: '運用・保証',
            score: ga,
            issues: normalized === 'tenant-a' ? 3 : 1,
            target: 90,
            sub_metrics: [
              { key: 'assurance_score', label: '運用・保証スコア', value: ga, unit: 'score' },
              {
                key: 'acknowledged_findings',
                label: '対応中（acknowledged）',
                value: normalized === 'tenant-a' ? 3 : 1,
                unit: 'count'
              },
              {
                key: 'total_findings',
                label: '検知総数',
                value: normalized === 'tenant-a' ? 12 : 5,
                unit: 'count'
              }
            ]
          },
          {
            key: 'ontology_foundation',
            label: '情報整備スコア',
            score: of,
            issues: normalized === 'tenant-a' ? 4 : 2,
            target: 90,
            sub_metrics: [
              { key: 'base_ontology_score', label: '情報整備スコア', value: of, unit: 'score' },
              {
                key: 'stale_or_aging_documents',
                label: '鮮度課題ドキュメント',
                value: normalized === 'tenant-a' ? 4 : 2,
                unit: 'count'
              },
              {
                key: 'ontology_score',
                label: 'オントロジー総合スコア',
                value: normalized === 'tenant-a' ? 24.5 : 55.0,
                unit: 'score'
              }
            ]
          },
          {
            key: 'ontology_usecase',
            label: 'ユースケース解決力',
            score: ou,
            issues: normalized === 'tenant-a' ? 3 : 1,
            target: 90,
            sub_metrics: [
              { key: 'use_case_readiness', label: 'ユースケース解決力', value: ou, unit: 'score' },
              {
                key: 'unresolved_candidates',
                label: '未解決候補',
                value: normalized === 'tenant-a' ? 3 : 1,
                unit: 'count'
              },
              {
                key: 'high_spread_entities',
                label: '高分散エンティティ',
                value: normalized === 'tenant-a' ? 2 : 1,
                unit: 'count'
              }
            ]
          }
        ]
      },
      trend: {
        rows: [
          {
            label: 'T-4',
            governance_sensitive: Math.max(0, gs - 8),
            governance_oversharing: Math.max(0, go - 8),
            governance_assurance: Math.max(0, ga - 8),
            ontology_foundation: Math.max(0, of - 8),
            ontology_usecase: Math.max(0, ou - 8)
          },
          {
            label: 'T-3',
            governance_sensitive: Math.max(0, gs - 6),
            governance_oversharing: Math.max(0, go - 6),
            governance_assurance: Math.max(0, ga - 6),
            ontology_foundation: Math.max(0, of - 6),
            ontology_usecase: Math.max(0, ou - 6)
          },
          {
            label: 'T-2',
            governance_sensitive: Math.max(0, gs - 4),
            governance_oversharing: Math.max(0, go - 4),
            governance_assurance: Math.max(0, ga - 4),
            ontology_foundation: Math.max(0, of - 4),
            ontology_usecase: Math.max(0, ou - 4)
          },
          {
            label: 'T-1',
            governance_sensitive: Math.max(0, gs - 2),
            governance_oversharing: Math.max(0, go - 2),
            governance_assurance: Math.max(0, ga - 2),
            ontology_foundation: Math.max(0, of - 2),
            ontology_usecase: Math.max(0, ou - 2)
          },
          {
            label: 'Now',
            governance_sensitive: gs,
            governance_oversharing: go,
            governance_assurance: ga,
            ontology_foundation: of,
            ontology_usecase: ou
          }
        ],
        target_score: 90,
        source: 'estimated'
      },
      kpis: {
        pending: 2,
        executing: 1,
        overdue: 0,
        rollback_required: 0
      },
      cards: {
        rows: [
          {
            plan_id: `plan-${suffix}-001`,
            title: `Plan ${suffix}-001`,
            domain: 'ontology',
            workflow_status: 'pending_approval',
            execution_status: 'idle',
            correlation_id: `corr-${suffix}-plan-001`,
            priority: 'P2',
            group: 'ontology',
            assignee: 'automation'
          }
        ],
        pagination: { limit: 100, offset: 0, total_count: 1 }
      },
      plan: {
        exists: true,
        operation: {
          plan_id: `plan-${suffix}-001`,
          domain: 'ontology',
          workflow_status: 'pending_approval',
          execution_status: 'idle',
          correlation_id: `corr-${suffix}-plan-001`
        }
      }
    },
    ontology: {
      overview: {
        unified_document_count: 1,
        entity_candidate_count: 1,
        unresolved_candidates: normalized === 'tenant-a' ? 3 : 1,
        stale_or_aging_documents: normalized === 'tenant-a' ? 4 : 2,
        high_spread_entities: normalized === 'tenant-a' ? 2 : 1,
        ontology_score: normalized === 'tenant-a' ? 0.245 : 0.55,
        base_ontology_score: normalized === 'tenant-a' ? 0.35 : 0.62,
        use_case_readiness: normalized === 'tenant-a' ? 0 : 0.45,
        signal_scores: {
          freshness: 0.72,
          duplication: 0.81,
          location: 0.76,
          overall: 0.763
        }
      },
      unified: {
        rows: [
          {
            item_id: `item-${suffix}-001`,
            plan_id: `plan-${suffix}-001`,
            title: `契約台帳-${suffix}.docx`,
            source: 'm365',
            freshness_status: 'aging',
            ai_eligible: true,
            transformed_at: '2026-03-10T09:00:00Z',
            content_quality_score: 0.64,
            remediation_state: 'pending_approval'
          }
        ],
        pagination: { limit: 200, offset: 0, total_count: 1 }
      },
      entityMaster: {
        rows: [
          {
            entity_id: `ent-${suffix}-001`,
            canonical_name: `株式会社${suffix.toUpperCase()}`,
            entity_type: 'organization',
            pii_flag: false,
            spread_factor: 4,
            status: 'active',
            updated_at: '2026-03-10T09:05:00Z'
          }
        ],
        pagination: { limit: 200, offset: 0, total_count: 1 }
      },
      audit: {
        rows: [
          {
            event: 'ontology.candidate.resolve_existing',
            operator: 'e2e-user',
            correlation_id: `corr-${suffix}-ontology-audit`,
            timestamp: '2026-03-10T09:10:00Z',
            source: 'ontology'
          }
        ],
        pagination: { limit: 200, offset: 0, total_count: 1 }
      },
      candidates: {
        rows: [
          {
            candidate_id: `cand-${suffix}-001`,
            surface_form: `候補語-${suffix}`,
            entity_type: 'organization',
            extraction_source: 'governance+ner',
            confidence: 0.58,
            pii_flag: false,
            item_id: `item-${suffix}-001`,
            received_at: '2026-03-10T09:11:00Z',
            status: 'pending',
            suggestions: [
              {
                entity_id: `ent-${suffix}-001`,
                canonical_name: `株式会社${suffix.toUpperCase()}`,
                score: 0.83,
                confidence: 0.91,
                pii_flag: false,
                updated_at: '2026-03-10T09:00:00Z'
              }
            ]
          }
        ],
        pagination: { limit: 100, offset: 0, total_count: 1 }
      }
    }
  }
}

const server = http.createServer(async (req, res) => {
  const urlObj = new URL(req.url || '/', `http://${req.headers.host}`)

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET,POST,DELETE,OPTIONS',
      'access-control-allow-headers': 'content-type,authorization,x-api-key'
    })
    res.end()
    return
  }

  if (urlObj.pathname === '/health') {
    json(res, 200, {
      status: 'healthy',
      pipeline_busy: false,
      working_directory: '.',
      input_directory: '.',
      configuration: {
        llm_binding: 'mock',
        llm_binding_host: 'mock',
        llm_model: 'mock',
        embedding_binding: 'mock',
        embedding_binding_host: 'mock',
        embedding_model: 'mock',
        kv_storage: 'mock',
        doc_status_storage: 'mock',
        graph_storage: 'mock',
        vector_storage: 'mock',
        summary_language: 'ja',
        force_llm_summary_on_merge: false,
        max_parallel_insert: 1,
        max_async: 1,
        embedding_func_max_async: 1,
        embedding_batch_num: 1,
        cosine_threshold: 0.2,
        min_rerank_score: 0,
        related_chunk_number: 5
      },
      production_gate: {
        overall: 'ok',
        strict_mode: true,
        checks: [
          { name: 'ontology_dummy_fallback_disabled', status: 'ok', detail: 'ok' },
          { name: 'governance_cloudwatch_fallback_disabled', status: 'ok', detail: 'ok' },
          { name: 'governance_policy_scope_fallback_disabled', status: 'ok', detail: 'ok' },
          { name: 'ontology_ops_monitoring', status: 'ok', detail: 'ok' }
        ]
      }
    })
    return
  }

  if (urlObj.pathname === '/auth-status') {
    json(res, 200, { auth_configured: true, auth_mode: 'enabled' })
    return
  }

  const guard = await ensureNoTenantOverride(req, urlObj)
  if (!guard.ok) {
    json(res, guard.statusCode, guard.payload)
    return
  }

  const tokenPayload = decodeTokenPayload(req.headers.authorization)
  const tenantId = resolveTenantId(tokenPayload)
  if (!tenantId && urlObj.pathname.startsWith('/governance')) {
    json(res, 401, { detail: 'JWT token is required to resolve tenant context.' })
    return
  }

  const safeTenantLabel = tenantId || 'unknown'
  const correlationId = `corr-${safeTenantLabel.replace(/[^a-zA-Z0-9_-]/g, '-')}-req`
  const responseHeaders = { 'x-correlation-id': correlationId }

  if (urlObj.pathname === '/__e2e/reset-connect-delete' && req.method === 'POST') {
    resetConnectDeleteE2eState()
    json(res, 200, { status: 'ok' }, responseHeaders)
    return
  }

  if (urlObj.pathname === '/connect/overview' && req.method === 'GET') {
    json(
      res,
      200,
      {
        tenant_id: safeTenantLabel,
        delivery_status: 'healthy',
        queue_backlog: 0,
        failed_jobs_24h: 0,
        next_subscription_renewal_at: '-',
        next_token_renewal_at: '-',
        next_delta_sync_at: '-'
      },
      responseHeaders
    )
    return
  }

  if (urlObj.pathname === '/connect/subscriptions' && req.method === 'GET') {
    const rows = tenantId === CONNECT_E2E_TENANT ? [...connectE2eSubscriptions] : []
    json(
      res,
      200,
      {
        rows,
        pagination: { limit: 200, offset: 0, total_count: rows.length }
      },
      responseHeaders
    )
    return
  }

  if (urlObj.pathname === '/connect/events' && req.method === 'GET') {
    json(
      res,
      200,
      {
        rows: [],
        pagination: { limit: 300, offset: 0, total_count: 0 },
        resolved_tenant_id: safeTenantLabel
      },
      responseHeaders
    )
    return
  }

  const deleteSubMatch = urlObj.pathname.match(/^\/connect\/subscriptions\/([^/]+)$/)
  if (deleteSubMatch && req.method === 'DELETE') {
    const subId = decodeURIComponent(deleteSubMatch[1])
    if (tenantId === CONNECT_E2E_TENANT) {
      const idx = connectE2eSubscriptions.findIndex((r) => r.id === subId)
      if (idx >= 0) {
        connectE2eSubscriptions.splice(idx, 1)
      }
      connectE2eFindingVisible = false
    }
    json(
      res,
      200,
      {
        tenant_id: safeTenantLabel,
        connection_id: urlObj.searchParams.get('connection_id') || 'conn-e2e-connect',
        subscription_id: subId,
        delete_mode: urlObj.searchParams.get('delete_mode') || 'safe',
        status: 'deleted',
        graph_unsubscribe_status: 'ok',
        deleted_at: new Date().toISOString(),
        governance_findings_close: {
          file_metadata_rows: 1,
          findings_closed: 1,
          findings_attempted: 1
        }
      },
      responseHeaders
    )
    return
  }

  const tenantData = createTenantData(tenantId)

  if (urlObj.pathname === '/governance/overview' && req.method === 'GET') {
    let overview = tenantData.overview
    if (tenantId === CONNECT_E2E_TENANT) {
      overview = {
        ...overview,
        initial_scan_gate_open: true,
        counts: {
          ...(overview.counts || {}),
          total_findings: connectE2eFindingVisible ? 1 : 0,
          active_findings: connectE2eFindingVisible ? 1 : 0,
          acknowledged: overview.counts?.acknowledged ?? 0
        }
      }
    }
    json(res, 200, overview, responseHeaders)
    return
  }
  if (urlObj.pathname === '/governance/findings' && req.method === 'GET') {
    if (tenantId === CONNECT_E2E_TENANT) {
      const rows = connectE2eFindingVisible
        ? [
            {
              tenant_id: CONNECT_E2E_TENANT,
              finding_id: 'finding-connect-e2e-001',
              source: 'm365',
              item_id: 'item-connect-e2e-001',
              item_name: 'e2e-connect.docx',
              risk_score: 5.0,
              raw_residual_risk: 0.5,
              risk_level: 'medium',
              status: 'open',
              workflow_status: 'open',
              container_id: 'drive-e2e',
              container_type: 'drive'
            }
          ]
        : []
      json(
        res,
        200,
        {
          rows,
          pagination: { limit: 300, offset: 0, total_count: rows.length }
        },
        responseHeaders
      )
      return
    }
    json(res, 200, tenantData.findings, responseHeaders)
    return
  }
  if (urlObj.pathname === '/governance/suppressions' && req.method === 'GET') {
    json(res, 200, tenantData.suppressions, responseHeaders)
    return
  }
  if (urlObj.pathname === '/governance/policies' && req.method === 'GET') {
    json(res, 200, tenantData.policies, responseHeaders)
    return
  }
  if (urlObj.pathname === '/governance/policies' && req.method === 'POST') {
    const isDryRun = String(urlObj.searchParams.get('dry_run') || 'false').toLowerCase() === 'true'
    if (isDryRun) {
      json(
        res,
        200,
        {
          estimated_affected_items: 12,
          estimated_new_findings: 4,
          estimated_resolved_findings: 2,
          sample_targets: []
        },
        responseHeaders
      )
      return
    }
    json(res, 200, { status: 'saved', policy_id: `policy-${tenantId}-new`, version: 1 }, responseHeaders)
    return
  }
  if (urlObj.pathname === '/governance/policies/simulate' && req.method === 'POST') {
    json(
      res,
      200,
      {
        estimated_affected_items: 12,
        estimated_new_findings: 4,
        estimated_resolved_findings: 2,
        sample_targets: []
      },
      responseHeaders
    )
    return
  }
  const policyUpdateMatch = urlObj.pathname.match(/^\/governance\/policies\/(.+)$/)
  if (policyUpdateMatch && req.method === 'PUT') {
    const isDryRun = String(urlObj.searchParams.get('dry_run') || 'false').toLowerCase() === 'true'
    if (isDryRun) {
      json(
        res,
        200,
        {
          estimated_affected_items: 10,
          estimated_new_findings: 3,
          estimated_resolved_findings: 1,
          sample_targets: []
        },
        responseHeaders
      )
      return
    }
    const policyId = decodeURIComponent(policyUpdateMatch[1])
    json(
      res,
      200,
      {
        status: 'updated',
        policy_id: policyId,
        updated_at: new Date().toISOString()
      },
      responseHeaders
    )
    return
  }
  if (urlObj.pathname === '/governance/scans/daily' && req.method === 'POST') {
    json(
      res,
      200,
      {
        job_id: `job-${tenantId}-manual`,
        status: 'accepted',
        accepted_at: '2026-03-10T08:55:00Z'
      },
      responseHeaders
    )
    return
  }
  if (urlObj.pathname === '/governance/scan-jobs' && req.method === 'GET') {
    json(res, 200, tenantData.scanJobs, responseHeaders)
    return
  }
  if (urlObj.pathname === '/governance/audit' && req.method === 'GET') {
    json(res, 200, tenantData.audit, responseHeaders)
    return
  }

  const remediationApproveMatch = urlObj.pathname.match(
    /^\/governance\/findings\/([^/]+)\/remediation\/approve$/
  )
  if (remediationApproveMatch && req.method === 'POST') {
    const approveFid = decodeURIComponent(remediationApproveMatch[1])
    if (approveFid === 'finding-tenant-a-approve-camel-e2e') {
      json(
        res,
        200,
        {
          findingId: approveFid,
          tenantId: tenantId,
          remediationState: 'executed',
          remediationMode: 'approval',
          allowedActions: ['rollback'],
          lastExecutionId: 'exec-approve-camel-01',
          result: {
            phase: 'execute',
            Results: [
              {
                actionType: 'remove_permissions',
                permissionId: 'perm-camel-1',
                Status: 'deleted',
                httpStatus: 204
              }
            ]
          }
        },
        responseHeaders
      )
      return
    }
  }

  const remediationGetMatch = urlObj.pathname.match(/^\/governance\/findings\/([^/]+)\/remediation$/)
  if (remediationGetMatch && req.method === 'GET') {
    const fid = decodeURIComponent(remediationGetMatch[1])
    if (fid === 'finding-tenant-a-approve-camel-e2e') {
      json(
        res,
        200,
        {
          tenant_id: tenantId,
          finding_id: fid,
          remediation_state: 'pending_approval',
          remediation_mode: 'approval',
          actions: [
            {
              action_type: 'remove_permissions',
              action_id: 'rm-camel',
              executable: true,
              permission_ids: ['perm-camel-1']
            }
          ],
          allowed_actions: ['propose', 'approve'],
          version: 1
        },
        responseHeaders
      )
      return
    }
    if (fid === 'finding-tenant-a-remediation-e2e') {
      json(
        res,
        200,
        {
          tenant_id: tenantId,
          finding_id: fid,
          remediation_state: 'executed',
          actions: [
            {
              action_type: 'remove_permissions',
              action_id: 'rm-e2e',
              executable: true,
              permission_ids: ['perm-e2e-1']
            }
          ],
          allowed_actions: ['rollback'],
          version: 2,
          approved_by: 'e2e@example.com',
          approved_at: '2026-03-10T09:00:00Z',
          last_execution_id: 'gov-rem-e2e01',
          result: {
            phase: 'execute',
            results: [
              {
                action_type: 'remove_permissions',
                action_id: 'rm-e2e',
                status: 'deleted',
                permission_id: 'perm-e2e-1',
                http_status: 204
              }
            ],
            post_verification: {
              immediate_rescore: false,
              success: true,
              error: null,
              deferred_to: 'connect_filemetadata_stream'
            }
          },
          updated_at: '2026-03-10T09:05:00Z'
        },
        responseHeaders
      )
      return
    }
    json(
      res,
      200,
      {
        tenant_id: tenantId,
        finding_id: fid,
        remediation_state: 'ai_proposed',
        actions: [{ action_type: 'manual_review', action_id: 'm1', executable: true }],
        allowed_actions: ['propose', 'approve'],
        version: 1
      },
      responseHeaders
    )
    return
  }

  if (urlObj.pathname === '/dashboard/readiness' && req.method === 'GET') {
    json(res, 200, tenantData.dashboard.readiness, responseHeaders)
    return
  }
  if (urlObj.pathname === '/dashboard/readiness/trend' && req.method === 'GET') {
    json(res, 200, tenantData.dashboard.trend, responseHeaders)
    return
  }
  if (urlObj.pathname === '/dashboard/recommended-actions' && req.method === 'GET') {
    json(res, 200, { rows: [{ priority: 'P1', domain: 'ガバナンス', summary: '高リスク検知を確認' }] }, responseHeaders)
    return
  }
  if (urlObj.pathname === '/dashboard/remediation/kpis' && req.method === 'GET') {
    json(res, 200, tenantData.dashboard.kpis, responseHeaders)
    return
  }
  if (urlObj.pathname === '/dashboard/remediation/cards' && req.method === 'GET') {
    json(res, 200, tenantData.dashboard.cards, responseHeaders)
    return
  }
  if (urlObj.pathname.startsWith('/dashboard/remediation/plans/') && req.method === 'GET') {
    const planId = urlObj.pathname.split('/').pop()
    json(
      res,
      200,
      {
        plan_id: String(planId || ''),
        exists: true,
        operation: { ...tenantData.dashboard.plan.operation, plan_id: String(planId || '') }
      },
      responseHeaders
    )
    return
  }

  if (urlObj.pathname === '/audit/records' && req.method === 'GET') {
    json(
      res,
      200,
      {
        rows: [
          {
            domain: 'governance',
            audit_id: `audit-${tenantId}-001`,
            occurred_at: '2026-03-10T08:42:00Z',
            operator: 'e2e-user',
            action: 'governance.findings.read',
            target: `tenant:${tenantId}`,
            correlation_id: `corr-${tenantId}-audit-001`
          }
        ],
        pagination: { limit: 100, offset: 0, total_count: 1 }
      },
      responseHeaders
    )
    return
  }

  if (urlObj.pathname === '/ontology/overview' && req.method === 'GET') {
    json(res, 200, tenantData.ontology.overview, responseHeaders)
    return
  }
  if (urlObj.pathname === '/ontology/unified-metadata' && req.method === 'GET') {
    json(res, 200, tenantData.ontology.unified, responseHeaders)
    return
  }
  if (urlObj.pathname === '/ontology/entity-master' && req.method === 'GET') {
    json(res, 200, tenantData.ontology.entityMaster, responseHeaders)
    return
  }
  if (urlObj.pathname === '/ontology/audit' && req.method === 'GET') {
    json(res, 200, tenantData.ontology.audit, responseHeaders)
    return
  }
  if (urlObj.pathname === '/ontology/entity-candidates' && req.method === 'GET') {
    json(res, 200, tenantData.ontology.candidates, responseHeaders)
    return
  }
  if (
    urlObj.pathname.match(/^\/ontology\/entity-candidates\/[^/]+\/resolve-existing$/) &&
    req.method === 'POST'
  ) {
    const candidateId = urlObj.pathname.split('/')[3]
    json(
      res,
      200,
      {
        status: 'resolved',
        candidate_id: candidateId,
        resolution_type: 'merge_existing',
        entity_id: `ent-${tenantId}-001`,
        canonical_name: `株式会社${tenantId.toUpperCase()}`,
        resolved_by: 'e2e-user'
      },
      responseHeaders
    )
    return
  }
  if (
    urlObj.pathname.match(/^\/ontology\/entity-candidates\/[^/]+\/register-new$/) &&
    req.method === 'POST'
  ) {
    const candidateId = urlObj.pathname.split('/')[3]
    json(
      res,
      200,
      {
        status: 'resolved',
        candidate_id: candidateId,
        resolution_type: 'register_new',
        entity_id: `ent-${tenantId}-new-001`,
        canonical_name: `新規語-${tenantId}`,
        resolved_by: 'e2e-user'
      },
      responseHeaders
    )
    return
  }
  if (urlObj.pathname === '/ontology/graph/by-item' && req.method === 'GET') {
    const itemId = urlObj.searchParams.get('item_id') || `item-${tenantId}-001`
    json(
      res,
      200,
      {
        nodes: [
          {
            id: `doc:${itemId}`,
            labels: [`契約台帳-${tenantId}.docx`],
            properties: { entity_type: 'document', item_id: itemId }
          },
          {
            id: `org:${tenantId}`,
            labels: [`株式会社${tenantId.toUpperCase()}`],
            properties: { entity_type: 'organization' }
          }
        ],
        edges: [
          {
            id: `edge:${itemId}:owned_by`,
            source: `doc:${itemId}`,
            target: `org:${tenantId}`,
            type: 'owned_by',
            properties: { weight: 1 }
          }
        ],
        is_truncated: false,
        start_node_id: `doc:${itemId}`,
        matched_by: 'item_id'
      },
      responseHeaders
    )
    return
  }
  if (urlObj.pathname === '/audit/exports' && req.method === 'POST') {
    const raw = await readBody(req)
    const body = raw ? JSON.parse(raw) : {}
    const format = body.format === 'pdf' ? 'pdf' : 'csv'
    const jobId = `audit-export-${Math.random().toString(36).slice(2, 10)}`
    const token = `token-${Math.random().toString(36).slice(2, 14)}`
    exportJobs.set(jobId, {
      tenantId,
      token,
      used: false,
      format
    })
    json(res, 200, { job_id: jobId, status: 'accepted' }, responseHeaders)
    return
  }
  if (urlObj.pathname.startsWith('/audit/exports/') && req.method === 'GET') {
    const parts = urlObj.pathname.split('/').filter(Boolean)
    const jobId = parts[2]
    const isDownload = parts[3] === 'download'
    const job = exportJobs.get(jobId)
    if (!job || job.tenantId !== tenantId) {
      json(res, 404, { detail: 'Job not found.' })
      return
    }
    if (isDownload) {
      const token = urlObj.searchParams.get('token')
      if (!token || token !== job.token) {
        json(res, 400, { detail: 'Invalid token.' })
        return
      }
      if (job.used) {
        json(res, 400, { detail: 'Token already used.' })
        return
      }
      job.used = true
      const content = 'occurred_at,domain,operator,action,target,correlation_id,audit_id\n'
      res.writeHead(200, {
        'content-type': job.format === 'pdf' ? 'text/plain; charset=utf-8' : 'text/csv; charset=utf-8',
        'content-disposition': `attachment; filename=${jobId}.${job.format === 'pdf' ? 'txt' : 'csv'}`,
        'access-control-allow-origin': '*'
      })
      res.end(content)
      return
    }
    json(
      res,
      200,
      {
        job_id: jobId,
        status: 'completed',
        format: job.format,
        download_url: `/audit/exports/${jobId}/download?token=${job.token}`
      },
      responseHeaders
    )
    return
  }

  json(res, 404, { detail: `mock route not found: ${urlObj.pathname}` })
})

server.listen(PORT, '127.0.0.1', () => {
   
  console.log(`mock governance api listening on http://127.0.0.1:${PORT}`)
})
