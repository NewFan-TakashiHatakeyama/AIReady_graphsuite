import { useEffect, useMemo, useState } from 'react'
import Button from '@/components/ui/Button'
import { GovernanceRemediationDetailResponse } from '@/api/graphsuite'
import {
  executionResultRows,
  planActionType,
  planPermissionIds,
  remediationActionsList,
  remediationDetailState,
  resultRowPermissionId,
  resultRowStatus,
} from '@/features/governance/remediationPayloadFields'
import DryRunDiffPanel from '@/features/governance/components/DryRunDiffPanel'
import OperationConfirmDialog from '@/features/governance/components/OperationConfirmDialog'
import { DiffLine, JsonDiffNode, RemediationOperationRecord } from '@/features/governance/remediationTypes'
import { GovernanceFinding } from '../types'

interface RemediationWorkflowPanelProps {
  selectedFinding: GovernanceFinding
  remediationDetail: GovernanceRemediationDetailResponse | null
  showRollbackWorkflowCta: boolean
  remediationBusy: boolean
  canRollbackRemediation: boolean
  canProposeRemediation: boolean
  canApproveRemediation: boolean
  canExecuteRemediation: boolean
  impactSummaryText: string
  executionSummaryText: string
  onRollback: () => void
  onApprove: () => void
}

const extractUserEmailFromPermission = (permission: Record<string, any>): string => {
  for (const key of ['grantedToV2', 'grantedTo']) {
    const granted = permission?.[key]
    const email = String(granted?.user?.email ?? '').trim()
    if (email) return email
  }
  for (const key of ['grantedToIdentitiesV2', 'grantedToIdentities']) {
    const identities = permission?.[key]
    if (!Array.isArray(identities)) continue
    for (const identity of identities) {
      const email = String(identity?.user?.email ?? '').trim()
      if (email) return email
    }
  }
  const invitedEmail = String(permission?.invitation?.email ?? '').trim()
  return invitedEmail
}

/** JSON 構造差分と同一の before/after 文字列（Dry-run Diff の表示をこれに揃える） */
const buildRemovePermissionSnapshotStrings = (
  resultRow: Record<string, any> | undefined
): { before: string; after: string } => {
  const r = resultRow as Record<string, unknown> | undefined
  const rollbackData = (r?.rollback_data ?? r?.rollbackData ?? {}) as Record<string, any>
  const email = extractUserEmailFromPermission(rollbackData) || '-'
  const roles = Array.isArray(rollbackData.roles)
    ? rollbackData.roles.map((role) => String(role ?? '').trim()).filter(Boolean)
    : []
  const afterStatus = (r ? resultRowStatus(r) : '') || 'planned'
  return {
    before: `email=${email},roles=${roles.join('|') || '-'},state=exists`,
    after: `state=${afterStatus}`,
  }
}

const RemediationWorkflowPanel = ({
  selectedFinding,
  remediationDetail,
  showRollbackWorkflowCta,
  remediationBusy,
  canRollbackRemediation,
  canProposeRemediation,
  canApproveRemediation,
  canExecuteRemediation,
  impactSummaryText,
  executionSummaryText,
  onRollback,
  onApprove,
}: RemediationWorkflowPanelProps) => {
  const [confirmActionType, setConfirmActionType] = useState<'execute' | 'rollback' | null>(null)
  const [isConfirmDialogOpen, setIsConfirmDialogOpen] = useState(false)
  const remediationActions = useMemo(
    () =>
      remediationActionsList(remediationDetail).map((action) => ({
        actionType: planActionType(action),
        scope: String(action.scope ?? '').trim(),
        permissionIds: planPermissionIds(action),
      })),
    [remediationDetail]
  )
  const executionResultsByPermissionId = useMemo(() => {
    const rows = executionResultRows(remediationDetail)
    const map = new Map<string, Record<string, any>>()
    rows.forEach((row) => {
      const rec = row as Record<string, unknown>
      const permissionId = resultRowPermissionId(rec)
      if (permissionId) {
        map.set(permissionId, row as Record<string, any>)
      }
    })
    return map
  }, [remediationDetail])
  const manualActionCount = useMemo(
    () =>
      remediationActions.filter((action) => {
        const type = action.actionType.toLowerCase()
        return type === 'manual_review' || type === 'manual_required'
      }).length,
    [remediationActions]
  )
  const actionTypes = useMemo(
    () =>
      remediationActions
        .map((action) => action.actionType)
        .filter(Boolean),
    [remediationActions]
  )
  const dryRunDiffFiles = useMemo(() => {
    if (!remediationActions.length) {
      return [
        {
          filePath: selectedFinding.itemPath || selectedFinding.itemId || selectedFinding.id,
          section: 'governance-remediation',
          lines: [
            {
              oldLineNumber: 1,
              newLineNumber: 1,
              type: 'context' as const,
              oldText: `risk_score=${selectedFinding.riskScore.toFixed(2)}`,
              newText: `risk_score=${selectedFinding.riskScore.toFixed(2)}`,
            },
          ],
        },
      ]
    }
    return remediationActions.map((action, index) => {
      const lines: DiffLine[] = []
      const normalizedActionType = String(action.actionType || 'unknown').trim().toLowerCase()
      if (normalizedActionType === 'remove_permissions' && action.permissionIds.length > 0) {
        action.permissionIds.forEach((permissionId, pidIdx) => {
          const resultRow = executionResultsByPermissionId.get(permissionId)
          const { before, after } = buildRemovePermissionSnapshotStrings(resultRow)
          const baseLine = index * 100 + pidIdx + 1
          lines.push({
            oldLineNumber: baseLine,
            newLineNumber: baseLine,
            type: 'remove' as const,
            oldText: `Before: 対象権限ID=${permissionId} / ${before}`,
            newText: '',
          })
          lines.push({
            oldLineNumber: null,
            newLineNumber: baseLine,
            type: 'add' as const,
            oldText: '',
            newText: `After: 対象権限ID=${permissionId} / ${after}`,
          })
        })
      } else {
        lines.push(
          {
            oldLineNumber: index + 1,
            newLineNumber: index + 1,
            type: 'remove' as const,
            oldText: 'Before: state=planned',
            newText: '',
          },
          {
            oldLineNumber: null,
            newLineNumber: index + 1,
            type: 'add' as const,
            oldText: '',
            newText: 'After: state=applied',
          }
        )
      }
      return {
        filePath: selectedFinding.itemPath || selectedFinding.itemId || selectedFinding.id,
        section: action.scope || `action-${index + 1}`,
        lines,
      }
    })
  }, [executionResultsByPermissionId, remediationActions, selectedFinding])
  const dryRunJsonTree = useMemo(
    () => {
      const nodes: JsonDiffNode[] = []
      remediationActions.forEach((action, index) => {
        const normalizedActionType = String(action.actionType || '').trim().toLowerCase()
        if (normalizedActionType === 'remove_permissions' && action.permissionIds.length > 0) {
          action.permissionIds.forEach((permissionId) => {
            const resultRow = executionResultsByPermissionId.get(permissionId)
            const { before, after } = buildRemovePermissionSnapshotStrings(resultRow)
            nodes.push({
              key: `${action.actionType || `action_${index + 1}`}:${permissionId}`,
              before,
              after,
              type: 'changed' as const,
            })
          })
          return
        }
        nodes.push({
          key: action.actionType || `action_${index + 1}`,
          before: 'state=planned',
          after: 'state=applied',
          type: 'changed' as const,
        })
      })
      return nodes
    },
    [executionResultsByPermissionId, remediationActions]
  )
  const operation = useMemo<RemediationOperationRecord>(() => {
    const remediationState = String(
      remediationDetailState(remediationDetail) ?? selectedFinding.remediationState ?? ''
    )
      .trim()
      .toLowerCase()
    const workflowStatus: RemediationOperationRecord['workflowStatus'] =
      remediationState === 'approved'
        ? 'pending_approval'
        : remediationState === 'executed' || remediationState === 'manual_required'
          ? 'completed'
          : remediationState === 'failed'
            ? 'waiting_reproposal'
            : 'proposed'
    const executionStatus: RemediationOperationRecord['executionStatus'] =
      remediationState === 'approved'
        ? 'executing'
        : remediationState === 'executed' || remediationState === 'manual_required'
          ? 'completed'
          : remediationState === 'failed'
            ? 'dry_run_failed'
            : 'dry_run_succeeded'
    const status: RemediationOperationRecord['status'] =
      remediationState === 'executed' || remediationState === 'manual_required'
        ? 'completed'
        : remediationState === 'failed'
          ? 'rollback_required'
          : remediationState === 'approved'
            ? 'approved'
            : 'under_review'
    const allowedActions: RemediationOperationRecord['allowedActions'] = []
    if (canApproveRemediation) allowedActions.push('approve')
    if (canProposeRemediation) allowedActions.push('dry-run')
    if (canExecuteRemediation) allowedActions.push('execute')
    if (canRollbackRemediation) allowedActions.push('rollback')
    return {
      planId: selectedFinding.planId,
      domain: 'governance',
      workflowStatus,
      executionStatus,
      status,
      tenantScope: `${selectedFinding.source}/${selectedFinding.itemPath}`,
      topPlaybook: actionTypes[0] || 'governance_remediation',
      riskAssessment: {
        exposureScore: Math.min(1, selectedFinding.riskScore / 100),
        sensitivityScore: 0,
        riskScore: Math.min(1, selectedFinding.riskScore / 100),
      },
      approvalRequired: true,
      rollbackAvailable: canRollbackRemediation,
      policyDecision: canApproveRemediation || canExecuteRemediation ? 'would_allow' : 'allow',
      preferredScopeOrder: ['site', 'folder', 'policy'],
      recommendedScope: selectedFinding.targetKind === 'folder' ? 'folder' : 'site',
      dryRunSummary: {
        targetCount: remediationActions.length || 1,
        predictedFailures: manualActionCount,
        changedFields: actionTypes.length > 0 ? actionTypes : ['permission_scope'],
        impactedUsers: selectedFinding.findingEvidence?.external_recipients?.length || 0,
        failedRiskLevel: selectedFinding.riskScore >= 55 ? 'high' : selectedFinding.riskScore >= 30 ? 'medium' : 'low',
      },
      impactSummary: {
        changedObjects: remediationActions.length || 1,
        predictedFailures: manualActionCount,
        impactedUsers: selectedFinding.findingEvidence?.external_recipients?.length || 0,
        confidenceScore: 0.89,
      },
      preview: {
        what: impactSummaryText,
        why: '過剰共有リスクの抑制',
        boundedBy: `${selectedFinding.source} / ${selectedFinding.itemPath}`,
      },
      blastRadius: {
        users: selectedFinding.findingEvidence?.external_recipients?.length || 0,
        groups: selectedFinding.matchedGuards.length,
        files: 1,
        externalLinks: selectedFinding.findingEvidence?.anonymous_links?.length || 0,
      },
      allowedActions,
      correlationId: remediationDetail?.last_execution_id || `finding-${selectedFinding.id}`,
      executionLogs: [
        {
          at: selectedFinding.lastEvaluatedAt,
          level: remediationDetail?.last_error ? 'error' : 'info',
          message: remediationDetail?.last_error || executionSummaryText,
        },
      ],
      executionId: remediationDetail?.last_execution_id,
      dryRunDiff: {
        files: dryRunDiffFiles,
        jsonTree: dryRunJsonTree,
      },
      governanceDetails: {
        oversharingFindings: selectedFinding.matchedGuards.length,
        externalLinksBefore: selectedFinding.findingEvidence?.anonymous_links?.length || 0,
        externalLinksAfter: 0,
        sensitiveItems: 0,
      },
    }
  }, [
    remediationDetail,
    selectedFinding,
    canApproveRemediation,
    canProposeRemediation,
    canExecuteRemediation,
    canRollbackRemediation,
    remediationActions.length,
    manualActionCount,
    actionTypes,
    impactSummaryText,
    executionSummaryText,
    dryRunDiffFiles,
    dryRunJsonTree,
  ])

  useEffect(() => {
    setConfirmActionType(null)
    setIsConfirmDialogOpen(false)
  }, [selectedFinding.id])

  const rollbackCapabilityText = canRollbackRemediation ? '高（可逆）' : '低（不可逆）'
  const dryRunSummaryText = `対象 ${operation.dryRunSummary.targetCount} 件 / 失敗予測 ${operation.dryRunSummary.predictedFailures} 件`
  const predictedImpactText = `影響ユーザー ${operation.dryRunSummary.impactedUsers ?? 0} / 変更 ${operation.impactSummary?.changedObjects ?? operation.dryRunSummary.targetCount}`
  const dryRunHumanReadableSteps = useMemo(() => {
    if (!remediationActions.length) {
      return ['現在は適用可能な是正アクションがありません。']
    }
    return remediationActions.map((action, idx) => {
      const actionType = String(action.actionType || '').trim().toLowerCase()
      if (actionType === 'remove_permissions') {
        if (action.permissionIds.length === 0) {
          return `${idx + 1}. 外部共有権限の削除を試みます（対象権限IDは取得できませんでした）。`
        }
        return `${idx + 1}. 外部共有権限を削除します（対象 permission_id: ${action.permissionIds.join(', ')}）。`
      }
      if (actionType === 'manual_review') {
        return `${idx + 1}. 自動適用できないため、運用者による手動レビューを実施します。`
      }
      return `${idx + 1}. アクション ${action.actionType || 'unknown'} を実行します。`
    })
  }, [remediationActions])

  const requestRollbackConfirmation = () => {
    setConfirmActionType('rollback')
    setIsConfirmDialogOpen(true)
  }

  const handleCloseConfirmDialog = () => {
    setIsConfirmDialogOpen(false)
    setConfirmActionType(null)
  }

  const handleConfirmAction = () => {
    if (confirmActionType === 'rollback') {
      onRollback()
    }
    handleCloseConfirmDialog()
  }

  return (
    <div className="rounded-md border p-3">
      <p className="text-xs text-muted-foreground">是正ワークフロー</p>
      <div className="mt-2 flex flex-wrap gap-2">
        {showRollbackWorkflowCta && (
          <Button
            size="sm"
            variant="outline"
            onClick={requestRollbackConfirmation}
            disabled={remediationBusy || !canRollbackRemediation}
          >
            ロールバック
          </Button>
        )}
      </div>
      {!showRollbackWorkflowCta && (
        <div className="mt-3 space-y-3">
          <div className="rounded-md border p-3 text-xs">
            <p className="font-medium text-foreground">影響範囲サマリー</p>
            <p className="mt-1 text-muted-foreground">{operation.preview?.what ?? '-'}</p>
            <div className="mt-2 grid grid-cols-2 gap-2 sm:grid-cols-4">
              <div className="rounded-md border p-2">
                <p className="text-[11px] text-muted-foreground">変更対象</p>
                <p className="font-medium">{operation.impactSummary?.changedObjects ?? 0}</p>
              </div>
              <div className="rounded-md border p-2">
                <p className="text-[11px] text-muted-foreground">影響ユーザー</p>
                <p className="font-medium">{operation.blastRadius?.users ?? 0}</p>
              </div>
              <div className="rounded-md border p-2">
                <p className="text-[11px] text-muted-foreground">関連ポリシー</p>
                <p className="font-medium">{operation.blastRadius?.groups ?? 0}</p>
              </div>
              <div className="rounded-md border p-2">
                <p className="text-[11px] text-muted-foreground">外部リンク</p>
                <p className="font-medium">{operation.blastRadius?.externalLinks ?? 0}</p>
              </div>
            </div>
          </div>
          <div className="space-y-2">
            <div className="rounded-md border bg-muted/20 p-3 text-xs">
              <p className="font-medium text-foreground">この Dry-run で実施する処理</p>
              <div className="mt-1 space-y-1 text-muted-foreground">
                {dryRunHumanReadableSteps.map((step) => (
                  <p key={step}>{step}</p>
                ))}
              </div>
            </div>
            <DryRunDiffPanel
              operation={operation}
              onApprove={onApprove}
              canApprove={!remediationBusy && canApproveRemediation}
              approveUnavailableMessage={
                canApproveRemediation
                  ? '処理中のため、一時的に承認できません。'
                  : 'この検知は手動対応モードのため、承認ではなく手動対応を実施してください。'
              }
            />
          </div>
        </div>
      )}
      {remediationDetail?.last_error && (
        <p className="mt-2 text-xs text-rose-700">{remediationDetail.last_error}</p>
      )}
      <OperationConfirmDialog
        isOpen={isConfirmDialogOpen}
        actionType={confirmActionType}
        planId={selectedFinding.planId}
        scopeText={`${selectedFinding.source} / ${selectedFinding.itemPath}`}
        dryRunSummaryText={dryRunSummaryText}
        predictedImpactText={predictedImpactText}
        rollbackCapabilityText={rollbackCapabilityText}
        reason="次アクションパネルでDry-run/影響範囲を確認済み"
        isConfirmDisabled={
          remediationBusy ||
          (confirmActionType === 'rollback' && !canRollbackRemediation)
        }
        onClose={handleCloseConfirmDialog}
        onConfirm={handleConfirmAction}
      />
    </div>
  )
}

export default RemediationWorkflowPanel
