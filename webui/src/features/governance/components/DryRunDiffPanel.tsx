import { useMemo, useState } from 'react'
import Badge from '@/components/ui/Badge'
import Button from '@/components/ui/Button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/Card'
import Input from '@/components/ui/Input'
import { JsonDiffNode, RemediationOperationRecord } from '@/features/governance/remediationTypes'
import { cn } from '@/lib/utils'

type DiffViewMode = 'split' | 'inline'

const tokenTextClass = (changed?: boolean) =>
  changed ? 'rounded bg-emerald-200/80 px-0.5 dark:bg-emerald-900/70' : undefined

const jsonNodeBadge = (type: JsonDiffNode['type']) => {
  if (type === 'added') return <Badge variant="outline">added</Badge>
  if (type === 'removed') return <Badge variant="destructive">removed</Badge>
  if (type === 'changed') return <Badge variant="secondary">changed</Badge>
  return <Badge variant="outline">unchanged</Badge>
}

const JsonTreeNode = ({ node, depth = 0 }: { node: JsonDiffNode; depth?: number }) => {
  const [open, setOpen] = useState(true)
  const hasChildren = Boolean(node.children?.length)
  return (
    <div className="space-y-1">
      <div
        className="flex min-w-0 flex-wrap items-center gap-2 text-xs"
        style={{ paddingLeft: `${depth * 12}px` }}
      >
        {hasChildren ? (
          <button className="text-muted-foreground w-5 text-left" onClick={() => setOpen((v) => !v)}>
            {open ? '▾' : '▸'}
          </button>
        ) : (
          <span className="w-5" />
        )}
        <span className="min-w-0 break-all font-medium">{node.key}</span>
        {jsonNodeBadge(node.type)}
        {node.before !== undefined && (
          <span className="min-w-0 break-all text-red-700 dark:text-red-300">before: {node.before}</span>
        )}
        {node.after !== undefined && (
          <span className="min-w-0 break-all text-emerald-700 dark:text-emerald-300">after: {node.after}</span>
        )}
      </div>
      {open && node.children?.map((child) => <JsonTreeNode key={`${node.key}-${child.key}`} node={child} depth={depth + 1} />)}
    </div>
  )
}

interface DryRunDiffPanelProps {
  operation: RemediationOperationRecord
  onApprove: () => void
  canApprove: boolean
  approveUnavailableMessage?: string
}

const DryRunDiffPanel = ({
  operation,
  onApprove,
  canApprove,
  approveUnavailableMessage,
}: DryRunDiffPanelProps) => {
  const [viewMode, setViewMode] = useState<DiffViewMode>('inline')
  const [selectedLineKey, setSelectedLineKey] = useState<string | null>(null)
  const [commentDraft, setCommentDraft] = useState('')
  const [commentMap, setCommentMap] = useState<Record<string, string[]>>({})

  const selectedComments = selectedLineKey ? commentMap[selectedLineKey] ?? [] : []

  const addComment = () => {
    if (!selectedLineKey || !commentDraft.trim()) return
    setCommentMap((prev) => ({
      ...prev,
      [selectedLineKey]: [...(prev[selectedLineKey] ?? []), commentDraft.trim()]
    }))
    setCommentDraft('')
  }

  const diffStats = useMemo(() => {
    let added = 0
    let removed = 0
    operation.dryRunDiff.files.forEach((file) => {
      file.lines.forEach((line) => {
        if (line.type === 'add') added += 1
        if (line.type === 'remove') removed += 1
      })
    })
    return { added, removed }
  }, [operation.dryRunDiff.files])

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <CardTitle>Dry-run Diff</CardTitle>
            <CardDescription>差分レビュー</CardDescription>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline">
              変更: {operation.impactSummary?.changedObjects ?? operation.dryRunSummary.targetCount}
            </Badge>
            <Badge variant="outline">
              失敗予測: {operation.impactSummary?.predictedFailures ?? operation.dryRunSummary.predictedFailures}
            </Badge>
            <Badge variant="outline">
              影響ユーザー: {operation.impactSummary?.impactedUsers ?? operation.dryRunSummary.impactedUsers ?? 0}
            </Badge>
            <Badge variant="outline">+{diffStats.added}</Badge>
            <Badge variant="destructive">-{diffStats.removed}</Badge>
            <Button
              size="sm"
              variant={viewMode === 'split' ? 'default' : 'outline'}
              onClick={() => setViewMode('split')}
            >
              Split
            </Button>
            <Button
              size="sm"
              variant={viewMode === 'inline' ? 'default' : 'outline'}
              onClick={() => setViewMode('inline')}
            >
              Inline
            </Button>
          </div>
        </div>
      </CardHeader>

      <CardContent className="min-w-0 space-y-4 overflow-x-hidden">
        {operation.domain === 'ontology' && operation.ontologyDryRunEvidence && (
          <div className="grid gap-3 lg:grid-cols-3">
            <div className="rounded-md border p-3 text-xs">
              <p className="text-sm font-medium">品質証跡（3軸）</p>
              <p className="mt-2">
                Freshness: {operation.ontologyDryRunEvidence.qualityDelta.freshnessBefore.toFixed(2)}
                {' -> '}
                {operation.ontologyDryRunEvidence.qualityDelta.freshnessAfter.toFixed(2)}
              </p>
              <p className="mt-1">
                Uniqueness: {operation.ontologyDryRunEvidence.qualityDelta.uniquenessBefore.toFixed(2)}
                {' -> '}
                {operation.ontologyDryRunEvidence.qualityDelta.uniquenessAfter.toFixed(2)}
              </p>
              <p className="mt-1">
                Relevance: {operation.ontologyDryRunEvidence.qualityDelta.relevanceBefore.toFixed(2)}
                {' -> '}
                {operation.ontologyDryRunEvidence.qualityDelta.relevanceAfter.toFixed(2)}
              </p>
            </div>
            <div className="rounded-md border p-3 text-xs lg:col-span-2">
              <p className="text-sm font-medium">正本判定根拠</p>
              <div className="mt-2 space-y-2">
                {operation.ontologyDryRunEvidence.canonicalSelectionReasons.map((decision) => (
                  <div key={decision.groupId} className="rounded-md border p-2">
                    <p className="font-medium">
                      {decision.groupId}
                      {' -> '}
                      {decision.selectedCanonicalId}
                    </p>
                    <p className="mt-1 text-muted-foreground">{decision.reasons.join(' / ')}</p>
                  </div>
                ))}
              </div>
              {operation.ontologyDryRunEvidence.lineageRelinkSummary && (
                <p className="mt-2 text-muted-foreground">
                  系譜再接続: edge {operation.ontologyDryRunEvidence.lineageRelinkSummary.relinkedEdges} /
                  doc {operation.ontologyDryRunEvidence.lineageRelinkSummary.affectedDocuments} /
                  orphan risk {operation.ontologyDryRunEvidence.lineageRelinkSummary.orphanRiskAfterRelink}
                </p>
              )}
            </div>
          </div>
        )}

        {operation.dryRunDiff.files.map((file, fileIdx) => {
          const fileKey = `${file.filePath}:${file.section}:${fileIdx}`
          return (
            <div key={fileKey} className="rounded-md border">
              <div className="border-b bg-muted/30 px-3 py-2 text-xs">
                <span className="font-medium break-all">{file.filePath}</span>
                <span className="text-muted-foreground ml-2 break-all">section: {file.section}</span>
              </div>
              <div className="max-h-[320px] overflow-y-auto overflow-x-hidden">
                {viewMode === 'split' ? (
                  <div>
                    {file.lines.map((line, idx) => {
                      const lineKey = `${fileKey}:${idx}`
                      const selected = selectedLineKey === lineKey
                      return (
                        <div
                          key={lineKey}
                          className={cn(
                            'grid grid-cols-[44px_minmax(0,1fr)_44px_minmax(0,1fr)] border-b text-xs',
                            selected && 'ring-1 ring-primary/40',
                            line.type === 'add' && 'bg-emerald-50/60 dark:bg-emerald-950/30',
                            line.type === 'remove' && 'bg-red-50/70 dark:bg-red-950/30'
                          )}
                          onClick={() => setSelectedLineKey(lineKey)}
                        >
                          <div className="border-r px-2 py-1 text-right text-muted-foreground">{line.oldLineNumber ?? ''}</div>
                          <div className="border-r px-2 py-1 font-mono break-all whitespace-pre-wrap">
                            {line.oldTokens
                              ? line.oldTokens.map((token, tokenIdx) => (
                                <span key={`${lineKey}-old-${tokenIdx}`} className={tokenTextClass(token.changed)}>
                                  {token.text}
                                </span>
                              ))
                              : line.oldText}
                          </div>
                          <div className="border-r px-2 py-1 text-right text-muted-foreground">{line.newLineNumber ?? ''}</div>
                          <div className="px-2 py-1 font-mono break-all whitespace-pre-wrap">
                            {line.newTokens
                              ? line.newTokens.map((token, tokenIdx) => (
                                <span key={`${lineKey}-new-${tokenIdx}`} className={tokenTextClass(token.changed)}>
                                  {token.text}
                                </span>
                              ))
                              : line.newText}
                          </div>
                        </div>
                      )
                    })}
                  </div>
                ) : (
                  <div>
                    {file.lines.map((line, idx) => {
                      const lineKey = `${fileKey}:${idx}`
                      const selected = selectedLineKey === lineKey
                      return (
                        <div
                          key={lineKey}
                          className={cn(
                            'grid grid-cols-[18px_56px_56px_1fr] border-b text-xs',
                            selected && 'ring-1 ring-primary/40',
                            line.type === 'add' && 'bg-emerald-50/60 dark:bg-emerald-950/30',
                            line.type === 'remove' && 'bg-red-50/70 dark:bg-red-950/30'
                          )}
                          onClick={() => setSelectedLineKey(lineKey)}
                        >
                          <div className="border-r px-1 py-1 text-center">{line.type === 'add' ? '+' : line.type === 'remove' ? '-' : ' '}</div>
                          <div className="border-r px-2 py-1 text-right text-muted-foreground">{line.oldLineNumber ?? ''}</div>
                          <div className="border-r px-2 py-1 text-right text-muted-foreground">{line.newLineNumber ?? ''}</div>
                          <div className="px-2 py-1 font-mono break-all whitespace-pre-wrap">
                            {line.newTokens
                              ? line.newTokens.map((token, tokenIdx) => (
                                <span key={`${lineKey}-inline-${tokenIdx}`} className={tokenTextClass(token.changed)}>
                                  {token.text}
                                </span>
                              ))
                              : line.newText || line.oldText}
                          </div>
                        </div>
                      )
                    })}
                  </div>
                )}
              </div>
            </div>
          )
        })}

        <div className="grid gap-4 lg:grid-cols-2">
          <div className="rounded-md border p-3">
            <p className="text-sm font-medium">JSON 構造差分</p>
            <details className="mt-2">
              <summary className="cursor-pointer text-xs text-muted-foreground">詳細を表示</summary>
              <div className="mt-2 space-y-1">
                {operation.dryRunDiff.jsonTree.map((node) => (
                  <JsonTreeNode key={node.key} node={node} />
                ))}
              </div>
            </details>
          </div>

          <div className="rounded-md border p-3">
            <p className="text-sm font-medium">行コメント / 承認フロー</p>
            <div className="flex gap-2">
              <Input
                value={commentDraft}
                onChange={(event) => setCommentDraft(event.target.value)}
                placeholder="この変更へのコメントを入力"
              />
              <Button size="sm" variant="outline" onClick={addComment} disabled={!selectedLineKey}>
                追加
              </Button>
            </div>
            <div className="mt-2 max-h-24 space-y-1 overflow-y-auto overflow-x-hidden text-xs">
              {selectedComments.map((comment, idx) => (
                <p key={`${selectedLineKey}-comment-${idx}`} className="rounded bg-muted/40 px-2 py-1">
                  {comment}
                </p>
              ))}
            </div>
            <div className="mt-3 flex gap-2">
              {canApprove ? (
                <Button size="sm" onClick={onApprove}>
                  承認
                </Button>
              ) : (
                <p className="text-xs text-muted-foreground">
                  {approveUnavailableMessage || 'この状態では承認できません。'}
                </p>
              )}
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

export default DryRunDiffPanel
