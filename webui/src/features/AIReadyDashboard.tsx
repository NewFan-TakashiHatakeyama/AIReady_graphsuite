import { useEffect, useMemo, useRef, useState } from 'react'
import Badge from '@/components/ui/Badge'
import Button from '@/components/ui/Button'
import Input from '@/components/ui/Input'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/Card'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/Select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/Table'
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  Radar,
  RadarChart,
  Tooltip,
  XAxis,
  YAxis
} from 'recharts'
import { cn } from '@/lib/utils'
import TrustSignalsPanel from '@/features/common/TrustSignalsPanel'
import ReadinessGaugeD3 from '@/features/common/ReadinessGaugeD3'
import ChartContainer from '@/features/common/ChartContainer'
import InitialScoringGateCard, {
  INITIAL_SCORING_GATE_DEFAULT_DESCRIPTION
} from '@/features/common/InitialScoringGateCard'
import { useGovernanceBatchScoringGate } from '@/features/common/useGovernanceBatchScoringGate'
import { useSettingsStore, Tab } from '@/stores/settings'
import { setOperationDeepLink } from '@/features/common/operationDeepLink'
import {
  DashboardReadinessApiResponse,
  DashboardReadinessTrendApiResponse,
  DashboardSignalSubMetricApiRow,
  DashboardRecommendedActionApiRow,
  getDashboardReadiness,
  getDashboardReadinessTrend,
  getDashboardRecommendedActions
} from '@/api/graphsuite'

type TopViewTab = 'score' | 'role-review'
type ScoreBand = 'red' | 'orange' | 'green' | 'blue'
type RemediationCategory = 'governance'
type RemediationPriority = 'P1' | 'P2' | 'P3'
type RemediationWorkflowStatus =
  | 'proposed'
  | 'pending_approval'
  | 'on_hold'
  | 'waiting_reproposal'
  | 'manual_in_progress'
  | 'completed'
type LaneMode = 'category' | 'priority'
type ReadinessSignalRow = {
  key: string
  label: string
  shortLabel: string
  score: number | null
  issues: number
  metricLabel: string
  target: number
  subMetrics: DashboardSignalSubMetricApiRow[]
}
type RemediationBoardItem = {
  id: string
  planId: string
  title: string
  category: RemediationCategory
  priority: RemediationPriority
  workflowStatus: RemediationWorkflowStatus
  executionStatus?: 'idle' | 'executing' | 'completed' | 'rollback_required'
  assignee: string
  dueAt: string
  targetRef: string
  summary: string
  targetTab: Tab
  targetPage: string
  focus: string
  riskScore?: number
  exposureScore?: number
  sensitivityScore?: number
  freshnessScore?: number
  uniquenessScore?: number
  relevanceScore?: number
  qualityDelta?: number
}

const READINESS_SLO_TARGET = 90
const scoreBand = (score: number): ScoreBand => {
  if (score < 50) return 'red'
  if (score < 70) return 'orange'
  if (score < 90) return 'green'
  return 'blue'
}
const scoreTextClass = (score: number | null): string => {
  if (score === null || Number.isNaN(score)) return 'text-muted-foreground'
  const band = scoreBand(score)
  if (band === 'red') return 'text-red-600'
  if (band === 'orange') return 'text-amber-600'
  if (band === 'green') return 'text-emerald-600'
  return 'text-blue-600'
}
const scoreHex = (score: number | null): string => {
  if (score === null || Number.isNaN(score)) return '#9ca3af'
  const band = scoreBand(score)
  if (band === 'red') return '#dc2626'
  if (band === 'orange') return '#f59e0b'
  if (band === 'green') return '#16a34a'
  return '#2563eb'
}

const easeOutCubic = (t: number): number => 1 - Math.pow(1 - t, 3)

const readinessBandComment = (score: number): string => {
  if (score >= 90) return '非常に安定した状態です。現行運用を維持しながら継続的改善を進めてください。'
  if (score >= 80) return '良好な状態です。優先課題を定期的に見直し、90%以上の維持を目指してください。'
  if (score >= 70) return '概ね良好です。重点項目の是正を進め、運用品質をさらに底上げしてください。'
  if (score >= 60) return '注意が必要な状態です。リスクの高い領域から順に改善対応を実施してください。'
  if (score >= 50) return '改善が必要な状態です。提案・承認・実行の運用フローを強化してください。'
  if (score >= 40) return '警戒水準です。主要KPIの劣化要因を特定し、短期集中で是正してください。'
  if (score >= 30) return '低水準です。運用体制と統制プロセスを見直し、優先課題へ集中対応してください。'
  if (score >= 20) return '重大な改善が必要です。即時の是正計画と監視強化を実施してください。'
  if (score >= 10) return '危険水準です。安全確保のため、緊急対応と影響範囲の隔離を検討してください。'
  return '最優先での対応が必要です。運用継続可否を含む緊急対策を実施してください。'
}

const categoryLabel = (category: RemediationCategory): string =>
  category === 'governance' ? 'ガバナンス' : 'ガバナンス'

const workflowStatusLabel = (status: RemediationWorkflowStatus): string => {
  const labels: Record<RemediationWorkflowStatus, string> = {
    proposed: '提案中',
    pending_approval: '承認待ち',
    on_hold: '保留',
    waiting_reproposal: '再提案待ち',
    manual_in_progress: '手動対応中',
    completed: '完了'
  }
  return labels[status]
}

const priorityVariant = (priority: RemediationPriority): 'destructive' | 'secondary' | 'outline' => {
  if (priority === 'P1') return 'destructive'
  if (priority === 'P2') return 'secondary'
  return 'outline'
}

const categoryVariant = (category: RemediationCategory): 'destructive' | 'secondary' | 'outline' => {
  if (category === 'governance') return 'secondary'
  return 'outline'
}

const AIReadyDashboard = () => {
  const [topViewTab, setTopViewTab] = useState<TopViewTab>('score')
  const [laneMode, setLaneMode] = useState<LaneMode>('category')
  const [priorityFilter, setPriorityFilter] = useState<'all' | RemediationPriority>('all')
  const [assigneeFilter, setAssigneeFilter] = useState<string>('all')
  const [searchText, setSearchText] = useState('')
  const [dashboardReadinessApi, setDashboardReadinessApi] = useState<DashboardReadinessApiResponse | null>(null)
  const [dashboardReadinessTrendApi, setDashboardReadinessTrendApi] = useState<DashboardReadinessTrendApiResponse | null>(null)
  const [dashboardActionsApi, setDashboardActionsApi] = useState<DashboardRecommendedActionApiRow[]>([])
  const [dashboardLoadError, setDashboardLoadError] = useState<string | null>(null)
  const scoringGate = useGovernanceBatchScoringGate({ pollWhileRunning: true })
  const isPreScanLocked = !scoringGate.hasCompletedScan
  const setCurrentTab = useSettingsStore.use.setCurrentTab()
  const [animatedReadiness, setAnimatedReadiness] = useState(0)
  const animatedReadinessRef = useRef(0)

  useEffect(() => {
    const loadKpiSources = async () => {
      try {
        const [readiness, actions] = await Promise.all([
          getDashboardReadiness(),
          getDashboardRecommendedActions()
        ])
        let readinessTrend: DashboardReadinessTrendApiResponse | null = null
        try {
          readinessTrend = await getDashboardReadinessTrend()
        } catch {
          readinessTrend = null
        }
        setDashboardReadinessApi(readiness)
        setDashboardReadinessTrendApi(readinessTrend)
        setDashboardActionsApi(actions.rows ?? [])
        setDashboardLoadError(null)
        void scoringGate.reload()
      } catch (error) {
        setDashboardLoadError(error instanceof Error ? error.message : 'Dashboard API load failed')
        setDashboardReadinessApi(null)
        setDashboardReadinessTrendApi(null)
        setDashboardActionsApi([])
      }
    }
    void loadKpiSources()
  }, [scoringGate.reload])

  const defaultSignalRows = useMemo(
    () => [
      {
        key: 'governance_sensitive',
        label: '機微情報の保護',
        shortLabel: '機微保護',
        score: 0,
        issues: 0,
        metricLabel: 'API待機',
        target: READINESS_SLO_TARGET,
        subMetrics: []
      },
      {
        key: 'governance_oversharing',
        label: '過剰共有の抑制',
        shortLabel: '過剰共有',
        score: 0,
        issues: 0,
        metricLabel: 'API待機',
        target: READINESS_SLO_TARGET,
        subMetrics: []
      },
      {
        key: 'governance_assurance',
        label: '運用・保証',
        shortLabel: '運用保証',
        score: 0,
        issues: 0,
        metricLabel: 'API待機',
        target: READINESS_SLO_TARGET,
        subMetrics: []
      },
      {
        key: 'ontology_foundation',
        label: '情報整備スコア',
        shortLabel: '情報整備',
        score: 0,
        issues: 0,
        metricLabel: 'API待機',
        target: READINESS_SLO_TARGET,
        subMetrics: []
      },
      {
        key: 'ontology_usecase',
        label: 'ユースケース解決力',
        shortLabel: 'ユースケース',
        score: 0,
        issues: 0,
        metricLabel: 'API待機',
        target: READINESS_SLO_TARGET,
        subMetrics: []
      }
    ] satisfies ReadinessSignalRow[],
    []
  )

  const signalRows = useMemo<ReadinessSignalRow[]>(
    () => {
      if (dashboardReadinessApi?.signals?.length) {
        return dashboardReadinessApi.signals.map((signal) => ({
          key: signal.key,
          label: signal.label,
          shortLabel: signal.label.replace('の', '/').slice(0, 8),
          score: signal.score ?? null,
          issues: signal.issues,
          metricLabel: 'API集計',
          target: signal.target || dashboardReadinessApi.target_score || READINESS_SLO_TARGET,
          subMetrics: signal.sub_metrics ?? []
        }))
      }
      return defaultSignalRows
    },
    [dashboardReadinessApi, defaultSignalRows]
  )

  const weightedReadiness = useMemo((): number | null => {
    if (dashboardReadinessApi === null) {
      return (
        signalRows.reduce((acc, row) => acc + (row.score ?? 0), 0) / Math.max(1, signalRows.length)
      )
    }
    const raw = dashboardReadinessApi.readiness_score
    if (raw === null || raw === undefined) {
      return null
    }
    return raw
  }, [dashboardReadinessApi, signalRows])

  const readinessUnavailable =
    dashboardReadinessApi !== null &&
    (dashboardReadinessApi.readiness_score === null || dashboardReadinessApi.readiness_score === undefined)

  const readinessRadarColor = scoreHex(weightedReadiness)

  const signalRadarChartData = useMemo(() => {
    if (dashboardLoadError) {
      return null
    }
    if (signalRows.some((row) => row.score === null)) {
      return null
    }
    return signalRows.map((row) => ({
      signal: row.shortLabel,
      score: Number((row.score as number).toFixed(1)),
      target: row.target
    }))
  }, [dashboardLoadError, signalRows])

  const signalBreakdownRows = useMemo(
    () =>
      signalRows.map((signal) => ({
        key: signal.key,
        label: signal.label,
        score: signal.score,
        details:
          signal.subMetrics.length > 0
            ? signal.subMetrics
            : signal.score === null
              ? []
              : [
                {
                  key: 'current-score',
                  label: '現状スコア',
                  value: signal.score,
                  unit: 'score' as const
                },
                {
                  key: 'slo-gap',
                  label: 'SLO差分',
                  value: Math.max(0, signal.target - signal.score),
                  unit: 'score' as const
                },
                {
                  key: 'issues',
                  label: '要対応件数',
                  value: Number(signal.issues),
                  unit: 'count' as const
                }
              ]
      })),
    [signalRows]
  )

  const readinessTrendChart = useMemo(() => {
    if (dashboardLoadError) return null
    if (dashboardReadinessTrendApi?.source === 'insufficient_data') return null
    if (dashboardReadinessTrendApi?.rows?.length) return dashboardReadinessTrendApi.rows
    if (signalRows.some((row) => row.score === null)) return null
    const scoreOf = (key: string): number => signalRows.find((row) => row.key === key)?.score ?? 0
    const governanceOversharing = scoreOf('governance_oversharing')
    const governanceSensitive = scoreOf('governance_sensitive')
    const governanceAssurance = scoreOf('governance_assurance')
    const ontologyFoundation = scoreOf('ontology_foundation')
    const ontologyUsecase = scoreOf('ontology_usecase')
    return [
      {
        label: 'T-4',
        governance_oversharing: Math.max(0, governanceOversharing - 8),
        governance_sensitive: Math.max(0, governanceSensitive - 8),
        governance_assurance: Math.max(0, governanceAssurance - 8),
        ontology_foundation: Math.max(0, ontologyFoundation - 8),
        ontology_usecase: Math.max(0, ontologyUsecase - 8)
      },
      {
        label: 'T-3',
        governance_oversharing: Math.max(0, governanceOversharing - 6),
        governance_sensitive: Math.max(0, governanceSensitive - 6),
        governance_assurance: Math.max(0, governanceAssurance - 6),
        ontology_foundation: Math.max(0, ontologyFoundation - 6),
        ontology_usecase: Math.max(0, ontologyUsecase - 6)
      },
      {
        label: 'T-2',
        governance_oversharing: Math.max(0, governanceOversharing - 4),
        governance_sensitive: Math.max(0, governanceSensitive - 4),
        governance_assurance: Math.max(0, governanceAssurance - 4),
        ontology_foundation: Math.max(0, ontologyFoundation - 4),
        ontology_usecase: Math.max(0, ontologyUsecase - 4)
      },
      {
        label: 'T-1',
        governance_oversharing: Math.max(0, governanceOversharing - 2),
        governance_sensitive: Math.max(0, governanceSensitive - 2),
        governance_assurance: Math.max(0, governanceAssurance - 2),
        ontology_foundation: Math.max(0, ontologyFoundation - 2),
        ontology_usecase: Math.max(0, ontologyUsecase - 2)
      },
      {
        label: 'Now',
        governance_oversharing: governanceOversharing,
        governance_sensitive: governanceSensitive,
        governance_assurance: governanceAssurance,
        ontology_foundation: ontologyFoundation,
        ontology_usecase: ontologyUsecase
      }
    ]
  }, [dashboardLoadError, dashboardReadinessTrendApi, signalRows])

  const actionQueue = useMemo(
    () => dashboardActionsApi,
    [dashboardActionsApi]
  )

  const boardItems = useMemo((): RemediationBoardItem[] => {
    return []
  }, [])

  useEffect(() => {
    if (topViewTab !== 'score') return
    if (weightedReadiness === null) {
      animatedReadinessRef.current = 0
      setAnimatedReadiness(0)
      return
    }
    const from = 0
    const to = Number(weightedReadiness.toFixed(1))
    const durationMs = 900
    const startedAt = performance.now()
    let frameId = 0

    animatedReadinessRef.current = 0
    setAnimatedReadiness(0)

    const tick = (now: number) => {
      const progress = Math.min(1, (now - startedAt) / durationMs)
      const eased = easeOutCubic(progress)
      const next = from + (to - from) * eased
      animatedReadinessRef.current = next
      setAnimatedReadiness(next)
      if (progress < 1) {
        frameId = window.requestAnimationFrame(tick)
      }
    }

    frameId = window.requestAnimationFrame(tick)
    return () => window.cancelAnimationFrame(frameId)
  }, [weightedReadiness, topViewTab])

  const openDeepLink = (item: Pick<RemediationBoardItem, 'targetTab' | 'targetPage' | 'focus'>) => {
    setOperationDeepLink({
      tab: item.targetTab,
      page: item.targetPage,
      focus: item.focus
    })
    setCurrentTab(item.targetTab)
  }

  const assigneeOptions = useMemo(
    () => Array.from(new Set(boardItems.map((item) => item.assignee))).sort((a, b) => a.localeCompare(b)),
    [boardItems]
  )

  const filteredBoardItems = useMemo(() => {
    const keyword = searchText.trim().toLowerCase()
    return boardItems.filter((item) => {
      if (priorityFilter !== 'all' && item.priority !== priorityFilter) return false
      if (assigneeFilter !== 'all' && item.assignee !== assigneeFilter) return false
      if (!keyword) return true
      return (
        item.title.toLowerCase().includes(keyword) ||
        item.planId.toLowerCase().includes(keyword) ||
        item.targetRef.toLowerCase().includes(keyword) ||
        item.summary.toLowerCase().includes(keyword)
      )
    })
  }, [boardItems, priorityFilter, assigneeFilter, searchText])

  const laneValues = useMemo(() => {
    if (laneMode === 'category') return ['governance', 'ontology'] as const
    return ['P1', 'P2', 'P3'] as const
  }, [laneMode])

  const laneLabel = (value: string): string => {
    if (laneMode === 'category') return categoryLabel(value as RemediationCategory)
    return value
  }

  const laneItems = (value: string): RemediationBoardItem[] => {
    if (laneMode === 'category') return filteredBoardItems.filter((item) => item.category === value)
    return filteredBoardItems.filter((item) => item.priority === value)
  }

  const isOverdue = (dueAt: string): boolean => {
    const normalized = dueAt.replace(' JST', '+09:00')
    const due = new Date(normalized).getTime()
    if (Number.isNaN(due)) return false
    return due < Date.now()
  }

  const remediationKpi = useMemo(() => {
    const pending = filteredBoardItems.filter((item) => item.workflowStatus === 'pending_approval').length
    const executing = filteredBoardItems.filter((item) => item.executionStatus === 'executing').length
    const overdue = filteredBoardItems.filter((item) => isOverdue(item.dueAt) && item.workflowStatus !== 'completed').length
    const rollback = filteredBoardItems.filter((item) => item.executionStatus === 'rollback_required').length
    return { pending, executing, overdue, rollback }
  }, [filteredBoardItems])

  return (
    <div className="h-full overflow-y-auto p-4">
      <div className="mx-auto w-full max-w-[1400px] space-y-4 pb-6">
        <Card>
          <CardHeader>
            <CardTitle>ダッシュボード</CardTitle>
            <CardDescription>AIReadyの状況を俯瞰し、優先対応と次のアクションを確認できます。</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap items-center gap-2">
              <button
                type="button"
                onClick={() => setTopViewTab('score')}
                className={cn(
                  'liquid-glass-tab rounded-full border px-4 py-2 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60',
                  topViewTab === 'score' ? 'liquid-glass-tab-active' : 'text-foreground'
                )}
                aria-current={topViewTab === 'score' ? 'page' : undefined}
              >
                AI Readyスコア
              </button>
              <button
                type="button"
                onClick={() => setTopViewTab('role-review')}
                className={cn(
                  'liquid-glass-tab rounded-full border px-4 py-2 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60',
                  topViewTab === 'role-review' ? 'liquid-glass-tab-active' : 'text-foreground'
                )}
                aria-current={topViewTab === 'role-review' ? 'page' : undefined}
              >
                是正ワークフロー
              </button>
            </div>
          </CardContent>
        </Card>

        {dashboardLoadError && (
          <Card className="border-rose-300 bg-rose-50">
            <CardContent className="p-3 text-sm text-rose-700">
              Dashboard API の取得に失敗しました: {dashboardLoadError}
            </CardContent>
          </Card>
        )}

        {topViewTab === 'score' ? (
          <>
            {isPreScanLocked && (
              <>
                <InitialScoringGateCard
                  description={INITIAL_SCORING_GATE_DEFAULT_DESCRIPTION}
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

            {(!isPreScanLocked || dashboardLoadError) && (
              <>
                <div className="grid gap-4 xl:grid-cols-3">
                  <Card className="xl:col-span-1 h-full flex flex-col">
                    <CardHeader>
                      <CardTitle>AIReady Readinessスコア</CardTitle>
                      <CardDescription>5シグナル（ガバナンス3軸 + オントロジー2軸）で統合評価します。</CardDescription>
                    </CardHeader>
                    <CardContent className="flex-1 pt-2">
                      <div className="h-[220px]">
                        {dashboardLoadError ? (
                          <div className="flex h-full flex-col items-center justify-center gap-2 px-3 text-center text-sm text-muted-foreground">
                            <p className="font-medium text-foreground">Readiness を取得できませんでした</p>
                            <p className="text-xs leading-relaxed">
                              上記のエラーメッセージを確認し、API（LightRAG）を再起動するか DynamoDB エンドポイント設定を見直してください。
                            </p>
                          </div>
                        ) : readinessUnavailable ? (
                          <div className="flex h-full flex-col items-center justify-center gap-2 px-3 text-center text-sm text-muted-foreground">
                            <p className="font-medium text-foreground">スコアはまだ表示されません</p>
                            <p className="text-xs leading-relaxed">
                              接続でデータソースにリンクし、ガバナンスで検知があるか、またはオントロジー用の統合メタデータ／候補が存在する場合に算出されます。
                            </p>
                          </div>
                        ) : (
                          <ReadinessGaugeD3
                            value={Number((weightedReadiness ?? 0).toFixed(1))}
                            target={READINESS_SLO_TARGET}
                          />
                        )}
                      </div>
                      <div className="-mt-1 text-center">
                        {dashboardLoadError ? (
                          <>
                            <p className="text-6xl font-bold leading-none text-muted-foreground">—</p>
                            <p className="mt-2 text-sm text-muted-foreground">
                              Dashboard API エラーのため数値を表示できません。
                            </p>
                          </>
                        ) : readinessUnavailable ? (
                          <>
                            <p className="text-6xl font-bold leading-none text-muted-foreground">—</p>
                            <p className="mt-2 text-sm text-muted-foreground">
                              データ接続とパイプライン実行後に Readiness が表示されます。
                            </p>
                          </>
                        ) : (
                          <>
                            <p
                              className="text-6xl font-bold leading-none transition-colors duration-150"
                              style={{ color: scoreHex(animatedReadiness) }}
                            >
                              {animatedReadiness.toFixed(1)}%
                            </p>
                            <p className="mt-2 text-sm text-muted-foreground">
                              {readinessBandComment(weightedReadiness ?? 0)}
                            </p>
                          </>
                        )}
                      </div>
                    </CardContent>
                  </Card>

                  <Card className="xl:col-span-2 h-full flex flex-col">
                    <CardHeader>
                      <CardTitle>AIReady Readinessスコア推移</CardTitle>
                      <CardDescription>ガバナンス/オントロジーの主要5指標推移を時系列で確認します。</CardDescription>
                    </CardHeader>
                    <CardContent className="flex-1 min-h-[280px]">
                      {readinessTrendChart === null ? (
                        <div className="flex min-h-[240px] items-center justify-center text-center text-sm text-muted-foreground px-4">
                          {dashboardLoadError
                            ? 'API エラーのため推移を表示できません。'
                            : '推移グラフは、Readiness スコアが算出可能になってから表示されます。'}
                        </div>
                      ) : (
                        <ChartContainer minHeight={240}>
                          {({ width, height }) => (
                            <LineChart data={readinessTrendChart} width={width} height={height}>
                              <CartesianGrid strokeDasharray="3 3" />
                              <XAxis dataKey="label" />
                              <YAxis domain={[50, 100]} />
                              <Tooltip />
                              <Legend />
                              <Line type="monotone" dataKey="governance_sensitive" name="機微情報保護" stroke="#ea580c" strokeWidth={2} dot={{ r: 2 }} />
                              <Line type="monotone" dataKey="governance_oversharing" name="過剰共有抑制" stroke="#dc2626" strokeWidth={2} dot={{ r: 2 }} />
                              <Line type="monotone" dataKey="governance_assurance" name="運用・保証" stroke="#2563eb" strokeWidth={2} dot={{ r: 2 }} />
                              <Line type="monotone" dataKey="ontology_foundation" name="情報整備" stroke="#7c3aed" strokeWidth={2} dot={{ r: 2 }} />
                              <Line type="monotone" dataKey="ontology_usecase" name="ユースケース解決力" stroke="#059669" strokeWidth={2} dot={{ r: 2 }} />
                            </LineChart>
                          )}
                        </ChartContainer>
                      )}
                    </CardContent>
                  </Card>
                </div>

                <Card>
                  <CardHeader>
                    <CardTitle>5シグナル詳細</CardTitle>
                    <CardDescription>ガバナンス3軸とオントロジー2軸を詳細指標で可視化します。</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="grid gap-4 xl:grid-cols-[1.05fr_1fr]">
                      <div className="h-[360px] xl:sticky xl:top-4">
                        {signalRadarChartData === null ? (
                          <div className="flex h-[320px] items-center justify-center text-center text-sm text-muted-foreground px-4">
                            {dashboardLoadError
                              ? 'API エラーのためレーダーを表示できません。'
                              : '一部シグナルが計測対象外のため、レーダーは表示されません。'}
                          </div>
                        ) : (
                          <ChartContainer minHeight={320}>
                            {({ width, height }) => (
                              <RadarChart data={signalRadarChartData} width={width} height={height}>
                                <PolarGrid />
                                <PolarAngleAxis dataKey="signal" />
                                <PolarRadiusAxis domain={[0, 100]} />
                                <Radar name="Readiness" dataKey="score" stroke={readinessRadarColor} fill={readinessRadarColor} fillOpacity={0.35} />
                                <Radar name="SLO" dataKey="target" stroke="#94a3b8" fill="#94a3b8" fillOpacity={0.08} />
                                <Legend />
                                <Tooltip />
                              </RadarChart>
                            )}
                          </ChartContainer>
                        )}
                      </div>
                      <div className="grid gap-3 grid-cols-1 md:grid-cols-2">
                        {signalBreakdownRows.map((row) => (
                          <Card key={row.key}>
                            <CardHeader className="pb-2">
                              <CardDescription>{row.label}</CardDescription>
                              <CardTitle className={scoreTextClass(row.score)}>
                                {row.score === null ? '—' : row.score.toFixed(1)}
                              </CardTitle>
                            </CardHeader>
                            <CardContent className="pt-0 space-y-1 text-xs text-muted-foreground">
                              {row.score === null && row.details.length === 0 ? (
                                <p>データ接続またはパイプラインの計測結果がないため、スコアは表示されません。</p>
                              ) : null}
                              {row.details.map((detail) => (
                                <p key={detail.key}>
                                  {detail.label}:{' '}
                                  <span
                                    className={cn(
                                      'font-medium',
                                      detail.unit === 'score' ? scoreTextClass(detail.value as number | null) : 'text-foreground'
                                    )}
                                  >
                                    {detail.value === null || detail.value === undefined
                                      ? '—'
                                      : detail.unit === 'count'
                                        ? Math.round(detail.value).toString()
                                        : detail.value.toFixed(1)}
                                    {detail.unit === 'percent' ? '%' : ''}
                                  </span>
                                </p>
                              ))}
                            </CardContent>
                          </Card>
                        ))}
                      </div>
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle>推奨アクションキュー</CardTitle>
                    <CardDescription>安全運用を優先した推奨アクションを表示します。</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>優先度</TableHead>
                          <TableHead>ドメイン</TableHead>
                          <TableHead>推奨アクション</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {actionQueue.map((item, idx) => (
                          <TableRow key={`${item.domain}-${idx}`}>
                            <TableCell>
                              <Badge variant={item.priority === 'P1' ? 'destructive' : 'secondary'}>{item.priority}</Badge>
                            </TableCell>
                            <TableCell>{item.domain}</TableCell>
                            <TableCell>{item.summary}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </CardContent>
                </Card>

                <TrustSignalsPanel
                  title="準備度運用チェック"
                  description="SLO監視と是正フローの観点で運用品質を確認します。"
                  uncertainty={
                    dashboardLoadError
                      ? 'Readiness API の取得に失敗したため、指標は更新されていません。'
                      : readinessUnavailable
                        ? 'Readiness はデータ接続と計測条件が揃うまで表示されません。'
                        : `Readiness ${(weightedReadiness ?? 0).toFixed(1)} / SLO ${READINESS_SLO_TARGET} / 低下中シグナル ${signalRows.filter((row) => (row.score ?? 0) < READINESS_SLO_TARGET).length} 件`
                  }
                  controllability="5シグナルごとに改善対象を切り分け、提案→承認→実行を段階制御できます。"
                  explainability="各シグナルのスコア根拠（件数/状態）をスコア詳細テーブルで説明できます。"
                  recovery="SLO割れ時は優先度付きアクションキューで復旧順序を明示し、劣化を短時間で回復します。"
                  evidenceability="finding_id / plan_id / correlation_id を監査ログで追跡し、後追い説明を担保します。"
                />
              </>
            )}
          </>
        ) : (
          <>
            <Card>
              <CardHeader>
                <CardTitle>是正ワークフロー確認ボード</CardTitle>
                <CardDescription>この画面では影響範囲と優先度を確認し、詳細画面で承認・実行します。</CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  <div>
                    <p className="mb-1 text-xs text-muted-foreground">優先度</p>
                    <div className="flex flex-wrap gap-2">
                      {(['all', 'P1', 'P2', 'P3'] as const).map((priority) => (
                        <button
                          key={priority}
                          type="button"
                          onClick={() => setPriorityFilter(priority)}
                          className={cn(
                            'liquid-glass-tab rounded-full border px-3 py-1.5 text-xs transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60',
                            priorityFilter === priority ? 'liquid-glass-tab-active' : 'text-foreground'
                          )}
                        >
                          {priority}
                        </button>
                      ))}
                    </div>
                  </div>
                  <div>
                    <p className="mb-1 text-xs text-muted-foreground">表示軸</p>
                    <Select value={laneMode} onValueChange={(value) => setLaneMode(value as LaneMode)}>
                      <SelectTrigger>
                        <SelectValue placeholder="表示軸を選択" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="category">カテゴリ別</SelectItem>
                        <SelectItem value="priority">優先度別</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <p className="mb-1 text-xs text-muted-foreground">担当者</p>
                    <Select value={assigneeFilter} onValueChange={setAssigneeFilter}>
                      <SelectTrigger>
                        <SelectValue placeholder="全員" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">全員</SelectItem>
                        {assigneeOptions.map((assignee) => (
                          <SelectItem key={assignee} value={assignee}>
                            {assignee}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <p className="mb-1 text-xs text-muted-foreground">検索</p>
                    <Input
                      value={searchText}
                      onChange={(event) => setSearchText(event.target.value)}
                      placeholder="plan_id / finding_id / item_id"
                    />
                  </div>
                </div>
              </CardContent>
            </Card>

            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
              <Card>
                <CardHeader className="pb-2">
                  <CardDescription>承認待ち</CardDescription>
                  <CardTitle>{remediationKpi.pending}</CardTitle>
                </CardHeader>
              </Card>
              <Card>
                <CardHeader className="pb-2">
                  <CardDescription>実行中</CardDescription>
                  <CardTitle>{remediationKpi.executing}</CardTitle>
                </CardHeader>
              </Card>
              <Card>
                <CardHeader className="pb-2">
                  <CardDescription>期限超過</CardDescription>
                  <CardTitle>{remediationKpi.overdue}</CardTitle>
                </CardHeader>
              </Card>
              <Card>
                <CardHeader className="pb-2">
                  <CardDescription>ロールバック要</CardDescription>
                  <CardTitle>{remediationKpi.rollback}</CardTitle>
                </CardHeader>
              </Card>
            </div>

            {laneValues.map((laneValue) => {
              const items = laneItems(String(laneValue))
              return (
                <Card key={String(laneValue)}>
                  <CardHeader className="pb-2">
                    <CardTitle>{laneLabel(String(laneValue))}</CardTitle>
                    <CardDescription>対象 {items.length} 件</CardDescription>
                  </CardHeader>
                  <CardContent className="overflow-x-auto">
                    <div
                      className="grid min-w-[780px] gap-3"
                      style={{ gridTemplateColumns: `repeat(${Math.max(assigneeOptions.length, 1)}, minmax(0, 1fr))` }}
                    >
                      {assigneeOptions.map((assignee) => {
                        const columnItems = items.filter((item) => item.assignee === assignee)
                        return (
                          <div key={assignee} className="rounded-lg border bg-muted/10 p-2">
                            <div className="mb-2 flex items-center justify-between">
                              <p className="text-xs font-semibold">{assignee}</p>
                              <Badge variant="outline">{columnItems.length}</Badge>
                            </div>
                            <div className="space-y-2">
                              {columnItems.length === 0 && (
                                <div className="rounded-md border border-dashed p-2 text-xs text-muted-foreground">
                                  アイテムなし
                                </div>
                              )}
                              {columnItems.map((item) => (
                                <div
                                  key={item.id}
                                  className="rounded-md border bg-background/70 p-2 space-y-2 cursor-pointer hover:border-primary/50 transition-colors"
                                  onClick={() => openDeepLink(item)}
                                  role="button"
                                  tabIndex={0}
                                  onKeyDown={(event) => {
                                    if (event.key === 'Enter' || event.key === ' ') {
                                      event.preventDefault()
                                      openDeepLink(item)
                                    }
                                  }}
                                >
                                  <div className="flex flex-wrap items-center gap-1">
                                    <Badge variant={categoryVariant(item.category)}>{categoryLabel(item.category)}</Badge>
                                    <Badge variant={priorityVariant(item.priority)}>{item.priority}</Badge>
                                    <Badge variant="outline">{workflowStatusLabel(item.workflowStatus)}</Badge>
                                  </div>
                                  <div>
                                    <p className="text-sm font-medium">{item.title}</p>
                                    <p className="text-[11px] text-muted-foreground">{item.planId}</p>
                                  </div>
                                  <div className="text-[11px] text-muted-foreground space-y-1">
                                    <p>担当: {item.assignee}</p>
                                    <p>期限: {item.dueAt}</p>
                                    <p>対象: {item.targetRef}</p>
                                    {item.category === 'governance' && (
                                      <p>
                                        リスク {item.riskScore?.toFixed(2)} / 露出 {item.exposureScore?.toFixed(2)} / 機微度{' '}
                                        {item.sensitivityScore?.toFixed(2)}
                                      </p>
                                    )}
                                    {item.category === 'ontology' && (
                                      <p>
                                        鮮度 {item.freshnessScore?.toFixed(2)} / 一意性 {item.uniquenessScore?.toFixed(2)} / 関連性{' '}
                                        {item.relevanceScore?.toFixed(2)} / 改善幅 +
                                        {(item.qualityDelta ?? 0).toFixed(2)}
                                      </p>
                                    )}
                                  </div>
                                  <p className="text-xs text-muted-foreground">{item.summary}</p>
                                  <div className="flex flex-wrap gap-1">
                                    <Button
                                      size="sm"
                                      variant="outline"
                                      onClick={(event) => {
                                        event.stopPropagation()
                                        openDeepLink(item)
                                      }}
                                    >
                                      詳細確認
                                    </Button>
                                  </div>
                                </div>
                              ))}
                            </div>
                          </div>
                        )
                      })}
                    </div>
                  </CardContent>
                </Card>
              )
            })}
          </>
        )}
      </div>
    </div>
  )
}

export default AIReadyDashboard
