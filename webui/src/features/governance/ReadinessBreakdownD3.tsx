import { useMemo, useState } from 'react'
import { scaleLinear } from 'd3'
import Badge from '@/components/ui/Badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/Card'
import { cn } from '@/lib/utils'

type ReadinessDetail = {
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
}

type ReadinessComponent = {
  key: string
  label: string
  score: number
  details: ReadinessDetail[]
}

interface ReadinessBreakdownD3Props {
  overallScore: number
  targetScore: number
  components: ReadinessComponent[]
  title?: string
  description?: string
  rootLabel?: string
  scoreLabel?: string
  targetLabel?: string
  lowerIsBetter?: boolean
  valueUnit?: string
  maxScaleValue?: number
  summaryCards?: Array<{
    key: string
    label: string
    score: number
    target?: number
    details?: ReadinessDetail[]
  }>
  weakestPosition?: 'top' | 'bottom'
  showComponentChart?: boolean
  showComposition?: boolean
  showPriorityList?: boolean
  computationBySummaryKey?: Record<
    string,
    {
      title: string
      subtitle?: string
      steps: Array<{ label: string; value: string; helper?: string }>
    }
  >
}

const colorByScore = (targetScore: number, lowerIsBetter: boolean, maxScaleValue: number) => {
  if (lowerIsBetter) {
    return scaleLinear<string>()
      .domain([0, targetScore, targetScore + maxScaleValue * 0.1, maxScaleValue])
      .range(['#15803d', '#22c55e', '#f59e0b', '#dc2626'])
      .clamp(true)
  }
  return scaleLinear<string>()
    .domain([0, targetScore - maxScaleValue * 0.1, targetScore, maxScaleValue])
    .range(['#dc2626', '#f59e0b', '#22c55e', '#15803d'])
    .clamp(true)
}

const scoreState = (score: number, targetScore: number, lowerIsBetter: boolean): 'good' | 'warn' | 'bad' => {
  if (lowerIsBetter) {
    if (score <= targetScore) return 'good'
    if (score <= targetScore + 10) return 'warn'
    return 'bad'
  }
  if (score >= targetScore) return 'good'
  if (score >= targetScore - 10) return 'warn'
  return 'bad'
}

const scoreBadgeVariant = (score: number, targetScore: number, lowerIsBetter: boolean): 'outline' | 'secondary' | 'destructive' => {
  const state = scoreState(score, targetScore, lowerIsBetter)
  if (state === 'good') return 'outline'
  if (state === 'warn') return 'secondary'
  return 'destructive'
}

const detailGreenShade = (index: number, total: number): string => {
  // Keep a consistent green hue and spread lightness
  const denominator = Math.max(1, total - 1)
  const t = index / denominator
  const lightness = 30 + t * 38 // 30% -> 68%
  return `hsl(145 72% ${lightness.toFixed(1)}%)`
}

const priorityBadgeTone = (index: number): string => {
  if (index === 0) return 'bg-rose-100 text-rose-800 border-rose-300'
  if (index === 1) return 'bg-amber-100 text-amber-900 border-amber-300'
  return 'bg-emerald-100 text-emerald-800 border-emerald-300'
}

export default function ReadinessBreakdownD3({
  overallScore,
  targetScore,
  components,
  title = 'Readinessスコア構成要素（D3可視化）',
  description = 'サンバーストで「総合 → 5要素 → 下位指標」を可視化。色はスコア（赤:低 / 緑:高）を示します。',
  rootLabel = 'Readiness',
  scoreLabel = '総合Readiness',
  targetLabel = 'Target',
  lowerIsBetter = false,
  valueUnit = '',
  maxScaleValue = 100,
  summaryCards,
  weakestPosition = 'top',
  showComponentChart = true,
  showComposition = true,
  showPriorityList = true,
  computationBySummaryKey
}: ReadinessBreakdownD3Props) {
  const sortedComponents = useMemo(
    () =>
      [...components].sort((a, b) => (lowerIsBetter ? b.score - a.score : a.score - b.score)),
    [components, lowerIsBetter]
  )
  const [activeFactorKey, setActiveFactorKey] = useState<string>(sortedComponents[0]?.key ?? '')
  const [activeSummaryKey, setActiveSummaryKey] = useState<string>(summaryCards?.[0]?.key ?? '')

  const fillScale = colorByScore(targetScore, lowerIsBetter, maxScaleValue)
  const factorBarScale = useMemo(
    () =>
      scaleLinear()
        .domain([0, Math.max(...components.map((component) => component.score), 1)])
        .range([0, 100])
        .clamp(true),
    [components]
  )
  const activeFactor =
    components.find((component) => component.key === activeFactorKey) ??
    sortedComponents[0]
  const activeSummaryCard = (summaryCards ?? []).find((card) => card.key === activeSummaryKey) ?? summaryCards?.[0]
  const detailTotal = Math.max(1, (activeFactor?.details ?? []).reduce((sum, detail) => sum + detail.score, 0))
  const detailSlices = useMemo(() => {
    let cumulative = 0
    return (activeFactor?.details ?? []).map((detail, index, arr) => {
      const ratio = detail.score / detailTotal
      const start = cumulative
      cumulative += ratio
      return {
        ...detail,
        ratio,
        start,
        end: cumulative,
        color: detailGreenShade(index, arr.length)
      }
    })
  }, [activeFactor, detailTotal])
  const detailGradient =
    detailSlices.length === 0
      ? 'conic-gradient(#e5e7eb 0% 100%)'
      : `conic-gradient(${detailSlices
        .map((slice) => {
          const from = (slice.start * 100).toFixed(2)
          const to = (slice.end * 100).toFixed(2)
          return `${slice.color} ${from}% ${to}%`
        })
        .join(', ')})`

  const weakestDetails = components
    .flatMap((component) =>
      component.details.map((detail) => ({
        id: `${component.key}-${detail.label}`,
        componentLabel: component.label,
        label: detail.label,
        score: detail.score
      }))
    )
    .sort((a, b) => (lowerIsBetter ? b.score - a.score : a.score - b.score))
    .slice(0, 5)

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-3">
          <div className={cn('grid gap-3', weakestPosition === 'top' ? 'xl:grid-cols-2' : 'xl:grid-cols-1')}>
            <div className="rounded-lg border p-3">
              {summaryCards && summaryCards.length > 0 ? (
                <div className="space-y-3">
                  {summaryCards.map((card) => {
                    const active = card.key === (activeSummaryCard?.key ?? '')
                    const cardComputation = computationBySummaryKey?.[card.key]
                    return (
                      <button
                        key={card.key}
                        type="button"
                        onClick={() => setActiveSummaryKey(card.key)}
                        className={cn(
                          'w-full rounded-md border p-3 text-left transition-all duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60',
                          active ? 'border-primary/40 bg-primary/5 shadow-sm' : 'hover:bg-accent'
                        )}
                        aria-pressed={active}
                      >
                        <p className="text-sm text-muted-foreground">{card.label}</p>
                        <div className="mt-1 flex items-center gap-2">
                          <p className="text-2xl font-semibold">{card.score.toFixed(1)}{valueUnit}</p>
                          <Badge
                            variant={scoreBadgeVariant(
                              card.score,
                              card.target ?? targetScore,
                              lowerIsBetter
                            )}
                          >
                            {scoreState(card.score, card.target ?? targetScore, lowerIsBetter) === 'good' ? '目標内' : '要改善'}
                          </Badge>
                        </div>
                        <p className="mt-1 text-xs text-muted-foreground">
                          {targetLabel}: {(card.target ?? targetScore).toFixed(1)}
                        </p>
                        {card.details && card.details.length > 0 && (
                          <p className="mt-1 text-[11px] text-muted-foreground">{active ? '内訳表示中' : 'クリックで内訳表示'}</p>
                        )}
                        {active && cardComputation && (
                          <div className="mt-3 rounded-md border bg-background/60 p-3">
                            <p className="text-xs text-muted-foreground">{cardComputation.title}</p>
                            {cardComputation.subtitle && (
                              <p className="mt-1 text-[11px] text-muted-foreground">{cardComputation.subtitle}</p>
                            )}
                            <div className="mt-2 grid gap-2 md:grid-cols-2">
                              {cardComputation.steps.map((step) => (
                                <div key={`${card.key}-${step.label}`} className="rounded-md border p-2">
                                  <p className="text-[11px] text-muted-foreground">{step.label}</p>
                                  <p className="mt-1 text-sm font-medium">{step.value}</p>
                                  {step.helper && <p className="mt-1 text-[11px] text-muted-foreground">{step.helper}</p>}
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                        {active && card.details && card.details.length > 0 && (
                          <div className="mt-3 rounded-md border bg-background/60 p-3">
                            <p className="text-xs text-muted-foreground">{card.label} の内訳</p>
                            <div className="mt-2 space-y-2">
                              {card.details.map((detail) => (
                                <div key={`${card.key}-${detail.label}`} className="space-y-1">
                                  <div className="flex items-center justify-between text-xs">
                                    <span>{detail.label}</span>
                                    <span className="font-medium">{detail.score.toFixed(1)}{valueUnit}</span>
                                  </div>
                                  {detail.note && (
                                    <p className="text-[11px] text-muted-foreground">{detail.note}</p>
                                  )}
                                  <div className="h-1.5 rounded-full bg-muted">
                                    <div
                                      className="h-1.5 rounded-full bg-emerald-500 transition-all"
                                      style={{ width: `${Math.max(2, detail.score)}%` }}
                                    />
                                  </div>
                                  {detail.evidence && detail.evidence.length > 0 && (
                                    <div className="rounded-sm border bg-background/70 p-2 text-[11px]">
                                      <p className="mb-1 text-muted-foreground">影響ファイル（上位）</p>
                                      <div className="space-y-1">
                                        {detail.evidence.slice(0, 3).map((entry) => (
                                          <div key={`${card.key}-${detail.label}-${entry.itemId}`} className="rounded-sm border p-1.5">
                                            <div className="flex items-center justify-between gap-2">
                                              <span className="line-clamp-1 font-medium">{entry.itemName}</span>
                                              <span className="text-muted-foreground">-{entry.impactPoints.toFixed(2)}pt</span>
                                            </div>
                                            <p className="line-clamp-1 text-muted-foreground">{entry.reason}</p>
                                          </div>
                                        ))}
                                      </div>
                                    </div>
                                  )}
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      </button>
                    )})}
                </div>
              ) : (
                <>
                  <p className="text-sm text-muted-foreground">{scoreLabel}</p>
                  <div className="mt-1 flex items-center gap-2">
                    <p className="text-3xl font-semibold">{overallScore.toFixed(1)}{valueUnit}</p>
                    <Badge variant={scoreBadgeVariant(overallScore, targetScore, lowerIsBetter)}>
                      {scoreState(overallScore, targetScore, lowerIsBetter) === 'good' ? '目標内' : '要改善'}
                    </Badge>
                  </div>
                  <p className="mt-2 text-sm text-muted-foreground">
                    {rootLabel} 全体の指標値です。下段チャートで要素ごと・内訳ごとに比較できます。
                  </p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    {targetLabel}: {targetScore.toFixed(1)}
                  </p>
                </>
              )}
            </div>

            {showPriorityList && weakestPosition === 'top' && (
              <div className="rounded-lg border p-3">
                <p className="text-sm text-muted-foreground">改善優先</p>
                <div className="mt-2 space-y-2">
                  {weakestDetails.map((item, index) => (
                    <div key={item.id} className="rounded-md border px-3 py-2">
                      <div className="flex items-center justify-between gap-3">
                        <span className={cn('rounded-full border px-2 py-0.5 text-[10px] font-semibold', priorityBadgeTone(index))}>
                          優先 {index + 1}
                        </span>
                        <span className="font-semibold text-sm">{item.score.toFixed(1)}{valueUnit}</span>
                      </div>
                      <p className="mt-1 text-sm">{item.label}</p>
                      <p className="text-xs text-muted-foreground">{item.componentLabel}</p>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          {showComponentChart && (
            <div className="rounded-lg border p-3">
              <p className="text-sm text-muted-foreground">要素別チャート</p>
              <div className="mt-3 space-y-2">
                {sortedComponents.map((component) => {
                  const active = activeFactor?.key === component.key
                  return (
                    <button
                      key={component.key}
                      type="button"
                      onClick={() => setActiveFactorKey(component.key)}
                      className={`w-full cursor-pointer rounded-md border px-3 py-2 text-left transition-all duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60 ${
                        active
                          ? 'bg-primary/5 border-primary/40 shadow-sm'
                          : 'hover:bg-accent'
                      }`}
                      aria-pressed={active}
                    >
                      <div className="flex items-center justify-between gap-3 text-sm">
                        <span className="font-medium">{component.label}</span>
                        <span className="flex items-center gap-2">
                          <span className="font-semibold">{component.score.toFixed(1)}{valueUnit}</span>
                          <span className="text-xs text-muted-foreground">{active ? '選択中' : 'クリック'}</span>
                        </span>
                      </div>
                      <div className="mt-2 h-2 rounded-full bg-muted">
                        <div
                          className="h-2 rounded-full transition-all"
                          style={{
                            width: `${Math.max(2, factorBarScale(component.score))}%`,
                            backgroundColor: fillScale(component.score)
                          }}
                        />
                      </div>
                    </button>
                  )
                })}
              </div>
            </div>
          )}

          {showComposition && (
            <div className="rounded-lg border p-3">
              <p className="text-sm text-muted-foreground">
                {activeFactor?.label ?? '内訳'} の内訳（構成比）
              </p>
              <div className="mt-3 grid gap-4 md:grid-cols-[220px_1fr]">
                <div className="flex items-center justify-center">
                  <div className="relative h-44 w-44 rounded-full" style={{ background: detailGradient }}>
                    <div className="absolute inset-7 rounded-full border bg-background/35 backdrop-blur-sm" />
                    <div className="absolute inset-0 flex flex-col items-center justify-center text-center">
                      <p className="text-xs text-muted-foreground">合計</p>
                      <p className="text-xl font-semibold">{detailTotal.toFixed(1)}{valueUnit}</p>
                    </div>
                  </div>
                </div>

                <div className="space-y-2">
                  {detailSlices
                    .slice()
                    .sort((a, b) => b.score - a.score)
                    .map((detail) => (
                      <div key={`${activeFactor?.key}-${detail.label}`} className="rounded-md border px-3 py-2">
                        <div className="flex items-center justify-between gap-3 text-sm">
                          <span className="flex items-center gap-2">
                            <span
                              className="h-2.5 w-2.5 rounded-full"
                              style={{ backgroundColor: detail.color }}
                            />
                            <span>{detail.label}</span>
                          </span>
                          <span className="font-semibold">
                            {detail.score.toFixed(1)}
                            {valueUnit}
                          </span>
                        </div>
                        <p className="mt-1 text-xs text-muted-foreground">
                          構成比 {(detail.ratio * 100).toFixed(1)}%
                        </p>
                      </div>
                    ))}
                </div>
              </div>
            </div>
          )}

          {showPriorityList && weakestPosition === 'bottom' && (
            <div className="rounded-lg border p-3">
              <p className="text-sm text-muted-foreground">改善優先</p>
              <div className="mt-2 space-y-2">
                {weakestDetails.map((item, index) => (
                  <div key={item.id} className="rounded-md border px-3 py-2">
                    <div className="flex items-center justify-between gap-3">
                      <span className={cn('rounded-full border px-2 py-0.5 text-[10px] font-semibold', priorityBadgeTone(index))}>
                        優先 {index + 1}
                      </span>
                      <span className="font-semibold text-sm">{item.score.toFixed(1)}{valueUnit}</span>
                    </div>
                    <p className="mt-1 text-sm">{item.label}</p>
                    <p className="text-xs text-muted-foreground">{item.componentLabel}</p>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
