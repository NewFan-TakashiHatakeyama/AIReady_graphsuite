import Button from '@/components/ui/Button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/Card'

export const INITIAL_SCORING_GATE_DEFAULT_DESCRIPTION =
  '接続直後は、まず「スコアリングを実行」を行って完了させてください。完了までダッシュボード・検知結果・例外レジストリは表示されません。'

export const ONTOLOGY_INITIAL_SCORING_GATE_DESCRIPTION =
  '接続直後は、まず「スコアリングを実行」を行って完了させてください。完了まで概要・オントロジースコア・オントロジーグラフなどの主要指標は表示されません（監査ログ・ヘルプは参照できます）。'

type InitialScoringGateCardProps = {
  title?: string
  description: string
  latestBatchScanJob: { status: string; startedAt: string } | null
  onRunScoring: () => void
  isScanInProgress: boolean
  runButtonLabel?: string
}

export default function InitialScoringGateCard({
  title = '初回スコアリングが未完了です',
  description,
  latestBatchScanJob,
  onRunScoring,
  isScanInProgress,
  runButtonLabel = 'スコアリングを実行'
}: InitialScoringGateCardProps) {
  return (
    <Card className="border-amber-300 bg-amber-50/40">
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-wrap items-center justify-between gap-3">
        <div className="text-sm text-muted-foreground">
          最新ジョブ状態: {latestBatchScanJob ? latestBatchScanJob.status : '未実行'}
          {latestBatchScanJob ? ` / ${latestBatchScanJob.startedAt}` : ''}
        </div>
        <Button variant="outline" size="sm" onClick={() => void onRunScoring()} disabled={isScanInProgress}>
          {isScanInProgress ? 'スコアリング実行中...' : runButtonLabel}
        </Button>
      </CardContent>
    </Card>
  )
}
