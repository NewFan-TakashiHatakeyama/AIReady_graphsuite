import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/Card'

interface TrustSignalsPanelProps {
  title?: string
  description?: string
  uncertainty: string
  controllability: string
  explainability: string
  recovery: string
  evidenceability: string
}

const TrustSignalsPanel = ({
  title = 'Trust UX',
  description = 'AI運用を安全に任せるための確認ポイント',
  uncertainty,
  controllability,
  explainability,
  recovery,
  evidenceability
}: TrustSignalsPanelProps) => {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        <div className="rounded-md border p-3 text-sm">
          <p className="text-xs text-muted-foreground">不確実性の提示</p>
          <p className="mt-1">{uncertainty}</p>
        </div>
        <div className="rounded-md border p-3 text-sm">
          <p className="text-xs text-muted-foreground">ユーザー制御</p>
          <p className="mt-1">{controllability}</p>
        </div>
        <div className="rounded-md border p-3 text-sm">
          <p className="text-xs text-muted-foreground">説明可能性</p>
          <p className="mt-1">{explainability}</p>
        </div>
        <div className="rounded-md border p-3 text-sm">
          <p className="text-xs text-muted-foreground">エラー回復</p>
          <p className="mt-1">{recovery}</p>
        </div>
        <div className="rounded-md border p-3 text-sm">
          <p className="text-xs text-muted-foreground">証跡性</p>
          <p className="mt-1">{evidenceability}</p>
        </div>
      </CardContent>
    </Card>
  )
}

export default TrustSignalsPanel
