import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/Card'

type GlossaryItem = {
  term: string
  description: string
}

interface FeatureOnboardingPanelProps {
  title: string
  purpose: string
  currentPageLabel: string
  currentPageDescription: string
  steps: string[]
  glossary: GlossaryItem[]
}

export default function FeatureOnboardingPanel({
  title,
  purpose,
  currentPageLabel,
  currentPageDescription,
  steps,
  glossary
}: FeatureOnboardingPanelProps) {
  return (
    <Card className="border-primary/30 bg-gradient-to-br from-primary/10 via-background to-background shadow-md">
      <CardHeader className="pb-3">
        <CardTitle className="text-base md:text-lg">{title}</CardTitle>
        <CardDescription className="leading-relaxed">{purpose}</CardDescription>
      </CardHeader>
      <CardContent className="grid gap-4 text-sm md:grid-cols-3">
        <div className="rounded-lg border bg-background/80 p-4">
          <p className="font-medium mb-1 text-foreground">現在の画面</p>
          <p className="text-muted-foreground leading-relaxed">
            <span className="font-medium text-foreground">{currentPageLabel}</span>
            {' - '}
            {currentPageDescription}
          </p>
        </div>

        <div className="rounded-lg border bg-background/80 p-4">
          <p className="font-medium mb-1 text-foreground">利用手順（3ステップ）</p>
          <ol className="list-decimal pl-5 space-y-1.5 text-muted-foreground">
            {steps.map((step) => (
              <li key={step}>{step}</li>
            ))}
          </ol>
        </div>

        <div className="rounded-lg border bg-background/80 p-4">
          <p className="font-medium mb-1 text-foreground">主要用語</p>
          <div className="space-y-1.5 text-muted-foreground leading-relaxed">
            {glossary.map((item) => (
              <p key={item.term}>
                <span className="font-medium text-foreground">{item.term}:</span> {item.description}
              </p>
            ))}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
