import { useEffect, useMemo, useState } from 'react'
import Button from '@/components/ui/Button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/Card'
import AuditWorkbench from '@/features/common/AuditWorkbench'
import { cn } from '@/lib/utils'
import { backendBaseUrl } from '@/lib/constants'
import {
  AuditDomainFilter,
  AuditRecordApiRow,
  createAuditExport,
  getAuditExportStatus,
  getAuditRecords
} from '@/api/graphsuite'
import { toast } from 'sonner'

type AuditDomain = 'connect' | 'governance' | 'ontology'

const domainLabels: Record<AuditDomain, string> = {
  connect: '接続',
  governance: 'ガバナンス',
  ontology: 'オントロジー'
}

const AuditCenter = () => {
  const [domain, setDomain] = useState<AuditDomain>('connect')
  const [rows, setRows] = useState<AuditRecordApiRow[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    const load = async () => {
      setLoading(true)
      try {
        const response = await getAuditRecords(domain as AuditDomainFilter, '', 500, 0)
        setRows(response.rows ?? [])
      } catch (error) {
        toast.error(error instanceof Error ? error.message : '監査ログの取得に失敗しました。')
      } finally {
        setLoading(false)
      }
    }
    void load()
  }, [domain])

  const waitExportReady = async (jobId: string) => {
    for (let i = 0; i < 20; i += 1) {
      const status = await getAuditExportStatus(jobId)
      if (status.status === 'completed' && status.download_url) return status.download_url
      if (status.status === 'failed') return null
      await new Promise((resolve) => setTimeout(resolve, 300))
    }
    return null
  }

  const runExport = async (format: 'csv' | 'pdf', query: string) => {
    const accepted = await createAuditExport(domain as AuditDomainFilter, query, format)
    const downloadUrl = await waitExportReady(accepted.job_id)
    if (!downloadUrl) {
      toast.error('エクスポート結果の取得に失敗しました。')
      return
    }
    const resolvedUrl = downloadUrl.startsWith('http') ? downloadUrl : `${backendBaseUrl}${downloadUrl}`
    window.open(resolvedUrl, '_blank', 'noopener,noreferrer')
  }

  const config = useMemo(() => {
    return {
      title: `${domainLabels[domain]}監査ログ 横断検索`,
      description: `${domainLabels[domain]}の証跡を横断検索し、CSV/PDFでエクスポートできます。`,
      rows: rows.map((row) => ({
        auditId: row.audit_id,
        operatedAt: row.occurred_at,
        operator: row.operator,
        action: row.action,
        target: row.target,
        correlationId: row.correlation_id
      })),
      columns: [
        { key: 'auditId', label: '監査ID' },
        { key: 'operatedAt', label: '操作日時' },
        { key: 'operator', label: '実行者' },
        { key: 'action', label: '操作内容' },
        { key: 'target', label: '対象' },
        { key: 'correlationId', label: '相関ID' }
      ],
      searchKeys: ['auditId', 'operatedAt', 'operator', 'action', 'target', 'correlationId'],
      queryParamKey: `${domain}AuditQ`
    }
  }, [domain, rows])

  return (
    <div className="h-full overflow-y-auto p-4">
      <div className="mx-auto w-full max-w-[1400px] space-y-4 pb-6">
        <Card className="border-primary/20 bg-gradient-to-r from-primary/10 via-background to-background shadow-md">
          <CardContent className="flex flex-wrap items-center justify-between gap-3 p-4">
            <div>
              <h1 className="text-xl font-semibold">監査ログ</h1>
              <p className="text-sm text-muted-foreground">
                接続 / ガバナンス / オントロジー の監査証跡を機能別に切り替えて確認します。
              </p>
            </div>
          </CardContent>
        </Card>

        <div className="rounded-xl border bg-card p-2 shadow-sm">
          <div className="flex flex-wrap gap-2">
            {(Object.keys(domainLabels) as AuditDomain[]).map((key) => (
              <Button
                key={key}
                size="sm"
                variant={domain === key ? 'default' : 'outline'}
                className={cn(domain === key ? 'liquid-glass-tab-active' : '')}
                onClick={() => setDomain(key)}
              >
                {domainLabels[key]}
              </Button>
            ))}
          </div>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>{domainLabels[domain]} 監査ログ</CardTitle>
            <CardDescription>対象機能: {domainLabels[domain]}</CardDescription>
          </CardHeader>
          <CardContent>
            {loading && <p className="mb-3 text-sm text-muted-foreground">監査ログを取得中です...</p>}
            <AuditWorkbench
              title={config.title}
              description={config.description}
              rows={config.rows}
              columns={config.columns}
              searchKeys={config.searchKeys}
              queryParamKey={config.queryParamKey}
              onExportCsv={(query) => runExport('csv', query)}
              onExportPdf={(query) => runExport('pdf', query)}
            />
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

export default AuditCenter
