import { useEffect, useMemo, useState } from 'react'
import Button from '@/components/ui/Button'
import Input from '@/components/ui/Input'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/Card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/Table'
import TablePageControls from '@/features/common/TablePageControls'

type AuditValue = string | number | null | undefined
type AuditRow = Record<string, AuditValue>

interface AuditWorkbenchProps {
  title: string
  description: string
  rows: AuditRow[]
  columns: Array<{ key: string; label: string }>
  searchKeys: string[]
  queryParamKey?: string
  onExportCsv?: (query: string) => Promise<void> | void
  onExportPdf?: (query: string) => Promise<void> | void
}

const toText = (value: AuditValue): string => (value === null || value === undefined ? '' : String(value))

const escapeCsv = (value: string): string => `"${value.replaceAll('"', '""')}"`

export default function AuditWorkbench({
  title,
  description,
  rows,
  columns,
  searchKeys,
  queryParamKey = 'auditQ',
  onExportCsv,
  onExportPdf
}: AuditWorkbenchProps) {
  const getQueryFromHash = (): string => {
    const hash = window.location.hash
    const queryIndex = hash.indexOf('?')
    if (queryIndex === -1) return ''
    const params = new URLSearchParams(hash.slice(queryIndex + 1))
    return params.get(queryParamKey) ?? ''
  }

  const [query, setQuery] = useState(getQueryFromHash)
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize, setPageSize] = useState(25)

  useEffect(() => {
    const hash = window.location.hash
    const queryIndex = hash.indexOf('?')
    const pathPart = queryIndex === -1 ? hash : hash.slice(0, queryIndex)
    const params = new URLSearchParams(queryIndex === -1 ? '' : hash.slice(queryIndex + 1))
    if (query.trim()) {
      params.set(queryParamKey, query)
    } else {
      params.delete(queryParamKey)
    }
    const newHash = params.toString() ? `${pathPart}?${params.toString()}` : pathPart
    if (newHash !== hash) {
      window.history.replaceState({}, '', `${window.location.pathname}${window.location.search}${newHash}`)
    }
  }, [query, queryParamKey])

  const filtered = useMemo(() => {
    if (!query.trim()) return rows
    const normalized = query.toLowerCase()
    return rows.filter((row) =>
      searchKeys.some((key) => toText(row[key]).toLowerCase().includes(normalized))
    )
  }, [rows, searchKeys, query])

  useEffect(() => {
    setCurrentPage(1)
  }, [query, pageSize])

  const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize))
  const pagedRows = useMemo(
    () => filtered.slice((currentPage - 1) * pageSize, currentPage * pageSize),
    [filtered, currentPage, pageSize]
  )

  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(totalPages)
  }, [currentPage, totalPages])

  const exportCsv = async () => {
    if (onExportCsv) {
      await onExportCsv(query)
      return
    }
    const exportedAt = new Date().toISOString()
    const metadata = [
      `"# title","${title.replaceAll('"', '""')}"`,
      `"# exportedAt","${exportedAt}"`,
      `"# query","${query.replaceAll('"', '""')}"`,
      `"# records","${filtered.length}"`
    ].join('\n')
    const header = columns.map((column) => escapeCsv(column.label)).join(',')
    const body = filtered
      .map((row) => columns.map((column) => escapeCsv(toText(row[column.key]))).join(','))
      .join('\n')
    const csv = `${metadata}\n\n${header}\n${body}`
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `${title.replaceAll(/\s+/g, '_')}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  }

  const exportPdfByPrint = async () => {
    if (onExportPdf) {
      await onExportPdf(query)
      return
    }
    const win = window.open('', '_blank', 'width=1200,height=800')
    if (!win) return
    const exportedAt = new Date().toISOString()

    const tableHeader = columns.map((column) => `<th>${column.label}</th>`).join('')
    const tableRows = filtered
      .map(
        (row) =>
          `<tr>${columns
            .map((column) => `<td>${toText(row[column.key]).replaceAll('<', '&lt;').replaceAll('>', '&gt;')}</td>`)
            .join('')}</tr>`
      )
      .join('')

    win.document.write(`
      <html>
        <head>
          <title>${title}</title>
          <style>
            body { font-family: Arial, sans-serif; padding: 24px; }
            h1 { font-size: 20px; margin: 0 0 8px 0; }
            p { margin: 0 0 16px 0; color: #555; }
            table { width: 100%; border-collapse: collapse; font-size: 12px; }
            th, td { border: 1px solid #ccc; padding: 8px; text-align: left; vertical-align: top; }
            th { background: #f5f5f5; }
          </style>
        </head>
        <body>
          <h1>${title}</h1>
          <p>${description}</p>
          <p><strong>Exported At:</strong> ${exportedAt}<br/>
             <strong>Query:</strong> ${query || '(none)'}<br/>
             <strong>Records:</strong> ${filtered.length}</p>
          <table>
            <thead><tr>${tableHeader}</tr></thead>
            <tbody>${tableRows}</tbody>
          </table>
        </body>
      </html>
    `)
    win.document.close()
    win.focus()
    win.print()
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex flex-wrap items-center gap-2">
          <Input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="監査ログを検索（ID / 実行者 / 操作 / 相関ID）"
            className="w-full max-w-[440px]"
          />
          <Button variant="outline" size="sm" onClick={() => void exportCsv()}>
            CSVを出力
          </Button>
          <Button variant="outline" size="sm" onClick={() => void exportPdfByPrint()}>
            PDFを出力
          </Button>
        </div>

        <TablePageControls
          totalRows={filtered.length}
          currentPage={currentPage}
          pageSize={pageSize}
          onPageChange={setCurrentPage}
          onPageSizeChange={setPageSize}
        />

        <Table>
          <TableHeader>
            <TableRow>
              {columns.map((column) => (
                <TableHead key={column.key}>{column.label}</TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {pagedRows.map((row, idx) => (
              <TableRow key={`${idx}-${toText(row[columns[0].key])}`}>
                {columns.map((column) => (
                  <TableCell key={column.key}>{toText(row[column.key])}</TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}
