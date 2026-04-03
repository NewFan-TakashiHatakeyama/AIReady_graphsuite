import Button from '@/components/ui/Button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/Select'

interface TablePageControlsProps {
  totalRows: number
  currentPage: number
  pageSize: number
  onPageSizeChange: (pageSize: number) => void
  onPageChange: (page: number) => void
}

const PAGE_SIZE_OPTIONS = [10, 25, 50] as const

export default function TablePageControls({
  totalRows,
  currentPage,
  pageSize,
  onPageSizeChange,
  onPageChange
}: TablePageControlsProps) {
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize))
  const start = totalRows === 0 ? 0 : Math.min((currentPage - 1) * pageSize + 1, totalRows)
  const end = totalRows === 0 ? 0 : Math.min(currentPage * pageSize, totalRows)

  return (
    <div className="mb-3 flex flex-wrap items-center justify-between gap-3 rounded-md border px-3 py-2">
      <p className="text-sm text-muted-foreground">
        {start}-{end} / {totalRows} 件
      </p>
      <div className="flex items-center gap-2">
        <span className="text-sm text-muted-foreground">表示件数</span>
        <Select value={String(pageSize)} onValueChange={(value) => onPageSizeChange(Number(value))}>
          <SelectTrigger className="h-8 w-20">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {PAGE_SIZE_OPTIONS.map((option) => (
              <SelectItem key={option} value={String(option)}>
                {option}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Button size="sm" variant="outline" onClick={() => onPageChange(Math.max(1, currentPage - 1))} disabled={currentPage <= 1}>
          前へ
        </Button>
        <span className="text-sm text-muted-foreground">
          {currentPage} / {totalPages}
        </span>
        <Button
          size="sm"
          variant="outline"
          onClick={() => onPageChange(Math.min(totalPages, currentPage + 1))}
          disabled={currentPage >= totalPages}
        >
          次へ
        </Button>
      </div>
    </div>
  )
}
