import Button from '@/components/ui/Button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from '@/components/ui/Dialog'

interface OperationConfirmDialogProps {
  isOpen: boolean
  actionType: 'execute' | 'rollback' | null
  planId: string
  scopeText: string
  dryRunSummaryText: string
  predictedImpactText: string
  rollbackCapabilityText: string
  reason: string
  isConfirmDisabled: boolean
  onClose: () => void
  onConfirm: () => void
}

const actionLabel = (actionType: OperationConfirmDialogProps['actionType']) => {
  if (actionType === 'execute') return '本実行'
  if (actionType === 'rollback') return 'ロールバック'
  return ''
}

const OperationConfirmDialog = ({
  isOpen,
  actionType,
  planId,
  scopeText,
  dryRunSummaryText,
  predictedImpactText,
  rollbackCapabilityText,
  reason,
  isConfirmDisabled,
  onClose,
  onConfirm
}: OperationConfirmDialogProps) => {
  const isExecute = actionType === 'execute'
  return (
    <Dialog open={isOpen} onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{actionLabel(actionType)}の最終確認</DialogTitle>
          <DialogDescription>
            {isExecute
              ? '何を・なぜ・どこまで変更するかを確認し、実行可否を最終判断してください。'
              : '復旧のためのロールバックです。何を戻し、どこまで巻き戻すかを確認してください。'}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-2 rounded-md border p-3 text-sm">
          <p>
            <span className="text-muted-foreground">何が（Plan ID）:</span> {planId}
          </p>
          <p>
            <span className="text-muted-foreground">どこまで（scope）:</span> {scopeText}
          </p>
          <p>
            <span className="text-muted-foreground">影響予測:</span> {predictedImpactText}
          </p>
          <p>
            <span className="text-muted-foreground">Dry-run 要約:</span> {dryRunSummaryText}
          </p>
          <p>
            <span className="text-muted-foreground">rollback 可否:</span> {rollbackCapabilityText}
          </p>
          <p>
            <span className="text-muted-foreground">なぜ（理由）:</span> {reason || '(未入力)'}
          </p>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            キャンセル
          </Button>
          <Button
            variant={isExecute ? 'default' : 'destructive'}
            onClick={onConfirm}
            disabled={isConfirmDisabled}
          >
            {actionLabel(actionType)}を実行
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

export default OperationConfirmDialog
