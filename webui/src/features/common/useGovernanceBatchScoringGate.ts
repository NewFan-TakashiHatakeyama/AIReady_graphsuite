import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { toast } from 'sonner'
import {
  getGovernanceOverview,
  getGovernanceScanJobs,
  runGovernanceDailyScan,
  type GovernanceScanJobApiRow
} from '@/api/graphsuite'

export type GovernanceBatchScanJob = {
  jobId: string
  status: 'queued' | 'running' | 'partial' | 'failed' | 'success'
  startedAt: string
}

const mapScanJobRow = (row: GovernanceScanJobApiRow): GovernanceBatchScanJob => {
  const raw = String(row.status ?? '')
  const status: GovernanceBatchScanJob['status'] =
    raw === 'accepted'
      ? 'queued'
      : raw === 'running'
        ? 'running'
        : raw === 'partial'
          ? 'partial'
          : raw === 'failed'
            ? 'failed'
            : 'success'
  return {
    jobId: String(row.job_id ?? ''),
    status,
    startedAt: String(row.accepted_at ?? '-')
  }
}

type GovernanceBatchScoringGateOptions = {
  pollWhileRunning?: boolean
  onScanCompleted?: (job: GovernanceBatchScanJob) => void | Promise<void>
}

export function useGovernanceBatchScoringGate(options?: GovernanceBatchScoringGateOptions) {
  const pollWhileRunning = options?.pollWhileRunning ?? false
  const onScanCompleted = options?.onScanCompleted
  const [jobs, setJobs] = useState<GovernanceBatchScanJob[]>([])
  const [gateOpenFromOverview, setGateOpenFromOverview] = useState(false)
  const lastCompletedJobIdRef = useRef<string>('')

  const reload = useCallback(async () => {
    try {
      const [jobsResponse, overviewResponse] = await Promise.all([
        getGovernanceScanJobs(200, 0),
        getGovernanceOverview()
      ])
      setJobs(jobsResponse.rows.map(mapScanJobRow))
      setGateOpenFromOverview(Boolean(overviewResponse.initial_scan_gate_open))
    } catch {
      setJobs([])
      setGateOpenFromOverview(false)
    }
  }, [])

  useEffect(() => {
    void reload()
  }, [reload])

  const hasCompletedScan = useMemo(
    () => gateOpenFromOverview || jobs.some((row) => row.status === 'success'),
    [gateOpenFromOverview, jobs]
  )

  const latestBatchScanJob = useMemo(() => jobs[0] ?? null, [jobs])

  const isScanInProgress = Boolean(
    latestBatchScanJob &&
      (latestBatchScanJob.status === 'queued' ||
        latestBatchScanJob.status === 'running' ||
        latestBatchScanJob.status === 'partial')
  )

  useEffect(() => {
    if (!pollWhileRunning || !isScanInProgress) return
    const timer = window.setInterval(() => {
      void reload()
    }, 5000)
    return () => window.clearInterval(timer)
  }, [isScanInProgress, pollWhileRunning, reload])

  useEffect(() => {
    if (!onScanCompleted || !latestBatchScanJob) return
    if (latestBatchScanJob.status !== 'success') return
    if (!latestBatchScanJob.jobId) return
    if (lastCompletedJobIdRef.current === latestBatchScanJob.jobId) return
    lastCompletedJobIdRef.current = latestBatchScanJob.jobId
    void Promise.resolve(onScanCompleted(latestBatchScanJob))
  }, [latestBatchScanJob, onScanCompleted])

  const runDailyScan = useCallback(async () => {
    try {
      const response = await runGovernanceDailyScan()
      if (response.status === 'disabled') {
        toast.message(
          response.message ??
            'Connect 手動同期はこの環境では利用できません。ダッシュボード表示のみ続行します。'
        )
      } else if (response.job_id) {
        toast.success(`スコアリングを起動しました: ${response.job_id}`)
      } else {
        toast.success(response.message ?? 'スコアリングを受け付けました')
      }
      await reload()
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'スコアリング起動に失敗しました')
    }
  }, [reload])

  return {
    jobs,
    reload,
    hasCompletedScan,
    latestBatchScanJob,
    isScanInProgress,
    runDailyScan
  }
}
