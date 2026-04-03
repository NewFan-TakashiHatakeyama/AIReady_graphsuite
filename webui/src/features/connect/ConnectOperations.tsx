import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import Badge, { BadgeProps } from '@/components/ui/Badge'
import Button from '@/components/ui/Button'
import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle
} from '@/components/ui/AlertDialog'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/Card'
import Checkbox from '@/components/ui/Checkbox'
import Input from '@/components/ui/Input'
import { Label } from '@/components/ui/Label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/Select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/Table'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/Tooltip'
import AuditWorkbench from '@/features/common/AuditWorkbench'
import FeatureOnboardingPanel from '@/features/common/FeatureOnboardingPanel'
import HoverHelpLabel from '@/features/common/HoverHelpLabel'
import TablePageControls from '@/features/common/TablePageControls'
import ChartContainer from '@/features/common/ChartContainer'
import { takeOperationDeepLinkForTab } from '@/features/common/operationDeepLink'
import { cn } from '@/lib/utils'
import { useAuthStore } from '@/stores/state'
import { ArrowRightCircle, PlusCircle } from 'lucide-react'
import { PolarAngleAxis, RadialBar, RadialBarChart } from 'recharts'
import {
  ConnectAuditApiRow,
  ConnectEventApiRow,
  ConnectJobApiRow,
  ConnectOnboardingDefaultsResponse,
  ConnectSiteOption,
  ConnectTeamChannelOption,
  ConnectTeamChannelOptionsResponse,
  ConnectOverviewResponse,
  ConnectOnboardingResponse,
  ConnectSubscriptionApiRow,
  createConnectOnboarding,
  deleteConnectSubscription,
  getConnectOnboardingDefaults,
  getConnectSiteOptions,
  getConnectTeamChannelOptions,
  getConnectAuditLogs,
  getConnectEvents,
  getConnectJobs,
  getConnectOverview,
  getConnectSubscriptions,
  resolveConnectSiteDiscovery,
  runConnectSyncCheck
} from '@/api/graphsuite'
import { ConnectPageKey, DeliveryStatus } from './types'
import { toast } from 'sonner'

const CONNECT_PAGES: Array<{ key: ConnectPageKey; label: string }> = [
  { key: 'overview', label: '概要' },
  { key: 'connections', label: '接続' },
  { key: 'help', label: 'ヘルプ' }
]

const CONNECT_PAGE_GUIDE: Record<ConnectPageKey, string> = {
  onboarding: '新規接続の設定を3ステップで完了します。初期導入時に利用します。',
  connections: '接続先ごとの利用状態と運用導線を確認します。',
  overview: '接続の健全性を確認します。障害・滞留・失敗ジョブを優先確認します。',
  subscriptions: 'Webhook購読の有効期限と状態を確認します。',
  events: '受信イベントの処理状態を確認し、失敗や重複を特定します。',
  jobs: '同期ジョブの進捗と失敗要因を確認します。',
  scopes: '監視対象（テナント/サイト/ドライブ）を確認します。',
  audit: '運用操作の監査証跡を確認します。',
  help: '接続の操作手順と主要用語を確認します。'
}

const CONNECT_GLOSSARY = [
  { term: 'Subscription', description: 'Microsoft Graph から変更通知を受け取るための購読設定です。' },
  { term: 'Event', description: 'ファイル変更などの通知1件を表します。' },
  { term: 'Job', description: 'イベントをもとに実行される同期/変換処理です。' }
]

const CONNECT_TAB_HELP: Record<ConnectPageKey, string> = {
  onboarding: '接続ソース・監視対象の設定のあと、確認して接続を作成します。',
  connections: 'M365接続の詳細とデータソースの利用可否を確認します。',
  overview: '接続の主要KPIを確認します。',
  subscriptions: 'Webhook購読の期限・状態を確認します。',
  events: '受信イベントの処理結果を確認します。',
  jobs: '同期ジョブの進捗と失敗内容を確認します。',
  scopes: '監視対象の範囲を確認します。',
  audit: '監査証跡を確認します。',
  help: '使い方と用語を確認します。'
}

const TOKYO_TIME_ZONE = 'Asia/Tokyo'
const TOKYO_DATE_TIME_FORMATTER = new Intl.DateTimeFormat('ja-JP', {
  timeZone: TOKYO_TIME_ZONE,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false
})

const formatDateTimeInTokyo = (value: string | null | undefined): string => {
  const raw = String(value ?? '').trim()
  if (!raw || raw === '-') return '-'
  const parsed = new Date(raw)
  if (Number.isNaN(parsed.getTime())) return raw
  return `${TOKYO_DATE_TIME_FORMATTER.format(parsed)} JST`
}

const parseResourceDisplay = (resource: string | null | undefined): { fileName: string; path: string } => {
  const raw = String(resource ?? '').trim()
  if (!raw) return { fileName: '-', path: '-' }
  try {
    const parsed = new URL(raw)
    const decodedPath = decodeURIComponent(parsed.pathname || '/')
    const pathSegments = decodedPath.split('/').filter(Boolean)
    const lastSegment = pathSegments[pathSegments.length - 1] || ''
    const fileName = lastSegment || parsed.hostname || raw
    return { fileName, path: `${parsed.hostname}${decodedPath}` }
  } catch {
    const normalized = raw.replace(/\\/g, '/')
    const segments = normalized.split('/').filter(Boolean)
    const fileName = segments[segments.length - 1] || normalized
    return { fileName, path: raw }
  }
}

const statusBadgeVariant = (status: string): BadgeProps['variant'] => {
  if (status === 'failed' || status === 'dead-lettered') return 'destructive'
  if (status === 'degraded' || status === 'expiring' || status === 'retrying') return 'secondary'
  return 'outline'
}

const statusBadgeClassName = (status: string): string => {
  if (status === 'healthy' || status === 'active' || status === 'success' || status === 'processed') {
    return 'border-emerald-300 bg-emerald-100 text-emerald-800'
  }
  if (status === 'degraded' || status === 'expiring' || status === 'retrying') {
    return 'border-amber-300 bg-amber-100 text-amber-900'
  }
  if (status === 'initializing') {
    return 'border-indigo-300 bg-indigo-100 text-indigo-900'
  }
  if (status === 'running' || status === 'queued') {
    return 'border-sky-300 bg-sky-100 text-sky-900'
  }
  if (status === 'duplicated') {
    return 'border-violet-300 bg-violet-100 text-violet-900'
  }
  return ''
}

const deliveryStatusText = (status: DeliveryStatus): string => {
  if (status === 'healthy') return '正常'
  if (status === 'degraded') return '要監視'
  return '障害'
}

const eventSuccessRate24h = (events: ConnectEventApiRow[] = []): number => {
  if (events.length === 0) return 0
  const success = events.filter((event) => event.status === 'processed').length
  return Number(((success / events.length) * 100).toFixed(1))
}

const retryingJobs24h = (jobs: ConnectJobApiRow[] = []): number =>
  jobs.filter((job) => job.status === 'retrying' || job.status === 'dead-lettered').length

const CONNECT_WIZARD_STEPS = [
  { key: 'scope', label: '接続ソース', decision: '利用するクラウド接続（現在は Microsoft 365）を選びます。通知まわりはシステム既定値を使います。' },
  { key: 'initial-sync', label: '監視対象設定', decision: 'サイトや Teams のチャネルなど、監視したい場所を選びます。' },
  { key: 'go-live', label: '確認・接続実行', decision: '内容を確認し、接続作成を実行します。' }
] as const

const CONNECT_SOURCE_CATALOG = [
  { id: 'm365', label: 'M365', iconPath: '/img/M365_icon.png', enabled: true },
  { id: 'google-drive', label: 'Google Drive', iconPath: '/img/google-drive_icon.png', enabled: false },
  { id: 'slack', label: 'Slack', iconPath: '/img/slack_icon.png', enabled: false },
  { id: 'mail', label: 'Mail', iconPath: '/img/mail_icon.png', enabled: false },
  { id: 'jira', label: 'Jira', iconPath: '/img/jira_icon.png', enabled: false },
  { id: 'notion', label: 'Notion', iconPath: '/img/notion_icon.png', enabled: false },
  { id: 'box', label: 'Box', iconPath: '/img/box_icon.png', enabled: false }
] as const

type M365ConnectionStep = 'subscriptions' | 'events' | 'jobs'

const M365_STEP_CONTENT: Record<
  M365ConnectionStep,
  { title: string; description: string; hint?: string }
> = {
  subscriptions: {
    title: 'M365接続状況',
    description: '購読状態の行をクリックすると受信イベントへ進みます。',
    hint:
      '「initializing」は Microsoft Graph の購読 ID が Connections / SSM にまだ無いときに表示されます（init_subscription 未完了・失敗時など）。購読が active になるまで通知は届きません。'
  },
  events: {
    title: '受信イベント',
    description: '選択中の接続リソースに関連する受信イベントを表示しています。',
    hint:
      'ドライブ接続では一覧は DynamoDB のファイルメタデータ（取り込み済み行）を元にしています。メタデータを削除した直後や Webhook / delta 同期が止まっていると 0 件になります（過去イベントは復元されません）。'
  },
  jobs: {
    title: '実行ジョブ',
    description: '受信イベントから遷移した関連ジョブを表示しています。'
  }
}

const paginateRows = <T,>(rows: T[], page: number, pageSize: number): T[] =>
  rows.slice((page - 1) * pageSize, page * pageSize)

const hasPlaceholderSubscription = (rows: ConnectSubscriptionApiRow[]): boolean =>
  rows.some((row) => row.is_placeholder || row.status === 'initializing' || row.reflection_status === 'pending')

const onboardingStatusLabel = (status?: string | null): string => {
  if (!status) return '未開始'
  if (status === 'succeeded') return '完了'
  if (status === 'initializing' || status === 'started') return '処理中'
  if (status === 'failed') return '失敗'
  if (status === 'skipped') return 'スキップ'
  return status
}

const extractDriveIdFromResource = (resource: string | undefined): string => {
  const normalized = String(resource ?? '').trim()
  const matched = normalized.match(/\/drives\/([^/]+)\/root/i)
  return matched?.[1]?.trim() ?? ''
}

/** Align with connect/src/connectors/m365/messages.parse_message_resource conversation_key. */
const extractConversationKeyFromMessageResource = (resource: string | undefined): string => {
  const normalized = String(resource ?? '')
    .trim()
    .replace(/^\/+/, '')
  const parts = normalized.split('/').filter(Boolean)
  if (
    parts.length >= 5 &&
    parts[0] === 'teams' &&
    parts[2] === 'channels' &&
    parts[4] === 'messages'
  ) {
    return `team:${parts[1]}:channel:${parts[3]}`
  }
  if (parts.length >= 3 && parts[0] === 'chats' && parts[2] === 'messages') {
    return `chat:${parts[1]}`
  }
  return ''
}

const scopeIdFromSubscriptionResource = (resource: string | undefined): string => {
  const driveId = extractDriveIdFromResource(resource)
  if (driveId) return `scope-${driveId}`
  const conversationKey = extractConversationKeyFromMessageResource(resource)
  return conversationKey ? `scope-msg-${conversationKey}` : ''
}

const OnboardingPage = ({
  onCompleted,
  existingSubscriptions
}: {
  onCompleted: (response: ConnectOnboardingResponse) => Promise<void> | void
  existingSubscriptions: ConnectSubscriptionApiRow[]
}) => {
  const [stepIndex, setStepIndex] = useState(0)
  const currentStep = CONNECT_WIZARD_STEPS[stepIndex]
  const [connectionCreated, setConnectionCreated] = useState(false)
  const [onboardingResult, setOnboardingResult] = useState<ConnectOnboardingResponse | null>(null)
  const [defaultsLoaded, setDefaultsLoaded] = useState(false)
  const [siteOptionsLoading, setSiteOptionsLoading] = useState(false)
  const [siteOptions, setSiteOptions] = useState<ConnectSiteOption[]>([])
  const [teamChannelOptionsLoading, setTeamChannelOptionsLoading] = useState(false)
  const [teamChannelOptions, setTeamChannelOptions] = useState<ConnectTeamChannelOption[]>([])
  const [teamChannelWarnings, setTeamChannelWarnings] = useState<string[]>([])
  const [teamPermissionGuide, setTeamPermissionGuide] = useState<{
    phase1: string[]
    phase2: string[]
  }>({ phase1: [], phase2: [] })
  const existingDriveIds = useMemo(
    () => new Set(existingSubscriptions.map((row) => extractDriveIdFromResource(row.resource)).filter(Boolean)),
    [existingSubscriptions]
  )
  const [draft, setDraft] = useState({
    tenantId: '',
    clientId: '',
    clientSecretParameter: 'MSGraphClientSecret',
    authMethod: 'client_secret',
    permissionProfile: 'sites_selected',
    notificationUrl: 'https://webhook.graphsuite.jp',
    clientStateParameter: 'MSGraphSecretClientState',
    siteUrl: '',
    siteQuery: '',
    selectedSiteOptionId: '',
    selectedSiteName: '',
    selectedTeamChannelKey: '',
    selectedTeamName: '',
    selectedChannelName: '',
    tlsVersion: 'v1_2',
    enableWaf: true,
    enableIpAllowList: true,
    sourceType: 'sharepoint',
    targetType: 'drive',
    siteId: '',
    driveId: '',
    teamId: '',
    channelId: '',
    chatId: '',
    connectionName: '',
    resourcePath: 'drives/{driveId}/root',
    changeType: 'updated',
    queueName: 'FileNotificationQueue-{tenant_id}',
    dlqName: 'FileNotificationDLQ-{tenant_id}',
    maxReceiveCount: '3',
    tokenRenewRate: 'rate(30 minutes)',
    subscriptionRenewRate: 'rate(1 day)',
    deltaReconcileCron: 'cron(0 3 * * ? *)',
    confirmNoHardCodedSecret: false,
    confirmPermissionGranted: false,
    /** Teams のみ: チャネルバンドル vs チャット（Chat ID） */
    teamsMonitorKind: 'channel' as 'channel' | 'chat',
  })
  const isDriveTarget = draft.targetType === 'drive'
  const isDuplicateDrive = Boolean(
    isDriveTarget && draft.driveId.trim() && existingDriveIds.has(draft.driveId.trim())
  )
  const teamsBundleDuplicateDrive = Boolean(
    draft.sourceType === 'teams' &&
      draft.driveId.trim() &&
      existingDriveIds.has(draft.driveId.trim())
  )

  const updateDraft = <K extends keyof typeof draft>(key: K, value: (typeof draft)[K]) => {
    setDraft((prev) => ({ ...prev, [key]: value }))
  }

  const teamChannelOptionKey = (option: ConnectTeamChannelOption): string =>
    `${option.team_id}::${option.channel_id}`

  const canonicalSiteBase = (rawUrl: string): string => {
    const normalized = String(rawUrl || '').trim()
    if (!normalized) return ''
    try {
      const parsed = new URL(normalized)
      const segments = parsed.pathname.split('/').filter(Boolean)
      if (segments.length >= 2 && (segments[0].toLowerCase() === 'sites' || segments[0].toLowerCase() === 'teams')) {
        return `${parsed.host.toLowerCase()}/${segments[0].toLowerCase()}/${segments[1].toLowerCase()}`
      }
      return `${parsed.host.toLowerCase()}${parsed.pathname.replace(/\/+$/, '').toLowerCase()}`
    } catch {
      return normalized.toLowerCase().replace(/\/+$/, '')
    }
  }

  const isTeamsDerivedSite = (site: ConnectSiteOption): boolean => {
    const normalizedSiteId = String(site.site_id || '').trim()
    const normalizedSiteBase = canonicalSiteBase(site.site_web_url)
    return teamChannelOptions.some((option) => {
      const optionSiteId = String(option.site_id || '').trim()
      if (normalizedSiteId && optionSiteId && normalizedSiteId === optionSiteId) {
        return true
      }
      const optionFolderBase = canonicalSiteBase(option.files_folder_web_url || '')
      return Boolean(normalizedSiteBase && optionFolderBase && normalizedSiteBase === optionFolderBase)
    })
  }

  const effectiveSiteSourceType = (site: ConnectSiteOption): 'teams' | 'sharepoint' | 'onedrive' => {
    if (isTeamsDerivedSite(site)) return 'teams'
    return site.source_type
  }

  const selectedTeamChannelOption = useMemo(
    () => teamChannelOptions.find((row) => teamChannelOptionKey(row) === draft.selectedTeamChannelKey),
    [teamChannelOptions, draft.selectedTeamChannelKey]
  )

  const applyDefaults = useCallback((defaults: ConnectOnboardingDefaultsResponse) => {
    setDraft((prev) => ({
      ...prev,
      tenantId: prev.tenantId || defaults.tenant_id || prev.tenantId,
      clientId: prev.clientId || defaults.client_id || prev.clientId,
      authMethod: defaults.auth_method || prev.authMethod,
      permissionProfile: defaults.permission_profile || prev.permissionProfile,
      notificationUrl: prev.notificationUrl === 'https://example.com/webhook'
        ? (defaults.notification_url || prev.notificationUrl)
        : prev.notificationUrl,
      clientSecretParameter: prev.clientSecretParameter || defaults.client_secret_parameter || prev.clientSecretParameter,
      clientStateParameter: prev.clientStateParameter || defaults.client_state_parameter || prev.clientStateParameter
    }))
  }, [])

  useEffect(() => {
    let active = true
    const loadDefaults = async () => {
      try {
        const defaults = await getConnectOnboardingDefaults()
        if (!active) return
        applyDefaults(defaults)
      } catch {
        // Keep onboarding usable even if defaults endpoint is unavailable.
      } finally {
        if (active) setDefaultsLoaded(true)
      }
    }
    void loadDefaults()
    return () => {
      active = false
    }
  }, [applyDefaults])

  const loadSiteOptions = async () => {
    setSiteOptionsLoading(true)
    try {
      const response = await getConnectSiteOptions({
        source_type: draft.sourceType as 'sharepoint' | 'teams' | 'onedrive',
        query: draft.siteQuery.trim()
      })
      setSiteOptions(response.rows)
      if (!response.rows.length) {
        toast('サイト候補が見つかりません', {
          description: '検索語を変えるか、データソース種別を確認してください。'
        })
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'サイト候補の取得に失敗しました。')
    } finally {
      setSiteOptionsLoading(false)
    }
  }

  const loadTeamAndChannelOptions = async () => {
    if (draft.sourceType !== 'teams') return
    setTeamChannelOptionsLoading(true)
    try {
      const response: ConnectTeamChannelOptionsResponse = await getConnectTeamChannelOptions({
        site_id: draft.siteId.trim(),
        team_query: '',
        channel_query: '',
      })
      setTeamChannelOptions(response.rows || [])
      setTeamChannelWarnings(response.warnings || [])
      setTeamPermissionGuide({
        phase1: response.required_application_permissions_phase1 || [],
        phase2: response.required_application_permissions_phase2 || [],
      })
      if (!response.rows?.length) {
        toast('Teamsチャネル候補が見つかりません', {
          description: '権限不足、検索条件、または対象サイトを確認してください。'
        })
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Teamsチャネル候補の取得に失敗しました。')
    } finally {
      setTeamChannelOptionsLoading(false)
    }
  }

  const resolveFromSelectedSite = async (selectedSiteId: string) => {
    if (!selectedSiteId) return
    try {
      const resolved = await resolveConnectSiteDiscovery({
        site_id: selectedSiteId
      })
      const selectedSite = siteOptions.find((site) => site.site_id === selectedSiteId)
      setDraft((prev) => ({
        ...prev,
        selectedSiteOptionId: selectedSiteId,
        selectedSiteName: selectedSite?.site_name || prev.selectedSiteName,
        siteId: resolved.site_id || prev.siteId,
        driveId: resolved.drive_id || prev.driveId,
        resourcePath: resolved.drive_id ? `drives/${resolved.drive_id}/root` : prev.resourcePath,
        connectionName:
          prev.connectionName ||
          selectedSite?.site_name ||
          resolved.suggested_connection_name ||
          prev.connectionName
      }))
      toast('サイト選択からIDを反映しました', {
        description: `site_id=${resolved.site_id} / drive_id=${resolved.drive_id}`
      })
      if (resolved.drive_id && existingDriveIds.has(resolved.drive_id)) {
        toast.warning('既存接続と同じ Drive ID です', {
          description: '別サイトを登録する場合は、他の候補を選択してください。'
        })
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'サイト選択の解決に失敗しました。')
    }
  }

  const applyTeamChannelSelection = (value: string) => {
    const selected = teamChannelOptions.find((option) => teamChannelOptionKey(option) === value)
    if (!selected) return
    setDraft((prev) => ({
      ...prev,
      selectedTeamChannelKey: value,
      selectedTeamName: selected.team_name || prev.selectedTeamName,
      selectedChannelName: selected.channel_name || prev.selectedChannelName,
      teamId: selected.team_id || prev.teamId,
      channelId: selected.channel_id || prev.channelId,
      siteId: selected.site_id || prev.siteId,
      driveId: selected.files_drive_id || prev.driveId,
      resourcePath:
        prev.sourceType === 'teams' && selected.team_id && selected.channel_id
          ? `teams/${selected.team_id}/channels/${selected.channel_id}/messages`
          : prev.targetType === 'channel'
            ? `teams/${selected.team_id}/channels/${selected.channel_id}/messages`
            : selected.files_drive_id
              ? `drives/${selected.files_drive_id}/root`
              : prev.resourcePath,
      connectionName:
        prev.connectionName ||
        `${selected.team_name} - ${selected.channel_name}` ||
        prev.connectionName
    }))
    if (selected.files_drive_id && existingDriveIds.has(selected.files_drive_id)) {
      toast.warning('既存接続と同じ Drive ID です', {
        description: '別のチーム/チャネル候補を選択してください。'
      })
    }
  }

  useEffect(() => {
    setDraft((prev) => {
      if (prev.sourceType === 'teams') {
        if (prev.teamId.trim() && prev.channelId.trim()) {
          const nextPath = `teams/${prev.teamId.trim()}/channels/${prev.channelId.trim()}/messages`
          if (prev.resourcePath === nextPath && prev.changeType === 'created,updated,deleted') return prev
          return { ...prev, resourcePath: nextPath, changeType: 'created,updated,deleted' }
        }
        if (prev.driveId.trim()) {
          const nextPath = `drives/${prev.driveId.trim()}/root`
          if (prev.resourcePath === nextPath && prev.changeType === 'updated') return prev
          return { ...prev, resourcePath: nextPath, changeType: 'updated' }
        }
        return prev
      }
      if (prev.targetType === 'channel' && prev.teamId.trim() && prev.channelId.trim()) {
        const nextPath = `teams/${prev.teamId.trim()}/channels/${prev.channelId.trim()}/messages`
        if (prev.resourcePath === nextPath) return prev
        return { ...prev, resourcePath: nextPath, changeType: 'created,updated,deleted' }
      }
      if (prev.targetType === 'chat' && prev.chatId.trim()) {
        const nextPath = `chats/${prev.chatId.trim()}/messages`
        if (prev.resourcePath === nextPath) return prev
        return { ...prev, resourcePath: nextPath, changeType: 'created,updated,deleted' }
      }
      if (prev.targetType === 'drive' && prev.driveId.trim()) {
        const nextPath = `drives/${prev.driveId.trim()}/root`
        if (prev.resourcePath === nextPath) return prev
        return { ...prev, resourcePath: nextPath, changeType: 'updated' }
      }
      return prev
    })
  }, [draft.sourceType, draft.targetType, draft.driveId, draft.teamId, draft.channelId, draft.chatId])

  const canMoveNext = (): boolean => {
    if (stepIndex === 0) {
      return Boolean(
        defaultsLoaded &&
          draft.notificationUrl.trim() &&
          draft.clientStateParameter.trim()
      )
    }
    if (stepIndex === 1) {
      if (draft.sourceType === 'teams') {
        if (draft.teamsMonitorKind === 'chat') {
          return Boolean(draft.chatId.trim())
        }
        const hasChannel = Boolean(draft.teamId.trim() && draft.channelId.trim())
        const hasDrive = Boolean(draft.driveId.trim())
        if (!hasChannel && !hasDrive) return false
        if (teamsBundleDuplicateDrive) return false
        return true
      }
      if (draft.targetType === 'drive') {
        return Boolean(draft.driveId.trim() && draft.resourcePath.trim() && !isDuplicateDrive)
      }
      if (draft.targetType === 'channel') {
        return Boolean(draft.teamId.trim() && draft.channelId.trim() && draft.resourcePath.trim())
      }
      if (draft.targetType === 'chat') {
        return Boolean(draft.chatId.trim() && draft.resourcePath.trim())
      }
      return false
    }
    if (stepIndex === 2) {
      return draft.confirmNoHardCodedSecret && draft.confirmPermissionGranted
    }
    return false
  }

  const executeCreateConnection = async () => {
    if (!canMoveNext()) {
      toast('必須項目が未入力です', {
        description: '確認チェックを含む必須項目を入力してください。'
      })
      return
    }
    if (draft.sourceType !== 'teams' && isDuplicateDrive) {
      toast.error('既存接続と同じリソースです', {
        description: '同じ Drive ID は登録できません。別サイトを選択してください。'
      })
      return
    }
    if (draft.sourceType === 'teams' && draft.teamsMonitorKind === 'channel' && teamsBundleDuplicateDrive) {
      toast.error('既存接続と同じリソースです', {
        description: '同じ Drive ID は登録できません。別のチーム/チャネルを選択してください。'
      })
      return
    }

    const baseConnectionName =
      draft.connectionName.trim() ||
      draft.selectedSiteName.trim() ||
      `${draft.selectedTeamName || ''} - ${draft.selectedChannelName || ''}`.trim() ||
      `${draft.tenantId.trim() || 'tenant'}-${draft.driveId.trim() || 'drive'}`

    const runSingleOnboarding = async (params: {
      resource_type: 'drive' | 'message'
      target_type: 'drive' | 'channel' | 'chat'
      drive_id: string
      resource_path: string
      change_type: string
      team_id: string
      channel_id: string
      chat_id: string
      connection_name: string
    }) => {
      return createConnectOnboarding({
        client_id: '',
        site_id: draft.siteId.trim(),
        drive_id: params.drive_id,
        notification_url: draft.notificationUrl.trim(),
        client_secret: '',
        client_state: draft.clientStateParameter.trim(),
        resource_type: params.resource_type,
        resource_path: params.resource_path,
        change_type: params.change_type,
        target_type: params.target_type,
        team_id: params.team_id,
        channel_id: params.channel_id,
        chat_id: params.chat_id,
        connection_name: params.connection_name,
        initialize_subscription: true
      })
    }

    try {
      if (draft.sourceType === 'teams') {
        if (draft.teamsMonitorKind === 'chat') {
          const cid = draft.chatId.trim()
          const response = await runSingleOnboarding({
            resource_type: 'message',
            target_type: 'chat',
            drive_id: '',
            resource_path: `chats/${cid}/messages`,
            change_type: 'created,updated,deleted',
            team_id: '',
            channel_id: '',
            chat_id: cid,
            connection_name:
              draft.connectionName.trim() ||
              `Teams Chat ${cid.length > 32 ? `${cid.slice(0, 32)}…` : cid}`
          })
          setConnectionCreated(true)
          setOnboardingResult(response)
          toast('Teams チャット接続を作成しました', {
            description: `tenant=${response.tenant_id} / 履歴はバックグラウンドで取り込みます（上限あり）`
          })
          await onCompleted(response)
          return
        }

        const hasChannel = Boolean(draft.teamId.trim() && draft.channelId.trim())
        const hasDrive = Boolean(draft.driveId.trim())
        const jobs: Array<() => Promise<ConnectOnboardingResponse>> = []

        if (hasDrive) {
          const d = draft.driveId.trim()
          jobs.push(() =>
            runSingleOnboarding({
              resource_type: 'drive',
              target_type: 'drive',
              drive_id: d,
              resource_path: `drives/${d}/root`,
              change_type: 'updated',
              team_id: '',
              channel_id: '',
              chat_id: '',
              connection_name: `${baseConnectionName} (Files)`
            })
          )
        }
        if (hasChannel) {
          const tid = draft.teamId.trim()
          const cid = draft.channelId.trim()
          jobs.push(() =>
            runSingleOnboarding({
              resource_type: 'message',
              target_type: 'channel',
              drive_id: '',
              resource_path: `teams/${tid}/channels/${cid}/messages`,
              change_type: 'created,updated,deleted',
              team_id: tid,
              channel_id: cid,
              chat_id: '',
              connection_name: `${baseConnectionName} (Channel messages)`
            })
          )
        }
        if (jobs.length === 0) {
          toast('作成する接続がありません', {
            description: 'チャネルに紐づくファイル（drive）またはチャンネルメッセージのいずれかを選んでください。'
          })
          return
        }

        let lastResponse: ConnectOnboardingResponse | null = null
        for (let i = 0; i < jobs.length; i += 1) {
          try {
            lastResponse = await jobs[i]()
          } catch (error) {
            toast.error(
              error instanceof Error ? error.message : '接続設定の作成に失敗しました。',
              { description: `${i + 1}/${jobs.length} 件目で失敗しました。先に作成された接続はそのまま残ります。` }
            )
            return
          }
        }
        if (lastResponse) {
          setConnectionCreated(true)
          setOnboardingResult(lastResponse)
          toast('Teams 向け接続を作成しました', {
            description: `${jobs.length} 件（Files / チャンネルメッセージの組み合わせ） tenant=${lastResponse.tenant_id}`
          })
          await onCompleted(lastResponse)
        }
        return
      }

      const response = await runSingleOnboarding({
        resource_type: draft.targetType === 'drive' ? 'drive' : 'message',
        target_type: draft.targetType as 'drive' | 'channel' | 'chat',
        drive_id: draft.driveId.trim(),
        resource_path: draft.resourcePath.trim(),
        change_type: draft.changeType.trim(),
        team_id: draft.teamId.trim(),
        channel_id: draft.channelId.trim(),
        chat_id: draft.chatId.trim(),
        connection_name: baseConnectionName
      })
      setConnectionCreated(true)
      setOnboardingResult(response)
      toast('接続設定を作成しました', {
        description:
          `tenant=${response.tenant_id} / connection_id=${response.connection_id ?? '-'} / bootstrap=${response.bootstrap_status ?? response.subscription_init_status ?? 'n/a'}`
      })
      await onCompleted(response)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : '接続設定の作成に失敗しました。')
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>接続 新規接続</CardTitle>
        <CardDescription>
          接続ソースと監視対象を選び、確認のうえ接続を作成します。通知 URL など運用パラメータは既定値を利用します。
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-2 md:grid-cols-3">
          {CONNECT_WIZARD_STEPS.map((step, idx) => (
            <div
              key={step.key}
              className={cn(
                'rounded-md border p-2 text-center text-sm',
                idx <= stepIndex ? 'border-primary/40 bg-primary/10' : 'text-muted-foreground'
              )}
            >
              <span className="font-medium">{idx + 1}.</span> {step.label}
            </div>
          ))}
        </div>

        <Card className="border-primary/30">
          <CardHeader className="pb-2">
            <CardDescription>現在ステップ</CardDescription>
            <CardTitle className="text-base">{currentStep.label}</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-muted-foreground space-y-2">
            <p>判断ポイント: {currentStep.decision}</p>
            {stepIndex === 0 && (
              <div className="grid gap-3 md:grid-cols-2">
                <div className="space-y-2 md:col-span-2">
                  <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
                    {CONNECT_SOURCE_CATALOG.map((source) => {
                      const isSelected = source.id === 'm365'
                      return (
                        <div
                          role="button"
                          tabIndex={source.enabled ? 0 : -1}
                          key={source.id}
                          className={cn(
                            'rounded-lg border p-3 text-left transition-all',
                            source.enabled
                              ? 'border-primary/40 bg-primary/5 hover:bg-primary/10'
                              : 'cursor-not-allowed opacity-65 border-dashed',
                            isSelected && 'ring-2 ring-primary/30'
                          )}
                          onClick={() => {
                            if (!source.enabled) return
                            updateDraft('sourceType', 'sharepoint')
                          }}
                          aria-disabled={!source.enabled}
                          onKeyDown={(event) => {
                            if (!source.enabled) return
                            if (event.key === 'Enter' || event.key === ' ' || event.key === 'Spacebar') {
                              event.preventDefault()
                              updateDraft('sourceType', 'sharepoint')
                            }
                          }}
                        >
                          <div className="flex items-center justify-between">
                            <div className="flex items-center gap-2">
                              <div className="flex h-8 w-8 items-center justify-center overflow-hidden p-0">
                                <img src={source.iconPath} alt={`${source.label} icon`} className="h-full w-full object-contain" loading="lazy" />
                              </div>
                              <span className="text-sm font-medium">{source.label}</span>
                            </div>
                            <Checkbox checked={source.enabled && isSelected} disabled aria-label={`${source.label} source availability`} />
                          </div>
                          <p className="mt-1 text-xs text-muted-foreground">
                            {source.enabled ? '利用可能（現在対応）' : '未対応（選択不可）'}
                          </p>
                        </div>
                      )
                    })}
                  </div>
                </div>
                {!defaultsLoaded && (
                  <p className="md:col-span-2 text-xs text-muted-foreground">
                    既存接続から通知まわりの既定値を読み込み中です。完了するまで「次へ」は進めません。
                  </p>
                )}
              </div>
            )}

            {stepIndex === 1 && (
              <div className="grid gap-3 md:grid-cols-2">
                <div className="space-y-1.5 md:col-span-2">
                  <Label>データソース種別</Label>
                  <Select
                    value={draft.sourceType}
                    onValueChange={(value) => {
                      const normalized = value as 'sharepoint' | 'teams' | 'onedrive'
                      setDraft((prev) => ({
                        ...prev,
                        sourceType: normalized,
                        targetType: 'drive',
                        teamsMonitorKind: 'channel',
                        selectedSiteOptionId: '',
                        selectedSiteName: '',
                        selectedTeamChannelKey: '',
                        selectedTeamName: '',
                        selectedChannelName: '',
                        teamId: '',
                        channelId: '',
                        chatId: '',
                      }))
                      setSiteOptions([])
                      setTeamChannelOptions([])
                      setTeamChannelWarnings([])
                    }}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="データソース種別を選択" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="sharepoint">SharePoint</SelectItem>
                      <SelectItem value="teams">Teams</SelectItem>
                      <SelectItem value="onedrive">OneDrive</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                {draft.sourceType === 'teams' && (
                  <div className="space-y-1.5 md:col-span-2">
                    <Label>Teams 監視の種類</Label>
                    <Select
                      value={draft.teamsMonitorKind}
                      onValueChange={(value) => {
                        const kind = value as 'channel' | 'chat'
                        setDraft((prev) => ({
                          ...prev,
                          teamsMonitorKind: kind,
                          chatId: kind === 'channel' ? '' : prev.chatId,
                        }))
                      }}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="channel">チャネル（ファイル・チャネルメッセージ）</SelectItem>
                        <SelectItem value="chat">1:1 / グループ チャット（Chat ID）</SelectItem>
                      </SelectContent>
                    </Select>
                    <p className="text-xs text-muted-foreground">
                      チャットを選ぶと Microsoft Graph の chats リソース（メッセージ一覧）を購読します。接続成功後に過去メッセージをバックグラウンドで取り込みます（環境変数による上限あり）。
                    </p>
                  </div>
                )}
                {draft.sourceType !== 'teams' && (
                  <div className="space-y-1.5 md:col-span-2">
                    <Label>監視ターゲット</Label>
                    <Select
                      value={draft.targetType}
                      onValueChange={(value) => {
                        const normalized = value as 'drive' | 'channel' | 'chat'
                        setDraft((prev) => ({
                          ...prev,
                          targetType: normalized,
                          resourcePath:
                            normalized === 'drive'
                              ? (prev.driveId.trim() ? `drives/${prev.driveId.trim()}/root` : prev.resourcePath)
                              : normalized === 'channel'
                                ? (prev.teamId.trim() && prev.channelId.trim()
                                  ? `teams/${prev.teamId.trim()}/channels/${prev.channelId.trim()}/messages`
                                  : prev.resourcePath)
                                : (prev.chatId.trim() ? `chats/${prev.chatId.trim()}/messages` : prev.resourcePath),
                          changeType: normalized === 'drive' ? 'updated' : 'created,updated,deleted',
                        }))
                      }}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="監視ターゲットを選択" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="drive">Files（drive）</SelectItem>
                        <SelectItem value="channel" disabled={draft.sourceType !== 'teams'}>
                          Teams Channel messages
                        </SelectItem>
                        <SelectItem value="chat">Teams Chat messages</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                )}
                {draft.sourceType !== 'teams' && (
                  <>
                    <div className="space-y-1.5 md:col-span-2">
                      <Label htmlFor="siteQuery">サイト名検索（任意）</Label>
                      <Input
                        id="siteQuery"
                        placeholder="例: test1"
                        value={draft.siteQuery}
                        onChange={(event) => updateDraft('siteQuery', event.target.value)}
                      />
                    </div>
                    <div className="space-y-1.5 md:col-span-2">
                      <Label>サイト名選択</Label>
                      <Select
                        value={draft.selectedSiteOptionId}
                        onValueChange={(value) => {
                          const selectedSite = siteOptions.find((site) => site.site_id === value)
                          updateDraft('selectedSiteOptionId', value)
                          updateDraft('selectedSiteName', selectedSite?.site_name ?? '')
                          if (!draft.connectionName.trim()) {
                            updateDraft('connectionName', selectedSite?.site_name ?? '')
                          }
                          void resolveFromSelectedSite(value)
                        }}
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="候補からサイトを選択" />
                        </SelectTrigger>
                        <SelectContent>
                          {siteOptions.length === 0 ? (
                            <SelectItem value="__empty" disabled>候補なし（先に候補取得）</SelectItem>
                          ) : (
                            siteOptions.map((site) => (
                              <SelectItem key={site.site_id} value={site.site_id}>
                                {`[${effectiveSiteSourceType(site)}] ${site.site_name}`}
                              </SelectItem>
                            ))
                          )}
                        </SelectContent>
                      </Select>
                      {draft.selectedSiteOptionId && (
                        <p className="text-xs text-muted-foreground">
                          {(() => {
                            const selectedSite = siteOptions.find((site) => site.site_id === draft.selectedSiteOptionId)
                            const src = selectedSite ? effectiveSiteSourceType(selectedSite) : '-'
                            const url =
                              siteOptions.find((site) => site.site_id === draft.selectedSiteOptionId)?.site_web_url ?? '-'
                            return `source=${src} / url=${url}`
                          })()}
                        </p>
                      )}
                      {(isDuplicateDrive || teamsBundleDuplicateDrive) && draft.sourceType !== 'teams' && (
                        <p className="text-xs text-amber-700">
                          既存接続と同じ Drive ID です。別サイト登録の場合は候補を見直してください。
                        </p>
                      )}
                    </div>
                    <div className="space-y-1.5 md:col-span-2">
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() => void loadSiteOptions()}
                        disabled={siteOptionsLoading}
                      >
                        {siteOptionsLoading ? '候補取得中...' : 'サイト候補取得（データソース種別から）'}
                      </Button>
                    </div>
                  </>
                )}
                {draft.sourceType === 'teams' && draft.teamsMonitorKind === 'chat' && (
                  <div className="space-y-1.5 md:col-span-2">
                    <Label htmlFor="teamsChatId">Chat ID</Label>
                    <Input
                      id="teamsChatId"
                      placeholder="19:xxxxxxxx@thread.v2"
                      value={draft.chatId}
                      onChange={(event) => updateDraft('chatId', event.target.value)}
                    />
                    <p className="text-xs text-muted-foreground">
                      Teams クライアントや Graph エクスプローラでチャット ID を確認してください。ゲスト
                      DM などは権限・ポリシーにより取得できない場合があります。
                    </p>
                  </div>
                )}
                {draft.sourceType === 'teams' && draft.teamsMonitorKind === 'channel' && (
                  <>
                    <div className="space-y-1.5 md:col-span-2">
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() => void loadTeamAndChannelOptions()}
                        disabled={teamChannelOptionsLoading}
                      >
                        {teamChannelOptionsLoading ? '取得中...' : 'Teamsチャネル候補取得（team/channel/filesFolder）'}
                      </Button>
                      {teamPermissionGuide.phase1.length > 0 && (
                        <p className="text-xs text-muted-foreground">
                          必要権限(Phase1): {teamPermissionGuide.phase1.join(', ')}
                        </p>
                      )}
                      {teamChannelWarnings.map((warning, idx) => (
                        <p key={`team-warning-${idx}`} className="text-xs text-amber-700">{warning}</p>
                      ))}
                    </div>
                    <div className="space-y-1.5 md:col-span-2">
                      <Label>Team / Channel 選択（任意）</Label>
                      <Select
                        value={draft.selectedTeamChannelKey}
                        onValueChange={(value) => {
                          updateDraft('selectedTeamChannelKey', value)
                          applyTeamChannelSelection(value)
                        }}
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="候補からTeam / Channelを選択" />
                        </SelectTrigger>
                        <SelectContent>
                          {teamChannelOptions.length === 0 ? (
                            <SelectItem value="__empty-team-channel" disabled>
                              候補なし（先にTeamsチャネル候補取得）
                            </SelectItem>
                          ) : (
                            teamChannelOptions.map((option) => (
                              <SelectItem key={teamChannelOptionKey(option)} value={teamChannelOptionKey(option)}>
                                {`${option.team_name} / ${option.channel_name} (${option.files_drive_id || 'drive未解決'})`}
                              </SelectItem>
                            ))
                          )}
                        </SelectContent>
                      </Select>
                      {selectedTeamChannelOption && (
                        <p className="text-xs text-muted-foreground">
                          filesFolder: {selectedTeamChannelOption.files_folder_web_url || '-'}
                        </p>
                      )}
                      {(isDuplicateDrive || teamsBundleDuplicateDrive) && (
                        <p className="text-xs text-amber-700">
                          既存接続と同じ Drive ID です。別チャネル登録の場合は候補を見直してください。
                        </p>
                      )}
                    </div>
                  </>
                )}
                {draft.sourceType !== 'teams' && draft.targetType === 'chat' && (
                  <div className="space-y-1.5 md:col-span-2">
                    <Label htmlFor="chatId">Chat ID</Label>
                    <Input
                      id="chatId"
                      placeholder="19:xxxxxxxx@thread.v2"
                      value={draft.chatId}
                      onChange={(event) => updateDraft('chatId', event.target.value)}
                    />
                    <p className="text-xs text-muted-foreground">
                      {'Microsoft Graph `/chats/{chatId}/messages` を監視します。'}
                    </p>
                  </div>
                )}
              </div>
            )}

            {stepIndex === 2 && (
              <div className="space-y-3">
                <div className="rounded-md border p-3">
                  <p className="font-medium text-foreground">接続作成サマリ</p>
                  <p>テナント: {draft.tenantId || '-'}</p>
                  <p>データソース: {draft.sourceType || '-'}</p>
                  <p>
                    監視の種類:{' '}
                    {draft.sourceType === 'teams'
                      ? draft.teamsMonitorKind === 'chat'
                        ? 'Teams チャット（1:1 / グループ）'
                        : 'Teams（ファイル + チャンネルメッセージ）'
                      : draft.targetType === 'drive'
                        ? 'サイトのファイル（ライブラリ）'
                        : draft.targetType === 'channel'
                          ? 'チャンネルメッセージ'
                          : draft.targetType === 'chat'
                            ? 'チャットメッセージ'
                            : draft.targetType || '-'}
                  </p>
                  {draft.sourceType !== 'teams' && (
                    <p>サイト: {draft.selectedSiteName || draft.connectionName || '-'}</p>
                  )}
                  <p>
                    Team / チャネル:{' '}
                    {draft.selectedTeamName || draft.selectedChannelName
                      ? `${draft.selectedTeamName || '-'} / ${draft.selectedChannelName || '-'}`
                      : '-'}
                  </p>
                  {draft.sourceType === 'teams' && draft.teamsMonitorKind === 'chat' && (
                    <p>Chat ID: {draft.chatId || '-'}</p>
                  )}
                  {draft.sourceType !== 'teams' && draft.targetType === 'chat' && (
                    <p>チャット: {draft.chatId ? '指定あり' : '未指定'}</p>
                  )}
                  {draft.sourceType === 'teams' && draft.teamsMonitorKind === 'channel' && (
                    <div className="mt-2 border-t pt-2 text-xs text-muted-foreground">
                      <p className="font-medium text-foreground">作成予定の接続</p>
                      {draft.driveId.trim() ? <p>ファイル（チャネルに紐づくライブラリ）</p> : null}
                      {draft.teamId.trim() && draft.channelId.trim() ? (
                        <p>
                          チャンネルメッセージ: {draft.selectedTeamName || '選択中の Team'} /{' '}
                          {draft.selectedChannelName || '選択中のチャネル'}
                        </p>
                      ) : null}
                    </div>
                  )}
                  {draft.sourceType === 'teams' && draft.teamsMonitorKind === 'chat' && (
                    <div className="mt-2 border-t pt-2 text-xs text-muted-foreground">
                      <p className="font-medium text-foreground">作成予定の接続</p>
                      <p>チャットメッセージ（購読 + 履歴バックフィル）</p>
                    </div>
                  )}
                  <p>
                    表示名（接続名）:{' '}
                    {(() => {
                      const fromTeams =
                        draft.selectedTeamName && draft.selectedChannelName
                          ? `${draft.selectedTeamName} / ${draft.selectedChannelName}`
                          : ''
                      const fromTeamsChat =
                        draft.sourceType === 'teams' &&
                        draft.teamsMonitorKind === 'chat' &&
                        draft.chatId.trim()
                          ? `Teams Chat ${draft.chatId.trim().length > 28 ? `${draft.chatId.trim().slice(0, 28)}…` : draft.chatId.trim()}`
                          : ''
                      const label =
                        draft.connectionName.trim() ||
                        draft.selectedSiteName.trim() ||
                        fromTeams ||
                        fromTeamsChat
                      return label || '-'
                    })()}
                  </p>
                </div>
                <label className="flex items-center gap-2">
                  <Checkbox
                    checked={draft.confirmNoHardCodedSecret}
                    onCheckedChange={(checked) => updateDraft('confirmNoHardCodedSecret', Boolean(checked))}
                  />
                  <span>シークレットをコードにハードコードしていない</span>
                </label>
                <label className="flex items-center gap-2">
                  <Checkbox
                    checked={draft.confirmPermissionGranted}
                    onCheckedChange={(checked) => updateDraft('confirmPermissionGranted', Boolean(checked))}
                  />
                  <span>Graph API 権限（Sites.Read.All / Files.Read.All / Group.Read.All / Team.ReadBasic.All 等）を付与済み</span>
                </label>
                <Button size="sm" onClick={() => void executeCreateConnection()} disabled={!canMoveNext()}>
                  接続を作成
                </Button>
                {connectionCreated && (
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant="outline" className="border-emerald-300 bg-emerald-100 text-emerald-800">
                      接続設定の作成が完了しました
                    </Badge>
                    <Badge
                      variant={onboardingResult?.bootstrap_status === 'failed' ? 'destructive' : 'outline'}
                      className={onboardingResult?.bootstrap_status === 'succeeded'
                        ? 'border-emerald-300 bg-emerald-100 text-emerald-800'
                        : onboardingResult?.bootstrap_status === 'failed'
                          ? ''
                          : 'border-indigo-300 bg-indigo-100 text-indigo-900'}
                    >
                      初回接続: {onboardingStatusLabel(onboardingResult?.bootstrap_status ?? onboardingResult?.subscription_init_status)}
                    </Badge>
                    {onboardingResult?.subscription_id && (
                      <Badge variant="outline">subscription_id: {onboardingResult.subscription_id}</Badge>
                    )}
                  </div>
                )}
              </div>
            )}
          </CardContent>
        </Card>

        <div className="flex items-center justify-between">
          <Button
            variant="outline"
            size="sm"
            disabled={stepIndex === 0}
            onClick={() => setStepIndex((prev) => Math.max(0, prev - 1))}
          >
            戻る
          </Button>
          <Badge variant={stepIndex === CONNECT_WIZARD_STEPS.length - 1 ? 'outline' : 'secondary'}>
            Step {stepIndex + 1}/{CONNECT_WIZARD_STEPS.length}
          </Badge>
          <Button
            size="sm"
            disabled={stepIndex === CONNECT_WIZARD_STEPS.length - 1 || !canMoveNext()}
            onClick={() => setStepIndex((prev) => Math.min(CONNECT_WIZARD_STEPS.length - 1, prev + 1))}
          >
            次へ
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}

const ConnectionsPage = ({
  deepLinkFocus,
  subscriptions,
  events,
  eventsTotalCount,
  eventsResolvedTenantId,
  eventsRequestedScopeId,
  jobs,
  jobsLoading,
  loading,
  loadError,
  onScopeSelected,
  onJobsNeeded,
  onDeleteSubscription
}: {
  deepLinkFocus?: string
  subscriptions: ConnectSubscriptionApiRow[]
  events: ConnectEventApiRow[]
  eventsTotalCount: number
  eventsResolvedTenantId?: string
  eventsRequestedScopeId?: string
  jobs: ConnectJobApiRow[]
  jobsLoading: boolean
  loading: boolean
  loadError: string | null
  onScopeSelected?: (scopeId: string) => void
  onJobsNeeded?: () => void
  onDeleteSubscription?: (row: ConnectSubscriptionApiRow, mode: 'safe' | 'force') => Promise<void> | void
}) => {
  const [viewMode, setViewMode] = useState<'source-list' | 'm365-detail'>('source-list')
  const [activeSourceId, setActiveSourceId] = useState<string>('m365')
  const [step, setStep] = useState<M365ConnectionStep>('subscriptions')
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize, setPageSize] = useState<number>(25)
  const [selectedSubscriptionId, setSelectedSubscriptionId] = useState<string | null>(null)
  const [selectedScopeId, setSelectedScopeId] = useState<string | null>(null)
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null)
  const [pendingDeleteSubscription, setPendingDeleteSubscription] = useState<ConnectSubscriptionApiRow | null>(null)
  const [deleteSubmitting, setDeleteSubmitting] = useState(false)

  const selectedScopeIdText = selectedScopeId ?? ''
  const decodedSelectedScopeId = selectedScopeIdText ? decodeURIComponent(selectedScopeIdText) : ''
  const strictlyFilteredEvents = selectedScopeIdText
    ? events.filter((event) => {
      const eventScope = String(event.scope_id ?? '')
      return eventScope === selectedScopeIdText || eventScope === decodedSelectedScopeId
    })
    : events
  // Defensive fallback: if scope id format differs between frontend/backend,
  // do not hide already-fetched rows.
  const visibleEvents =
    selectedScopeIdText && strictlyFilteredEvents.length === 0 && events.length > 0
      ? events
      : strictlyFilteredEvents
  const visibleJobs =
    step === 'jobs' && selectedJobId
      ? jobs.filter(
        (job) =>
          job.id === selectedJobId ||
          String(job.correlation_id ?? '') === selectedJobId ||
          String(job.event_id ?? '') === selectedJobId
      )
      : jobs

  const totalRows = useMemo(() => {
    if (step === 'subscriptions') return subscriptions.length
    if (step === 'events') return Math.max(visibleEvents.length, eventsTotalCount)
    return visibleJobs.length
  }, [step, subscriptions.length, visibleEvents.length, visibleJobs.length, eventsTotalCount])
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize))

  const pagedSubscriptions = useMemo(
    () => paginateRows(subscriptions, currentPage, pageSize),
    [subscriptions, currentPage, pageSize]
  )
  const pagedEvents = useMemo(
    () => paginateRows(visibleEvents, currentPage, pageSize),
    [visibleEvents, currentPage, pageSize]
  )
  const pagedJobs = useMemo(
    () => paginateRows(visibleJobs, currentPage, pageSize),
    [visibleJobs, currentPage, pageSize]
  )

  useEffect(() => {
    setCurrentPage(1)
  }, [step, selectedSubscriptionId, selectedScopeId, selectedJobId, pageSize])

  useEffect(() => {
    if (currentPage > totalPages) {
      setCurrentPage(totalPages)
    }
  }, [currentPage, totalPages])

  useEffect(() => {
    if (!deepLinkFocus) return
    if (deepLinkFocus === 'job:failed_or_retrying') {
      setActiveSourceId('m365')
      setViewMode('m365-detail')
      setStep('jobs')
      setSelectedJobId(null)
      return
    }
    if (deepLinkFocus.startsWith('job:id:')) {
      setActiveSourceId('m365')
      setViewMode('m365-detail')
      setStep('jobs')
      setSelectedJobId(deepLinkFocus.replace('job:id:', ''))
    }
  }, [deepLinkFocus])

  useEffect(() => {
    if (!selectedSubscriptionId) return
    const exists = subscriptions.some((row) => row.id === selectedSubscriptionId)
    if (exists) return
    setSelectedSubscriptionId(null)
    setSelectedScopeId(null)
    onScopeSelected?.('')
  }, [onScopeSelected, selectedSubscriptionId, subscriptions])

  useEffect(() => {
    if (step !== 'jobs') return
    onJobsNeeded?.()
  }, [onJobsNeeded, step])

  return (
    <div className="space-y-4">
      {viewMode === 'source-list' && (
        <Card>
          <CardHeader>
            <CardTitle>接続データソース</CardTitle>
            <CardDescription>
              現在はM365のみ接続済みです。その他サービスは段階的に追加予定です。
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
              {CONNECT_SOURCE_CATALOG.map((source) => (
                <Card
                  key={source.id}
                  className={cn(
                    source.enabled ? 'border-primary/40 bg-primary/5' : 'border-dashed',
                    activeSourceId === source.id && 'ring-2 ring-primary/30',
                    'transition-all duration-150'
                  )}
                >
                  <CardContent className="p-0">
                    <div
                      role="button"
                      tabIndex={source.enabled ? 0 : -1}
                      className={cn(
                        'w-full rounded-lg p-4 text-left transition-all duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60',
                        source.enabled ? 'cursor-pointer hover:bg-primary/10' : 'cursor-not-allowed opacity-70'
                      )}
                      aria-disabled={!source.enabled}
                      onClick={() => {
                        if (!source.enabled) return
                        setActiveSourceId(source.id)
                        setStep('subscriptions')
                        setSelectedSubscriptionId(null)
                        setSelectedScopeId(null)
                        onScopeSelected?.('')
                        setSelectedJobId(null)
                        if (source.id === 'm365') setViewMode('m365-detail')
                      }}
                      onKeyDown={(event) => {
                        if (!source.enabled) return
                        if (event.key === 'Enter' || event.key === ' ' || event.key === 'Spacebar') {
                          event.preventDefault()
                          setActiveSourceId(source.id)
                          setStep('subscriptions')
                          setSelectedSubscriptionId(null)
                          setSelectedScopeId(null)
                          onScopeSelected?.('')
                          setSelectedJobId(null)
                          if (source.id === 'm365') setViewMode('m365-detail')
                        }
                      }}
                    >
                      <div className="space-y-3">
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-2">
                            <div className="flex h-9 w-9 items-center justify-center overflow-hidden p-0">
                              <img
                                src={source.iconPath}
                                alt={`${source.label} icon`}
                                className="h-full w-full object-contain"
                                loading="lazy"
                              />
                            </div>
                            <span className="text-sm font-medium">{source.label}</span>
                          </div>
                          <Checkbox checked={source.enabled} disabled aria-label={`${source.label} connection status`} />
                        </div>
                        <p className="text-xs text-muted-foreground">{source.enabled ? '利用可能' : '実装予定'}</p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {viewMode === 'm365-detail' && activeSourceId === 'm365' && (
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between gap-2">
              <CardTitle>{M365_STEP_CONTENT[step].title}</CardTitle>
              <Button size="sm" variant="outline" onClick={() => setViewMode('source-list')}>
                接続ソースへ戻る
              </Button>
            </div>
            <CardDescription className="space-y-2">
              <span>{M365_STEP_CONTENT[step].description}</span>
              {M365_STEP_CONTENT[step].hint ? (
                <span className="block text-xs text-muted-foreground">{M365_STEP_CONTENT[step].hint}</span>
              ) : null}
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {selectedSubscriptionId && (
              <Badge variant="outline">選択中 購読ID: {selectedSubscriptionId}</Badge>
            )}
            {selectedScopeId && <Badge variant="outline">選択中 Scope: {selectedScopeId}</Badge>}
            {step === 'events' && <Badge variant="outline">resolved tenant: {eventsResolvedTenantId || '-'}</Badge>}
            {step === 'events' && <Badge variant="outline">requested scope: {eventsRequestedScopeId || '-'}</Badge>}
            {step === 'events' && <Badge variant="outline">events loaded: {events.length} / total: {eventsTotalCount}</Badge>}
            {selectedJobId && <Badge variant="outline">選択中 Job: {selectedJobId}</Badge>}
            {(loading || jobsLoading) && <p className="text-sm text-muted-foreground">接続データを取得中です...</p>}
            {loadError && <p className="text-sm text-rose-600">{loadError}</p>}

            {totalRows > 0 && (
              <TablePageControls
                totalRows={totalRows}
                currentPage={currentPage}
                pageSize={pageSize}
                onPageSizeChange={setPageSize}
                onPageChange={setCurrentPage}
              />
            )}

            {step === 'subscriptions' && (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead title="接続対象のサイト名（接続名）です。">サイト名</TableHead>
                    <TableHead title="購読対象のGraphリソースパスです。">リソース</TableHead>
                    <TableHead title="通知検証用のクライアント状態です。">クライアント状態</TableHead>
                    <TableHead title="initializing = Graph 購読 ID 未確定。active = 通知可能な購読が保存済み。">
                      状態
                    </TableHead>
                    <TableHead title="クリックで受信イベントへ遷移します。">遷移先</TableHead>
                    <TableHead title="不要な購読を削除します。">操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedSubscriptions.map((row) => (
                    <TableRow
                      key={row.id}
                      tabIndex={0}
                      className="cursor-pointer hover:bg-muted/40"
                      onClick={() => {
                        const scopeId = scopeIdFromSubscriptionResource(row.resource)
                        if (!scopeId) return
                        setSelectedSubscriptionId(row.id)
                        setSelectedScopeId(scopeId || null)
                        onScopeSelected?.(scopeId)
                        setStep('events')
                      }}
                      onKeyDown={(event) => {
                        if (event.key === 'Enter' || event.key === ' ' || event.key === 'Spacebar') {
                          event.preventDefault()
                          const scopeId = scopeIdFromSubscriptionResource(row.resource)
                          if (!scopeId) return
                          setSelectedSubscriptionId(row.id)
                          setSelectedScopeId(scopeId || null)
                          onScopeSelected?.(scopeId)
                          setStep('events')
                        }
                      }}
                    >
                      <TableCell>
                        <div className="flex flex-col">
                          <span>{row.connection_name || '（サイト名未設定）'}</span>
                          <span className="text-xs text-muted-foreground">{row.id}</span>
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="flex flex-col">
                          <span>{row.resource}</span>
                          <span className="text-xs text-muted-foreground">
                            {row.resource_type === 'message' ? `message/${row.target_type || 'unknown'}` : 'drive'}
                          </span>
                        </div>
                      </TableCell>
                      <TableCell>{row.client_state_verified ? '一致' : '不一致'}</TableCell>
                      <TableCell>
                        <Badge variant={statusBadgeVariant(row.status)} className={statusBadgeClassName(row.status)}>
                          {row.status}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <span className="inline-flex items-center gap-1 text-xs text-muted-foreground">
                          {scopeIdFromSubscriptionResource(row.resource)
                            ? <>受信イベント <ArrowRightCircle className="h-3.5 w-3.5" /></>
                            : <>遷移不可（リソース形式未対応）</>}
                        </span>
                      </TableCell>
                      <TableCell>
                        <Button
                          size="sm"
                          variant="outline"
                          className="text-rose-700 hover:bg-rose-50 hover:text-rose-800"
                          onClick={(event) => {
                            event.preventDefault()
                            event.stopPropagation()
                            setPendingDeleteSubscription(row)
                          }}
                        >
                          削除
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}

            <AlertDialog
              open={Boolean(pendingDeleteSubscription)}
              onOpenChange={(open) => {
                if (deleteSubmitting) return
                if (!open) setPendingDeleteSubscription(null)
              }}
            >
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>接続削除モードを選択</AlertDialogTitle>
                  <AlertDialogDescription>
                    {pendingDeleteSubscription
                      ? `subscription_id=${pendingDeleteSubscription.id}`
                      : ''}
                    <br />
                    安全モードは Graph解除失敗時に削除しません。強制モードは失敗時でもローカル接続を削除します。
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel disabled={deleteSubmitting}>キャンセル</AlertDialogCancel>
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={!pendingDeleteSubscription || deleteSubmitting}
                    onClick={async () => {
                      if (!pendingDeleteSubscription) return
                      setDeleteSubmitting(true)
                      try {
                        await onDeleteSubscription?.(pendingDeleteSubscription, 'safe')
                        setPendingDeleteSubscription(null)
                      } finally {
                        setDeleteSubmitting(false)
                      }
                    }}
                  >
                    安全モードで削除
                  </Button>
                  <Button
                    size="sm"
                    className="bg-rose-600 text-white hover:bg-rose-700"
                    disabled={!pendingDeleteSubscription || deleteSubmitting}
                    onClick={async () => {
                      if (!pendingDeleteSubscription) return
                      setDeleteSubmitting(true)
                      try {
                        await onDeleteSubscription?.(pendingDeleteSubscription, 'force')
                        setPendingDeleteSubscription(null)
                      } finally {
                        setDeleteSubmitting(false)
                      }
                    }}
                  >
                    強制モードで削除
                  </Button>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>

            {step === 'events' && (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead title="受信イベントの一意IDです。">イベントID</TableHead>
                    <TableHead title="イベントを受信した日時です。">受信日時</TableHead>
                    <TableHead title="変更種別（作成/更新/削除など）です。">変更種別</TableHead>
                    <TableHead title="変更対象のリソースです。">リソース</TableHead>
                    <TableHead title="イベント処理の進捗状態です。">状態</TableHead>
                    <TableHead>関連ジョブ</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedEvents.map((row) => {
                    const relatedJobId = jobs.find((job) => job.event_id === row.id)?.id
                    const resourceDisplay = parseResourceDisplay(row.resource)
                    return (
                      <TableRow key={row.id}>
                        <TableCell>{row.id}</TableCell>
                        <TableCell>{formatDateTimeInTokyo(row.received_at)}</TableCell>
                        <TableCell>{row.change_type}</TableCell>
                        <TableCell className="max-w-[420px]">
                          <div className="flex flex-col">
                            <span className="truncate">{resourceDisplay.fileName}</span>
                            <span className="truncate text-xs text-muted-foreground">{resourceDisplay.path}</span>
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge variant={statusBadgeVariant(row.status)} className={statusBadgeClassName(row.status)}>
                            {row.status}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          {relatedJobId ? (
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => {
                                setSelectedJobId(relatedJobId)
                                setStep('jobs')
                              }}
                            >
                              {relatedJobId}
                            </Button>
                          ) : (
                            '-'
                          )}
                        </TableCell>
                      </TableRow>
                    )
                  })}
                  {visibleEvents.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={6} className="text-center text-muted-foreground">
                        該当する受信イベントはありません。
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            )}

            {step === 'jobs' && (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead title="ジョブの一意IDです。">ジョブID</TableHead>
                    <TableHead title="ジョブ種別です。">種別</TableHead>
                    <TableHead title="実行開始日時です。">開始日時</TableHead>
                    <TableHead title="ジョブの進捗状態です。">状態</TableHead>
                    <TableHead title="直近の実行ログメッセージです。">最新メッセージ</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedJobs.map((row) => (
                    <TableRow key={row.id} className={cn(selectedJobId === row.id && 'bg-primary/5')}>
                      <TableCell>{row.id}</TableCell>
                      <TableCell>{row.job_type}</TableCell>
                      <TableCell>{formatDateTimeInTokyo(row.started_at)}</TableCell>
                      <TableCell>
                        <Badge variant={statusBadgeVariant(row.status)} className={statusBadgeClassName(row.status)}>
                          {row.status}
                        </Badge>
                      </TableCell>
                      <TableCell className="max-w-[480px] truncate">{row.last_message}</TableCell>
                    </TableRow>
                  ))}
                  {visibleJobs.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={5} className="text-center text-muted-foreground">
                        該当する実行ジョブはありません。
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            )}

          </CardContent>
        </Card>
      )}
    </div>
  )
}

const OverviewPage = ({
  overview,
  events,
  jobs,
  totalReceivedEventsCount
}: {
  overview: ConnectOverviewResponse
  events: ConnectEventApiRow[]
  jobs: ConnectJobApiRow[]
  totalReceivedEventsCount: number
}) => {
  const backlogCount = overview.queue_backlog
  const backlogTarget = 15
  const backlogMax = 80
  const isWaitingForFirstEvent = totalReceivedEventsCount === 0
  const waitingBadgeClassName = 'border-slate-300 bg-slate-100 text-slate-700'
  const backlogPercent = Math.min(100, Math.round((backlogCount / backlogMax) * 100))
  const backlogAssessment = backlogCount <= backlogTarget ? '正常' : backlogCount <= 30 ? '注意' : '要対応'
  const backlogColor = isWaitingForFirstEvent
    ? '#9ca3af'
    : backlogAssessment === '正常'
      ? '#10b981'
      : backlogAssessment === '注意'
        ? '#f59e0b'
        : '#ef4444'
  const backlogChartData = [{ name: 'backlog', value: backlogPercent, fill: backlogColor }]

  const successRate = isWaitingForFirstEvent ? 0 : eventSuccessRate24h(events)
  const successColor = isWaitingForFirstEvent ? '#9ca3af' : successRate >= 95 ? '#10b981' : successRate >= 80 ? '#f59e0b' : '#ef4444'
  const successData = [{ name: 'success', value: successRate, fill: successColor }]
  const failedJobs = overview.failed_jobs_24h
  const retryingJobs = retryingJobs24h(jobs)

  return (
    <div className="space-y-4">
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        <Card>
          <CardHeader className="pb-2">
            <CardDescription>
              <HoverHelpLabel
                label="受信イベント件数（アクティブ接続対象）"
                helpText="アクティブ接続のみを対象に現在確認できる受信イベント件数です。右側バッジは配信状態を示します。"
              />
            </CardDescription>
            <CardTitle className="flex items-center gap-2">
              {totalReceivedEventsCount} 件
              <Badge
                variant={isWaitingForFirstEvent ? 'outline' : statusBadgeVariant(overview.delivery_status)}
                className={isWaitingForFirstEvent ? waitingBadgeClassName : statusBadgeClassName(overview.delivery_status)}
              >
                {isWaitingForFirstEvent ? '接続待ち' : deliveryStatusText(overview.delivery_status)}
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-muted-foreground">
            <p>
              {isWaitingForFirstEvent
                ? '接続後に最初の受信イベントが到着すると件数と状態を表示します。'
                : 'アクティブ接続対象で確認できる受信イベント件数を表示します。'}
            </p>
            <div className="h-2 overflow-hidden rounded bg-muted">
              <div
                className={cn(
                  'h-full',
                  isWaitingForFirstEvent
                    ? 'bg-slate-400'
                    : overview.delivery_status === 'healthy'
                      ? 'bg-emerald-500'
                      : overview.delivery_status === 'degraded'
                        ? 'bg-amber-500'
                        : 'bg-destructive'
                )}
                style={{
                  width:
                    isWaitingForFirstEvent
                      ? '20%'
                      : overview.delivery_status === 'healthy'
                        ? '100%'
                        : overview.delivery_status === 'degraded'
                          ? '65%'
                          : '35%'
                }}
              />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardDescription>
              <HoverHelpLabel label="キュー滞留件数" helpText="処理待ちキューに残っているイベント件数です。" />
            </CardDescription>
            <CardTitle className="flex items-center gap-2">
              {backlogCount} 件
              <Badge
                variant={
                  isWaitingForFirstEvent
                    ? 'outline'
                    : backlogAssessment === '要対応'
                      ? 'destructive'
                      : backlogAssessment === '注意'
                        ? 'secondary'
                        : 'outline'
                }
                className={
                  isWaitingForFirstEvent
                    ? waitingBadgeClassName
                    : backlogAssessment === '正常'
                      ? 'border-emerald-300 bg-emerald-100 text-emerald-800'
                      : undefined
                }
              >
                {isWaitingForFirstEvent ? '接続待ち' : backlogAssessment}
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="flex items-center gap-3">
            <div className="h-24 w-24">
              <ChartContainer minHeight={72}>
                {({ width, height }) => (
                  <RadialBarChart width={width} height={height} innerRadius="65%" outerRadius="100%" barSize={10} data={backlogChartData} startAngle={90} endAngle={-270}>
                    <PolarAngleAxis type="number" domain={[0, 100]} tick={false} />
                    <RadialBar background dataKey="value" cornerRadius={8} />
                  </RadialBarChart>
                )}
              </ChartContainer>
            </div>
            <div className="text-sm text-muted-foreground">
              {isWaitingForFirstEvent ? (
                <>
                  <p>接続待ちのため滞留評価は未開始です。</p>
                  <p>最初のイベント受信後に目標判定を開始します。</p>
                </>
              ) : (
                <>
                  <p>目標: {backlogTarget} 件以下</p>
                  <p>
                    {backlogCount > backlogTarget
                      ? `目標より${backlogCount - backlogTarget}件多い状態`
                      : '目標範囲内'}
                  </p>
                </>
              )}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardDescription>
              <HoverHelpLabel label="失敗ジョブ数（24時間）" helpText="直近24時間で失敗したジョブ数です。" />
            </CardDescription>
            <CardTitle className="flex items-center gap-2">
              {failedJobs} 件
              <Badge
                variant={isWaitingForFirstEvent ? 'outline' : failedJobs === 0 ? 'outline' : failedJobs <= 2 ? 'secondary' : 'destructive'}
                className={isWaitingForFirstEvent ? waitingBadgeClassName : failedJobs === 0 ? 'border-emerald-300 bg-emerald-100 text-emerald-800' : undefined}
              >
                {isWaitingForFirstEvent ? '接続待ち' : failedJobs === 0 ? '正常' : failedJobs <= 2 ? '注意' : '要対応'}
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-muted-foreground">
            <p>
              {isWaitingForFirstEvent ? '接続待ちのため失敗ジョブ評価は未開始です。' : '直近24時間の失敗ジョブ件数です。'}
            </p>
            <div className="h-2 overflow-hidden rounded bg-muted">
              <div
                className={
                  isWaitingForFirstEvent
                    ? 'h-full bg-slate-400'
                    : failedJobs <= 2
                      ? 'h-full bg-amber-500'
                      : 'h-full bg-destructive'
                }
                style={{ width: isWaitingForFirstEvent ? '20%' : `${Math.min(100, failedJobs * 20)}%` }}
              />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardDescription>
              <HoverHelpLabel label="イベント成功率（24時間）" helpText="受信イベントの正常処理率です。" />
            </CardDescription>
            <CardTitle className="flex items-center gap-2">
              {successRate}%
              <Badge
                variant={isWaitingForFirstEvent ? 'outline' : successRate >= 95 ? 'outline' : successRate >= 80 ? 'secondary' : 'destructive'}
                className={isWaitingForFirstEvent ? waitingBadgeClassName : successRate >= 95 ? 'border-emerald-300 bg-emerald-100 text-emerald-800' : undefined}
              >
                {isWaitingForFirstEvent ? '接続待ち' : successRate >= 95 ? '正常' : successRate >= 80 ? '注意' : '要対応'}
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="flex items-center gap-3">
            <div className="h-20 w-20">
              <ChartContainer minHeight={72}>
                {({ width, height }) => (
                  <RadialBarChart width={width} height={height} innerRadius="65%" outerRadius="100%" barSize={9} data={successData} startAngle={90} endAngle={-270}>
                    <PolarAngleAxis type="number" domain={[0, 100]} tick={false} />
                    <RadialBar background dataKey="value" cornerRadius={8} />
                  </RadialBarChart>
                )}
              </ChartContainer>
            </div>
            <p className="text-sm text-muted-foreground">
              {isWaitingForFirstEvent
                ? '接続待ちのため成功率評価は未開始です。'
                : 'processed / total による簡易成功率です。'}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardDescription>
              <HoverHelpLabel label="再試行・隔離件数（24時間）" helpText="再試行中または隔離されたジョブ数です。" />
            </CardDescription>
            <CardTitle className="flex items-center gap-2">
              {retryingJobs} 件
              <Badge
                variant={isWaitingForFirstEvent ? 'outline' : retryingJobs === 0 ? 'outline' : retryingJobs <= 2 ? 'secondary' : 'destructive'}
                className={isWaitingForFirstEvent ? waitingBadgeClassName : retryingJobs === 0 ? 'border-emerald-300 bg-emerald-100 text-emerald-800' : undefined}
              >
                {isWaitingForFirstEvent ? '接続待ち' : retryingJobs === 0 ? '正常' : retryingJobs <= 2 ? '注意' : '要対応'}
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-muted-foreground">
            <p>
              {isWaitingForFirstEvent
                ? '接続待ちのため再試行/隔離監視は未開始です。'
                : '失敗予兆として優先確認してください。'}
            </p>
            <div className="h-2 overflow-hidden rounded bg-muted">
              <div
                className={
                  isWaitingForFirstEvent
                    ? 'h-full bg-slate-400'
                    : retryingJobs <= 2
                      ? 'h-full bg-amber-500'
                      : 'h-full bg-destructive'
                }
                style={{ width: isWaitingForFirstEvent ? '20%' : `${Math.min(100, retryingJobs * 25)}%` }}
              />
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader className="pb-2">
            <CardDescription>
              <HoverHelpLabel
                label="次回アクセストークン更新"
                helpText="次にアクセストークンの更新が必要になる日時です。認証失効を防ぐための運用目安として確認します。"
              />
            </CardDescription>
            <CardTitle className="text-base">{formatDateTimeInTokyo(overview.next_token_renewal_at)}</CardTitle>
          </CardHeader>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardDescription>
              <HoverHelpLabel
                label="次回差分同期"
                helpText="次に差分同期を実行予定の日時です。データ取り込み遅延や鮮度低下の予兆確認に利用します。"
              />
            </CardDescription>
            <CardTitle className="text-base">{formatDateTimeInTokyo(overview.next_delta_sync_at)}</CardTitle>
          </CardHeader>
        </Card>
      </div>
    </div>
  )
}


const AuditPage = ({ rows }: { rows: ConnectAuditApiRow[] }) => (
  <AuditWorkbench
    title="接続監査ログ 横断検索"
    description="接続操作の証跡を横断検索し、CSV/PDFでエクスポートできます。"
    rows={rows.map((row) => ({
      auditId: row.id,
      operatedAt: formatDateTimeInTokyo(row.operated_at),
      operator: row.operator,
      action: row.action,
      target: `${row.target_type}:${row.target_id}`,
      correlationId: row.correlation_id ?? '-'
    }))}
    columns={[
      { key: 'auditId', label: '監査ID' },
      { key: 'operatedAt', label: '操作日時' },
      { key: 'operator', label: '実行者' },
      { key: 'action', label: '操作内容' },
      { key: 'target', label: '対象' },
      { key: 'correlationId', label: '相関ID' }
    ]}
    searchKeys={['auditId', 'operator', 'action', 'target', 'correlationId', 'operatedAt']}
    queryParamKey="connectAuditQ"
  />
)

const ConnectOperations = () => {
  const username = useAuthStore((state) => state.username)
  const [overview, setOverview] = useState<ConnectOverviewResponse>({
    tenant_id: '',
    delivery_status: 'healthy',
    queue_backlog: 0,
    failed_jobs_24h: 0,
    next_subscription_renewal_at: '-',
    next_token_renewal_at: '-',
    next_delta_sync_at: '-'
  })
  const [subscriptions, setSubscriptions] = useState<ConnectSubscriptionApiRow[]>([])
  const [events, setEvents] = useState<ConnectEventApiRow[]>([])
  const [eventsTotalCount, setEventsTotalCount] = useState(0)
  const [totalReceivedEventsCount, setTotalReceivedEventsCount] = useState(0)
  const [eventsResolvedTenantId, setEventsResolvedTenantId] = useState<string>('')
  const [eventsRequestedScopeId, setEventsRequestedScopeId] = useState<string>('')
  const [jobs, setJobs] = useState<ConnectJobApiRow[]>([])
  const [auditRows, setAuditRows] = useState<ConnectAuditApiRow[]>([])
  const [loading, setLoading] = useState(false)
  const [jobsLoading, setJobsLoading] = useState(false)
  const [auditLoading, setAuditLoading] = useState(false)
  const [hasAttemptedJobsLoad, setHasAttemptedJobsLoad] = useState(false)
  const [hasAttemptedAuditLoad, setHasAttemptedAuditLoad] = useState(false)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [page, setPage] = useState<ConnectPageKey>('overview')
  const [deepLinkFocus, setDeepLinkFocus] = useState<string | undefined>()
  const hasLoadedInitiallyRef = useRef(false)
  const [selectedScopeIdForEvents, setSelectedScopeIdForEvents] = useState<string>('')

  const pollSubscriptionReflection = useCallback(async () => {
    // Allow Dynamo/Graph to catch up after multi-connection (Teams) onboarding; ~14s total.
    const attempts = 12
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const subscriptionsResponse = await getConnectSubscriptions()
      const rows = subscriptionsResponse.rows ?? []
      setSubscriptions(rows)
      if (!hasPlaceholderSubscription(rows)) return true
      await new Promise((resolve) => setTimeout(resolve, 1200))
    }
    return false
  }, [])

  const loadConnectData = useCallback(async () => {
    setLoading(true)
    setLoadError(null)
    try {
      // Keep initial page load fast by fetching only core data first.
      const [overviewResponse, subscriptionsResponse, eventsResponse] = await Promise.all([
        getConnectOverview(),
        getConnectSubscriptions(),
        getConnectEvents('', '', 300, 0)
      ])
      setOverview(overviewResponse)
      setSubscriptions(subscriptionsResponse.rows ?? [])
      setEvents(eventsResponse.rows ?? [])
      setEventsTotalCount(eventsResponse.pagination?.total_count ?? (eventsResponse.rows ?? []).length)
      setTotalReceivedEventsCount(eventsResponse.pagination?.total_count ?? (eventsResponse.rows ?? []).length)
      setEventsResolvedTenantId(eventsResponse.resolved_tenant_id ?? '')
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : '接続データの取得に失敗しました。')
    } finally {
      setLoading(false)
    }
  }, [])

  const loadConnectEventsData = useCallback(async (scopeId: string = '') => {
    try {
      setEventsRequestedScopeId(scopeId)
      const eventsResponse = await getConnectEvents(scopeId, '', 300, 0)
      setEvents(eventsResponse.rows ?? [])
      setEventsTotalCount(eventsResponse.pagination?.total_count ?? (eventsResponse.rows ?? []).length)
      if (!scopeId) {
        setTotalReceivedEventsCount(eventsResponse.pagination?.total_count ?? (eventsResponse.rows ?? []).length)
      }
      setEventsResolvedTenantId(eventsResponse.resolved_tenant_id ?? '')
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : '接続イベントの取得に失敗しました。')
    }
  }, [])

  const loadConnectJobsData = useCallback(async () => {
    setJobsLoading(true)
    try {
      const jobsResponse = await getConnectJobs('', '', 300, 0)
      setJobs(jobsResponse.rows ?? [])
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : '接続ジョブの取得に失敗しました。')
    } finally {
      setJobsLoading(false)
    }
  }, [])

  const loadConnectAuditData = useCallback(async () => {
    setAuditLoading(true)
    try {
      const auditResponse = await getConnectAuditLogs('', 300, 0)
      setAuditRows(auditResponse.rows ?? [])
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : '接続監査ログの取得に失敗しました。')
    } finally {
      setAuditLoading(false)
    }
  }, [])

  const handleDeleteSubscription = useCallback(
    async (row: ConnectSubscriptionApiRow, mode: 'safe' | 'force') => {
      const subscriptionId = String(row.id ?? '').trim()
      const connectionId = String(row.connection_id ?? '').trim()
      if (!subscriptionId) {
        toast.error('削除対象の購読IDを取得できませんでした。')
        return
      }
      try {
        const result = await deleteConnectSubscription(subscriptionId, connectionId, mode)
        toast.success('接続を削除しました。', {
          description: `mode=${mode} / connection_id=${result.connection_id} / graph=${result.graph_unsubscribe_status}`
        })
        await loadConnectData()
        await loadConnectEventsData(selectedScopeIdForEvents)
      } catch (error) {
        toast.error(error instanceof Error ? error.message : '接続の削除に失敗しました。')
      }
    },
    [loadConnectData, loadConnectEventsData, selectedScopeIdForEvents]
  )

  const runSyncCheck = async () => {
    try {
      const response = await runConnectSyncCheck(username ?? undefined)
      const runAt = formatDateTimeInTokyo(new Date().toISOString())
      toast('同期状態更新を受け付けました', {
        description: `${runAt} / status=${response.status} / correlation_id=${response.correlation_id}`
      })
      await loadConnectData()
      await loadConnectEventsData(selectedScopeIdForEvents)
      if (page === 'connections') {
        await loadConnectJobsData()
      }
      if (page === 'audit') {
        await loadConnectAuditData()
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : '同期状態更新に失敗しました。')
    }
  }

  useEffect(() => {
    const deepLink = takeOperationDeepLinkForTab('connect-operations')
    if (!deepLink?.page) return
    if (deepLink.page === 'subscriptions' || deepLink.page === 'events' || deepLink.page === 'scopes') {
      setPage('connections')
    } else if (CONNECT_PAGES.some((item) => item.key === deepLink.page)) {
      setPage(deepLink.page as ConnectPageKey)
    }
    setDeepLinkFocus(deepLink.focus)
  }, [])

  useEffect(() => {
    // React.StrictMode in development can invoke effects twice.
    // Guard duplicate initial fetch to avoid unnecessary heavy API calls.
    if (hasLoadedInitiallyRef.current) return
    hasLoadedInitiallyRef.current = true
    void loadConnectData()
  }, [loadConnectData])

  useEffect(() => {
    if (page !== 'connections') {
      setHasAttemptedJobsLoad(false)
    }
  }, [page])

  useEffect(() => {
    if (page !== 'audit') {
      setHasAttemptedAuditLoad(false)
      return
    }
    if (hasAttemptedAuditLoad || auditLoading) return
    setHasAttemptedAuditLoad(true)
    void loadConnectAuditData()
  }, [auditLoading, hasAttemptedAuditLoad, loadConnectAuditData, page])

  useEffect(() => {
    if (page !== 'connections') return
    if (!selectedScopeIdForEvents) return
    void loadConnectEventsData(selectedScopeIdForEvents)
  }, [loadConnectEventsData, page, selectedScopeIdForEvents])

  return (
    <div className="h-full overflow-y-auto p-4">
      <div className="mx-auto w-full max-w-[1400px] space-y-4 pb-6">
        <Card className="border-primary/20 bg-gradient-to-r from-primary/10 via-background to-background shadow-md">
          <CardContent className="flex flex-wrap items-center justify-between gap-3 p-4">
            <div>
              <h1 className="text-xl font-semibold">接続</h1>
              <p className="text-sm text-muted-foreground">
                接続状態と同期処理の健全性を管理します。現在は M365 の運用監視に対応しています。
              </p>
            </div>
            <div className="flex items-center gap-2">
              <Button
                onClick={() => setPage('onboarding')}
                className="h-11 rounded-full border-2 border-emerald-300 bg-emerald-600 px-6 font-bold text-white shadow-lg hover:bg-emerald-700 hover:shadow-xl"
              >
                <PlusCircle className="h-4 w-4" />
                新規接続
              </Button>
              <Button size="sm" onClick={() => void runSyncCheck()} disabled={loading}>同期状態を更新</Button>
            </div>
          </CardContent>
        </Card>
        {page !== 'onboarding' && (
          <div className="rounded-xl border bg-card p-2 shadow-sm">
            <TooltipProvider>
              <div className="flex flex-wrap gap-2">
                {CONNECT_PAGES.map((item) => (
                  <Tooltip key={item.key}>
                    <TooltipTrigger asChild>
                      <button
                        type="button"
                        onClick={() => setPage(item.key)}
                        onKeyDown={(event) => {
                          if (event.key === 'Enter' || event.key === ' ' || event.key === 'Space' || event.key === 'Spacebar') {
                            event.preventDefault()
                            setPage(item.key)
                          }
                        }}
                        aria-current={page === item.key ? 'page' : undefined}
                        className={cn(
                          'liquid-glass-tab rounded-full border px-4 py-2 text-sm font-medium transition-all duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60',
                          page === item.key
                            ? 'liquid-glass-tab-active shadow-sm'
                            : 'text-foreground'
                        )}
                      >
                        {item.label}
                      </button>
                    </TooltipTrigger>
                    <TooltipContent side="bottom">{CONNECT_TAB_HELP[item.key]}</TooltipContent>
                  </Tooltip>
                ))}
              </div>
            </TooltipProvider>
          </div>
        )}

        {page === 'onboarding' && (
          <OnboardingPage
            existingSubscriptions={subscriptions}
            onCompleted={async (response) => {
              // T-074: reflect onboarding result immediately and keep polling until subscription is ready.
              setSubscriptions(response.subscriptions.rows ?? [])
              setPage('connections')
              if (hasPlaceholderSubscription(response.subscriptions.rows ?? [])) {
                toast('購読の即時反映を確認中です', {
                  description: 'バックグラウンドで再取得を行い、接続反映完了を待機します。'
                })
                const reflected = await pollSubscriptionReflection()
                if (!reflected) {
                  toast.warning('購読反映が遅延しています', {
                    description: '接続画面の「同期状態を更新」から再試行してください。'
                  })
                }
              }
              await loadConnectData()
              await loadConnectJobsData()
            }}
          />
        )}
        {page === 'connections' && (
          <ConnectionsPage
            deepLinkFocus={deepLinkFocus}
            subscriptions={subscriptions}
            events={events}
            eventsTotalCount={eventsTotalCount}
            eventsResolvedTenantId={eventsResolvedTenantId}
            eventsRequestedScopeId={eventsRequestedScopeId}
            jobs={jobs}
            jobsLoading={jobsLoading}
            loading={loading}
            loadError={loadError}
            onScopeSelected={(scopeId) => setSelectedScopeIdForEvents(scopeId)}
            onJobsNeeded={() => {
              if (hasAttemptedJobsLoad || jobsLoading) return
              setHasAttemptedJobsLoad(true)
              void loadConnectJobsData()
            }}
            onDeleteSubscription={handleDeleteSubscription}
          />
        )}
        {page === 'overview' && (
          <OverviewPage
            overview={overview}
            events={events}
            jobs={jobs}
            totalReceivedEventsCount={totalReceivedEventsCount}
          />
        )}
        {page === 'audit' && <AuditPage rows={auditRows} />}
        {page === 'help' && (
          <FeatureOnboardingPanel
            title="接続ガイド"
            purpose="通知受信と同期処理の健全性を継続監視し、障害時に迅速に復旧するための画面です。"
            currentPageLabel={CONNECT_PAGES.find((item) => item.key === page)?.label ?? 'ヘルプ'}
            currentPageDescription={CONNECT_PAGE_GUIDE[page]}
            steps={[
              '「概要」で障害・滞留・失敗ジョブの有無を確認する。',
              '「接続」で購読状態と監視範囲を確認する。',
              '「受信イベント」「実行ジョブ」で失敗要因を特定し、必要に応じて再実行する。'
            ]}
            glossary={CONNECT_GLOSSARY}
          />
        )}
      </div>
    </div>
  )
}

export default ConnectOperations
