export type DeliveryStatus = 'healthy' | 'degraded' | 'failed'

export type ConnectPageKey =
  | 'onboarding'
  | 'connections'
  | 'overview'
  | 'subscriptions'
  | 'events'
  | 'jobs'
  | 'scopes'
  | 'audit'
  | 'help'

export interface ConnectOverviewStats {
  deliveryStatus: DeliveryStatus
  queueBacklog: number
  failedJobs24h: number
  nextSubscriptionRenewalAt: string
  nextTokenRenewalAt: string
  nextDeltaSyncAt: string
}

export interface ConnectSubscription {
  id: string
  resource: string
  expirationAt: string
  clientStateVerified: boolean
  status: 'initializing' | 'active' | 'expiring' | 'failed'
}

export interface ConnectEvent {
  id: string
  receivedAt: string
  changeType: 'create' | 'update' | 'delete' | 'permission_change'
  resource: string
  idempotencyKey: string
  status: 'queued' | 'processed' | 'duplicated' | 'failed'
}

export interface ConnectJob {
  id: string
  jobType: 'ingestion' | 'delta_sync' | 'governance_trigger'
  startedAt: string
  status: 'queued' | 'running' | 'success' | 'retrying' | 'failed' | 'dead-lettered'
  lastMessage: string
}

export interface ConnectScope {
  id: string
  tenantId: string
  site: string
  drive: string
  excludedPathCount: number
  lastDeltaSyncAt: string
}

export interface ConnectAuditRecord {
  id: string
  operatedAt: string
  operator: string
  action: string
  targetType: 'subscription' | 'event' | 'job' | 'scope'
  targetId: string
  correlationId: string
}
