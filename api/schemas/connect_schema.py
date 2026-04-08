"""DTO schemas for Connect backend APIs."""

from __future__ import annotations

from pydantic import BaseModel, field_validator, model_validator

from schemas.dashboard_audit_schema import PaginationDto


class ConnectOverviewDto(BaseModel):
    tenant_id: str
    delivery_status: str
    queue_backlog: int
    failed_jobs_24h: int
    next_subscription_renewal_at: str
    next_token_renewal_at: str
    next_delta_sync_at: str


class ConnectSubscriptionDto(BaseModel):
    id: str
    connection_id: str | None = None
    connection_name: str | None = None
    resource: str
    expiration_at: str
    client_state_verified: bool
    status: str
    resource_type: str = "drive"
    target_type: str | None = None
    is_placeholder: bool = False
    reflection_status: str = "ready"
    tenant_hint: str


class ConnectSubscriptionsResponse(BaseModel):
    rows: list[ConnectSubscriptionDto]
    pagination: PaginationDto


class GovernanceFindingsCloseSummaryDto(BaseModel):
    file_metadata_rows: int
    findings_closed: int
    findings_attempted: int


class ConnectSubscriptionDeleteResponse(BaseModel):
    tenant_id: str
    connection_id: str
    subscription_id: str
    delete_mode: str
    status: str
    graph_unsubscribe_status: str
    deleted_at: str
    governance_findings_close: GovernanceFindingsCloseSummaryDto | None = None


class ConnectScopeDto(BaseModel):
    id: str
    subscription_id: str
    tenant_id: str
    site: str
    drive: str
    excluded_path_count: int
    last_delta_sync_at: str


class ConnectScopesResponse(BaseModel):
    rows: list[ConnectScopeDto]
    pagination: PaginationDto


class ConnectEventDto(BaseModel):
    id: str
    scope_id: str
    received_at: str
    change_type: str
    resource: str
    idempotency_key: str
    status: str


class ConnectEventsResponse(BaseModel):
    rows: list[ConnectEventDto]
    pagination: PaginationDto
    resolved_tenant_id: str | None = None


class ConnectJobDto(BaseModel):
    id: str
    event_id: str | None = None
    job_type: str
    started_at: str
    status: str
    last_message: str
    correlation_id: str | None = None
    source: str | None = None


class ConnectJobsResponse(BaseModel):
    rows: list[ConnectJobDto]
    pagination: PaginationDto


class ConnectAuditDto(BaseModel):
    id: str
    operated_at: str
    operator: str
    action: str
    target_type: str
    target_id: str
    correlation_id: str | None = None
    source: str | None = None


class ConnectAuditResponse(BaseModel):
    rows: list[ConnectAuditDto]
    pagination: PaginationDto


class ConnectSyncCheckResponse(BaseModel):
    tenant_id: str
    status: str
    lambda_function_name: str
    status_code: int
    requested_by: str
    correlation_id: str
    requested_at: str


class ConnectOnboardingRequest(BaseModel):
    client_id: str = ""
    site_id: str = ""
    drive_id: str
    notification_url: str
    client_secret: str = ""
    client_state: str = ""
    connection_name: str = ""
    initialize_subscription: bool = True
    resource_type: str = "drive"
    resource_path: str = ""
    change_type: str = "updated"
    target_type: str = "drive"
    team_id: str = ""
    channel_id: str = ""
    chat_id: str = ""

    @field_validator("notification_url", mode="before")
    @classmethod
    def _strip_notification_url(cls, value: object) -> str:
        return "" if value is None else str(value).strip()

    @field_validator("notification_url")
    @classmethod
    def _notification_url_required(cls, value: str) -> str:
        if not value:
            raise ValueError("notification_url must not be empty.")
        return value

    @field_validator("drive_id", mode="before")
    @classmethod
    def _strip_drive_id(cls, value: object) -> str:
        return "" if value is None else str(value).strip()

    @model_validator(mode="after")
    def _drive_id_required_for_file_subscriptions(self) -> ConnectOnboardingRequest:
        resource = str(self.resource_type or "drive").strip().lower()
        if resource == "drive" and not str(self.drive_id or "").strip():
            raise ValueError("drive_id is required for drive (file) subscriptions.")
        return self


class ConnectOnboardingResponse(BaseModel):
    tenant_id: str
    status: str
    connection_id: str | None = None
    bootstrap_status: str | None = None
    subscription_id: str | None = None
    subscription_status: str | None = None
    bootstrap_error: str | None = None
    subscription_initialized: bool
    subscription_init_status: str | None = None
    subscription_reflection_ready: bool = True
    subscription_reflection_wait_ms: int = 0
    subscriptions: ConnectSubscriptionsResponse


class ConnectOnboardingDefaultsResponse(BaseModel):
    tenant_id: str
    client_id: str
    auth_method: str
    permission_profile: str
    notification_url: str
    client_secret_parameter: str
    client_state_parameter: str


class ConnectSiteDiscoveryRequest(BaseModel):
    azure_tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""
    site_url: str = ""
    site_id: str = ""


class ConnectSiteDiscoveryResponse(BaseModel):
    site_id: str
    drive_id: str
    site_name: str
    site_web_url: str
    suggested_connection_name: str


class ConnectSiteOptionDto(BaseModel):
    site_id: str
    site_name: str
    site_web_url: str
    source_type: str


class ConnectSiteOptionsRequest(BaseModel):
    azure_tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""
    source_type: str = "sharepoint"
    query: str = ""


class ConnectSiteOptionsResponse(BaseModel):
    rows: list[ConnectSiteOptionDto]


class ConnectTeamChannelOptionsRequest(BaseModel):
    azure_tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""
    team_query: str = ""
    channel_query: str = ""
    site_id: str = ""
    max_teams: int = 20
    max_channels_per_team: int = 30


class ConnectTeamChannelOptionDto(BaseModel):
    team_id: str
    team_name: str
    team_mail: str = ""
    channel_id: str
    channel_name: str
    membership_type: str = ""
    files_drive_id: str = ""
    files_folder_web_url: str = ""
    files_folder_name: str = ""
    site_id: str = ""
    site_web_url: str = ""
    source_type: str = "teams"
    discovery_status: str = "ready"
    error_message: str = ""


class ConnectTeamChannelOptionsResponse(BaseModel):
    rows: list[ConnectTeamChannelOptionDto]
    warnings: list[str] = []
    required_application_permissions_phase1: list[str] = []
    required_application_permissions_phase2: list[str] = []
