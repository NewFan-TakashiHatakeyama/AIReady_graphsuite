"""DTO schemas for dashboard and cross-domain audit APIs."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class PaginationDto(BaseModel):
    limit: int
    offset: int
    total_count: int


class DashboardSignalDto(BaseModel):
    key: str
    label: str
    score: float | None = None
    issues: int
    target: int
    sub_metrics: list["DashboardSignalSubMetricDto"] = Field(default_factory=list)


class DashboardSignalSubMetricDto(BaseModel):
    key: str
    label: str
    value: float | None = None
    unit: Literal["score", "count", "percent"] = "score"


class DashboardReadinessResponse(BaseModel):
    readiness_score: float | None = None
    target_score: int
    signals: list[DashboardSignalDto]


class DashboardReadinessTrendPointDto(BaseModel):
    label: str
    governance_oversharing: float
    governance_sensitive: float
    governance_assurance: float
    ontology_foundation: float
    ontology_usecase: float


class DashboardReadinessTrendResponse(BaseModel):
    rows: list[DashboardReadinessTrendPointDto]
    target_score: int
    source: Literal["snapshot", "estimated", "insufficient_data"] = "estimated"


class DashboardRecommendedActionDto(BaseModel):
    priority: Literal["P1", "P2", "P3"]
    domain: str
    summary: str


class DashboardRecommendedActionsResponse(BaseModel):
    rows: list[DashboardRecommendedActionDto]


class AuditRecordDto(BaseModel):
    domain: Literal["connect", "governance", "ontology"]
    audit_id: str
    occurred_at: str
    operator: str
    action: str
    target: str
    correlation_id: str


class AuditRecordsResponse(BaseModel):
    rows: list[AuditRecordDto]
    pagination: PaginationDto


class AuditExportRequest(BaseModel):
    domain: Literal["all", "connect", "governance", "ontology"] = "all"
    q: str = ""
    format: Literal["csv", "pdf"] = "csv"


class AuditExportAcceptedResponse(BaseModel):
    job_id: str
    status: Literal["accepted"]


class AuditExportStatusResponse(BaseModel):
    job_id: str
    status: Literal["accepted", "completed", "failed"]
    format: Literal["csv", "pdf"]
    download_url: str | None = None
    created_at: str | None = None
    expires_at: str | None = None
    error_message: str | None = None
