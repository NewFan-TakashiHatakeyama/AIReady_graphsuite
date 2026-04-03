"""Policy domain models for Governance policy-as-data execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Decision = Literal["allow", "warn", "review", "block"]
RiskLevel = Literal["low", "medium", "high", "critical"]
RemediationMode = Literal["auto", "approval", "owner_review", "manual", "recommend_only"]
RolloutStage = Literal["dry_run", "pilot", "active"]


@dataclass(frozen=True)
class PolicyRule:
    rule_id: str
    vector: str
    effect: Decision
    severity: RiskLevel
    remediation_mode: RemediationMode
    remediation_action: str
    reason_codes: list[str] = field(default_factory=list)
    conditions: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PolicyScope:
    scope_type: str
    department_ids: list[str] = field(default_factory=list)
    site_ids: list[str] = field(default_factory=list)
    container_patterns: list[str] = field(default_factory=list)
    principal_group_ids: list[str] = field(default_factory=list)
    partner_domains_allowlist: list[str] = field(default_factory=list)
    criticality: str = "medium"
    use_cases: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PolicyRollout:
    stage: RolloutStage = "active"
    dry_run: bool = False


@dataclass(frozen=True)
class Policy:
    policy_id: str
    tenant_id: str
    layer: str
    title: str
    description: str
    priority: int = 100
    enabled: bool = True
    scope: PolicyScope = field(default_factory=lambda: PolicyScope(scope_type="organization"))
    rules: list[PolicyRule] = field(default_factory=list)
    rollout: PolicyRollout = field(default_factory=PolicyRollout)
    version: int = 1
    updated_by: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class PolicyContext:
    tenant_id: str
    department_id: str = ""
    site_id: str = ""
    container_path: str = ""
    owner_id: str = ""
    principal_ids: list[str] = field(default_factory=list)
    use_case: str = ""
    item_metadata: dict[str, Any] = field(default_factory=dict)
    content_signals: dict[str, Any] = field(default_factory=dict)
    content_analysis: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EffectivePolicySnapshot:
    effective_policy_id: str
    effective_policy_version: int
    matched_policy_ids: list[str]
    decision: Decision
    risk_level: RiskLevel
    remediation_mode: RemediationMode
    remediation_action: str
    decision_trace: list[str] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)
    policy_hash: str = ""
    decision_source: str = "global"
    expected_audience: str = "internal_need_to_know"
    expected_department: str = "unknown"
    expectation_gap_reason: str = ""
    expectation_gap_score: float = 0.0
