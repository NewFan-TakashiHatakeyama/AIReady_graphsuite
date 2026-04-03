"""Policy repository adapters for SSM and DynamoDB."""

from __future__ import annotations

from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

from services.policy_models import Policy, PolicyRollout, PolicyRule, PolicyScope
from shared.config import get_env, get_env_bool
from shared.dynamodb import get_table

_ssm = None
_policy_table = None


def _get_ssm():
    global _ssm
    if _ssm is None:
        _ssm = boto3.client("ssm")
    return _ssm


def _get_policy_table():
    global _policy_table
    if _policy_table is None:
        _policy_table = get_table(get_env("POLICY_SCOPE_TABLE_NAME", "AIReadyGov-PolicyScope"))
    return _policy_table


def load_global_policy_values() -> dict[str, str]:
    """Load global policy values from SSM path."""
    values: dict[str, str] = {}
    next_token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Path": "/aiready/governance", "Recursive": True}
        if next_token:
            kwargs["NextToken"] = next_token
        response = _get_ssm().get_parameters_by_path(**kwargs)
        for param in response.get("Parameters", []):
            name = str(param.get("Name", "")).strip()
            if not name:
                continue
            values[name] = str(param.get("Value", "")).strip()
        next_token = response.get("NextToken")
        if not next_token:
            break
    return values


def load_scope_policy_rows(tenant_id: str) -> list[dict[str, Any]]:
    if not tenant_id:
        return []
    table = _get_policy_table()
    response = table.query(KeyConditionExpression=Key("tenant_id").eq(tenant_id))
    rows = list(response.get("Items", []))
    while response.get("LastEvaluatedKey"):
        response = table.query(
            KeyConditionExpression=Key("tenant_id").eq(tenant_id),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        rows.extend(response.get("Items", []))
    return rows


def _to_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    return default


def _normalize_rule(row: dict[str, Any], idx: int) -> PolicyRule:
    return PolicyRule(
        rule_id=str(row.get("rule_id") or f"rule-{idx}"),
        vector=str(row.get("vector") or row.get("when", {}).get("vector", "")).strip().lower(),
        effect=str(row.get("effect", "review")).strip().lower(),  # type: ignore[arg-type]
        severity=str(row.get("severity", "medium")).strip().lower(),  # type: ignore[arg-type]
        remediation_mode=str(row.get("remediation_mode", "manual")).strip().lower(),  # type: ignore[arg-type]
        remediation_action=str(row.get("remediation_action", "request_review")).strip(),
        reason_codes=[str(v) for v in (row.get("reason_codes") or []) if str(v).strip()],
        conditions=row.get("conditions") if isinstance(row.get("conditions"), dict) else {},
    )


def normalize_policy(row: dict[str, Any], tenant_id: str) -> Policy:
    scope_data = row.get("scope") if isinstance(row.get("scope"), dict) else {}
    rollout_data = row.get("rollout") if isinstance(row.get("rollout"), dict) else {}
    rules_data = row.get("rules")
    if not isinstance(rules_data, list):
        # backward compatibility with existing schema
        rule = {
            "rule_id": str(row.get("policy_id", "scope-policy")) + "-legacy",
            "vector": str((row.get("conditions") or {}).get("vector", "")).strip().lower(),
            "effect": str((row.get("actions") or {}).get("effect", "review")).strip().lower(),
            "severity": str((row.get("actions") or {}).get("severity", "medium")).strip().lower(),
            "remediation_mode": str((row.get("actions") or {}).get("remediation_mode", "manual")).strip().lower(),
            "remediation_action": str((row.get("actions") or {}).get("remediation_action", "request_review")).strip(),
            "reason_codes": (row.get("actions") or {}).get("reason_codes", []),
            "conditions": row.get("conditions") if isinstance(row.get("conditions"), dict) else {},
        }
        rules_data = [rule]

    scope = PolicyScope(
        scope_type=str(row.get("scope_type") or scope_data.get("scope_type") or "organization").strip().lower(),
        department_ids=[str(v) for v in (scope_data.get("department_ids") or row.get("department_ids") or []) if str(v).strip()],
        site_ids=[str(v) for v in (scope_data.get("site_ids") or row.get("site_ids") or []) if str(v).strip()],
        container_patterns=[
            str(v) for v in (scope_data.get("container_patterns") or row.get("container_patterns") or []) if str(v).strip()
        ],
        principal_group_ids=[
            str(v)
            for v in (scope_data.get("principal_group_ids") or row.get("principal_group_ids") or [])
            if str(v).strip()
        ],
        partner_domains_allowlist=[
            str(v)
            for v in (scope_data.get("partner_domains_allowlist") or row.get("partner_domains_allowlist") or [])
            if str(v).strip()
        ],
        criticality=str(scope_data.get("criticality") or row.get("criticality") or "medium").strip().lower(),
        use_cases=[str(v).strip().lower() for v in (scope_data.get("use_cases") or row.get("use_cases") or []) if str(v).strip()],
    )
    rollout = PolicyRollout(
        stage=str(rollout_data.get("stage") or "active").strip().lower(),  # type: ignore[arg-type]
        dry_run=_to_bool(rollout_data.get("dry_run"), default=False),
    )
    rules = [_normalize_rule(rule if isinstance(rule, dict) else {}, idx) for idx, rule in enumerate(rules_data, start=1)]

    return Policy(
        policy_id=str(row.get("policy_id") or "policy-unknown"),
        tenant_id=tenant_id,
        layer=str(row.get("layer") or row.get("scope_type") or "scope").strip().lower(),
        title=str(row.get("title") or row.get("name") or "scope-policy"),
        description=str(row.get("description") or ""),
        priority=int(row.get("priority", 100)),
        enabled=_to_bool(row.get("enabled"), default=str(row.get("status", "active")).strip().lower() == "active"),
        scope=scope,
        rules=rules,
        rollout=rollout,
        version=int(row.get("version", 1)),
        updated_by=str(row.get("operator") or row.get("updated_by") or ""),
        updated_at=str(row.get("updated_at") or ""),
    )


def _build_global_policy(
    *,
    tenant_id: str,
    policy_id: str,
    title: str,
    enabled: bool,
    vector: str,
    effect: str,
    severity: str,
    remediation_mode: str,
    remediation_action: str,
    reason_code: str,
    priority: int,
) -> Policy:
    return Policy(
        policy_id=policy_id,
        tenant_id=tenant_id,
        layer="organization",
        title=title,
        description="global policy synthesized from SSM",
        priority=priority,
        enabled=enabled,
        scope=PolicyScope(scope_type="organization"),
        rules=[
            PolicyRule(
                rule_id=f"{policy_id}-rule",
                vector=vector,
                effect=effect,  # type: ignore[arg-type]
                severity=severity,  # type: ignore[arg-type]
                remediation_mode=remediation_mode,  # type: ignore[arg-type]
                remediation_action=remediation_action,
                reason_codes=[reason_code],
                conditions={},
            )
        ],
        rollout=PolicyRollout(stage="active", dry_run=False),
        version=1,
        updated_by="system",
        updated_at="",
    )


def _get_global_flag(name: str, default: bool = False) -> bool:
    try:
        response = _get_ssm().get_parameter(Name=f"/aiready/governance/{name}")
    except Exception:
        return default
    value = str((response.get("Parameter") or {}).get("Value", "")).strip()
    return _to_bool(value, default=default)


def _build_global_policies(tenant_id: str) -> list[Policy]:
    policies: list[Policy] = []

    if _get_global_flag("policy_public_link_block_enabled", default=False):
        policies.append(
            _build_global_policy(
                tenant_id=tenant_id,
                policy_id="global-public-link-block",
                title="Global: block public link",
                enabled=True,
                vector="public_link",
                effect="block",
                severity="critical",
                remediation_mode="auto",
                remediation_action="remove_permissions",
                reason_code="GLOBAL_PUBLIC_LINK_BLOCK",
                priority=980,
            )
        )

    if _get_global_flag("policy_org_link_edit_review_enabled", default=False):
        policies.append(
            _build_global_policy(
                tenant_id=tenant_id,
                policy_id="global-org-link-edit-review",
                title="Global: org link edit requires review",
                enabled=True,
                vector="org_link_edit",
                effect="review",
                severity="high",
                remediation_mode="approval",
                remediation_action="remove_permissions",
                reason_code="GLOBAL_ORG_LINK_EDIT_REVIEW",
                priority=970,
            )
        )

    if _get_global_flag("policy_external_specific_people_review_enabled", default=False):
        policies.append(
            _build_global_policy(
                tenant_id=tenant_id,
                policy_id="global-external-specific-people-review",
                title="Global: external specific people requires review",
                enabled=True,
                vector="specific_people_external",
                effect="review",
                severity="high",
                remediation_mode="approval",
                remediation_action="remove_permissions",
                reason_code="GLOBAL_EXTERNAL_SPECIFIC_REVIEW",
                priority=960,
            )
        )

    if _get_global_flag("policy_external_domain_share_auto_remediation_enabled", default=False):
        policies.append(
            _build_global_policy(
                tenant_id=tenant_id,
                policy_id="global-external-domain-share-auto-remediation",
                title="Global: external domain share auto remediation",
                enabled=True,
                vector="external_domain_share",
                effect="block",
                severity="high",
                remediation_mode="approval",
                remediation_action="remove_permissions",
                reason_code="GLOBAL_EXTERNAL_DOMAIN_SHARE_AUTO_REMEDIATION",
                priority=959,
            )
        )

    if _get_global_flag("policy_external_email_direct_share_auto_remediation_enabled", default=False):
        policies.append(
            _build_global_policy(
                tenant_id=tenant_id,
                policy_id="global-external-email-direct-share-auto-remediation",
                title="Global: external email direct share auto remediation",
                enabled=True,
                vector="external_email_direct_share",
                effect="block",
                severity="high",
                remediation_mode="approval",
                remediation_action="remove_permissions",
                reason_code="GLOBAL_EXTERNAL_EMAIL_DIRECT_SHARE_AUTO_REMEDIATION",
                priority=958,
            )
        )

    if _get_global_flag("policy_guest_direct_share_auto_remediation_enabled", default=False):
        policies.append(
            _build_global_policy(
                tenant_id=tenant_id,
                policy_id="global-guest-direct-share-auto-remediation",
                title="Global: guest direct share auto remediation",
                enabled=True,
                vector="guest_direct_share",
                effect="block",
                severity="high",
                remediation_mode="approval",
                remediation_action="remove_permissions",
                reason_code="GLOBAL_GUEST_DIRECT_SHARE_AUTO_REMEDIATION",
                priority=957,
            )
        )

    if _get_global_flag("policy_all_users_block_enabled", default=False):
        policies.append(
            _build_global_policy(
                tenant_id=tenant_id,
                policy_id="global-all-users-block",
                title="Global: block all-users sharing",
                enabled=True,
                vector="all_users",
                effect="block",
                severity="critical",
                remediation_mode="owner_review",
                remediation_action="request_site_owner_review",
                reason_code="GLOBAL_ALL_USERS_BLOCK",
                priority=950,
            )
        )
    return policies


def _scope_policies_disabled() -> bool:
    # PoC default is global-only policy operation.
    return get_env_bool("GOVERNANCE_POC_DISABLE_SCOPE_POLICIES", default=True)


def list_active_policies(tenant_id: str) -> list[Policy]:
    normalized: list[Policy] = []
    if not _scope_policies_disabled():
        try:
            rows = load_scope_policy_rows(tenant_id)
        except Exception:
            rows = []
        normalized = [normalize_policy(row, tenant_id) for row in rows]
    has_data_driven_global = any(
        policy.layer in {"organization", "global"} or policy.scope.scope_type == "organization"
        for policy in normalized
    )
    global_policies = [] if has_data_driven_global else _build_global_policies(tenant_id)
    all_policies = [*normalized, *global_policies]
    return [policy for policy in all_policies if policy.enabled]
