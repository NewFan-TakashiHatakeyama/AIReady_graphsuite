"""Resolve effective policy snapshot from layered policy data."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from services.policy_models import (
    EffectivePolicySnapshot,
    Policy,
    PolicyContext,
)
from shared.config import get_env
from shared.metrics import emit_count

_DECISION_PRIORITY: dict[str, int] = {
    "allow": 1,
    "approve": 2,
    "restrict": 3,
    "warn": 4,
    "review": 5,
    "deny": 6,
    "block": 7,
}


def _policy_applies(policy: Policy, context: PolicyContext) -> bool:
    scope = policy.scope
    if scope.department_ids and context.department_id not in scope.department_ids:
        return False
    if scope.site_ids and context.site_id not in scope.site_ids:
        return False
    if scope.principal_group_ids and not set(context.principal_ids).intersection(set(scope.principal_group_ids)):
        return False
    if scope.use_cases and context.use_case and context.use_case not in scope.use_cases:
        return False
    return True


def _rule_applies(rule_vector: str, exposure_vectors: list[str]) -> bool:
    if not rule_vector:
        return False
    return rule_vector in exposure_vectors


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _lookup_field(data: dict[str, Any], field: str) -> Any:
    current: Any = data
    for token in str(field or "").split("."):
        key = token.strip()
        if not key:
            return None
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _evaluate_predicate(predicate: dict[str, Any], data: dict[str, Any]) -> bool:
    field = str(predicate.get("field", "")).strip()
    op = str(predicate.get("op", "eq")).strip().lower()
    expected = predicate.get("value")
    actual = _lookup_field(data, field)
    if op == "eq":
        return actual == expected
    if op == "neq":
        return actual != expected
    if op == "in":
        return isinstance(expected, list) and actual in expected
    if op == "not_in":
        return isinstance(expected, list) and actual not in expected
    if op == "contains":
        if isinstance(actual, list):
            return expected in actual
        if isinstance(actual, str):
            return str(expected or "") in actual
        return False
    if op == "gte":
        return _safe_float(actual) >= _safe_float(expected)
    if op == "lte":
        return _safe_float(actual) <= _safe_float(expected)
    return False


def _evaluate_conditions(conditions: dict[str, Any], data: dict[str, Any]) -> bool:
    if not isinstance(conditions, dict) or not conditions:
        return False
    if "all" in conditions:
        all_items = conditions.get("all")
        return isinstance(all_items, list) and all(
            _evaluate_conditions(item, data) if isinstance(item, dict) and any(k in item for k in ("all", "any", "not")) else _evaluate_predicate(item, data)
            for item in all_items
            if isinstance(item, dict)
        )
    if "any" in conditions:
        any_items = conditions.get("any")
        return isinstance(any_items, list) and any(
            _evaluate_conditions(item, data) if isinstance(item, dict) and any(k in item for k in ("all", "any", "not")) else _evaluate_predicate(item, data)
            for item in any_items
            if isinstance(item, dict)
        )
    if "not" in conditions:
        nested = conditions.get("not")
        if not isinstance(nested, dict):
            return False
        if any(k in nested for k in ("all", "any", "not")):
            return not _evaluate_conditions(nested, data)
        return not _evaluate_predicate(nested, data)
    return _evaluate_predicate(conditions, data)


def _to_risk(effect: str, severity: str) -> str:
    if severity in {"low", "medium", "high", "critical"}:
        return severity
    if effect == "block":
        return "critical"
    if effect == "review":
        return "high"
    if effect == "warn":
        return "medium"
    return "low"


def _expected_audience_from_context(context: PolicyContext) -> str:
    raw = (
        context.content_signals.get("expected_audience")
        if isinstance(context.content_signals, dict)
        else None
    )
    if not raw:
        raw = (context.item_metadata or {}).get("expected_audience")
    value = str(raw or "").strip().lower()
    if not value:
        return "internal_need_to_know"
    return value


def _expected_department_from_context(context: PolicyContext) -> str:
    raw = (
        context.content_signals.get("expected_department")
        if isinstance(context.content_signals, dict)
        else None
    )
    if not raw:
        raw = (context.item_metadata or {}).get("expected_department")
    value = str(raw or "").strip()
    return value or "unknown"


def _evaluate_expectation_gap(
    *,
    expected_audience: str,
    exposure_vectors: list[str],
) -> dict[str, Any] | None:
    vectors = {str(v).strip().lower() for v in exposure_vectors if str(v).strip()}
    has_public = bool(vectors.intersection({"public_link", "all_users"}))
    has_external = bool(
        vectors.intersection(
            {"guest", "external_domain", "external_domain_not_allowlisted", "specific_people_external"}
        )
    )
    has_wide_internal = bool(vectors.intersection({"org_link", "org_link_view", "org_link_edit"}))

    if expected_audience == "owner_only":
        if has_public or has_wide_internal:
            return {
                "decision": "block",
                "risk_level": "critical",
                "remediation_mode": "auto",
                "remediation_action": "remove_permissions",
                "reason_code": "EXPECTATION_GAP_OWNER_ONLY_BROAD",
                "gap_reason": "owner_only document is exposed to broad audience",
                "gap_score": 0.95,
            }
        if has_external:
            return {
                "decision": "review",
                "risk_level": "high",
                "remediation_mode": "owner_review",
                "remediation_action": "request_review",
                "reason_code": "EXPECTATION_GAP_OWNER_ONLY_EXTERNAL",
                "gap_reason": "owner_only document has external sharing",
                "gap_score": 0.85,
            }

    if expected_audience == "department_only":
        if has_public:
            return {
                "decision": "block",
                "risk_level": "critical",
                "remediation_mode": "owner_review",
                "remediation_action": "request_review",
                "reason_code": "EXPECTATION_GAP_DEPARTMENT_ONLY_PUBLIC",
                "gap_reason": "department_only document is public/all-users",
                "gap_score": 0.90,
            }
        if has_external or has_wide_internal:
            return {
                "decision": "review",
                "risk_level": "high",
                "remediation_mode": "approval",
                "remediation_action": "request_review",
                "reason_code": "EXPECTATION_GAP_DEPARTMENT_ONLY_WIDE",
                "gap_reason": "department_only document has outside-department exposure",
                "gap_score": 0.75,
            }

    if expected_audience == "internal_need_to_know":
        if has_public:
            return {
                "decision": "block",
                "risk_level": "critical",
                "remediation_mode": "owner_review",
                "remediation_action": "request_review",
                "reason_code": "EXPECTATION_GAP_NEED_TO_KNOW_PUBLIC",
                "gap_reason": "internal_need_to_know document is public/all-users",
                "gap_score": 0.85,
            }
        if has_external:
            return {
                "decision": "review",
                "risk_level": "high",
                "remediation_mode": "approval",
                "remediation_action": "request_review",
                "reason_code": "EXPECTATION_GAP_NEED_TO_KNOW_EXTERNAL",
                "gap_reason": "internal_need_to_know document has external exposure",
                "gap_score": 0.70,
            }

    if expected_audience == "organization" and has_public:
        return {
            "decision": "warn",
            "risk_level": "medium",
            "remediation_mode": "manual",
            "remediation_action": "request_review",
            "reason_code": "EXPECTATION_GAP_ORGANIZATION_PUBLIC",
            "gap_reason": "organization audience document is publicly exposed",
            "gap_score": 0.55,
        }

    return None


def resolve_effective_policy(
    *,
    context: PolicyContext,
    policies: list[Policy],
    exposure_vectors: list[str],
) -> EffectivePolicySnapshot:
    candidates = [policy for policy in policies if _policy_applies(policy, context)]
    candidates.sort(key=lambda p: (p.priority, p.version), reverse=True)

    best_effect = "allow"
    best_risk = "low"
    best_mode = "manual"
    best_action = "request_review"
    decision_source = "fallback"
    matched: list[str] = []
    trace: list[str] = []
    reason_codes: list[str] = []
    effective_id = "default-oversharing-policy"
    effective_version = 1
    expected_audience = _expected_audience_from_context(context)
    expected_department = _expected_department_from_context(context)
    expectation_gap_reason = ""
    expectation_gap_score = 0.0
    field_data = {
        **(context.item_metadata or {}),
        "vector": "",
        "exposure_vectors": list(exposure_vectors),
        "analysis_confidence": _safe_float((context.item_metadata or {}).get("analysis_confidence", 0.0)),
    }

    for policy in candidates:
        for rule in policy.rules:
            if rule.conditions:
                field_data["vector"] = str(rule.vector or "").strip()
                field_data["rule_vector"] = str(rule.vector or "").strip()
                field_data["rule_id"] = str(rule.rule_id or "").strip()
                rule_matched = _evaluate_conditions(rule.conditions, field_data)
            else:
                rule_matched = _rule_applies(rule.vector, exposure_vectors)
            if not rule_matched:
                continue
            matched.append(policy.policy_id)
            trace.append(f"{policy.policy_id}:{rule.effect}")
            reason_codes.extend(rule.reason_codes)
            current_priority = _DECISION_PRIORITY.get(best_effect, 0)
            next_priority = _DECISION_PRIORITY.get(rule.effect, 0)
            if next_priority >= current_priority:
                best_effect = rule.effect
                best_risk = _to_risk(rule.effect, rule.severity)
                best_mode = rule.remediation_mode
                best_action = rule.remediation_action
                effective_id = policy.policy_id
                effective_version = policy.version
                decision_source = "global"

    if matched:
        trace.insert(0, f"global_rule_hit:{effective_id}")

    expectation_gap_enabled = str(
        get_env("GOVERNANCE_POC_EXPECTATION_GAP_ENABLED", "true")
    ).strip().lower() in {"1", "true", "yes", "on"}
    if expectation_gap_enabled:
        gap = _evaluate_expectation_gap(
            expected_audience=expected_audience,
            exposure_vectors=exposure_vectors,
        )
        if gap:
            emit_count("AIReadyGov.ExpectedAudience.GapDetected")
            expectation_gap_reason = str(gap.get("gap_reason", "")).strip()
            expectation_gap_score = _safe_float(gap.get("gap_score", 0.0))
            trace.append(f"expectation_gap_hit:{expectation_gap_reason}")
            reason_codes.append(str(gap.get("reason_code", "EXPECTATION_GAP")))
            current_priority = _DECISION_PRIORITY.get(best_effect, 0)
            next_priority = _DECISION_PRIORITY.get(str(gap.get("decision", "review")), 0)
            if next_priority > current_priority:
                best_effect = str(gap.get("decision", "review"))
                best_risk = str(gap.get("risk_level", "high"))
                best_mode = str(gap.get("remediation_mode", "owner_review"))
                best_action = str(gap.get("remediation_action", "request_review"))
                effective_id = "expected-audience-gap"
                effective_version = 1
                decision_source = "gap"

    try:
        confidence_threshold = float(get_env("GOVERNANCE_CONTENT_CONFIDENCE_THRESHOLD", "0.7"))
    except Exception:
        confidence_threshold = 0.7
    analysis_confidence = _safe_float(field_data.get("analysis_confidence", 0.0))
    if analysis_confidence < confidence_threshold and best_effect in {"block", "deny", "warn", "allow"}:
        trace.append(
            f"failsafe:confidence<{confidence_threshold:.2f}({analysis_confidence:.2f})=>review"
        )
        best_effect = "review"
        best_risk = max(best_risk, "high", key=lambda x: {"low": 1, "medium": 2, "high": 3, "critical": 4}.get(x, 0))
        best_mode = "owner_review"
        if best_action in {"", "remove_permissions"}:
            best_action = "request_review"
        decision_source = "fallback"

    snapshot_dict: dict[str, Any] = {
        "effective_policy_id": effective_id,
        "effective_policy_version": effective_version,
        "matched_policy_ids": sorted(set(matched)),
        "decision": best_effect,
        "risk_level": best_risk,
        "remediation_mode": best_mode,
        "remediation_action": best_action,
        "decision_trace": trace,
        "reason_codes": sorted(set(reason_codes)),
        "decision_source": decision_source,
        "expected_audience": expected_audience,
        "expected_department": expected_department,
        "expectation_gap_reason": expectation_gap_reason,
        "expectation_gap_score": expectation_gap_score,
    }
    snapshot_dict["policy_hash"] = "sha256:" + hashlib.sha256(
        json.dumps(snapshot_dict, ensure_ascii=True, sort_keys=True).encode("utf-8")
    ).hexdigest()

    return EffectivePolicySnapshot(**snapshot_dict)
