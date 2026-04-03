"""Evaluate governance vectors with effective policy semantics."""

from __future__ import annotations

from services.policy_models import EffectivePolicySnapshot


VECTOR_ALIASES = {
    "org_link": "org_link_view",
    "org_link_editable": "org_link_edit",
    "external_domain": "external_domain_not_allowlisted",
    "guest": "specific_people_external",
}


def normalize_vectors(vectors: list[str] | None) -> list[str]:
    normalized: list[str] = []
    for raw in vectors or []:
        value = str(raw).strip().lower()
        if not value:
            continue
        normalized.append(VECTOR_ALIASES.get(value, value))
    return sorted(set(normalized))


def evaluate_policy_snapshot(snapshot: EffectivePolicySnapshot) -> dict[str, object]:
    """Return finding-friendly policy fields."""
    return {
        "decision": snapshot.decision,
        "risk_level": snapshot.risk_level,
        "remediation_mode": snapshot.remediation_mode,
        "remediation_action": snapshot.remediation_action,
        "effective_policy_id": snapshot.effective_policy_id,
        "effective_policy_version": snapshot.effective_policy_version,
        "matched_policy_ids": snapshot.matched_policy_ids,
        "decision_trace": snapshot.decision_trace,
        "reason_codes": snapshot.reason_codes,
        "policy_hash": snapshot.policy_hash,
        "decision_source": snapshot.decision_source,
        "expected_audience": snapshot.expected_audience,
        "expected_department": snapshot.expected_department,
        "expectation_gap_reason": snapshot.expectation_gap_reason,
        "expectation_gap_score": snapshot.expectation_gap_score,
    }
