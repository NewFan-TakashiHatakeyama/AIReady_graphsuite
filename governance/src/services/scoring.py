"""Risk aggregation helpers based on detection counts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from services.exposure_vectors import FileMetadata, extract_exposure_vectors, parse_permissions

ROLE_WEIGHTS: dict[str, float] = {
    "view": 0.20,
    "read": 0.20,
    "reader": 0.20,
    "comment": 0.35,
    "commenter": 0.35,
    "edit": 0.60,
    "write": 0.60,
    "writer": 0.60,
    "owner": 1.00,
    "manage": 1.00,
    "fullcontrol": 1.00,
}

@dataclass
class ExposureResult:
    score: float
    vectors: list[str]
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class SensitivityResult:
    score: float
    factors: list[str] = field(default_factory=list)
    is_preliminary: bool = True


@dataclass
class RiskAggregationResult:
    risk_type_counts: dict[str, int] = field(default_factory=dict)
    exposure_vector_counts: dict[str, int] = field(default_factory=dict)
    total_detected_risks: int = 0
    risk_level: str = "none"


RISK_LEVEL_THRESHOLDS: tuple[tuple[int, str], ...] = (
    (8, "critical"),
    (5, "high"),
    (2, "medium"),
    (1, "low"),
)

RISK_TYPE_ALIASES: dict[str, str] = {
    "highly_confidential": "high_sensitivity",
    "confidential": "medium_sensitivity",
    "pii_data": "pii",
    "personal_data": "pii",
    "credential": "secret",
    "credentials": "secret",
}


def _categorize_permission_level(role_score: float) -> str:
    if role_score >= ROLE_WEIGHTS["manage"]:
        return "manage"
    if role_score >= ROLE_WEIGHTS["edit"]:
        return "edit"
    if role_score >= ROLE_WEIGHTS["comment"]:
        return "comment"
    return "view"


def calculate_exposure_score(metadata: FileMetadata) -> ExposureResult:
    vectors = extract_exposure_vectors(metadata)
    permissions = parse_permissions(metadata.permissions)
    vector_set = set(vectors)

    audience_scope_label = "individual"
    audience_scope = 0.05
    if "public_link" in vector_set:
        audience_scope_label = "public"
        audience_scope = 1.00
    elif "all_users" in vector_set:
        audience_scope_label = "organization"
        audience_scope = 0.70
    elif any(v in vector_set for v in {"guest", "guest_direct_share", "external_email_direct_share", "external_domain_share", "external_domain"}):
        audience_scope_label = "external_org"
        audience_scope = 0.85
    elif "org_link" in vector_set:
        audience_scope_label = "organization"
        audience_scope = 0.55

    discoverability_label = "hidden"
    discoverability = 0.10
    if "public_link" in vector_set:
        discoverability_label = "browsable"
        discoverability = 1.00
    elif "org_link" in vector_set:
        discoverability_label = "link_only"
        discoverability = 0.35

    role_scores: list[float] = []
    for entry in permissions:
        for r in entry.get("roles", []) or []:
            role_scores.append(ROLE_WEIGHTS.get(str(r).strip().lower(), 0.20))
    privilege_strength = sum(role_scores) / len(role_scores) if role_scores else 0.20
    permission_max_level_score = max(role_scores) if role_scores else ROLE_WEIGHTS["view"]

    externality_label = "internal_only"
    externality = 0.00
    if "public_link" in vector_set:
        externality_label = "public_internet"
        externality = 1.00
    elif any(v in vector_set for v in {"external_domain_share", "external_domain"}):
        externality_label = "external_domain"
        externality = 0.80
    elif any(v in vector_set for v in {"guest", "guest_direct_share", "external_email_direct_share"}):
        externality_label = "external_named"
        externality = 0.60

    reshare_label = "none"
    reshare = 0.10
    if "org_link_editable" in vector_set or permission_max_level_score >= ROLE_WEIGHTS["edit"]:
        reshare_label = "allowed"
        reshare = 0.80

    permission_outlier = 0.0
    if int(metadata.permissions_count) > 10:
        permission_outlier = min(1.0, max(0.0, (int(metadata.permissions_count) - 10) / 200.0))

    exposure_composite = min(
        1.0,
        max(
            0.0,
        0.35 * audience_scope
        + 0.25 * privilege_strength
        + 0.15 * discoverability
        + 0.15 * externality
        + 0.05 * reshare
            + 0.05 * permission_outlier,
        ),
    )

    return ExposureResult(
        score=round(exposure_composite, 4),
        vectors=vectors,
        details={
            "audience_scope": audience_scope_label,
            "audience_scope_score": round(audience_scope, 4),
            "privilege_strength_score": round(privilege_strength, 4),
            "permission_weighted_level": _categorize_permission_level(privilege_strength),
            "permission_max_level": _categorize_permission_level(permission_max_level_score),
            "permission_max_level_score": round(permission_max_level_score, 4),
            "discoverability": discoverability_label,
            "discoverability_score": round(discoverability, 4),
            "externality": externality_label,
            "externality_score": round(externality, 4),
            "reshare_capability": reshare_label,
            "reshare_capability_score": round(reshare, 4),
            "permission_outlier_score": round(permission_outlier, 4),
            "broken_inheritance_score": 0.0,
        },
    )


def _normalize_risk_type(name: Any) -> str:
    normalized = str(name or "").strip().lower()
    if not normalized:
        return ""
    return RISK_TYPE_ALIASES.get(normalized, normalized)


def calculate_risk_type_counts(content_signals: dict[str, Any] | None) -> dict[str, int]:
    if not isinstance(content_signals, dict):
        return {}
    counts: dict[str, int] = {}
    categories = content_signals.get("doc_categories", [])
    if isinstance(categories, list):
        for category in categories:
            key = _normalize_risk_type(category)
            if key:
                counts[key] = counts.get(key, 0) + 1
    level = str(content_signals.get("doc_sensitivity_level", "none")).strip().lower()
    if level in {"low", "medium", "high", "critical"}:
        key = f"sensitivity_{level}"
        counts[key] = counts.get(key, 0) + 1
    if bool(content_signals.get("contains_pii", False)):
        counts["pii"] = counts.get("pii", 0) + 1
    if bool(content_signals.get("contains_secret", False)):
        counts["secret"] = counts.get("secret", 0) + 1
    return dict(sorted(counts.items()))


def calculate_exposure_vector_counts(vectors: list[str] | None) -> dict[str, int]:
    counts: dict[str, int] = {}
    for vector in vectors or []:
        key = str(vector or "").strip().lower()
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def classify_risk_level(total_detected_risks: int) -> str:
    for min_count, label in RISK_LEVEL_THRESHOLDS:
        if int(total_detected_risks) >= min_count:
            return label
    return "none"


def summarize_detected_risks(
    *,
    exposure_vectors: list[str] | None,
    content_signals: dict[str, Any] | None = None,
) -> RiskAggregationResult:
    risk_type_counts = calculate_risk_type_counts(content_signals)
    exposure_vector_counts = calculate_exposure_vector_counts(exposure_vectors)
    total_detected_risks = sum(risk_type_counts.values()) + sum(exposure_vector_counts.values())
    has_exposure_vectors = sum(exposure_vector_counts.values()) > 0
    risk_level = classify_risk_level(total_detected_risks)
    # Content-only detections (no sharing exposure vectors) should not escalate
    # above low. This keeps low-risk labeling aligned with PoC operations.
    if not has_exposure_vectors and total_detected_risks > 0:
        risk_level = "low"
    return RiskAggregationResult(
        risk_type_counts=risk_type_counts,
        exposure_vector_counts=exposure_vector_counts,
        total_detected_risks=total_detected_risks,
        risk_level=risk_level,
    )


def compute_ai_eligible(
    risk_level: str,
    total_detected_risks: int = 0,
    pii_detected: bool = False,
    secrets_detected: bool = False,
) -> bool:
    return (
        str(risk_level).strip().lower() in {"high", "critical"}
        and int(total_detected_risks) > 0
        and not pii_detected
        and not secrets_detected
    )
