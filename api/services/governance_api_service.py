"""Governance API application service for MS-3 endpoints."""

from __future__ import annotations

import os
import uuid
import logging
import json
import sys
import subprocess
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import unquote, urlparse

from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from services.aws_clients import (
    get_dynamodb_resource,
    get_lambda_client,
    get_logs_client,
    get_ssm_client,
)
from services.governance_repository import (
    list_governance_findings,
)
from services.runtime_config import load_aws_runtime_config

_runtime_config = load_aws_runtime_config()
_dynamodb_resource = None
_lambda_client = None
_logs_client = None
_ssm_client = None
logger = logging.getLogger(__name__)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()


def _to_plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, list):
        return [_to_plain(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    return value


def _resource():
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = get_dynamodb_resource(_runtime_config)
    return _dynamodb_resource


def _lambda():
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = get_lambda_client(_runtime_config)
    return _lambda_client


def _logs():
    global _logs_client
    if _logs_client is None:
        _logs_client = get_logs_client(_runtime_config)
    return _logs_client


def _ssm():
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = get_ssm_client(_runtime_config)
    return _ssm_client


def _table(name: str):
    return _resource().Table(name)


def _policy_scope_table_name() -> str:
    return (os.getenv("GOVERNANCE_POLICY_SCOPE_TABLE_NAME") or "AIReadyGov-PolicyScope").strip()


def _scope_policies_disabled() -> bool:
    return str(
        os.getenv("GOVERNANCE_POC_DISABLE_SCOPE_POLICIES", "true")
    ).strip().lower() in {"1", "true", "yes", "on"}


def _scan_job_table_name() -> str:
    return (os.getenv("GOVERNANCE_SCAN_JOB_TABLE_NAME") or "AIReadyGov-ScanJob").strip()


def _audit_table_name() -> str:
    return (os.getenv("GOVERNANCE_AUDIT_TABLE_NAME") or "AIReadyGov-AuditLog").strip()


def _analyze_lambda_name() -> str:
    return (
        os.getenv("GOVERNANCE_ANALYZE_EXPOSURE_LAMBDA_NAME") or "AIReadyGov-analyzeExposure"
    ).strip()


def _remediation_lambda_name() -> str:
    return (
        os.getenv("GOVERNANCE_REMEDIATE_FINDING_LAMBDA_NAME") or "AIReadyGov-remediateFinding"
    ).strip()


def _allow_local_remediation_fallback() -> bool:
    raw = os.getenv("GOVERNANCE_REMEDIATION_LOCAL_FALLBACK")
    if raw is None:
        return not _runtime_config.governance_api_strict_mode
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _report_bucket_name() -> str:
    return (os.getenv("GOVERNANCE_REPORT_BUCKET") or "").strip()


class GovernanceRemediationProxyError(RuntimeError):
    """Proxy error raised when remediation lambda returns non-200."""

    def __init__(self, status_code: int, message: str):
        self.status_code = int(status_code)
        super().__init__(message)


def _invoke_remediation_local(payload: dict[str, Any]) -> dict[str, Any]:
    """Local fallback for remediation proxy when Lambda is not deployed.

    This keeps API-side business logic out of process by delegating execution to
    the governance handler entrypoint in a dedicated Python process.
    """
    governance_src = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "governance", "src")
    )
    script = (
        "import json,sys;"
        "from handlers.remediate_finding import handler;"
        "event=json.loads(sys.argv[1]);"
        "result=handler(event,None);"
        "print(json.dumps(result, ensure_ascii=False, default=str))"
    )
    local_env = os.environ.copy()
    # Ensure governance handler required table envs are available in fallback subprocess.
    if not local_env.get("FINDING_TABLE_NAME"):
        local_env["FINDING_TABLE_NAME"] = (
            os.getenv("GOVERNANCE_FINDING_TABLE_NAME") or "AIReadyGov-ExposureFinding"
        )
    if not local_env.get("CONNECT_TABLE_NAME"):
        local_env["CONNECT_TABLE_NAME"] = (
            os.getenv("GOVERNANCE_CONNECT_TABLE_NAME") or "AIReadyConnect-FileMetadata"
        )

    process = subprocess.run(
        [sys.executable, "-c", script, json.dumps(payload, ensure_ascii=True)],
        cwd=governance_src,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env=local_env,
    )
    if process.returncode != 0:
        stderr = (process.stderr or "").strip()
        raise GovernanceRemediationProxyError(
            500,
            f"local remediation fallback failed: {stderr or f'exit={process.returncode}'}",
        )
    output_lines = [line.strip() for line in (process.stdout or "").splitlines() if line.strip()]
    raw_payload = output_lines[-1] if output_lines else "{}"
    try:
        lambda_payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise GovernanceRemediationProxyError(
            500,
            f"invalid local remediation payload: {raw_payload[:200]}",
        ) from exc

    handler_status = int(lambda_payload.get("statusCode", 500))
    body = lambda_payload.get("body", {})
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            body = {"message": body}
    if not isinstance(body, dict):
        body = {"message": str(body)}
    if handler_status >= 400:
        message = str(body.get("error") or body.get("message") or "remediation failed")
        raise GovernanceRemediationProxyError(handler_status, message)
    return body


def _list_all_findings(tenant_id: str) -> list[dict[str, Any]]:
    response = list_governance_findings(
        tenant_id=tenant_id,
        limit=500,
        offset=0,
        statuses=["new", "open", "acknowledged", "closed", "completed", "remediated"],
        include_document_analysis=False,
    )
    return [_to_plain(row) for row in response.get("rows", [])]


def _is_high_risk(row: dict[str, Any]) -> bool:
    level = str(row.get("risk_level", "")).lower()
    if level in {"high", "critical"}:
        return True
    if level == "none":
        level = "low"
    if level in {"low", "medium"}:
        return False
    score = row.get("risk_score")
    try:
        return float(score) >= 55.0
    except Exception:
        return False


def _is_action_required(row: dict[str, Any]) -> bool:
    status_value = str(row.get("status", "")).lower()
    workflow_status = str(row.get("workflow_status", "")).lower()
    if status_value not in {"new", "open", "acknowledged"} and workflow_status != "acknowledged":
        return False
    level = str(row.get("risk_level", "")).lower()
    if level in {"medium", "high", "critical"}:
        return True
    if level in {"low", "none"}:
        return False
    score = row.get("risk_score")
    try:
        return float(score) >= 30.0
    except Exception:
        return False


def _expiring_suppressions(findings: list[dict[str, Any]], within_hours: int) -> list[dict[str, Any]]:
    now = _now_utc()
    threshold = now + timedelta(hours=max(1, within_hours))
    rows: list[dict[str, Any]] = []
    for row in findings:
        status_value = str(row.get("status", "")).lower()
        workflow_status = str(row.get("workflow_status", "")).lower()
        if status_value != "acknowledged" and workflow_status != "acknowledged":
            continue
        review_due_at = str(
            row.get("exception_review_due_at", "") or row.get("suppress_until", "")
        ).strip()
        if not review_due_at:
            continue
        try:
            parsed = datetime.fromisoformat(review_due_at.replace("Z", "+00:00"))
        except ValueError:
            continue
        if now <= parsed <= threshold:
            rows.append(row)
    rows.sort(
        key=lambda item: str(
            item.get("exception_review_due_at", "") or item.get("suppress_until", "")
        )
    )
    return rows


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _risk_level_from_score(score: float) -> str:
    if score >= 80.0:
        return "critical"
    if score >= 55.0:
        return "high"
    if score >= 30.0:
        return "medium"
    return "low"


def _resolve_raw_residual_risk(row: dict[str, Any]) -> float:
    raw = row.get("raw_residual_risk")
    if raw is not None:
        return _clamp01(_safe_float(raw, 0.0))
    score = _safe_float(row.get("risk_score"), 0.0)
    return _clamp01(score / 100.0)


def _resolve_scan_confidence(row: dict[str, Any]) -> float:
    explicit = row.get("scan_confidence")
    if explicit is not None:
        return _clamp01(_safe_float(explicit, 0.6))
    scan_mode = str(row.get("scan_mode", "")).strip().lower()
    if scan_mode == "content_scanned":
        return 1.0
    if scan_mode == "metadata_only":
        return 0.7
    if scan_mode in {"unsupported_or_skipped", "skipped"}:
        return 0.5
    if scan_mode == "partial":
        return 0.75
    return 0.6


def _resolve_exception_debt(row: dict[str, Any]) -> float:
    exception_type = str(row.get("exception_type", "none")).strip().lower()
    if not exception_type or exception_type == "none":
        return 0.0
    base_map = {
        "false_positive": 0.0,
        "compensating_control": 0.4,
        "temporary_accept": 0.65,
        "permanent_accept": 0.85,
    }
    debt = base_map.get(exception_type, 0.6)
    due_at = str(
        row.get("exception_review_due_at", "") or row.get("suppress_until", "")
    ).strip()
    if due_at:
        try:
            due_dt = datetime.fromisoformat(due_at.replace("Z", "+00:00"))
            if _now_utc() > due_dt:
                debt = min(1.0, debt + 0.15)
        except ValueError:
            pass
    return _clamp01(debt)


def _build_v12_overview(
    findings: list[dict[str, Any]],
) -> dict[str, Any]:
    if not findings:
        return {
            "governance_score": 100.0,
            "subscores": {
                "oversharing_control": 100.0,
                "assurance": 100.0,
            },
            "subscores_breakdown": {
                "oversharing_control": [],
                "assurance": [],
            },
            "coverage": {
                "coverage_score": 1.0,
                "inventory_coverage": 1.0,
                "content_scan_coverage": 1.0,
                "supported_format_coverage": 1.0,
                "fresh_scan_coverage": 1.0,
                "permission_detail_coverage": 1.0,
            },
            "confidence": {
                "level": "High",
                "scan_confidence": 1.0,
            },
            "risk_summary": {
                "governance_raw": 0.0,
                "exception_debt": 0.0,
                "coverage_penalty": 0.0,
            },
        }

    raw_values = [_resolve_raw_residual_risk(row) for row in findings]
    avg_raw = sum(raw_values) / len(findings)
    exposure_values = [_clamp01(_safe_float(row.get("exposure_score"), 0.5)) for row in findings]
    oversharing_risk = sum(raw * exposure for raw, exposure in zip(raw_values, exposure_values)) / len(findings)

    scan_confidences = [_resolve_scan_confidence(row) for row in findings]
    avg_scan_confidence = sum(scan_confidences) / len(findings)

    exception_debts = [_resolve_exception_debt(row) for row in findings]
    avg_exception_debt = sum(exception_debts) / len(findings)

    content_scan_coverage = sum(
        1.0
        for row in findings
        if str(row.get("scan_mode", "")).strip().lower() == "content_scanned"
        or str(row.get("sensitivity_scan_at", "")).strip()
    ) / len(findings)

    supported_format_coverage = sum(
        1.0
        for row in findings
        if str(row.get("scan_mode", "")).strip().lower() not in {"unsupported_or_skipped"}
    ) / len(findings)

    fresh_scan_coverage_count = 0
    for row in findings:
        scan_at = str(row.get("sensitivity_scan_at", "")).strip()
        if not scan_at:
            continue
        try:
            parsed = datetime.fromisoformat(scan_at.replace("Z", "+00:00"))
            if _now_utc() - parsed <= timedelta(days=30):
                fresh_scan_coverage_count += 1
        except ValueError:
            continue
    fresh_scan_coverage = fresh_scan_coverage_count / len(findings)

    permission_detail_coverage = sum(
        1.0
        for row in findings
        if row.get("permission_weighted_level") is not None
        or row.get("permission_max_level") is not None
        or str(row.get("permissions_summary", "")).strip()
    ) / len(findings)

    inventory_values = [
        _clamp01(_safe_float(row.get("inventory_coverage_snapshot"), 1.0))
        for row in findings
        if row.get("inventory_coverage_snapshot") is not None
    ]
    inventory_coverage = (
        sum(inventory_values) / len(inventory_values) if inventory_values else 1.0
    )

    coverage_score = _clamp01(
        0.25 * inventory_coverage
        + 0.35 * content_scan_coverage
        + 0.15 * supported_format_coverage
        + 0.15 * fresh_scan_coverage
        + 0.10 * permission_detail_coverage
    )
    coverage_penalty = _clamp01(1.0 - coverage_score)

    governance_raw = _clamp01(avg_raw)
    governance_score = round(
        _clamp_score(
            100.0
            * (
                1.0
                - _clamp01(
                    0.60 * governance_raw
                    + 0.20 * avg_exception_debt
                    + 0.20 * coverage_penalty
                )
            )
        ),
        1,
    )

    oversharing_control = round(_clamp_score(100.0 * (1.0 - oversharing_risk)), 1)
    assurance = round(
        _clamp_score(
            100.0
            * (
                0.25 * (1.0 - governance_raw)
                + 0.25 * (1.0 - avg_exception_debt)
                + 0.25 * coverage_score
                + 0.15 * fresh_scan_coverage
                + 0.10 * avg_scan_confidence
            )
        ),
        1,
    )

    confidence_level = (
        "High"
        if avg_scan_confidence >= 0.8 and coverage_score >= 0.8
        else "Medium"
        if avg_scan_confidence >= 0.55 and coverage_score >= 0.55
        else "Low"
    )

    avg_audience_scope = sum(
        _clamp01(_safe_float(row.get("audience_scope_score"), 0.0))
        for row in findings
    ) / len(findings)
    avg_public_link_risk = sum(
        _clamp01(
            _safe_float(
                row.get("public_link_risk_score"),
                1.0 if _has_vector(row, "public_link") else 0.0,
            )
        )
        for row in findings
    ) / len(findings)
    avg_privilege_strength = sum(
        _clamp01(_safe_float(row.get("privilege_strength_score"), 0.0))
        for row in findings
    ) / len(findings)
    avg_discoverability = sum(
        _clamp01(_safe_float(row.get("discoverability_score"), 0.0))
        for row in findings
    ) / len(findings)
    avg_externality = sum(
        _clamp01(_safe_float(row.get("externality_score"), 0.0))
        for row in findings
    ) / len(findings)
    avg_reshare = sum(
        _clamp01(_safe_float(row.get("reshare_capability_score"), 0.0))
        for row in findings
    ) / len(findings)
    avg_permission_outlier = sum(
        _clamp01(_safe_float(row.get("permission_outlier_score"), 0.0))
        for row in findings
    ) / len(findings)
    avg_age_factor = sum(
        _clamp01(_safe_float(row.get("age_factor"), 0.0))
        for row in findings
    ) / len(findings)

    subscores_breakdown = {
        "oversharing_control": [
            {
                "key": "broad_audience_risk",
                "label": "公開到達篁E��リスク",
                "value": round(avg_audience_scope, 4),
                "score": round(_clamp_score(100.0 * (1.0 - avg_audience_scope)), 1),
            },
            {
                "key": "public_link_risk",
                "label": "公開リンクリスク",
                "value": round(avg_public_link_risk, 4),
                "score": round(_clamp_score(100.0 * (1.0 - avg_public_link_risk)), 1),
            },
            {
                "key": "privilege_excess_risk",
                "label": "過剰権限リスク",
                "value": round(avg_privilege_strength, 4),
                "score": round(_clamp_score(100.0 * (1.0 - avg_privilege_strength)), 1),
            },
            {
                "key": "discoverability_risk",
                "label": "発見可能性リスク",
                "value": round(avg_discoverability, 4),
                "score": round(_clamp_score(100.0 * (1.0 - avg_discoverability)), 1),
            },
            {
                "key": "external_boundary_risk",
                "label": "外部共有墁E��リスク",
                "value": round(avg_externality, 4),
                "score": round(_clamp_score(100.0 * (1.0 - avg_externality)), 1),
            },
            {
                "key": "reshare_risk",
                "label": "再�E有リスク",
                "value": round(avg_reshare, 4),
                "score": round(_clamp_score(100.0 * (1.0 - avg_reshare)), 1),
            },
            {
                "key": "permission_outlier_risk",
                "label": "権限数異常リスク",
                "value": round(avg_permission_outlier, 4),
                "score": round(_clamp_score(100.0 * (1.0 - avg_permission_outlier)), 1),
            },
        ],
        "assurance": [
            {
                "key": "aging_open_risk",
                "label": "放置期間リスク",
                "value": round(avg_age_factor, 4),
                "score": round(_clamp_score(100.0 * (1.0 - avg_age_factor)), 1),
            },
            {
                "key": "exception_debt",
                "label": "例外負債",
                "value": round(avg_exception_debt, 4),
                "score": round(_clamp_score(100.0 * (1.0 - avg_exception_debt)), 1),
            },
            {
                "key": "coverage_score",
                "label": "カバレチE��",
                "value": round(coverage_score, 4),
                "score": round(_clamp_score(100.0 * coverage_score), 1),
            },
            {
                "key": "rescan_freshness",
                "label": "再スキャン鮮度",
                "value": round(fresh_scan_coverage, 4),
                "score": round(_clamp_score(100.0 * fresh_scan_coverage), 1),
            },
            {
                "key": "scan_confidence",
                "label": "スキャン信頼度",
                "value": round(avg_scan_confidence, 4),
                "score": round(_clamp_score(100.0 * avg_scan_confidence), 1),
            },
        ],
    }

    return {
        "governance_score": governance_score,
        "subscores": {
            "oversharing_control": oversharing_control,
            "assurance": assurance,
        },
        "subscores_breakdown": subscores_breakdown,
        "coverage": {
            "coverage_score": round(coverage_score, 4),
            "inventory_coverage": round(inventory_coverage, 4),
            "content_scan_coverage": round(content_scan_coverage, 4),
            "supported_format_coverage": round(supported_format_coverage, 4),
            "fresh_scan_coverage": round(fresh_scan_coverage, 4),
            "permission_detail_coverage": round(permission_detail_coverage, 4),
        },
        "confidence": {
            "level": confidence_level,
            "scan_confidence": round(avg_scan_confidence, 4),
        },
        "risk_summary": {
            "governance_raw": round(governance_raw, 4),
            "exception_debt": round(avg_exception_debt, 4),
            "coverage_penalty": round(coverage_penalty, 4),
        },
    }


def _resolve_last_batch_run_at(tenant_id: str, findings: list[dict[str, Any]]) -> str | None:
    bucket_name = _report_bucket_name()
    if bucket_name:
        # Extension point for S3 report-based timestamp.
        _ = bucket_name
    timestamps = [str(row.get("last_evaluated_at", "")).strip() for row in findings]
    timestamps = [value for value in timestamps if value]
    return max(timestamps) if timestamps else None


def _protection_scores(findings: list[dict[str, Any]]) -> dict[str, float]:
    if not findings:
        return {
            "oversharing_protection": 1.0,
            "overall": 1.0,
        }
    total = len(findings)
    high_risk = sum(1 for row in findings if _is_high_risk(row))
    oversharing_protection = round(max(0.0, 1.0 - high_risk / total), 3)
    return {
        "oversharing_protection": oversharing_protection,
        "overall": oversharing_protection,
    }


def _clamp_score(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


def _avg(values: list[float], default: float) -> float:
    if not values:
        return round(default, 2)
    return round(sum(values) / len(values), 2)


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _is_active_status(row: dict[str, Any]) -> bool:
    return str(row.get("status", "")).lower() in {"new", "open", "acknowledged"}


def _round1_score(value: float) -> float:
    return round(_clamp_score(value), 1)


def _has_vector(row: dict[str, Any], vector: str) -> bool:
    vectors = row.get("exposure_vectors") or []
    if not isinstance(vectors, list):
        return False
    normalized = {str(item).strip().lower() for item in vectors}
    return vector.strip().lower() in normalized


def _is_remediated_status(row: dict[str, Any]) -> bool:
    """Treat completed, suppressed, or closed findings as addressed (includes legacy remediated)."""
    return str(row.get("status", "")).lower() in {
        "acknowledged",
        "closed",
        "completed",
        "remediated",
    }


def _display_item_name(row: dict[str, Any]) -> str:
    item_name = str(row.get("item_name", "")).strip()
    if item_name:
        return item_name
    item_url = str(row.get("item_url", "")).strip()
    if not item_url:
        return str(row.get("item_id", "")).strip()
    parsed = urlparse(item_url)
    path = unquote(parsed.path or "")
    if path:
        candidate = path.rsplit("/", 1)[-1].strip()
        if candidate:
            return candidate
    query_file = ""
    if "file=" in parsed.query:
        for token in parsed.query.split("&"):
            if token.startswith("file="):
                query_file = unquote(token.split("=", 1)[1]).strip()
                break
    return query_file or str(row.get("item_id", "")).strip()


def _evidence_entry(row: dict[str, Any], *, reason: str, impact_points: float) -> dict[str, Any]:
    return {
        "item_id": str(row.get("item_id", "")).strip(),
        "item_name": _display_item_name(row),
        "item_url": str(row.get("item_url", "")).strip(),
        "status": str(row.get("status", "")).strip(),
        "reason": reason,
        "impact_points": round(float(impact_points), 2),
    }


def _ratio_score(good_count: int, total_count: int) -> float:
    if total_count <= 0:
        return 100.0
    return _round1_score(100.0 * (good_count / total_count))


def _negative_ratio_score(issue_count: int, total_count: int) -> float:
    if total_count <= 0:
        return 100.0
    return _round1_score(100.0 - (100.0 * (issue_count / total_count)))


def _detail_metric_payload(
    *,
    score: float,
    total_count: int,
    issue_count: int,
    evidence: list[dict[str, Any]],
) -> dict[str, Any]:
    deducted = _round1_score(100.0 - score)
    return {
        "score": score,
        "total_count": int(total_count),
        "issue_count": int(issue_count),
        "deducted_points": deducted,
        "evidence": evidence[:20],
    }


def _risk_to_protection(intermediate_risk: float, coefficient: float) -> dict[str, float]:
    normalized_raw = 100.0 - intermediate_risk * coefficient
    normalized_clamped = _clamp_score(normalized_raw)
    return {
        "intermediate_risk": round(intermediate_risk, 2),
        "normalized_raw": round(normalized_raw, 2),
        "normalized_clamped": round(normalized_clamped, 2),
        "score": round(normalized_clamped, 1),
    }


def _baseline_adjusted_risk(factor_score: float, activity_score: float, ai_amplification: float) -> float:
    """
    Protection normalization risk should not be penalized by activity alone.
    Baseline 1.0 means "no factor issue", so only the amount over baseline contributes.
    """
    return max(0.0, float(factor_score) - 1.0) * float(activity_score) * float(ai_amplification)


def _protection_score_breakdown(findings: list[dict[str, Any]]) -> dict[str, Any]:
    active_findings = [row for row in findings if _is_active_status(row)]
    base_rows = active_findings if active_findings else findings

    exposure = _avg([_to_float(row.get("exposure_score"), 1.0) for row in base_rows], default=1.0)
    activity = _avg([_to_float(row.get("activity_score"), 1.0) for row in base_rows], default=1.0)
    ai_amplification = _avg([_to_float(row.get("ai_amplification"), 1.0) for row in base_rows], default=1.0)

    oversharing_intermediate = _baseline_adjusted_risk(exposure, activity, ai_amplification)
    oversharing_metric = _risk_to_protection(oversharing_intermediate, coefficient=2.563)

    total = len(base_rows)
    everyone_issue_rows = [row for row in base_rows if _has_vector(row, "all_users")]
    public_link_issue_rows = [row for row in base_rows if _has_vector(row, "public_link")]
    excessive_rows = [row for row in base_rows if _has_vector(row, "excessive_permissions")]
    excessive_non_remediated_rows = [row for row in excessive_rows if not _is_remediated_status(row)]

    everyone_score = _negative_ratio_score(len(everyone_issue_rows), total)
    public_link_score = _negative_ratio_score(len(public_link_issue_rows), total)
    if excessive_rows:
        excessive_score = _ratio_score(
            len(excessive_rows) - len(excessive_non_remediated_rows), len(excessive_rows)
        )
    else:
        excessive_score = 100.0

    per_total_impact = 100.0 / max(1, total)
    per_excessive_impact = 100.0 / max(1, len(excessive_rows))

    evidence = {
        "everyone_public": [
            _evidence_entry(row, reason="all_users (Everyone) detected", impact_points=per_total_impact)
            for row in everyone_issue_rows
        ],
        "public_link_exposure": [
            _evidence_entry(row, reason="public_link detected", impact_points=per_total_impact)
            for row in public_link_issue_rows
        ],
        "excessive_permission_remediation": [
            _evidence_entry(
                row,
                reason="excessive_permissions not remediated (status not acknowledged/closed)",
                impact_points=per_excessive_impact,
            )
            for row in excessive_non_remediated_rows
        ],
    }

    return {
        "factors": {
            "exposure": exposure,
            "activity": activity,
            "ai_amplification": ai_amplification,
        },
        "oversharing": {
            "metric": oversharing_metric,
            "score": oversharing_metric["score"],
            "details": {
                "everyone_public": everyone_score,
                "public_link_exposure": public_link_score,
                "excessive_permission_remediation": excessive_score,
            },
        },
        "detail_evidence": {
            "everyone_public": _detail_metric_payload(
                score=everyone_score,
                total_count=total,
                issue_count=len(everyone_issue_rows),
                evidence=evidence["everyone_public"],
            ),
            "public_link_exposure": _detail_metric_payload(
                score=public_link_score,
                total_count=total,
                issue_count=len(public_link_issue_rows),
                evidence=evidence["public_link_exposure"],
            ),
            "excessive_permission_remediation": _detail_metric_payload(
                score=excessive_score,
                total_count=len(excessive_rows),
                issue_count=len(excessive_non_remediated_rows),
                evidence=evidence["excessive_permission_remediation"],
            ),
        },
    }


def _connect_manual_sync_available(tenant_id: str) -> bool:
    """Connect の pullFileMetadata 手動同期がこのテナントで利用できるか。"""
    try:
        from services.connect_settings import load_connect_settings, validate_connect_tenant_access

        settings = load_connect_settings()
        if not (settings.pull_file_metadata_lambda_name or "").strip():
            return False
        validate_connect_tenant_access(settings, tenant_id)
        return True
    except Exception:
        return False


def _has_success_scan_job(tenant_id: str) -> bool:
    try:
        rows = _load_scan_jobs(tenant_id)
        return any(str(j.get("status", "")).lower() == "success" for j in rows)
    except Exception:
        return False


def get_governance_overview(tenant_id: str) -> dict[str, Any]:
    findings = _list_all_findings(tenant_id)
    for row in findings:
        if not str(row.get("risk_level", "")).strip():
            score = _safe_float(row.get("risk_score"), 0.0)
            row["risk_level"] = _risk_level_from_score(score)
    active_findings = []
    for row in findings:
        status_value = str(row.get("status", "")).lower()
        workflow_status = str(row.get("workflow_status", "")).lower()
        if status_value in {"new", "open", "acknowledged"} or workflow_status == "acknowledged":
            active_findings.append(row)
    high_risk_count = sum(1 for row in active_findings if _is_high_risk(row))
    action_required_count = sum(1 for row in findings if _is_action_required(row))
    expiring = _expiring_suppressions(findings, within_hours=24)
    acknowledged_count = sum(
        1 for row in findings if str(row.get("status", "")).lower() == "acknowledged"
    )
    v12 = _build_v12_overview(findings)
    force_initial_gate = str(os.getenv("GOVERNANCE_INITIAL_SCAN_GATE_OPEN", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    initial_scan_gate_open = force_initial_gate or (
        _has_success_scan_job(tenant_id)
        or len(findings) > 0
        or not _connect_manual_sync_available(tenant_id)
    )
    return {
        "high_risk_count": high_risk_count,
        "action_required_count": action_required_count,
        "expiring_suppressions_24h": len(expiring),
        "last_batch_run_at": _resolve_last_batch_run_at(tenant_id, findings),
        "initial_scan_gate_open": initial_scan_gate_open,
        "governance_score": v12["governance_score"],
        "subscores": v12["subscores"],
        "subscores_breakdown": v12["subscores_breakdown"],
        "coverage": v12["coverage"],
        "confidence": v12["confidence"],
        "risk_summary": v12["risk_summary"],
        "protection_scores": _protection_scores(findings),
        "protection_score_breakdown": _protection_score_breakdown(findings),
        "counts": {
            "total_findings": len(findings),
            "active_findings": len(active_findings),
            "acknowledged": acknowledged_count,
        },
    }


def list_governance_suppressions(
    tenant_id: str,
    *,
    limit: int = 200,
    offset: int = 0,
    expiring_within_hours: int | None = None,
) -> dict[str, Any]:
    findings = _list_all_findings(tenant_id)
    rows = [
        row
        for row in findings
        if str(row.get("status", "")).lower() == "acknowledged"
        or str(row.get("workflow_status", "")).lower() == "acknowledged"
    ]
    if expiring_within_hours is not None:
        expiring_ids = {
            str(row.get("finding_id", ""))
            for row in _expiring_suppressions(findings, within_hours=expiring_within_hours)
        }
        rows = [row for row in rows if str(row.get("finding_id", "")) in expiring_ids]
    rows.sort(
        key=lambda item: str(
            item.get("exception_review_due_at", "") or item.get("suppress_until", "")
        )
    )
    bounded_limit = max(1, min(int(limit), 500))
    bounded_offset = max(0, int(offset))
    paged = rows[bounded_offset : bounded_offset + bounded_limit]
    return {
        "rows": paged,
        "pagination": {
            "limit": bounded_limit,
            "offset": bounded_offset,
            "total_count": len(rows),
        },
    }


def trigger_governance_daily_scan(
    tenant_id: str,
    *,
    operator: str,
    correlation_id: str,
) -> dict[str, Any]:
    """日次バッチは廃止。Connect の manual-sync-check で Delta→FileMetadata 更新→Streams→analyzeExposure。"""
    corr = (correlation_id or "").strip() or str(uuid.uuid4())
    op = (operator or "").strip() or "governance-ui"
    try:
        from services.connect_service import trigger_connect_sync_check

        sync_result = trigger_connect_sync_check(
            tenant_id,
            requested_by=op,
            correlation_id=corr,
        )
    except ValueError as exc:
        logger.info(
            "Governance scan: Connect realtime sync unavailable: %s",
            exc,
        )
        return {
            "status": "disabled",
            "reason": "connect_realtime_unavailable",
            "message": (
                "Connect manual sync is not available for this tenant or environment. "
                "Configure Connect tenant registry and CONNECT_PULL_FILE_METADATA_LAMBDA_NAME, "
                "or rely on automatic FileMetadata stream processing."
            ),
            "tenant_id": tenant_id,
            "detail": str(exc),
        }

    if str(sync_result.get("status", "")).lower() != "accepted":
        return {
            "status": "failed",
            "reason": "connect_sync_not_accepted",
            "tenant_id": tenant_id,
            "sync": sync_result,
        }

    job_id = str(uuid.uuid4())
    accepted_at = _iso(_now_utc())
    _record_scan_job(
        tenant_id=tenant_id,
        job_id=job_id,
        status="success",
        operator=op,
        correlation_id=corr,
        accepted_at=accepted_at,
        source="connect_realtime_sync",
    )
    return {
        "job_id": job_id,
        "status": "accepted",
        "accepted_at": accepted_at,
        "lambda_name": sync_result.get("lambda_function_name"),
        "tenant_id": tenant_id,
        "mode": "realtime_connect_sync",
        "message": (
            "Connect delta sync started; exposure findings update via DynamoDB Streams (analyzeExposure)."
        ),
    }


def _record_scan_job(
    *,
    tenant_id: str,
    job_id: str,
    status: str,
    operator: str,
    correlation_id: str,
    accepted_at: str,
    source: str | None = None,
) -> None:
    table_name = _scan_job_table_name()
    if not table_name:
        return
    try:
        item: dict[str, Any] = {
            "tenant_id": tenant_id,
            "job_id": job_id,
            "status": status,
            "operator": operator,
            "correlation_id": correlation_id,
            "accepted_at": accepted_at,
        }
        if source:
            item["source"] = source
        _table(table_name).put_item(Item=item)
    except Exception:
        # Keep API available even before ScanJob table provisioning.
        return


def list_governance_policies(tenant_id: str) -> dict[str, Any]:
    global_policies = _load_global_policies()
    all_rows = [] if _scope_policies_disabled() else _load_scope_policies(tenant_id)
    global_policy_rows = [row for row in all_rows if _is_global_policy_row(row)]
    scope_rows = [row for row in all_rows if not _is_global_policy_row(row)]
    normalized_scope: list[dict[str, Any]] = []
    for row in scope_rows:
        normalized_scope.append(
            {
                **row,
                "rollout": row.get("rollout", {"stage": "active", "dry_run": False}),
                "version": int(row.get("version", 1)),
            }
        )
    normalized_global: list[dict[str, Any]] = []
    for row in global_policy_rows:
        normalized_global.append(
            {
                **row,
                "rollout": row.get("rollout", {"stage": "active", "dry_run": False}),
                "version": int(row.get("version", 1)),
            }
        )
    response = {
        "global_policies": [{"name": key, "value": value} for key, value in sorted(global_policies.items())],
        "global_policy_rows": normalized_global,
        "scope_policies": normalized_scope,
        "policy_versions": [
            {
                "policy_id": str(row.get("policy_id", "")),
                "version": int(row.get("version", 1)),
                "updated_at": str(row.get("updated_at", "")),
            }
            for row in [*normalized_global, *normalized_scope]
        ],
        "estimated_impacts": [
            {
                "policy_id": str(row.get("policy_id", "")),
                "estimated_affected_count": int(row.get("estimated_affected_count", 0)),
            }
            for row in [*normalized_global, *normalized_scope]
        ],
        # backward compatibility keys
        "global": global_policies,
        "scope": normalized_scope,
        "scope_mode": {
            "enabled": not _scope_policies_disabled(),
            "reason": "poc_global_only" if _scope_policies_disabled() else "enabled",
            "message": (
                "Scope policies are disabled in PoC mode. Global policies are used as the single baseline."
                if _scope_policies_disabled()
                else "Scope policies are enabled."
            ),
        },
    }
    return response


def _load_global_policies() -> dict[str, str]:
    values: dict[str, str] = {}
    next_token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Path": "/aiready/governance", "Recursive": True}
        if next_token:
            kwargs["NextToken"] = next_token
        response = _ssm().get_parameters_by_path(**kwargs)
        for param in response.get("Parameters", []):
            name = str(param.get("Name", "")).strip()
            if not name:
                continue
            values[name] = str(param.get("Value", ""))
        next_token = response.get("NextToken")
        if not next_token:
            break
    return values


def _load_scope_policies(tenant_id: str) -> list[dict[str, Any]]:
    table_name = _policy_scope_table_name()
    rows: list[dict[str, Any]] = []
    table = _table(table_name)
    try:
        response = table.query(
            KeyConditionExpression=Key("tenant_id").eq(tenant_id),
        )
        rows.extend(response.get("Items", []))
        while response.get("LastEvaluatedKey"):
            response = table.query(
                KeyConditionExpression=Key("tenant_id").eq(tenant_id),
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            rows.extend(response.get("Items", []))
    except Exception as query_exc:
        try:
            response = table.scan(FilterExpression=Attr("tenant_id").eq(tenant_id))
            rows.extend(response.get("Items", []))
        except Exception as scan_exc:
            if _is_table_missing_error(query_exc) or _is_table_missing_error(scan_exc):
                if _runtime_config.governance_allow_missing_policy_scope_fallback:
                    # T-057: allow controlled compatibility mode during phased migration.
                    logger.warning(
                        "PolicyScope table is not available yet; return empty scope policies. "
                        "table_name=%s tenant_id=%s",
                        table_name,
                        tenant_id,
                    )
                    return []
                raise RuntimeError(
                    "PolicyScope table is not available and fallback is disabled. "
                    "Enable GOVERNANCE_ALLOW_MISSING_POLICY_SCOPE_FALLBACK=true "
                    "or provision the table before enabling strict mode."
                ) from scan_exc
            raise
    return [_to_plain(row) for row in rows]


def _is_table_missing_error(exc: Exception) -> bool:
    if not isinstance(exc, ClientError):
        return False
    error_code = str(exc.response.get("Error", {}).get("Code", "")).strip()
    return error_code == "ResourceNotFoundException"


def _is_global_policy_row(row: dict[str, Any]) -> bool:
    layer = str(row.get("layer", "")).strip().lower()
    scope_type = str(row.get("scope_type", "")).strip().lower()
    scope = row.get("scope") if isinstance(row.get("scope"), dict) else {}
    nested_scope_type = str(scope.get("scope_type", "")).strip().lower()
    return layer in {"organization", "global"} or scope_type == "organization" or nested_scope_type == "organization"


def _build_global_policy_item(
    *,
    tenant_id: str,
    policy_id: str,
    payload: dict[str, Any],
    operator: str,
    correlation_id: str,
    now: str,
    version: int,
    existing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base = existing or {}
    rollout = payload.get("rollout") if isinstance(payload.get("rollout"), dict) else base.get("rollout", {})
    if not isinstance(rollout, dict):
        rollout = {}
    return {
        **base,
        "tenant_id": tenant_id,
        "policy_id": policy_id,
        "layer": "organization",
        "name": str(payload.get("name", base.get("name", policy_id))).strip() or policy_id,
        "description": str(payload.get("description", base.get("description", ""))).strip(),
        "scope_type": "organization",
        "scope_value": str(payload.get("scope_value", base.get("scope_value", "organization"))).strip() or "organization",
        "status": str(payload.get("status", base.get("status", "active"))).strip().lower() or "active",
        "priority": int(payload.get("priority", base.get("priority", 900))),
        "version": int(version),
        "estimated_affected_count": int(payload.get("estimated_affected_count", base.get("estimated_affected_count", 0))),
        "conditions": payload.get("conditions", base.get("conditions", {})),
        "actions": payload.get("actions", base.get("actions", {})),
        "rules": payload.get("rules", base.get("rules", [])),
        "scope": payload.get("scope", base.get("scope", {"scope_type": "organization"})),
        "rollout": {
            "stage": str(rollout.get("stage", "active")).strip().lower(),
            "dry_run": bool(rollout.get("dry_run", False)),
        },
        "operator": operator,
        "correlation_id": correlation_id,
        "updated_at": now,
    }


def create_governance_policy(
    tenant_id: str,
    payload: dict[str, Any],
    *,
    operator: str,
    correlation_id: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    policy_type = str(payload.get("policy_type", "scope")).strip().lower()
    now = _iso(_now_utc())
    if policy_type == "global":
        if isinstance(payload.get("rules"), list):
            policy_id = str(payload.get("policy_id") or f"global-policy-{uuid.uuid4().hex[:10]}")
            if dry_run:
                return simulate_governance_policy(tenant_id=tenant_id, payload=payload)
            item = _build_global_policy_item(
                tenant_id=tenant_id,
                policy_id=policy_id,
                payload=payload,
                operator=operator,
                correlation_id=correlation_id,
                now=now,
                version=int(payload.get("version", 1)),
            )
            _table(_policy_scope_table_name()).put_item(Item=item)
            return {
                "policy_type": "global",
                "policy_id": policy_id,
                "version": int(item["version"]),
                "updated_at": now,
                "item": item,
            }
        parameter_name = str(payload.get("parameter_name", "")).strip()
        parameter_value = str(payload.get("parameter_value", "")).strip()
        if not parameter_name:
            raise ValueError("parameter_name is required when policy_type=global.")
        _ssm().put_parameter(
            Name=parameter_name,
            Value=parameter_value,
            Type="String",
            Overwrite=True,
        )
        return {
            "policy_type": "global",
            "parameter_name": parameter_name,
            "updated_at": now,
        }
    if _scope_policies_disabled():
        raise ValueError(
            "scope policies are disabled in PoC mode. Use global policy settings instead."
        )

    policy_id = str(payload.get("policy_id") or f"policy-{uuid.uuid4().hex[:10]}")
    rollout = payload.get("rollout") if isinstance(payload.get("rollout"), dict) else {}
    rollout_stage = str(rollout.get("stage") or ("dry_run" if dry_run else "active")).strip().lower()
    rollout_dry_run = bool(rollout.get("dry_run", dry_run))
    rules = payload.get("rules") if isinstance(payload.get("rules"), list) else []
    scope = payload.get("scope") if isinstance(payload.get("scope"), dict) else {}
    if dry_run or rollout_dry_run:
        return simulate_governance_policy(tenant_id=tenant_id, payload=payload)
    item = {
        "tenant_id": tenant_id,
        "policy_id": policy_id,
        "layer": str(payload.get("layer") or payload.get("scope_type") or "scope").strip().lower(),
        "name": str(payload.get("name", "")).strip() or "scope-policy",
        "scope_type": str(payload.get("scope_type", "folder")).strip(),
        "scope_value": str(payload.get("scope_value", "")).strip(),
        "status": str(payload.get("status", "active")).strip(),
        "priority": int(payload.get("priority", 100)),
        "version": int(payload.get("version", 1)),
        "estimated_affected_count": int(payload.get("estimated_affected_count", 0)),
        "conditions": payload.get("conditions", {}),
        "actions": payload.get("actions", {}),
        "rules": rules,
        "scope": scope,
        "rollout": {
            "stage": rollout_stage,
            "dry_run": rollout_dry_run,
        },
        "operator": operator,
        "correlation_id": correlation_id,
        "updated_at": now,
    }
    _table(_policy_scope_table_name()).put_item(Item=item)
    return {
        "policy_type": "scope",
        "policy_id": policy_id,
        "version": int(item["version"]),
        "estimated_affected_count": int(item.get("estimated_affected_count", 0)),
        "impact_by_risk_level": {
            "critical": 0,
            "high": int(item.get("estimated_affected_count", 0)),
            "medium": 0,
            "low": 0,
        },
        "item": item,
    }


def update_governance_policy(
    tenant_id: str,
    policy_id: str,
    payload: dict[str, Any],
    *,
    operator: str,
    correlation_id: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    normalized_policy_id = str(policy_id or "").strip()
    if not normalized_policy_id:
        raise ValueError("policy_id is required.")
    now = _iso(_now_utc())
    policy_type = str(payload.get("policy_type", "")).strip().lower()
    if policy_type == "global" or normalized_policy_id.startswith("/aiready/governance/"):
        has_global_rules = isinstance(payload.get("rules"), list)
        if has_global_rules:
            table = _table(_policy_scope_table_name())
            existing = table.get_item(Key={"tenant_id": tenant_id, "policy_id": normalized_policy_id}).get("Item")
            base_item = existing if isinstance(existing, dict) else None
            next_version = int(payload.get("version", int((base_item or {}).get("version", 0)) + 1))
            merged_payload = {**(base_item or {}), **payload}
            if dry_run:
                return simulate_governance_policy(tenant_id=tenant_id, payload=merged_payload)
            updated_item = _build_global_policy_item(
                tenant_id=tenant_id,
                policy_id=normalized_policy_id,
                payload=merged_payload,
                operator=operator,
                correlation_id=correlation_id,
                now=now,
                version=next_version,
                existing=base_item,
            )
            table.put_item(Item=updated_item)
            return {
                "policy_type": "global",
                "policy_id": normalized_policy_id,
                "version": int(updated_item["version"]),
                "updated_at": now,
                "updated_by": operator,
                "item": updated_item,
            }
        if dry_run:
            return simulate_governance_policy(tenant_id=tenant_id, payload=payload)
        parameter_name = str(payload.get("parameter_name") or normalized_policy_id).strip()
        parameter_value = str(payload.get("parameter_value", "")).strip()
        if not parameter_name:
            raise ValueError("parameter_name is required when policy_type=global.")
        _ssm().put_parameter(
            Name=parameter_name,
            Value=parameter_value,
            Type="String",
            Overwrite=True,
        )
        return {
            "policy_type": "global",
            "policy_id": parameter_name,
            "parameter_name": parameter_name,
            "updated_at": now,
            "updated_by": operator,
        }
    if _scope_policies_disabled():
        raise ValueError(
            "scope policies are disabled in PoC mode. Only global policies can be updated."
        )
    table = _table(_policy_scope_table_name())
    existing = table.get_item(Key={"tenant_id": tenant_id, "policy_id": normalized_policy_id}).get("Item")

    if not existing:
        raise ValueError(f"policy_id '{normalized_policy_id}' was not found for tenant '{tenant_id}'.")
    if dry_run:
        merged_payload = {**existing, **payload}
        return simulate_governance_policy(tenant_id=tenant_id, payload=merged_payload)

    current_version = int(existing.get("version", 1))
    next_version = int(payload.get("version", current_version + 1))
    rollout = payload.get("rollout") if isinstance(payload.get("rollout"), dict) else existing.get("rollout", {})
    if not isinstance(rollout, dict):
        rollout = {}

    updated_item: dict[str, Any] = {
        **existing,
        "layer": str(payload.get("layer", existing.get("layer", existing.get("scope_type", "scope")))).strip().lower(),
        "name": str(payload.get("name", existing.get("name", "scope-policy"))).strip() or "scope-policy",
        "scope_type": str(payload.get("scope_type", existing.get("scope_type", "folder"))).strip(),
        "scope_value": str(payload.get("scope_value", existing.get("scope_value", ""))).strip(),
        "status": str(payload.get("status", existing.get("status", "active"))).strip(),
        "priority": int(payload.get("priority", existing.get("priority", 100))),
        "version": next_version,
        "estimated_affected_count": int(
            payload.get("estimated_affected_count", existing.get("estimated_affected_count", 0))
        ),
        "conditions": payload.get("conditions", existing.get("conditions", {})),
        "actions": payload.get("actions", existing.get("actions", {})),
        "rules": payload.get("rules", existing.get("rules", [])),
        "scope": payload.get("scope", existing.get("scope", {})),
        "rollout": {
            "stage": str(rollout.get("stage", "active")).strip().lower(),
            "dry_run": bool(rollout.get("dry_run", False)),
        },
        "operator": operator,
        "correlation_id": correlation_id,
        "updated_at": now,
    }
    table.put_item(Item=updated_item)
    return {
        "policy_type": "scope",
        "policy_id": normalized_policy_id,
        "version": int(updated_item["version"]),
        "updated_at": now,
        "item": updated_item,
    }


def simulate_governance_policy(tenant_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    del payload
    findings = _list_all_findings(tenant_id)
    sample = findings[: min(len(findings), 20)]
    high_or_critical = [
        row for row in findings if str(row.get("risk_level", "")).strip().lower() in {"high", "critical"}
    ]
    impact_by_category: dict[str, int] = {}
    for row in findings:
        content_signals = row.get("content_signals")
        categories = []
        if isinstance(content_signals, dict) and isinstance(content_signals.get("doc_categories"), list):
            categories = [str(v).strip().lower() for v in content_signals.get("doc_categories", []) if str(v).strip()]
        if not categories:
            categories = ["uncategorized"]
        for category in categories:
            impact_by_category[category] = impact_by_category.get(category, 0) + 1
    return {
        "estimated_affected_items": len(findings),
        "estimated_new_findings": len(high_or_critical),
        "estimated_resolved_findings": max(0, len(findings) - len(high_or_critical)),
        "impact_by_category": dict(sorted(impact_by_category.items(), key=lambda entry: entry[0])),
        "sample_targets": [
            {
                "finding_id": str(row.get("finding_id", "")),
                "item_id": str(row.get("item_id", "")),
                "risk_level": str(row.get("risk_level", "")),
            }
            for row in sample
        ],
    }


def list_governance_scan_jobs(
    tenant_id: str,
    *,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    rows = _load_scan_jobs(tenant_id)
    rows.sort(key=lambda item: str(item.get("accepted_at", "")), reverse=True)
    bounded_limit = max(1, min(int(limit), 500))
    bounded_offset = max(0, int(offset))
    return {
        "rows": rows[bounded_offset : bounded_offset + bounded_limit],
        "pagination": {
            "limit": bounded_limit,
            "offset": bounded_offset,
            "total_count": len(rows),
        },
    }


def _load_scan_jobs(tenant_id: str) -> list[dict[str, Any]]:
    table_name = _scan_job_table_name()
    table = _table(table_name)
    try:
        items: list[dict[str, Any]] = []
        response = table.query(KeyConditionExpression=Key("tenant_id").eq(tenant_id))
        items.extend(response.get("Items", []))
        while response.get("LastEvaluatedKey"):
            response = table.query(
                KeyConditionExpression=Key("tenant_id").eq(tenant_id),
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            items.extend(response.get("Items", []))
        return [_to_plain(item) for item in items]
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"ResourceNotFoundException"}:
            return []
        if _runtime_config.governance_allow_cloudwatch_fallback:
            return _load_jobs_from_logs(tenant_id)
        raise
    except Exception:
        if _runtime_config.governance_allow_cloudwatch_fallback:
            return _load_jobs_from_logs(tenant_id)
        raise


def _load_jobs_from_logs(tenant_id: str) -> list[dict[str, Any]]:
    group_name = f"/aws/lambda/{_analyze_lambda_name()}"
    pattern = f'"tenant_id":"{tenant_id}"'
    try:
        response = _logs().filter_log_events(
            logGroupName=group_name,
            filterPattern=pattern,
            limit=100,
        )
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    for event in response.get("events", []):
        rows.append(
            {
                "tenant_id": tenant_id,
                "job_id": str(event.get("eventId", "")),
                "status": "unknown",
                "accepted_at": datetime.fromtimestamp(
                    int(event.get("timestamp", 0)) / 1000,
                    tz=timezone.utc,
                ).isoformat(),
                "source": "cloudwatch_logs",
            }
        )
    return rows


def list_governance_audit_logs(
    tenant_id: str,
    *,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    rows = _load_audit_rows(tenant_id)
    rows.sort(key=lambda item: str(item.get("timestamp", item.get("accepted_at", ""))), reverse=True)
    bounded_limit = max(1, min(int(limit), 500))
    bounded_offset = max(0, int(offset))
    return {
        "rows": rows[bounded_offset : bounded_offset + bounded_limit],
        "pagination": {
            "limit": bounded_limit,
            "offset": bounded_offset,
            "total_count": len(rows),
        },
    }


def _invoke_remediation_lambda(payload: dict[str, Any]) -> dict[str, Any]:
    requested_action = str(payload.get("action", "")).strip().lower()
    try:
        response = _lambda().invoke(
            FunctionName=_remediation_lambda_name(),
            InvocationType="RequestResponse",
            Payload=json.dumps(payload, ensure_ascii=True).encode("utf-8"),
        )
    except ClientError as exc:
        error_code = str(exc.response.get("Error", {}).get("Code", "")).strip()
        if error_code == "ResourceNotFoundException" and _allow_local_remediation_fallback():
            logger.warning(
                "remediation lambda not found. using local fallback handler.",
                extra={"lambda_name": _remediation_lambda_name()},
            )
            return _invoke_remediation_local(payload)
        raise GovernanceRemediationProxyError(
            500,
            f"Error invoking governance finding remediation: {exc}",
        ) from exc
    raw_status_code = int(response.get("StatusCode", 500))
    if raw_status_code < 200 or raw_status_code >= 300:
        raise GovernanceRemediationProxyError(
            raw_status_code,
            f"remediation lambda invoke failed status={raw_status_code}",
        )

    payload_stream = response.get("Payload")
    raw_payload = payload_stream.read().decode("utf-8") if payload_stream else "{}"
    try:
        lambda_payload = json.loads(raw_payload or "{}")
    except json.JSONDecodeError as exc:
        raise GovernanceRemediationProxyError(
            500, f"invalid remediation lambda payload: {raw_payload[:200]}"
        ) from exc

    handler_status = int(lambda_payload.get("statusCode", 500))
    body = lambda_payload.get("body", {})
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            body = {"message": body}
    if not isinstance(body, dict):
        body = {"message": str(body)}

    if handler_status >= 400:
        message = str(body.get("error") or body.get("message") or "remediation failed")
        # Backward compatibility:
        # when deployed remediation Lambda does not support newer actions yet,
        # fallback to local governance handler (if allowed) for development continuity.
        if requested_action in {"register_exception", "exception", "rollback", "mark_complete", "complete"} and "unsupported action" in message.lower():
            logger.warning(
                "remediation lambda returned unsupported action; trying local fallback handler.",
                extra={"action": requested_action, "lambda_name": _remediation_lambda_name()},
            )
            try:
                return _invoke_remediation_local(payload)
            except Exception:
                # If local fallback is unavailable in this runtime, preserve original handler error.
                pass
        raise GovernanceRemediationProxyError(handler_status, message)
    return body


def get_governance_finding_remediation(tenant_id: str, finding_id: str) -> dict[str, Any]:
    return _invoke_remediation_lambda(
        {
            "tenant_id": tenant_id,
            "finding_id": finding_id,
            "action": "get",
        }
    )


def propose_governance_remediation(
    tenant_id: str,
    finding_id: str,
    *,
    operator: str,
    force: bool = False,
) -> dict[str, Any]:
    return _invoke_remediation_lambda(
        {
            "tenant_id": tenant_id,
            "finding_id": finding_id,
            "action": "propose",
            "operator": operator,
            "force": bool(force),
        }
    )


def approve_governance_remediation(
    tenant_id: str,
    finding_id: str,
    *,
    operator: str,
) -> dict[str, Any]:
    return _invoke_remediation_lambda(
        {
            "tenant_id": tenant_id,
            "finding_id": finding_id,
            "action": "approve",
            "operator": operator,
        }
    )


def execute_governance_remediation(
    tenant_id: str,
    finding_id: str,
    *,
    operator: str,
) -> dict[str, Any]:
    return _invoke_remediation_lambda(
        {
            "tenant_id": tenant_id,
            "finding_id": finding_id,
            "action": "execute",
            "operator": operator,
        }
    )


def rollback_governance_remediation(
    tenant_id: str,
    finding_id: str,
    *,
    operator: str,
) -> dict[str, Any]:
    return _invoke_remediation_lambda(
        {
            "tenant_id": tenant_id,
            "finding_id": finding_id,
            "action": "rollback",
            "operator": operator,
        }
    )


def mark_governance_finding_completed(
    tenant_id: str,
    finding_id: str,
    *,
    operator: str,
) -> dict[str, Any]:
    return _invoke_remediation_lambda(
        {
            "tenant_id": tenant_id,
            "finding_id": finding_id,
            "action": "mark_complete",
            "operator": operator,
        }
    )


def register_governance_finding_exception(
    tenant_id: str,
    finding_id: str,
    *,
    operator: str,
    exception_type: str,
    duration_days: int | None = None,
    exception_review_due_at: str | None = None,
    reason: str | None = None,
    exception_ticket: str | None = None,
    scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "tenant_id": tenant_id,
        "finding_id": finding_id,
        "action": "register_exception",
        "operator": operator,
        "exception_type": exception_type,
    }
    if duration_days is not None:
        payload["duration_days"] = int(duration_days)
    if exception_review_due_at:
        payload["exception_review_due_at"] = str(exception_review_due_at).strip()
    if reason:
        payload["reason"] = str(reason).strip()
    if exception_ticket:
        payload["exception_ticket"] = str(exception_ticket).strip()
    if isinstance(scope, dict):
        payload["scope"] = scope
    # Prefer local handler for register_exception to avoid dependency on
    # deployed remediation Lambda action compatibility during phased rollout.
    try:
        return _invoke_remediation_local(payload)
    except Exception:
        return _invoke_remediation_lambda(payload)


def _load_audit_rows(tenant_id: str) -> list[dict[str, Any]]:
    table_name = _audit_table_name()
    table = _table(table_name)
    try:
        items: list[dict[str, Any]] = []
        response = table.query(KeyConditionExpression=Key("tenant_id").eq(tenant_id))
        items.extend(response.get("Items", []))
        while response.get("LastEvaluatedKey"):
            response = table.query(
                KeyConditionExpression=Key("tenant_id").eq(tenant_id),
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            items.extend(response.get("Items", []))
        return [_to_plain(item) for item in items]
    except Exception:
        if _runtime_config.governance_allow_cloudwatch_fallback:
            return _load_audit_from_logs(tenant_id)
        raise


def _load_audit_from_logs(tenant_id: str) -> list[dict[str, Any]]:
    groups = [
        f"/aws/lambda/{_analyze_lambda_name()}",
        "/aws/lambda/AIReadyGov-detectSensitivity",
    ]
    rows: list[dict[str, Any]] = []
    for group_name in groups:
        try:
            response = _logs().filter_log_events(
                logGroupName=group_name,
                filterPattern=f'"tenant_id":"{tenant_id}"',
                limit=40,
            )
        except Exception:
            continue
        for event in response.get("events", []):
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "event": "governance.audit.log",
                    "message": str(event.get("message", "")),
                    "timestamp": datetime.fromtimestamp(
                        int(event.get("timestamp", 0)) / 1000,
                        tz=timezone.utc,
                    ).isoformat(),
                    "source": "cloudwatch_logs",
                }
            )
    return rows
