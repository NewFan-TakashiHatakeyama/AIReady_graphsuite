"""Finding-level remediation workflow implemented in Governance domain."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from services.remediation_catalog import RemediationAction, build_m365_action_plan
from services.remediation_graph_client import RemediationGraphClient, RemediationGraphError
from shared.config import get_env, get_ssm_parameter
from shared.dynamodb import get_table
from shared.logger import get_logger

logger = get_logger(__name__)
TOKYO_TZ = ZoneInfo("Asia/Tokyo")
_EXCEPTION_TYPES = {
    "temporary_accept",
    "permanent_accept",
    "compensating_control",
    "false_positive",
    "business_required",
}

_finding_table = None
_connect_table = None


class RemediationConflictError(ValueError):
    """Raised when requested remediation transition is not allowed."""


def _now_iso() -> str:
    return datetime.now(TOKYO_TZ).isoformat()


def _get_finding_table():
    global _finding_table
    if _finding_table is None:
        table_name = get_env("FINDING_TABLE_NAME")
        _finding_table = get_table(table_name)
    return _finding_table


def _get_connect_table():
    global _connect_table
    if _connect_table is None:
        table_name = get_env("CONNECT_TABLE_NAME", "AIReadyConnect-FileMetadata")
        _connect_table = get_table(table_name)
    return _connect_table


def set_finding_table(table) -> None:
    """テスト用: Finding テーブル参照を差し替える。"""
    global _finding_table
    _finding_table = table


def set_connect_table(table) -> None:
    """テスト用: Connect テーブル参照を差し替える。"""
    global _connect_table
    _connect_table = table


def _read_secure_parameter(name: str) -> str:
    if not name:
        return ""
    client = boto3.client("ssm")
    try:
        response = client.get_parameter(Name=name, WithDecryption=True)
    except ClientError:
        return ""
    return str((response.get("Parameter") or {}).get("Value") or "").strip()


def _to_plain(value: Any) -> Any:
    if hasattr(value, "to_eng_string"):
        text = value.to_eng_string()
        return int(text) if "." not in text else float(text)
    if isinstance(value, list):
        return [_to_plain(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    return value


def _get_finding(tenant_id: str, finding_id: str) -> dict[str, Any]:
    item = _get_finding_table().get_item(
        Key={"tenant_id": tenant_id, "finding_id": finding_id}
    ).get("Item")
    if not item:
        raise ValueError(f"Finding not found: {finding_id}")
    return _to_plain(item)


def _get_file_metadata(tenant_id: str, item_id: str) -> dict[str, Any] | None:
    if not item_id:
        return None
    table = _get_connect_table()

    # Connect FileMetadata PK is drive_id + item_id.
    # We resolve the target row by tenant_id via GSI first.
    try:
        query_kwargs: dict[str, Any] = {
            "IndexName": "GSI-ModifiedAt",
            "KeyConditionExpression": Key("tenant_id").eq(tenant_id),
            "FilterExpression": Attr("item_id").eq(item_id),
            "ScanIndexForward": False,
        }
        while True:
            response = table.query(**query_kwargs)
            rows = response.get("Items", [])
            if rows:
                return _to_plain(rows[0])
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            query_kwargs["ExclusiveStartKey"] = last_key
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", "")).strip()
        if code not in {"ValidationException", "ResourceNotFoundException"}:
            raise

    # Fallback for environments where the GSI is unavailable.
    try:
        scan_kwargs: dict[str, Any] = {
            "FilterExpression": Attr("tenant_id").eq(tenant_id) & Attr("item_id").eq(item_id),
        }
        while True:
            scan_response = table.scan(**scan_kwargs)
            scan_rows = scan_response.get("Items", [])
            if scan_rows:
                return _to_plain(scan_rows[0])
            last_key = scan_response.get("LastEvaluatedKey")
            if not last_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_key
    except ClientError:
        return None
    return None


def _parse_permissions(raw_permissions: Any) -> list[dict[str, Any]]:
    if isinstance(raw_permissions, list):
        return raw_permissions
    if not isinstance(raw_permissions, str):
        return []
    try:
        parsed = json.loads(raw_permissions)
    except Exception:
        return []
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        entries = parsed.get("entries")
        return entries if isinstance(entries, list) else []
    return []


def _parse_label_map(raw: str) -> dict[str, str]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    mapping: dict[str, str] = {}
    for key, value in parsed.items():
        normalized_key = str(key or "").strip()
        normalized_value = str(value or "").strip()
        if normalized_key and normalized_value:
            mapping[normalized_key] = normalized_value
            mapping[normalized_key.lower()] = normalized_value
    return mapping


def _resolve_sensitivity_label_id(tenant_id: str, action: dict[str, Any]) -> str:
    payload = action.get("payload") if isinstance(action.get("payload"), dict) else {}
    raw_label_id = str(
        (payload or {}).get("sensitivity_label_id")
        or (payload or {}).get("label_id")
        or ""
    ).strip()
    if raw_label_id:
        return raw_label_id

    recommended_label = str((payload or {}).get("recommended_label") or "").strip()
    if recommended_label:
        tenant_label_map = _parse_label_map(
            get_ssm_parameter(f"/aiready/connect/{tenant_id}/sensitivity_label_map", "")
        )
        if recommended_label in tenant_label_map:
            return tenant_label_map[recommended_label]
        if recommended_label.lower() in tenant_label_map:
            return tenant_label_map[recommended_label.lower()]

        global_label_map = _parse_label_map(
            get_ssm_parameter("/aiready/connect/sensitivity_label_map", "")
        )
        if recommended_label in global_label_map:
            return global_label_map[recommended_label]
        if recommended_label.lower() in global_label_map:
            return global_label_map[recommended_label.lower()]

        env_label_map = _parse_label_map(
            get_env("GOVERNANCE_SENSITIVITY_LABEL_MAP_JSON", "")
        )
        if recommended_label in env_label_map:
            return env_label_map[recommended_label]
        if recommended_label.lower() in env_label_map:
            return env_label_map[recommended_label.lower()]

    return str(
        get_ssm_parameter(f"/aiready/connect/{tenant_id}/default_sensitivity_label_id", "")
        or get_ssm_parameter("/aiready/connect/default_sensitivity_label_id", "")
        or get_env("MS_GRAPH_DEFAULT_SENSITIVITY_LABEL_ID", "")
    ).strip()


def _label_automation_mode() -> str:
    raw = str(get_env("GOVERNANCE_LABEL_AUTOMATION_MODE", "batch") or "").strip().lower()
    if raw in {"disabled", "manual"}:
        return "disabled"
    if raw in {"realtime", "batch"}:
        return raw
    return "batch"


def _label_batch_hour_jst() -> int:
    raw = str(get_env("GOVERNANCE_LABEL_AUTOMATION_BATCH_HOUR_JST", "3") or "").strip()
    try:
        hour = int(raw)
    except ValueError:
        return 3
    return max(0, min(23, hour))


def _label_daily_limit() -> int:
    raw = str(get_env("GOVERNANCE_LABEL_AUTOMATION_DAILY_LIMIT", "20") or "").strip()
    try:
        limit = int(raw)
    except ValueError:
        return 20
    return max(0, limit)


def _is_label_manual_required_http_status(status_code: int | None) -> bool:
    return int(status_code or 0) in {400, 402, 403, 405, 415, 422}


def _consume_label_daily_quota(tenant_id: str) -> tuple[bool, str]:
    """Return (allowed, reason_if_denied)."""
    mode = _label_automation_mode()
    if mode == "disabled":
        return False, "label_automation_disabled"
    if mode == "realtime":
        # Realtime mode is intended for normal remediation execution path.
        # Skip batch-window and quota storage checks in this mode.
        return True, ""
    if mode == "batch":
        now = datetime.now(TOKYO_TZ)
        if now.hour != _label_batch_hour_jst():
            return False, "label_automation_outside_batch_window"

    daily_limit = _label_daily_limit()
    if daily_limit <= 0:
        return False, "label_automation_daily_limit_exceeded"

    # Keep low-frequency control tenant/day scoped in SSM.
    today = datetime.now(TOKYO_TZ).strftime("%Y-%m-%d")
    quota_param = f"/aiready/governance/{tenant_id}/label_automation_usage/{today}"
    current_raw = str(get_ssm_parameter(quota_param, "0") or "0").strip()
    try:
        current = int(current_raw)
    except ValueError:
        current = 0
    if current >= daily_limit:
        return False, "label_automation_daily_limit_exceeded"
    try:
        boto3.client("ssm").put_parameter(
            Name=quota_param,
            Value=str(current + 1),
            Type="String",
            Overwrite=True,
        )
    except Exception:
        logger.warning("label automation quota update failed; fallback to manual")
        return False, "label_automation_quota_check_failed"
    return True, ""


def _is_manual_like_mode(remediation_mode: str | None) -> bool:
    normalized_mode = str(remediation_mode or "").strip().lower()
    return normalized_mode in {"owner_review", "manual", "recommend_only"}


def _state_allowed_actions(state: str, remediation_mode: str | None = None) -> list[str]:
    normalized = str(state or "ai_proposed").strip().lower()
    if _is_manual_like_mode(remediation_mode):
        if normalized == "executed":
            return ["propose", "rollback"]
        if normalized in {"failed", "manual_required"}:
            return ["propose", "rollback"]
        return ["propose"]
    if normalized == "approved":
        return ["propose", "execute"]
    if normalized == "executed":
        return ["propose", "rollback"]
    if normalized == "manual_required":
        return ["propose", "rollback"]
    if normalized == "failed":
        return ["propose", "approve", "rollback"]
    return ["propose", "approve"]


def _normalize_exception_type(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in _EXCEPTION_TYPES:
        raise ValueError(
            "exception_type must be one of temporary_accept/permanent_accept/compensating_control/false_positive/business_required."
        )
    return normalized


def _resolve_exception_review_due_at(
    *,
    review_due_at: str | None,
    duration_days: int | None,
) -> str:
    if review_due_at:
        raw = str(review_due_at).strip()
        try:
            datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("exception_review_due_at must be ISO8601 datetime.") from exc
        return raw
    if duration_days is None:
        raise ValueError("Either exception_review_due_at or duration_days is required.")
    if int(duration_days) <= 0:
        raise ValueError("duration_days must be positive.")
    return (datetime.now(TOKYO_TZ) + timedelta(days=int(duration_days))).replace(
        microsecond=0
    ).isoformat()


def _build_exception_scope_hash(
    *,
    tenant_id: str,
    finding_id: str,
    finding: dict[str, Any],
    scope: dict[str, Any] | None,
) -> str:
    base_scope = {
        "tenant_id": tenant_id,
        "finding_id": finding_id,
        "source": str(finding.get("source") or ""),
        "item_id": str(finding.get("item_id") or ""),
        "container_id": str(finding.get("container_id") or ""),
        "matched_guards": finding.get("matched_guards") if isinstance(finding.get("matched_guards"), list) else [],
    }
    if isinstance(scope, dict):
        base_scope["scope"] = scope
    digest = hashlib.sha256(
        json.dumps(base_scope, ensure_ascii=True, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return digest[:32]


def _update_finding_remediation(
    tenant_id: str,
    finding_id: str,
    *,
    remediation_state: str,
    remediation_actions: list[dict[str, Any]] | None = None,
    remediation_version: int | None = None,
    remediation_result: dict[str, Any] | None = None,
    remediation_last_error: str | None = None,
    remediation_last_execution_id: str | None = None,
    remediation_idempotency_key: str | None = None,
    remediation_approved_by: str | None = None,
    remediation_approved_at: str | None = None,
    remediation_executed_at: str | None = None,
) -> None:
    expr = ["remediation_state = :state", "remediation_updated_at = :updated_at"]
    values: dict[str, Any] = {
        ":state": remediation_state,
        ":updated_at": _now_iso(),
    }
    if remediation_actions is not None:
        expr.append("remediation_actions = :actions")
        values[":actions"] = remediation_actions
    if remediation_version is not None:
        expr.append("remediation_version = :version")
        values[":version"] = int(remediation_version)
    if remediation_result is not None:
        expr.append("remediation_result = :result")
        values[":result"] = remediation_result
    if remediation_last_error is not None:
        expr.append("remediation_last_error = :error")
        values[":error"] = remediation_last_error
    if remediation_last_execution_id is not None:
        expr.append("remediation_last_execution_id = :execution_id")
        values[":execution_id"] = remediation_last_execution_id
    if remediation_idempotency_key is not None:
        expr.append("remediation_idempotency_key = :idem")
        values[":idem"] = remediation_idempotency_key
    if remediation_approved_by is not None:
        expr.append("remediation_approved_by = :approved_by")
        values[":approved_by"] = remediation_approved_by
    if remediation_approved_at is not None:
        expr.append("remediation_approved_at = :approved_at")
        values[":approved_at"] = remediation_approved_at
    if remediation_executed_at is not None:
        expr.append("remediation_executed_at = :executed_at")
        values[":executed_at"] = remediation_executed_at

    _get_finding_table().update_item(
        Key={"tenant_id": tenant_id, "finding_id": finding_id},
        UpdateExpression="SET " + ", ".join(expr),
        ExpressionAttributeValues=values,
        ConditionExpression="attribute_exists(finding_id)",
    )


def _mark_finding_completed(tenant_id: str, finding_id: str) -> None:
    """Reflect successful remediation: lifecycle status を完了扱いにする。"""
    now = _now_iso()
    _get_finding_table().update_item(
        Key={"tenant_id": tenant_id, "finding_id": finding_id},
        UpdateExpression="""
            SET #status = :status,
                workflow_status = :workflow_status,
                exception_type = :exception_type,
                exception_review_due_at = :exception_review_due_at,
                suppress_until = :suppress_until,
                acknowledged_reason = :ack_reason,
                acknowledged_by = :ack_by,
                acknowledged_at = :ack_at,
                last_evaluated_at = :evaluated_at,
                remediated_at = :remediated_at
        """,
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": "completed",
            ":workflow_status": "normal",
            ":exception_type": "none",
            ":exception_review_due_at": None,
            ":suppress_until": None,
            ":ack_reason": None,
            ":ack_by": None,
            ":ack_at": None,
            ":evaluated_at": now,
            ":remediated_at": now,
        },
        ConditionExpression="attribute_exists(finding_id)",
    )


def _mark_finding_reopened_after_rollback(tenant_id: str, finding_id: str) -> None:
    """Re-open finding lifecycle state after rollback."""
    now = _now_iso()
    _get_finding_table().update_item(
        Key={"tenant_id": tenant_id, "finding_id": finding_id},
        UpdateExpression="""
            SET #status = :status,
                workflow_status = :workflow_status,
                last_evaluated_at = :evaluated_at,
                rollback_at = :rollback_at
        """,
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": "open",
            ":workflow_status": "normal",
            ":evaluated_at": now,
            ":rollback_at": now,
        },
        ConditionExpression="attribute_exists(finding_id)",
    )


def get_remediation_detail(tenant_id: str, finding_id: str) -> dict[str, Any]:
    finding = _get_finding(tenant_id, finding_id)
    state = str(finding.get("remediation_state") or "ai_proposed").strip().lower()
    remediation_mode = str(finding.get("remediation_mode") or "").strip().lower()
    actions = finding.get("remediation_actions") or []
    if not actions:
        metadata = _get_file_metadata(tenant_id, str(finding.get("item_id") or ""))
        actions = [
            action.to_dict()
            for action in build_m365_action_plan(
                matched_guards=finding.get("matched_guards"),
                exposure_vectors=finding.get("exposure_vectors"),
                permissions=_parse_permissions((metadata or {}).get("permissions")),
                owner_user_id=str((metadata or {}).get("created_by_user_id") or ""),
                pii_detected=bool(finding.get("pii_detected", False)),
                secrets_detected=bool(finding.get("secrets_detected", False)),
                remediation_mode=remediation_mode,
                remediation_action=str(finding.get("remediation_action") or ""),
                content_signals=finding.get("content_signals"),
            )
        ]
    return {
        "tenant_id": tenant_id,
        "finding_id": finding_id,
        "remediation_state": state,
        "remediation_mode": remediation_mode,
        "actions": actions,
        "allowed_actions": _state_allowed_actions(state, remediation_mode),
        "version": int(finding.get("remediation_version") or 1),
        "approved_by": finding.get("remediation_approved_by"),
        "approved_at": finding.get("remediation_approved_at"),
        "last_execution_id": finding.get("remediation_last_execution_id"),
        "last_error": finding.get("remediation_last_error"),
        "result": finding.get("remediation_result"),
        "updated_at": finding.get("remediation_updated_at"),
        "exception_type": finding.get("exception_type"),
        "exception_review_due_at": finding.get("exception_review_due_at") or finding.get("suppress_until"),
        "exception_approved_by": finding.get("exception_approved_by"),
        "exception_ticket": finding.get("exception_ticket"),
        "exception_scope_hash": finding.get("exception_scope_hash"),
    }


def register_exception(
    tenant_id: str,
    finding_id: str,
    *,
    registered_by: str,
    exception_type: str,
    exception_review_due_at: str | None = None,
    duration_days: int | None = None,
    reason: str | None = None,
    exception_ticket: str | None = None,
    scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    finding = _get_finding(tenant_id, finding_id)
    normalized_exception_type = _normalize_exception_type(exception_type)
    resolved_due_at = _resolve_exception_review_due_at(
        review_due_at=exception_review_due_at,
        duration_days=duration_days,
    )

    now = _now_iso()
    scope_hash = _build_exception_scope_hash(
        tenant_id=tenant_id,
        finding_id=finding_id,
        finding=finding,
        scope=scope,
    )
    acknowledged_reason = str(reason or "").strip() or "exception_registered"
    ticket = str(exception_ticket or "").strip() or None

    table = _get_finding_table()
    table.update_item(
        Key={"tenant_id": tenant_id, "finding_id": finding_id},
        UpdateExpression="""
            SET workflow_status = :workflow_status,
                exception_type = :exception_type,
                exception_review_due_at = :review_due_at,
                suppress_until = :review_due_at,
                exception_approved_by = :approved_by,
                exception_ticket = :ticket,
                exception_scope_hash = :scope_hash,
                acknowledged_reason = :reason,
                acknowledged_by = :ack_by,
                acknowledged_at = :ack_at,
                remediation_result = :result,
                remediation_updated_at = :updated_at,
                last_evaluated_at = :updated_at
        """,
        ExpressionAttributeValues={
            ":workflow_status": "acknowledged",
            ":exception_type": normalized_exception_type,
            ":review_due_at": resolved_due_at,
            ":approved_by": registered_by,
            ":ticket": ticket,
            ":scope_hash": scope_hash,
            ":reason": acknowledged_reason,
            ":ack_by": registered_by,
            ":ack_at": now,
            ":updated_at": now,
            ":result": {
                "phase": "register_exception",
                "registered_by": registered_by,
                "registered_at": now,
                "exception_type": normalized_exception_type,
                "exception_review_due_at": resolved_due_at,
                "exception_ticket": ticket,
                "exception_scope_hash": scope_hash,
            },
        },
        ConditionExpression="attribute_exists(finding_id)",
    )
    detail = get_remediation_detail(tenant_id, finding_id)
    detail["exception_registered"] = True
    return detail


def mark_finding_completed(
    tenant_id: str,
    finding_id: str,
    *,
    completed_by: str,
) -> dict[str, Any]:
    """Mark low-risk finding as completed without remediation execution."""
    finding = _get_finding(tenant_id, finding_id)
    risk_level = str(finding.get("risk_level") or "").strip().lower()
    if risk_level not in {"low", "none"}:
        raise RemediationConflictError(
            f"mark_complete is allowed only for low/none risk_level (current={risk_level or 'unknown'})"
        )

    now = _now_iso()
    _mark_finding_completed(tenant_id, finding_id)
    _update_finding_remediation(
        tenant_id,
        finding_id,
        remediation_state="executed",
        remediation_last_error="",
        remediation_approved_by=completed_by,
        remediation_approved_at=now,
        remediation_executed_at=now,
        remediation_result={
            "phase": "mark_complete",
            "completed_by": completed_by,
            "completed_at": now,
            "manual_required": False,
            "results": [
                {
                    "action_type": "complete",
                    "status": "completed",
                }
            ],
            "post_verification": {
                "immediate_rescore": False,
                "success": True,
                "deferred_to": "none",
                "error": None,
            },
        },
    )
    detail = get_remediation_detail(tenant_id, finding_id)
    detail["marked_completed"] = True
    return detail


def propose_remediation(
    tenant_id: str,
    finding_id: str,
    *,
    proposed_by: str,
    force: bool = False,
) -> dict[str, Any]:
    finding = _get_finding(tenant_id, finding_id)
    if str(finding.get("source") or "").strip().lower() != "m365":
        raise ValueError("Only m365 findings support automated remediation.")
    current_state = str(finding.get("remediation_state") or "ai_proposed").strip().lower()
    if current_state == "executed" and not force:
        raise RemediationConflictError("Remediation is already executed.")

    metadata = _get_file_metadata(tenant_id, str(finding.get("item_id") or ""))
    actions = [
        action.to_dict()
        for action in build_m365_action_plan(
            matched_guards=finding.get("matched_guards"),
            exposure_vectors=finding.get("exposure_vectors"),
            permissions=_parse_permissions((metadata or {}).get("permissions")),
            owner_user_id=str((metadata or {}).get("created_by_user_id") or ""),
            pii_detected=bool(finding.get("pii_detected", False)),
            secrets_detected=bool(finding.get("secrets_detected", False)),
            remediation_mode=str(finding.get("remediation_mode") or ""),
            remediation_action=str(finding.get("remediation_action") or ""),
            content_signals=finding.get("content_signals"),
        )
    ]

    next_version = int(finding.get("remediation_version") or 0) + 1
    _update_finding_remediation(
        tenant_id,
        finding_id,
        remediation_state="ai_proposed",
        remediation_actions=actions,
        remediation_version=next_version,
        remediation_last_error=None,
        remediation_result={
            "phase": "propose",
            "proposed_by": proposed_by,
            "proposed_at": _now_iso(),
            "action_count": len(actions),
        },
    )
    detail = get_remediation_detail(tenant_id, finding_id)
    detail["proposal_generated"] = True
    return detail


def approve_remediation(
    tenant_id: str,
    finding_id: str,
    *,
    approved_by: str,
) -> dict[str, Any]:
    finding = _get_finding(tenant_id, finding_id)
    finding_remediation_mode = str(finding.get("remediation_mode") or "").strip().lower()
    if _is_manual_like_mode(finding_remediation_mode):
        raise RemediationConflictError(
            f"approve is not allowed for remediation_mode={finding_remediation_mode or 'unknown'}"
        )
    current_state = str(finding.get("remediation_state") or "ai_proposed").strip().lower()
    if current_state not in {"ai_proposed", "pending_approval", "failed"}:
        raise RemediationConflictError(
            f"approve is not allowed for remediation_state={current_state}"
        )
    if not finding.get("remediation_actions"):
        # Operator UX: allow direct approve even when proposal was not generated yet.
        # We regenerate latest proposal first, then continue approval flow.
        propose_remediation(
            tenant_id,
            finding_id,
            proposed_by=approved_by,
            force=False,
        )
        finding = _get_finding(tenant_id, finding_id)
        if not finding.get("remediation_actions"):
            raise RemediationConflictError("No remediation proposal exists.")

    now = _now_iso()
    _update_finding_remediation(
        tenant_id,
        finding_id,
        remediation_state="approved",
        remediation_approved_by=approved_by,
        remediation_approved_at=now,
        remediation_last_error=None,
        remediation_result={
            "phase": "approve",
            "approved_by": approved_by,
            "approved_at": now,
        },
    )
    detail = get_remediation_detail(tenant_id, finding_id)
    detail["approved"] = True
    return detail


def _resolve_graph_credentials(tenant_id: str) -> tuple[str, str, str]:
    tenant_prefix = f"/aiready/connect/{tenant_id}"
    azure_tenant_id = (
        get_ssm_parameter(f"{tenant_prefix}/tenant_id", "")
        or get_ssm_parameter("MSGraphTenantId", "")
        or get_env("MS_GRAPH_TENANT_ID", "")
    ).strip()
    client_id = (
        get_ssm_parameter(f"{tenant_prefix}/client_id", "")
        or get_ssm_parameter("MSGraphClientId", "")
        or get_env("MS_GRAPH_CLIENT_ID", "")
    ).strip()
    client_secret = (
        _read_secure_parameter(f"{tenant_prefix}/client_secret")
        or _read_secure_parameter("MSGraphClientSecret")
        or get_env("MS_GRAPH_CLIENT_SECRET", "")
    ).strip()
    if not azure_tenant_id or not client_id or not client_secret:
        raise ValueError("Graph credentials are not configured for remediation.")
    return azure_tenant_id, client_id, client_secret


def _upsert_connect_permissions_snapshot(
    *,
    tenant_id: str,
    drive_id: str,
    item_id: str,
    graph_item: dict[str, Any],
    previous_metadata: dict[str, Any] | None,
    change_type: str = "remediation-execute",
) -> None:
    """Persist live Graph permissions into Connect FileMetadata.

    This closes the gap between remediation execution and stream-driven rescoring by
    ensuring Connect metadata reflects the post-remediation ACL immediately.
    """
    table = _get_connect_table()
    permissions = graph_item.get("permissions")
    if not isinstance(permissions, list):
        permissions = []
    permissions_json = json.dumps(permissions, ensure_ascii=False, default=str)
    modified_at = str(graph_item.get("lastModifiedDateTime") or "").strip() or _now_iso()

    item_name = str(graph_item.get("name") or "").strip()
    if not item_name:
        item_name = str((previous_metadata or {}).get("item_name") or "").strip()
    web_url = str(graph_item.get("webUrl") or "").strip()
    if not web_url:
        web_url = str((previous_metadata or {}).get("web_url") or "").strip()
    source_metadata_raw = (previous_metadata or {}).get("source_metadata")
    source_metadata: dict[str, Any] = {}
    if isinstance(source_metadata_raw, str) and source_metadata_raw.strip():
        try:
            parsed = json.loads(source_metadata_raw)
            if isinstance(parsed, dict):
                source_metadata = parsed
        except Exception:
            source_metadata = {}

    tenant_domains = {
        str(domain).strip().lower()
        for domain in (source_metadata.get("tenant_domains") or [])
        if str(domain).strip()
    }
    permission_targets: list[dict[str, Any]] = []
    external_recipients: set[str] = set()
    org_edit_links: list[str] = []
    anonymous_links: list[str] = []

    def _is_external_email(email: str) -> bool:
        normalized = str(email or "").strip().lower()
        if not normalized:
            return False
        if "#ext#" in normalized:
            return True
        if "@" not in normalized:
            return False
        domain = normalized.rsplit("@", 1)[-1]
        if not tenant_domains:
            return not domain.endswith(".onmicrosoft.com")
        return domain not in tenant_domains

    def _extract_identities(entry: dict[str, Any]) -> list[dict[str, Any]]:
        identities: list[dict[str, Any]] = []
        for key in ("grantedToV2", "grantedTo"):
            principal = entry.get(key)
            if isinstance(principal, dict) and isinstance(principal.get("user"), dict):
                identities.append(principal["user"])
        for key in ("grantedToIdentitiesV2", "grantedToIdentities"):
            principals = entry.get(key)
            if not isinstance(principals, list):
                continue
            for principal in principals:
                if isinstance(principal, dict) and isinstance(principal.get("user"), dict):
                    identities.append(principal["user"])
        return identities

    def _resolve_role(entry: dict[str, Any]) -> str:
        roles = [
            str(role).strip().lower()
            for role in (entry.get("roles") or [])
            if str(role).strip()
        ]
        if any(role in {"owner", "manage", "fullcontrol"} for role in roles):
            return "owner"
        if any(role in {"write", "edit"} for role in roles):
            return "write"
        if any(role in {"read", "view"} for role in roles):
            return "read"
        return "read"

    for permission in permissions:
        if not isinstance(permission, dict):
            continue
        permission_id = str(permission.get("id") or "").strip()
        link = permission.get("link") if isinstance(permission.get("link"), dict) else {}
        scope = str(link.get("scope") or "").strip().lower()
        link_type = str(link.get("type") or "").strip().lower()
        if scope == "organization" and (link_type == "edit" or _resolve_role(permission) in {"write", "owner"}):
            if permission_id:
                org_edit_links.append(permission_id)
        if scope == "anonymous" and permission_id:
            anonymous_links.append(permission_id)

        for identity in _extract_identities(permission):
            email = str(identity.get("email") or "").strip().lower()
            principal = email or str(identity.get("id") or "").strip() or str(identity.get("displayName") or "").strip()
            if not principal:
                continue
            is_external = _is_external_email(email) if email else False
            if is_external and email:
                external_recipients.add(email)
            permission_targets.append(
                {
                    "principal": principal,
                    "role": _resolve_role(permission),
                    "is_external": bool(is_external),
                    "scope": scope or "direct",
                }
            )

    source_metadata["external_recipients"] = sorted(external_recipients)
    source_metadata["org_edit_links"] = sorted(set(org_edit_links))
    source_metadata["anonymous_links"] = sorted(set(anonymous_links))
    source_metadata["permission_targets"] = permission_targets
    source_metadata["permission_delta"] = []
    source_metadata["permissions_sync_source"] = str(change_type).replace("-", "_")
    source_metadata["permissions_synced_at"] = _now_iso()

    update_expression = """
        SET tenant_id = :tenant_id,
            #permissions = :permissions,
            permissions_count = :permissions_count,
            modified_at = :modified_at,
            item_name = :item_name,
            web_url = :web_url,
            #source = :source,
            source_metadata = :source_metadata,
            last_change_type = :last_change_type
    """
    table.update_item(
        Key={"drive_id": drive_id, "item_id": item_id},
        UpdateExpression=update_expression,
        ExpressionAttributeNames={
            "#permissions": "permissions",
            "#source": "source",
        },
        ExpressionAttributeValues={
            ":tenant_id": tenant_id,
            ":permissions": permissions_json,
            ":permissions_count": int(len(permissions)),
            ":modified_at": modified_at,
            ":item_name": item_name,
            ":web_url": web_url,
            ":source": "m365",
            ":source_metadata": json.dumps(source_metadata, ensure_ascii=False),
            ":last_change_type": str(change_type),
        },
    )


def execute_remediation(
    tenant_id: str,
    finding_id: str,
    *,
    executed_by: str,
) -> dict[str, Any]:
    finding = _get_finding(tenant_id, finding_id)
    finding_remediation_mode = str(finding.get("remediation_mode") or "").strip().lower()
    if finding_remediation_mode in {"owner_review", "manual", "recommend_only"}:
        raise RemediationConflictError(
            f"execute is not allowed for remediation_mode={finding_remediation_mode or 'unknown'}"
        )
    current_state = str(finding.get("remediation_state") or "ai_proposed").strip().lower()
    if current_state != "approved":
        raise RemediationConflictError(
            f"execute is not allowed for remediation_state={current_state}"
        )

    actions = finding.get("remediation_actions") or []
    if not actions:
        raise RemediationConflictError("No remediation actions to execute.")

    metadata = _get_file_metadata(tenant_id, str(finding.get("item_id") or ""))
    drive_id = str((metadata or {}).get("drive_id") or "").strip()
    item_id = str(finding.get("item_id") or "").strip()
    if not drive_id or not item_id:
        raise ValueError("drive_id or item_id is missing for execution.")

    action_hash = hashlib.sha256(
        json.dumps(actions, ensure_ascii=True, sort_keys=True).encode("utf-8")
    ).hexdigest()[:24]
    version = int(finding.get("remediation_version") or 1)
    idempotency_key = f"{finding_id}:{version}:{action_hash}"

    if (
        str(finding.get("remediation_idempotency_key") or "").strip() == idempotency_key
        and str(finding.get("remediation_state") or "").strip().lower() == "executed"
    ):
        detail = get_remediation_detail(tenant_id, finding_id)
        detail["replayed"] = True
        return detail

    azure_tenant_id, client_id, client_secret = _resolve_graph_credentials(tenant_id)
    graph_client = RemediationGraphClient(
        azure_tenant_id=azure_tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )
    execution_id = f"gov-rem-{uuid4().hex[:12]}"
    started_at = _now_iso()
    action_results: list[dict[str, Any]] = []
    manual_required = False
    permission_backups = {
        str(permission.get("id") or "").strip(): permission
        for permission in _parse_permissions((metadata or {}).get("permissions"))
        if str(permission.get("id") or "").strip()
    }

    try:
        for action in actions:
            action_type = str(action.get("action_type") or "").strip().lower()
            executable = bool(action.get("executable", True))
            if not executable or action_type == "manual_review":
                manual_required = True
                action_results.append(
                    {
                        "action_type": action_type,
                        "action_id": action.get("action_id"),
                        "status": "manual_required",
                    }
                )
                continue
            if action_type == "remove_permissions":
                for permission_id in action.get("permission_ids", []):
                    result = graph_client.delete_permission(
                        drive_id=drive_id,
                        item_id=item_id,
                        permission_id=str(permission_id),
                    )
                    backup = permission_backups.get(str(permission_id))
                    action_results.append(
                        {
                            "action_type": action_type,
                            "action_id": action.get("action_id"),
                            "rollback_data": backup,
                            **result,
                        }
                    )
                continue
            action_results.append(
                {
                    "action_type": action_type,
                    "action_id": action.get("action_id"),
                    "status": "skipped",
                }
            )

        next_state = "manual_required" if manual_required else "executed"
        # Persist post-remediation ACL to Connect metadata so stream-driven rescoring
        # can pick up the same permission state without waiting for external delta timing.
        refreshed_item = graph_client.get_drive_item_with_permissions(
            drive_id=drive_id,
            item_id=item_id,
        )
        _upsert_connect_permissions_snapshot(
            tenant_id=tenant_id,
            drive_id=drive_id,
            item_id=item_id,
            graph_item=refreshed_item,
            previous_metadata=metadata,
            change_type="remediation-execute",
        )
        # リスク再計算は Connect FileMetadata の更新 → DynamoDB Streams → analyzeExposure に委ねる。
        post_verify = {
            "immediate_rescore": False,
            "success": True,
            "error": None,
            "deferred_to": "connect_filemetadata_stream",
        }
        logger.info(
            "execute_remediation: post_verify deferred to Connect stream-driven rescoring",
            extra={"tenant_id": tenant_id, "finding_id": finding_id, "item_id": item_id},
        )
        _update_finding_remediation(
            tenant_id,
            finding_id,
            remediation_state=next_state,
            remediation_last_execution_id=execution_id,
            remediation_executed_at=_now_iso(),
            remediation_last_error=None,
            remediation_idempotency_key=idempotency_key,
            remediation_result={
                "phase": "execute",
                "started_at": started_at,
                "executed_by": executed_by,
                "results": action_results,
                "manual_required": manual_required,
                "post_verification": post_verify,
            },
        )
        # 是正完了（手動要の一部残りも含む）: ライフサイクルを完了にし抑止をクリアする。
        _mark_finding_completed(tenant_id, finding_id)
    except Exception as exc:
        logger.error(f"execute_remediation failed: {exc}")
        _update_finding_remediation(
            tenant_id,
            finding_id,
            remediation_state="failed",
            remediation_last_execution_id=execution_id,
            remediation_last_error=str(exc)[:500],
            remediation_idempotency_key=idempotency_key,
            remediation_result={
                "phase": "execute",
                "started_at": started_at,
                "executed_by": executed_by,
                "results": action_results,
            },
        )
        raise

    detail = get_remediation_detail(tenant_id, finding_id)
    detail["execution_id"] = execution_id
    detail["replayed"] = False
    return detail


def rollback_remediation(
    tenant_id: str,
    finding_id: str,
    *,
    rolled_back_by: str,
) -> dict[str, Any]:
    finding = _get_finding(tenant_id, finding_id)
    current_state = str(finding.get("remediation_state") or "").strip().lower()
    if current_state not in {"executed", "manual_required", "failed"}:
        raise RemediationConflictError(
            f"rollback is not allowed for remediation_state={current_state}"
        )

    previous_result = finding.get("remediation_result") or {}
    results = previous_result.get("results") if isinstance(previous_result, dict) else None
    if not isinstance(results, list) or not results:
        raise RemediationConflictError("No remediation execution result to rollback.")

    metadata = _get_file_metadata(tenant_id, str(finding.get("item_id") or ""))
    drive_id = str((metadata or {}).get("drive_id") or "").strip()
    item_id = str(finding.get("item_id") or "").strip()
    if not drive_id or not item_id:
        raise ValueError("drive_id or item_id is missing for rollback.")

    azure_tenant_id, client_id, client_secret = _resolve_graph_credentials(tenant_id)
    graph_client = RemediationGraphClient(
        azure_tenant_id=azure_tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )

    rollback_id = f"gov-rbk-{uuid4().hex[:12]}"
    rollback_results: list[dict[str, Any]] = []
    manual_required = False

    for result in results:
        if not isinstance(result, dict):
            continue
        action_type = str(result.get("action_type") or "").strip().lower()
        status = str(result.get("status") or "").strip().lower()
        if action_type == "remove_permissions":
            if status != "deleted":
                continue
            backup = result.get("rollback_data")
            if not isinstance(backup, dict):
                manual_required = True
                rollback_results.append(
                    {
                        "action_type": action_type,
                        "permission_id": result.get("permission_id"),
                        "status": "manual_required",
                        "reason": "rollback_data_missing",
                    }
                )
                continue
            restored = graph_client.restore_permission(
                drive_id=drive_id,
                item_id=item_id,
                backup=backup,
            )
            if str(restored.get("status") or "").strip().lower() != "restored":
                manual_required = True
            rollback_results.append(
                {
                    "action_type": action_type,
                    **restored,
                }
            )
            continue
        if action_type == "apply_sensitivity_label":
            manual_required = True

    next_state = "manual_required" if manual_required else "ai_proposed"
    refreshed_item = graph_client.get_drive_item_with_permissions(
        drive_id=drive_id,
        item_id=item_id,
    )
    _upsert_connect_permissions_snapshot(
        tenant_id=tenant_id,
        drive_id=drive_id,
        item_id=item_id,
        graph_item=refreshed_item,
        previous_metadata=metadata,
        change_type="remediation-rollback",
    )
    _update_finding_remediation(
        tenant_id,
        finding_id,
        remediation_state=next_state,
        remediation_last_error=None,
        remediation_last_execution_id=rollback_id,
        remediation_result={
            "phase": "rollback",
            "rolled_back_by": rolled_back_by,
            "rolled_back_at": _now_iso(),
            "source_execution_id": finding.get("remediation_last_execution_id"),
            "results": rollback_results,
            "manual_required": manual_required,
        },
    )
    _mark_finding_reopened_after_rollback(tenant_id, finding_id)
    detail = get_remediation_detail(tenant_id, finding_id)
    detail["rollback_id"] = rollback_id
    return detail

