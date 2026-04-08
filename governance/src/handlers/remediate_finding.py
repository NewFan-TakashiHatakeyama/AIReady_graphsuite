"""Remediation workflow handler for finding-level operations."""

from __future__ import annotations

import json
from typing import Any

from services.remediation_graph_client import RemediationGraphError
from services.remediation_workflow import (
    RemediationConflictError,
    approve_remediation,
    execute_remediation,
    get_remediation_detail,
    mark_finding_completed,
    propose_remediation,
    register_exception,
    rollback_remediation,
)
from shared.config import get_env
from shared.logger import get_logger

logger = get_logger(__name__)


def _execution_mode() -> str:
    raw = str(
        get_env(
            "GOVERNANCE_REMEDIATION_EXECUTION_MODE",
            "approval_then_auto_execute",
        )
    ).strip().lower()
    if raw in {"approval_then_execute", "approval_then_auto_execute"}:
        return raw
    return "approval_then_auto_execute"


def _parse_body(event: dict[str, Any]) -> dict[str, Any]:
    body = event.get("body")
    if body is None:
        return {}
    if isinstance(body, dict):
        return body
    if isinstance(body, str) and body.strip():
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Dispatch remediation workflow actions.

    Required keys:
        tenant_id, finding_id, action in {"get","propose","approve","execute","rollback","register_exception","mark_complete"}
    """
    _ = context
    body = _parse_body(event)
    tenant_id = str(body.get("tenant_id") or event.get("tenant_id") or "").strip()
    finding_id = str(body.get("finding_id") or event.get("finding_id") or "").strip()
    action = str(body.get("action") or event.get("action") or "get").strip().lower()
    operator = str(body.get("operator") or event.get("operator") or "system").strip()
    force = bool(body.get("force", False))

    if not tenant_id or not finding_id:
        return {"statusCode": 400, "body": json.dumps({"error": "tenant_id and finding_id are required"})}

    try:
        if action == "propose":
            payload = propose_remediation(
                tenant_id,
                finding_id,
                proposed_by=operator,
                force=force,
            )
        elif action == "approve":
            payload = approve_remediation(
                tenant_id,
                finding_id,
                approved_by=operator,
            )
            if _execution_mode() in {"approval_then_auto_execute", "approval_then_execute"}:
                approved_mode = str(payload.get("remediation_mode") or "").strip().lower()
                skip_auto = approved_mode in {"owner_review", "manual", "recommend_only"}
                if not skip_auto:
                    payload = execute_remediation(
                        tenant_id,
                        finding_id,
                        executed_by=operator,
                    )
                else:
                    payload["auto_execute_skipped"] = True
                    payload["auto_execute_skip_reason"] = (
                        f"remediation_mode={approved_mode or 'unknown'}"
                    )
        elif action == "execute":
            payload = execute_remediation(
                tenant_id,
                finding_id,
                executed_by=operator,
            )
        elif action == "rollback":
            payload = rollback_remediation(
                tenant_id,
                finding_id,
                rolled_back_by=operator,
            )
        elif action == "get":
            payload = get_remediation_detail(tenant_id, finding_id)
        elif action in {"mark_complete", "complete"}:
            payload = mark_finding_completed(
                tenant_id,
                finding_id,
                completed_by=operator,
            )
        elif action in {"register_exception", "exception"}:
            payload = register_exception(
                tenant_id,
                finding_id,
                registered_by=operator,
                exception_type=str(body.get("exception_type") or event.get("exception_type") or "").strip(),
                exception_review_due_at=str(
                    body.get("exception_review_due_at")
                    or event.get("exception_review_due_at")
                    or ""
                ).strip()
                or None,
                duration_days=(
                    int(body.get("duration_days"))
                    if body.get("duration_days") is not None
                    else (
                        int(event.get("duration_days"))
                        if event.get("duration_days") is not None
                        else None
                    )
                ),
                reason=str(body.get("reason") or event.get("reason") or "").strip() or None,
                exception_ticket=str(
                    body.get("exception_ticket") or event.get("exception_ticket") or ""
                ).strip()
                or None,
                scope=(
                    body.get("scope")
                    if isinstance(body.get("scope"), dict)
                    else (event.get("scope") if isinstance(event.get("scope"), dict) else None)
                ),
            )
        else:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": f"unsupported action: {action}"}),
            }
        return {"statusCode": 200, "body": json.dumps(payload, ensure_ascii=False, default=str)}
    except RemediationConflictError as exc:
        return {"statusCode": 409, "body": json.dumps({"error": str(exc)})}
    except RemediationGraphError as exc:
        # Graph refused the operation (e.g. createLink 403: tenant policy, missing app role, sensitivity).
        err = str(exc)
        body: dict[str, Any] = {"error": err, "upstream": "microsoft_graph"}
        if "status=403" in err:
            body["hint"] = (
                "Microsoft Graph returned 403. Typical causes: SharePoint/Teams admin disabled "
                "anonymous or organization sharing links; sensitivity / DLP; or the app registration "
                "lacks application permissions to create that link type on this site."
            )
        elif "status=415" in err:
            body["hint"] = (
                "Microsoft Graph returned 415 for sensitivity label assignment. "
                "Typical causes: unsupported item/content type for label API, "
                "or request body/label operation not accepted by the target workload."
            )
        elif "status=402" in err:
            body["hint"] = (
                "Microsoft Graph returned 402. Sensitivity label assignment may require "
                "metered API billing linkage (Microsoft.GraphServices/accounts)."
            )
        return {"statusCode": 502, "body": json.dumps(body, ensure_ascii=False)}
    except ValueError as exc:
        return {"statusCode": 400, "body": json.dumps({"error": str(exc)})}
    except Exception as exc:
        logger.error(f"remediate_finding handler failed: {exc}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(exc)})}
