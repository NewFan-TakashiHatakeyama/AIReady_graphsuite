"""Governance detection results repository."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from boto3.dynamodb.conditions import Attr, Key

from services.aws_clients import get_dynamodb_client, get_dynamodb_resource
from services.runtime_config import load_aws_runtime_config
from services.tenant_db_resolver import TenantDbResolver


DEFAULT_QUERY_LIMIT = 200
MAX_QUERY_LIMIT = 500
MAX_QUERY_ITEMS = 5000

_runtime_config = load_aws_runtime_config()
_tenant_db_resolver = TenantDbResolver(_runtime_config)
_dynamodb_resource = None
_dynamodb_client = None


def _get_dynamodb_resource():
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = get_dynamodb_resource(_runtime_config)
    return _dynamodb_resource


def _get_dynamodb_client():
    global _dynamodb_client
    if _dynamodb_client is None:
        _dynamodb_client = get_dynamodb_client(_runtime_config)
    return _dynamodb_client


def _resolve_tables(tenant_id: str):
    binding = _tenant_db_resolver.resolve(tenant_id)
    dynamodb_resource = _get_dynamodb_resource()
    finding_table = dynamodb_resource.Table(binding.governance_finding_table_name)
    return finding_table


def _to_dynamodb_attr(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, int):
        return {"N": str(value)}
    if isinstance(value, float):
        return {"N": str(value)}
    return {"S": str(value)}


def _from_dynamodb_item(item: dict[str, Any]) -> dict[str, Any]:
    plain: dict[str, Any] = {}
    for key, value in item.items():
        if "S" in value:
            plain[key] = value["S"]
        elif "N" in value:
            number = value["N"]
            plain[key] = int(number) if "." not in number else float(number)
        elif "BOOL" in value:
            plain[key] = bool(value["BOOL"])
        elif "NULL" in value:
            plain[key] = None
        elif "L" in value:
            plain[key] = [_from_dynamodb_item({"v": v})["v"] for v in value["L"]]
        elif "M" in value:
            plain[key] = _from_dynamodb_item(value["M"])
    return plain


def _to_plain_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, list):
        return [_to_plain_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_plain_value(v) for k, v in value.items()}
    return value


def _is_action_required_row(row: dict[str, Any]) -> bool:
    status = str(row.get("status", "")).strip().lower()
    if status not in {"new", "open", "acknowledged"}:
        return False
    level = str(row.get("risk_level", "")).strip().lower()
    if level in {"medium", "high", "critical"}:
        return True
    score = row.get("risk_score")
    try:
        return float(score) >= 5.0
    except Exception:
        return False


def _parse_source_metadata(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return {}
    text = raw.strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _resolve_target_type(row: dict[str, Any]) -> str:
    container_type = str(row.get("container_type", "")).strip().lower()
    if any(token in container_type for token in ("folder", "directory")):
        return "folder"
    if any(token in container_type for token in ("file", "document")):
        return "file"

    metadata = _parse_source_metadata(row.get("source_metadata"))
    if bool(metadata.get("is_folder")):
        return "folder"
    if bool(metadata.get("is_file")):
        return "file"
    metadata_item_type = str(metadata.get("item_type", "")).strip().lower()
    if metadata_item_type in {"folder", "directory"}:
        return "folder"
    if metadata_item_type in {"file", "document"}:
        return "file"

    item_name = str(row.get("item_name", "")).strip()
    if item_name.endswith("/"):
        return "folder"
    if "." in item_name and not item_name.endswith("."):
        return "file"

    mime_type = str(row.get("mime_type", "")).strip().lower()
    if mime_type and mime_type != "inode/directory":
        return "file"
    if mime_type == "inode/directory":
        return "folder"

    return "unknown"


def _query_findings_by_status_gsi(
    tenant_id: str, statuses: list[str]
) -> list[dict[str, Any]]:
    table = _resolve_tables(tenant_id)
    seen_finding_ids: set[str] = set()
    items: list[dict[str, Any]] = []
    for status_value in statuses:
        query_kwargs: dict[str, Any] = {
            "IndexName": "GSI-StatusFinding",
            "KeyConditionExpression": Key("tenant_id").eq(tenant_id)
            & Key("status").eq(status_value),
        }
        last_evaluated_key = None
        while True:
            if last_evaluated_key is not None:
                query_kwargs["ExclusiveStartKey"] = last_evaluated_key
            response = table.query(**query_kwargs)
            for item in response.get("Items", []):
                finding_id = str(item.get("finding_id", ""))
                if not finding_id or finding_id in seen_finding_ids:
                    continue
                seen_finding_ids.add(finding_id)
                items.append(item)
            last_evaluated_key = response.get("LastEvaluatedKey")
            if last_evaluated_key is None or len(items) >= MAX_QUERY_ITEMS:
                break
    return items


def _query_findings(tenant_id: str, statuses: list[str] | None) -> list[dict[str, Any]]:
    table = _resolve_tables(tenant_id)
    if statuses:
        try:
            return _query_findings_by_status_gsi(tenant_id, statuses)
        except Exception:
            # Fallback to base query + filter for environments without GSI.
            pass

    query_kwargs: dict[str, Any] = {
        "KeyConditionExpression": Key("tenant_id").eq(tenant_id),
    }
    filter_expression = None
    if statuses:
        status_expression = Attr("status").is_in(statuses)
        filter_expression = (
            status_expression
            if filter_expression is None
            else filter_expression & status_expression
        )
    if filter_expression is not None:
        query_kwargs["FilterExpression"] = filter_expression

    items: list[dict[str, Any]] = []
    last_evaluated_key = None
    while True:
        if last_evaluated_key is not None:
            query_kwargs["ExclusiveStartKey"] = last_evaluated_key
        response = table.query(**query_kwargs)
        items.extend(response.get("Items", []))
        last_evaluated_key = response.get("LastEvaluatedKey")
        if last_evaluated_key is None or len(items) >= MAX_QUERY_ITEMS:
            break

    return items


def get_governance_finding_by_id(
    tenant_id: str,
    finding_id: str,
    *,
    include_document_analysis: bool = True,
) -> dict[str, Any] | None:
    normalized_tenant_id = str(tenant_id or "").strip()
    normalized_finding_id = str(finding_id or "").strip()
    if not normalized_tenant_id or not normalized_finding_id:
        raise ValueError("tenant_id and finding_id are required in governance repository.")

    table = _resolve_tables(normalized_tenant_id)
    response = table.get_item(
        Key={
            "tenant_id": normalized_tenant_id,
            "finding_id": normalized_finding_id,
        }
    )
    item = response.get("Item")
    if not item:
        return None

    row = _to_plain_value(item)
    if not str(row.get("target_type", "")).strip():
        row["target_type"] = _resolve_target_type(row)
    del include_document_analysis
    return row


def list_governance_findings(
    tenant_id: str,
    limit: int = DEFAULT_QUERY_LIMIT,
    offset: int = 0,
    statuses: list[str] | None = None,
    include_document_analysis: bool = True,
    action_required_only: bool = False,
) -> dict[str, Any]:
    normalized_tenant_id = str(tenant_id or "").strip()
    if not normalized_tenant_id:
        raise ValueError("tenant_id is required in governance repository.")

    normalized_limit = max(1, min(limit, MAX_QUERY_LIMIT))
    normalized_offset = max(0, offset)

    findings = _query_findings(tenant_id=normalized_tenant_id, statuses=statuses)
    findings = [_to_plain_value(item) for item in findings]
    for item in findings:
        if not str(item.get("target_type", "")).strip():
            item["target_type"] = _resolve_target_type(item)
        rl = str(item.get("risk_level", "")).strip().lower()
        if rl == "none":
            item["risk_level"] = "low"
        # Oversharing policy snapshot fields (backward compatible defaults)
        if item.get("effective_policy_id") is None:
            item["effective_policy_id"] = ""
        if item.get("effective_policy_version") is None:
            item["effective_policy_version"] = 1
        if not isinstance(item.get("matched_policy_ids"), list):
            item["matched_policy_ids"] = []
        if not isinstance(item.get("decision_trace"), list):
            item["decision_trace"] = []
        if not isinstance(item.get("reason_codes"), list):
            item["reason_codes"] = []
        if item.get("decision") is None:
            item["decision"] = "review"
        if item.get("remediation_mode") is None:
            item["remediation_mode"] = "manual"
        if item.get("remediation_action") is None:
            item["remediation_action"] = "request_review"
        if not isinstance(item.get("content_signals"), dict):
            item["content_signals"] = {
                "doc_sensitivity_level": "none",
                "doc_categories": [],
                "contains_pii": False,
                "contains_secret": False,
                "confidence": 0.0,
                "expected_audience": "internal_need_to_know",
                "expected_department": "unknown",
                "expected_department_confidence": 0.0,
                "justification": "",
            }
        if not isinstance(item.get("content_analysis"), dict):
            item["content_analysis"] = {
                "analysis_status": "unknown",
                "decision_source": "fallback",
                "model_id": "",
                "prompt_version": "",
                "confidence": float(item["content_signals"].get("confidence", 0.0)),
            }
        if item.get("decision_source") is None:
            item["decision_source"] = "fallback"
        if item.get("expected_audience") is None:
            item["expected_audience"] = str(item["content_signals"].get("expected_audience", "internal_need_to_know"))
        if item.get("expected_department") is None:
            item["expected_department"] = str(item["content_signals"].get("expected_department", "unknown"))
        if item.get("expectation_gap_reason") is None:
            item["expectation_gap_reason"] = ""
        if item.get("expectation_gap_score") is None:
            item["expectation_gap_score"] = 0.0
    if action_required_only:
        findings = [item for item in findings if _is_action_required_row(item)]
    findings.sort(key=lambda item: str(item.get("last_evaluated_at", "")), reverse=True)

    paged = findings[normalized_offset : normalized_offset + normalized_limit]
    del include_document_analysis

    return {
        "rows": paged,
        "pagination": {
            "limit": normalized_limit,
            "offset": normalized_offset,
            "total_count": len(findings),
            "scan_capped": len(findings) >= MAX_QUERY_ITEMS,
            "next_offset": (
                normalized_offset + normalized_limit
                if normalized_offset + normalized_limit < len(findings)
                else None
            ),
        },
    }
