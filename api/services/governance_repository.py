"""Governance detection results repository."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from services.aws_clients import get_dynamodb_client, get_dynamodb_resource
from services.runtime_config import load_aws_runtime_config
from services.tenant_db_resolver import TenantDbResolver


DEFAULT_QUERY_LIMIT = 200
MAX_QUERY_LIMIT = 500
MAX_QUERY_ITEMS = 5000

logger = logging.getLogger(__name__)
TOKYO_TZ = ZoneInfo("Asia/Tokyo")

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


def generate_governance_finding_id(tenant_id: str, source: str, item_id: str) -> str:
    """Governance Lambda `finding_manager.generate_finding_id` と同一規則。"""
    raw = f"{tenant_id}:{source}:{item_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def _finding_source_variants(primary: str) -> tuple[str, ...]:
    """既存データ差異向けに m365 / microsoft365 の別名を試す。"""
    p = str(primary or "m365").strip() or "m365"
    ordered: list[str] = []
    for candidate in (p, "m365", "microsoft365"):
        c = str(candidate).strip().lower()
        if c and c not in ordered:
            ordered.append(c)
    return tuple(ordered)


def _try_close_governance_finding(table: Any, item_tenant_id: str, finding_id: str) -> bool:
    """存在する Finding を closed にする。無ければ False。"""
    now = datetime.now(TOKYO_TZ).isoformat()
    try:
        table.update_item(
            Key={"tenant_id": item_tenant_id, "finding_id": finding_id},
            UpdateExpression="SET #st = :status, last_evaluated_at = :now",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":status": "closed",
                ":now": now,
            },
            ConditionExpression="attribute_exists(finding_id)",
        )
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", "") or "")
        if code == "ConditionalCheckFailedException":
            return False
        raise


def close_governance_findings_for_connect_drive(
    *,
    resolver_tenant_id: str,
    drive_id: str,
    file_metadata_table: Any,
) -> dict[str, int]:
    """Connect FileMetadata の drive 配下に対応する ExposureFinding をベストエフォートで closed にする。

    FileMetadata 行を drive_id で列挙し、tenant_id / item_id / source から finding_id を決定的に算出して更新する。
    Streams 非依存（接続 safe 削除で Metadata が残る場合の整合用）。

    IAM: API ロールに対象 `governance_finding_table` への dynamodb:UpdateItem が必要。

    Args:
        resolver_tenant_id: TenantDbResolver 解決キー（呼び出しコンテキストのテナント）。
        drive_id: 対象ドライブ ID。
        file_metadata_table: boto3 DynamoDB Table（FileMetadata）。

    Returns:
        file_metadata_rows, findings_closed, findings_attempted の件数。
    """
    normalized_drive = str(drive_id or "").strip()
    if not normalized_drive:
        return {"file_metadata_rows": 0, "findings_closed": 0, "findings_attempted": 0}

    binding = _tenant_db_resolver.resolve(resolver_tenant_id)
    finding_table = _get_dynamodb_resource().Table(binding.governance_finding_table_name)

    fm_rows = 0
    closed = 0
    attempted = 0
    last_key = None
    while True:
        qkwargs: dict[str, Any] = {
            "KeyConditionExpression": Key("drive_id").eq(normalized_drive),
            "ProjectionExpression": "tenant_id, item_id, #src",
            "ExpressionAttributeNames": {"#src": "source"},
            "Limit": 200,
        }
        if last_key:
            qkwargs["ExclusiveStartKey"] = last_key
        response = file_metadata_table.query(**qkwargs)
        items = response.get("Items", [])
        for item in items:
            fm_rows += 1
            item_id = str(item.get("item_id") or "").strip()
            if not item_id:
                continue
            item_tid = str(item.get("tenant_id") or resolver_tenant_id).strip() or resolver_tenant_id
            primary_source = str(item.get("source") or "m365").strip() or "m365"
            for source in _finding_source_variants(primary_source):
                attempted += 1
                fid = generate_governance_finding_id(item_tid, source, item_id)
                if _try_close_governance_finding(finding_table, item_tid, fid):
                    closed += 1
                    break
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break

    return {
        "file_metadata_rows": fm_rows,
        "findings_closed": closed,
        "findings_attempted": attempted,
    }


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
    restrict_item_ids: set[str] | None = None,
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
    if restrict_item_ids is not None:
        findings = [
            item
            for item in findings
            if str(item.get("item_id") or "").strip() in restrict_item_ids
        ]
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
