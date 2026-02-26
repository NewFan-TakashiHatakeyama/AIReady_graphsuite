"""schemaTransform Lambda handler."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3

from src.models.unified_metadata import UnifiedMetadata
from src.shared.document_analysis_client import get_document_analysis
from src.shared.freshness import calculate_freshness_status
from src.shared.governance_client import (
    DEFAULT_GOVERNANCE_RESULT,
    lookup_governance_finding,
)
from src.shared.lineage_client import record_lineage_event
from src.shared.logger import log_structured
from src.shared.metrics import publish_metric

SOURCE = "microsoft365"
SCHEMA_VERSION = "1.0"
DELETE_TTL_DAYS = 30

_dynamodb_resource = None


def handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    """Entry point for DynamoDB Streams events."""
    records = event.get("Records", [])
    processed = 0
    errors = 0

    for record in records:
        try:
            _process_record(record)
            processed += 1
        except Exception as exc:
            errors += 1
            log_structured(
                "ERROR",
                "schemaTransform failed to process record",
                error=str(exc),
            )

    _safe_publish_metric("SchemaTransformProcessed", processed)
    if errors > 0:
        _safe_publish_metric("SchemaTransformErrors", errors)

    return {"processed": processed, "errors": errors}


def _get_dynamodb_resource() -> Any:
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    return _dynamodb_resource


def _get_table(table_env_name: str) -> Any:
    table_name = os.environ.get(table_env_name, "")
    if not table_name:
        raise ValueError(f"Environment variable '{table_env_name}' is required")
    return _get_dynamodb_resource().Table(table_name)


def _process_record(record: dict[str, Any]) -> None:
    event_name = record.get("eventName", "")
    dynamodb_data = record.get("dynamodb", {})

    if event_name == "REMOVE":
        keys = _deserialize_dynamodb_image(dynamodb_data.get("Keys", {}))
        tenant_id = keys.get("tenant_id") or _extract_tenant_from_arn(record)
        item_id = keys.get("item_id", "")
        if not tenant_id or not item_id:
            return
        _handle_delete(tenant_id=tenant_id, item_id=item_id)
        return

    new_image = dynamodb_data.get("NewImage", {})
    if not new_image:
        return

    source_item = _deserialize_dynamodb_image(new_image)
    tenant_id = str(source_item.get("tenant_id", ""))
    item_id = str(source_item.get("item_id", ""))
    if not tenant_id or not item_id:
        return

    if bool(source_item.get("is_deleted", False)):
        _handle_delete(tenant_id=tenant_id, item_id=item_id)
        return

    if bool(source_item.get("is_folder", False)):
        return

    governance_result = _lookup_governance_defaulting(tenant_id=tenant_id, item_id=item_id)

    now_iso = datetime.now(timezone.utc).isoformat()
    last_modified = str(source_item.get("modified_at") or now_iso)
    risk_level = str(governance_result.get("risk_level", "none") or "none")

    # high/critical は常に AI 非許可に寄せる。
    ai_eligible = bool(governance_result.get("ai_eligible", True))
    if risk_level in {"high", "critical"}:
        ai_eligible = False

    lineage_id = str(uuid.uuid4())
    model = UnifiedMetadata(
        tenant_id=tenant_id,
        item_id=item_id,
        title=str(source_item.get("name", "")),
        content_type=str(source_item.get("mime_type", "application/octet-stream")),
        author=str(source_item.get("created_by", "unknown")),
        last_modified=last_modified,
        access_scope=str(source_item.get("sharing_scope", "private")),
        access_control=_parse_json_field(source_item.get("permissions")),
        classification=str(governance_result.get("classification", "unclassified")),
        source_identifiers=_build_source_identifiers(source_item),
        origin_url=str(source_item.get("web_url", "")),
        size_bytes=_to_int(source_item.get("size"), default=0),
        hierarchy_path=str(source_item.get("path", "")),
        extensions=_build_extensions(source_item),
        source=SOURCE,
        lineage_id=lineage_id,
        risk_level=risk_level,
        pii_detected=bool(governance_result.get("pii_detected", False)),
        ai_eligible=ai_eligible,
        finding_id=governance_result.get("finding_id"),
        freshness_status=calculate_freshness_status(last_modified),
        schema_version=SCHEMA_VERSION,
        transformed_at=now_iso,
        is_deleted=False,
    )

    _apply_document_analysis_enrichment(model)
    _get_table("UNIFIED_METADATA_TABLE").put_item(Item=model.to_dynamodb_item())

    record_lineage_event(
        function_name=os.environ["LINEAGE_FUNCTION_NAME"],
        lineage_id=lineage_id,
        job_name="schemaTransform",
        input_dataset=f"FileMetadata/{tenant_id}/{item_id}",
        output_dataset=f"UnifiedMetadata/{tenant_id}/{item_id}",
        event_type="COMPLETE",
        metadata={"operation": "UPSERT"},
        tenant_id=tenant_id,
    )


def _handle_delete(tenant_id: str, item_id: str) -> None:
    table = _get_table("UNIFIED_METADATA_TABLE")
    existing = table.get_item(Key={"tenant_id": tenant_id, "item_id": item_id}).get("Item", {})
    now = datetime.now(timezone.utc)
    ttl_epoch = int(now.timestamp()) + (DELETE_TTL_DAYS * 86400)

    item = dict(existing)
    item.update(
        {
            "tenant_id": tenant_id,
            "item_id": item_id,
            "is_deleted": True,
            "deleted_at": now.isoformat(),
            "ttl": ttl_epoch,
        }
    )
    table.put_item(Item=item)

    record_lineage_event(
        function_name=os.environ["LINEAGE_FUNCTION_NAME"],
        lineage_id=str(uuid.uuid4()),
        job_name="schemaTransform",
        input_dataset=f"FileMetadata/{tenant_id}/{item_id}",
        output_dataset=f"UnifiedMetadata/{tenant_id}/{item_id}",
        event_type="COMPLETE",
        metadata={"operation": "DELETE"},
        tenant_id=tenant_id,
    )
    _safe_publish_metric("SchemaTransformDeleted", 1)


def _lookup_governance_defaulting(tenant_id: str, item_id: str) -> dict[str, Any]:
    try:
        return lookup_governance_finding(
            tenant_id=tenant_id,
            file_id=item_id,
            finding_table_name=os.environ["GOVERNANCE_FINDING_TABLE"],
        )
    except Exception as exc:
        log_structured(
            "WARN",
            "Governance finding lookup failed in schemaTransform, defaulting",
            tenant_id=tenant_id,
            item_id=item_id,
            error=str(exc),
        )
        return dict(DEFAULT_GOVERNANCE_RESULT)


def _apply_document_analysis_enrichment(model: UnifiedMetadata) -> None:
    try:
        doc = get_document_analysis(tenant_id=model.tenant_id, item_id=model.item_id)
    except Exception as exc:
        log_structured(
            "WARN",
            "DocumentAnalysis lookup failed",
            tenant_id=model.tenant_id,
            item_id=model.item_id,
            error=str(exc),
        )
        _safe_publish_metric("DocumentAnalysisLookupMiss", 1)
        return

    if not doc:
        _safe_publish_metric("DocumentAnalysisLookupMiss", 1)
        return

    analysis_status = str(doc.get("analysis_status") or doc.get("status") or "")
    if analysis_status != "completed":
        _safe_publish_metric("DocumentAnalysisLookupMiss", 1)
        return

    model.document_summary = str(doc.get("document_summary") or doc.get("summary") or "")[
        :500
    ]
    model.summary_language = str(doc.get("summary_language") or "")
    model.topic_keywords = _normalize_keywords(doc.get("topic_keywords"))
    model.embedding_ref = str(doc.get("embedding_ref") or "")
    model.analysis_id = str(doc.get("analysis_id") or "")
    model.summary_generated_at = str(
        doc.get("summary_generated_at") or doc.get("analyzed_at") or ""
    )


def _build_source_identifiers(source_item: dict[str, Any]) -> dict[str, Any]:
    identifiers = {"drive_id": source_item.get("drive_id")}
    identifiers.update(_parse_json_field(source_item.get("sharepoint_ids")))
    return {k: v for k, v in identifiers.items() if v is not None and v != ""}


def _build_extensions(source_item: dict[str, Any]) -> dict[str, Any]:
    return {
        "governance_flags": _normalize_string_list(source_item.get("governance_flags")),
        "sharepoint_ids": _parse_json_field(source_item.get("sharepoint_ids")),
        "created_at": source_item.get("created_at"),
        "modified_by": source_item.get("modified_by"),
        "is_folder": bool(source_item.get("is_folder", False)),
        "synced_at": source_item.get("synced_at"),
    }


def _deserialize_dynamodb_image(image: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, type_value in image.items():
        result[key] = _deserialize_attribute(type_value)
    return result


def _deserialize_attribute(type_value: dict[str, Any]) -> Any:
    if "S" in type_value:
        return type_value["S"]
    if "N" in type_value:
        value = type_value["N"]
        return int(value) if value.isdigit() else float(value)
    if "BOOL" in type_value:
        return type_value["BOOL"]
    if "NULL" in type_value:
        return None
    if "SS" in type_value:
        return type_value["SS"]
    if "L" in type_value:
        return [_deserialize_attribute(v) for v in type_value["L"]]
    if "M" in type_value:
        return {k: _deserialize_attribute(v) for k, v in type_value["M"].items()}
    return type_value


def _extract_tenant_from_arn(record: dict[str, Any]) -> str:
    arn = record.get("eventSourceARN", "")
    if "/table/" in arn:
        table_name = arn.split("/table/")[1].split("/")[0]
    elif "/" in arn:
        table_name = arn.split("/")[1]
    else:
        table_name = ""
    extracted = table_name.replace("FileMetadata-", "")
    if extracted == table_name:
        return os.environ.get("TENANT_ID", "")
    return extracted


def _parse_json_field(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        if not value:
            return {}
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _normalize_keywords(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        if not value:
            return []
        try:
            parsed = json.loads(value)
            return [str(v) for v in parsed] if isinstance(parsed, list) else [value]
        except json.JSONDecodeError:
            return [value]
    return []


def _normalize_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        return [value]
    return []


def _to_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_publish_metric(metric_name: str, value: float) -> None:
    try:
        publish_metric(metric_name, value)
    except Exception as exc:
        log_structured(
            "WARN",
            "CloudWatch metric publish failed",
            metric_name=metric_name,
            value=value,
            error=str(exc),
        )
