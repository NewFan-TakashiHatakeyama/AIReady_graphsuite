"""entityResolver Lambda handler (DynamoDB projection only)."""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError

from src.models.entity_candidate import EntityCandidate
from src.shared.entity_id import compute_canonical_hash
from src.shared.inference import OntologyInferenceService
from src.shared.lineage_client import record_lineage_event
from src.shared.logger import log_structured
from src.shared.metrics import publish_metric
from src.shared.json_normalizer import parse_json_container

ALLOWED_EXTRACTION_SOURCES = {
    "governance+ner",
    "ner",
    "governance",
    "connect_metadata",
}
ALLOWED_SOURCES = {"document_analysis"}

TOKYO_TZ = ZoneInfo("Asia/Tokyo")
_dynamodb_resource: Any | None = None


def handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    """Process entity resolution queue messages."""
    del context
    processed = 0
    skipped = 0
    errors = 0
    candidates_processed = 0

    for record in event.get("Records", []):
        try:
            body = _parse_record_body(record)
            if not body:
                skipped += 1
                continue

            candidates = _expand_message_to_candidates(body)
            if not candidates:
                skipped += 1
                continue

            for candidate in candidates:
                result = _resolve_entity(candidate)
                _record_lineage_for_resolution(candidate, result)
                candidates_processed += 1
                _safe_publish_metric("EntityResolverProcessed", 1)

            processed += 1
            _safe_publish_metric("EntityResolverDocumentsProcessed", 1)
        except ValueError as exc:
            skipped += 1
            log_structured(
                "WARN", "Skipped unsupported entity resolver message", error=str(exc)
            )
        except Exception as exc:  # pragma: no cover
            errors += 1
            log_structured(
                "ERROR", "Entity resolver failed to process message", error=str(exc)
            )

    _safe_publish_metric("EntityResolutionMessagesProcessed", processed)
    if candidates_processed:
        _safe_publish_metric("EntityResolutionCandidatesProcessed", candidates_processed)
    if skipped:
        _safe_publish_metric("EntityResolutionMessagesSkipped", skipped)
    if errors:
        _safe_publish_metric("EntityResolutionErrors", errors)

    return {"processed": processed, "skipped": skipped, "errors": errors}


def _parse_record_body(record: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize SQS record body into dict."""
    if not isinstance(record, dict):
        return None
    body = record.get("body")
    if isinstance(body, dict):
        return body
    parsed = parse_json_container(body)
    if isinstance(parsed, dict):
        return parsed
    return None


def _expand_message_to_candidates(message: dict[str, Any]) -> list[EntityCandidate]:
    """Expand incoming message into candidate list."""
    message_type = str(message.get("message_type") or "").strip()
    if message_type == "entity_resolution_document_request" or isinstance(
        message.get("entity_candidates"), list
    ):
        return _process_document_message(message)
    raise ValueError("Only document-level entity resolution messages are supported.")


def _process_document_message(message: dict[str, Any]) -> list[EntityCandidate]:
    """Convert document-level message into candidate list."""
    source = str(message.get("source") or "").strip()
    if source not in ALLOWED_SOURCES:
        raise ValueError(f"Unsupported source: {source}")

    tenant_id = str(message.get("tenant_id") or "")
    if not tenant_id:
        raise ValueError("tenant_id is required")

    source_item_id = str(message.get("source_item_id") or message.get("item_id") or "")
    if not source_item_id:
        raise ValueError("source_item_id is required")

    entity_candidates = message.get("entity_candidates")
    if not isinstance(entity_candidates, list):
        raise ValueError("entity_candidates is required")

    analysis_id = str(message.get("analysis_id") or "")
    source_title = str(message.get("source_title") or "")
    extracted_at = str(message.get("requested_at") or message.get("extracted_at") or "")
    default_extraction_source = str(message.get("extraction_source") or "ner")
    document_lineage_id = str(message.get("lineage_id") or "")

    expanded: list[EntityCandidate] = []
    for entry in entity_candidates:
        if not isinstance(entry, dict):
            continue
        surface_form = str(entry.get("surface_form") or entry.get("text") or "").strip()
        if not surface_form:
            continue
        label = str(entry.get("ner_label") or entry.get("label") or entry.get("entity_type") or "")
        context_snippet = str(entry.get("context_snippet") or "")
        payload = {
            "message_type": "entity_resolution_request",
            "source": source,
            "tenant_id": tenant_id,
            "source_item_id": source_item_id,
            "candidate_id": str(entry.get("candidate_id") or uuid.uuid4()),
            "surface_form": surface_form,
            "normalized_form": str(entry.get("normalized_form") or surface_form),
            "entity_type": str(
                entry.get("entity_type")
                or _map_label_to_entity_type(
                    label,
                    surface_form=surface_form,
                    context_snippet=context_snippet,
                )
            ),
            "pii_flag": bool(entry.get("pii_flag", False)),
            "pii_category": str(entry.get("pii_category") or entry.get("pii_type") or ""),
            "extraction_source": str(entry.get("extraction_source") or default_extraction_source),
            "confidence": float(entry.get("confidence", 0.0)),
            "mention_count": int(entry.get("mention_count", 1)),
            "context_snippet": context_snippet,
            "ner_label": label,
            "language": str(entry.get("language") or ""),
            "source_title": str(entry.get("source_title") or source_title),
            "extracted_at": str(entry.get("extracted_at") or extracted_at),
            "analysis_id": str(entry.get("analysis_id") or analysis_id),
            "lineage_id": str(entry.get("lineage_id") or document_lineage_id),
        }
        expanded.append(_process_message(payload))
    return expanded


def _process_message(message: dict[str, Any]) -> EntityCandidate:
    """Validate and normalize into EntityCandidate."""
    source = str(message.get("source") or "").strip()
    if source not in ALLOWED_SOURCES:
        raise ValueError(f"Unsupported source: {source}")

    extraction_source = str(message.get("extraction_source") or "").strip()
    if extraction_source not in ALLOWED_EXTRACTION_SOURCES:
        raise ValueError(f"Unsupported extraction_source: {extraction_source}")

    tenant_id = str(message.get("tenant_id") or "")
    if not tenant_id:
        raise ValueError("tenant_id is required")

    source_item_id = str(message.get("source_item_id") or message.get("item_id") or "")
    if not source_item_id:
        raise ValueError("source_item_id is required")

    candidate_id = str(message.get("candidate_id") or "")
    if not candidate_id:
        raise ValueError("candidate_id is required")

    return EntityCandidate(
        candidate_id=candidate_id,
        tenant_id=tenant_id,
        source_item_id=source_item_id,
        surface_form=str(message.get("surface_form") or ""),
        normalized_form=str(
            message.get("normalized_form") or message.get("surface_form") or ""
        ),
        entity_type=str(message.get("entity_type") or "concept"),
        pii_flag=bool(message.get("pii_flag", False)),
        extraction_source=extraction_source,
        confidence=float(message.get("confidence", 0.0)),
        mention_count=int(message.get("mention_count", 1)),
        context_snippet=str(message.get("context_snippet") or ""),
        ner_label=str(message.get("ner_label") or ""),
        language=str(message.get("language") or ""),
        source_title=str(message.get("source_title") or ""),
        extracted_at=str(message.get("requested_at") or message.get("extracted_at") or ""),
        pii_category=str(message.get("pii_category") or ""),
        analysis_id=str(message.get("analysis_id") or ""),
        lineage_id=str(message.get("lineage_id") or ""),
        source=source,
    )

def _resolve_entity(candidate: EntityCandidate) -> dict[str, str]:
    """Resolve entity into DynamoDB projection table."""
    entity_id = _stable_entity_id(candidate.entity_type, candidate.normalized_form)
    table = _get_entity_master_table()
    if table is None:
        return {"action": "skipped", "entity_id": entity_id}

    existing = table.get_item(Key={"tenant_id": candidate.tenant_id, "entity_id": entity_id}).get("Item")
    existing_mention_count = int((existing or {}).get("mention_count") or 0)
    mention_count = existing_mention_count + max(1, int(candidate.mention_count))
    existing_confidence = _to_float((existing or {}).get("confidence"), default=0.0)
    confidence = max(existing_confidence, float(candidate.confidence))
    now = datetime.now(TOKYO_TZ).isoformat()

    table.put_item(
        Item={
            "tenant_id": candidate.tenant_id,
            "entity_id": entity_id,
            "canonical_value": candidate.normalized_form,
            "entity_type": candidate.entity_type,
            "pii_flag": bool(candidate.pii_flag),
            "confidence": Decimal(str(confidence)),
            "status": "active",
            "extraction_source": _normalize_extraction_source_for_db(candidate.extraction_source),
            "mention_count": mention_count,
            "first_seen_at": str((existing or {}).get("first_seen_at") or now),
            "updated_at": now,
        }
    )
    if existing:
        _safe_publish_metric("ExistingEntitiesMatched", 1)
        return {"action": "matched", "entity_id": entity_id}

    _safe_publish_metric("NewEntitiesCreated", 1)
    return {"action": "created", "entity_id": entity_id}


def _record_lineage_for_resolution(candidate: EntityCandidate, result: dict[str, str]) -> None:
    function_name = os.environ.get("LINEAGE_FUNCTION_NAME", "")
    if not function_name:
        return
    record_lineage_event(
        function_name=function_name,
        lineage_id=candidate.lineage_id or str(uuid.uuid4()),
        job_name="entityResolver",
        input_dataset=f"EntityResolutionQueue/{candidate.tenant_id}/{candidate.source_item_id}",
        output_dataset=f"DynamoDB/EntityMasterProjection/{candidate.tenant_id}/{result['entity_id']}",
        event_type="COMPLETE",
        metadata={
            "action": result["action"],
            "candidate_id": candidate.candidate_id,
            "analysis_id": candidate.analysis_id,
        },
        tenant_id=candidate.tenant_id,
    )


def _normalize_extraction_source_for_db(extraction_source: str) -> str:
    if extraction_source == "governance+ner":
        return "governance"
    return extraction_source


def _stable_entity_id(entity_type: str, normalized_form: str) -> str:
    canonical_hash = compute_canonical_hash(normalized_form)
    return f"{entity_type}_{canonical_hash[:24]}"


def _map_label_to_entity_type(
    label: str,
    *,
    surface_form: str = "",
    context_snippet: str = "",
) -> str:
    return OntologyInferenceService().infer_entity_type(
        surface_form=str(surface_form or ""),
        ner_label=str(label or ""),
        context_snippet=str(context_snippet or ""),
    )


def _to_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_dynamodb_resource() -> Any:
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    return _dynamodb_resource


def _get_entity_master_table() -> Any | None:
    table_name = os.environ.get("ENTITY_MASTER_TABLE", "").strip()
    if not table_name:
        return None
    return _get_dynamodb_resource().Table(table_name)


def _safe_publish_metric(metric_name: str, value: float) -> None:
    try:
        publish_metric(metric_name, value)
    except ClientError as exc:
        log_structured(
            "WARN",
            "CloudWatch metric publish failed",
            metric_name=metric_name,
            value=value,
            error=str(exc),
        )
