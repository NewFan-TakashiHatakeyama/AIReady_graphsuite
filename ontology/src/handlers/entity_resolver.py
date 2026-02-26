"""entityResolver Lambda handler."""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3

from src.models.entity_candidate import EntityCandidate
from src.shared.aurora_client import get_aurora_connection
from src.shared.config import (
    get_ssm_parameter,
    get_tenant_parameter_path,
)
from src.shared.entity_id import compute_canonical_hash, generate_entity_id
from src.shared.lineage_client import record_lineage_event
from src.shared.logger import log_structured
from src.shared.matcher import calculate_match_score
from src.shared.metrics import publish_metric

ALLOWED_EXTRACTION_SOURCES = {
    "governance+ner",
    "ner",
    "governance",
    "noun_chunk",
    "domain_dict",
    "connect_metadata",
}
ALLOWED_SOURCES = {"document_analysis", "noun_extractor", "governance_pii"}
LEGACY_EXTRACTION_SOURCE_BY_SOURCE = {
    "noun_extractor": "ner",
    "governance_pii": "governance",
}

PII_ALIAS_THRESHOLD = 5
_migrations_checked = False

FIND_BY_HASH_SQL = """
SELECT entity_id, entity_type, canonical_hash, pii_flag, extraction_source, status, confidence
FROM ontology.entity_master
WHERE canonical_hash = %s
  AND entity_type = %s
  AND status = 'active'
LIMIT 1;
"""

BLOCKING_CANDIDATES_SQL = """
SELECT entity_id, entity_type, canonical_hash, canonical_value_text, pii_flag, confidence
FROM ontology.entity_master
WHERE entity_type = %s
  AND status = 'active'
  AND LEFT(canonical_hash, 8) = LEFT(%s, 8)
LIMIT 10;
"""

INSERT_ENTITY_SQL = """
INSERT INTO ontology.entity_master (
    entity_id, entity_type, canonical_value, canonical_value_text, canonical_hash,
    pii_flag, pii_category, extraction_source, confidence, mention_count, status,
    created_at, updated_at
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, 'active',
    NOW(), NOW()
)
ON CONFLICT (entity_id) DO NOTHING;
"""

INSERT_PII_ENTITY_SQL = """
INSERT INTO ontology.entity_master (
    entity_id, entity_type, canonical_value, canonical_hash,
    pii_flag, pii_category, extraction_source, confidence, mention_count, status,
    created_at, updated_at
) VALUES (
    %s, %s, pgp_sym_encrypt(%s, %s), %s,
    true, %s, %s, %s, %s, 'active',
    NOW(), NOW()
)
ON CONFLICT (entity_id) DO NOTHING;
"""

INSERT_ALIAS_SQL = """
INSERT INTO ontology.entity_aliases (
    alias_id, entity_id, alias_value, alias_value_text, alias_hash,
    alias_type, source_system, source_document_id, confidence, created_at
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, NOW()
)
ON CONFLICT (alias_hash, entity_id) DO NOTHING;
"""

INSERT_PII_ALIAS_SQL = """
INSERT INTO ontology.entity_aliases (
    alias_id, entity_id, alias_value, alias_hash,
    alias_type, source_system, source_document_id, confidence, created_at
) VALUES (
    %s, %s, pgp_sym_encrypt(%s, %s), %s,
    %s, %s, %s, %s, NOW()
)
ON CONFLICT (alias_hash, entity_id) DO NOTHING;
"""

UPDATE_MATCHED_ENTITY_SQL = """
UPDATE ontology.entity_master
SET confidence = GREATEST(confidence, %s),
    mention_count = mention_count + %s,
    updated_at = NOW()
WHERE entity_id = %s;
"""

COUNT_ALIASES_SQL = """
SELECT COUNT(*) AS alias_count
FROM ontology.entity_aliases
WHERE entity_id = %s;
"""

INSERT_AUDIT_LOG_SQL = """
INSERT INTO ontology.entity_audit_log (log_id, entity_id, action, actor, detail, created_at)
VALUES (%s, %s, %s, %s, %s::jsonb, NOW());
"""


def handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    """Entry point for SQS events."""
    del context
    conn = get_aurora_connection()
    _ensure_schema_ready(conn)
    processed = 0
    skipped = 0
    errors = 0

    for record in event.get("Records", []):
        try:
            body = _parse_record_body(record)
            if not body:
                skipped += 1
                continue
            candidate = _process_message(body)
            encryption_key = _get_pii_encryption_key(candidate.tenant_id)
            result = _resolve_entity(conn, candidate, encryption_key)
            _record_lineage_for_resolution(candidate, result)
            processed += 1
            _safe_publish_metric("EntityResolverProcessed", 1)
        except ValueError as exc:
            skipped += 1
            log_structured(
                "WARN", "Skipped unsupported entity resolver message", error=str(exc)
            )
        except Exception as exc:  # pragma: no cover - defensive fallback
            errors += 1
            log_structured(
                "ERROR", "Entity resolver failed to process message", error=str(exc)
            )

    _safe_publish_metric("EntityResolutionMessagesProcessed", processed)
    if skipped:
        _safe_publish_metric("EntityResolutionMessagesSkipped", skipped)
    if errors:
        _safe_publish_metric("EntityResolutionErrors", errors)

    return {"processed": processed, "skipped": skipped, "errors": errors}


def _parse_record_body(record: dict[str, Any]) -> dict[str, Any] | None:
    body = record.get("body")
    if isinstance(body, dict):
        return body
    if isinstance(body, str) and body:
        parsed = json.loads(body)
        if isinstance(parsed, dict):
            return parsed
    return None


def _process_message(message: dict[str, Any]) -> EntityCandidate:
    """Normalize integrated or legacy message into EntityCandidate."""
    source = str(message.get("source") or "").strip()
    if source not in ALLOWED_SOURCES:
        raise ValueError(f"Unsupported source: {source}")

    raw_extraction_source = str(message.get("extraction_source") or "").strip()
    extraction_source = raw_extraction_source
    if not extraction_source:
        extraction_source = LEGACY_EXTRACTION_SOURCE_BY_SOURCE.get(source, "")
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


def _resolve_entity(
    conn: Any, candidate: EntityCandidate, encryption_key: str
) -> dict[str, str]:
    canonical_hash = compute_canonical_hash(candidate.normalized_form)
    start = time.perf_counter()
    try:
        with conn.cursor() as cur:
            existing = _find_existing_by_hash(
                cur=cur,
                canonical_hash=canonical_hash,
                entity_type=candidate.entity_type,
            )
            if not existing:
                blocking_candidates = _find_blocking_candidates(
                    cur=cur,
                    canonical_hash=canonical_hash,
                    entity_type=candidate.entity_type,
                )
                existing = _pick_best_blocking_match(
                    candidate=candidate,
                    blocking_candidates=blocking_candidates,
                )

            if existing:
                _add_alias(
                    cur=cur,
                    entity_id=str(existing["entity_id"]),
                    candidate=candidate,
                    encryption_key=encryption_key,
                )
                _update_entity_after_match(cur=cur, entity_id=str(existing["entity_id"]), candidate=candidate)
                _insert_audit_log(
                    cur=cur,
                    entity_id=str(existing["entity_id"]),
                    action="ALIAS_ADDED",
                    detail={
                        "candidate_id": candidate.candidate_id,
                        "source_item_id": candidate.source_item_id,
                    },
                )
                conn.commit()
                if candidate.pii_flag:
                    check_pii_aggregation_alert(
                        conn=conn,
                        entity_id=str(existing["entity_id"]),
                        entity_type=candidate.entity_type,
                        tenant_id=candidate.tenant_id,
                    )
                    _safe_publish_metric("PIIEntitiesRegistered", 1)
                _safe_publish_metric("ExistingEntitiesMatched", 1)
                _safe_publish_metric("AliasesAdded", 1)
                return {"action": "matched", "entity_id": str(existing["entity_id"])}

            entity_id = _create_entity(cur=cur, candidate=candidate, encryption_key=encryption_key)
            _add_alias(
                cur=cur,
                entity_id=entity_id,
                candidate=candidate,
                encryption_key=encryption_key,
            )
            _insert_audit_log(
                cur=cur,
                entity_id=entity_id,
                action="ENTITY_CREATED",
                detail={
                    "candidate_id": candidate.candidate_id,
                    "source_item_id": candidate.source_item_id,
                },
            )
            conn.commit()
            if candidate.pii_flag:
                check_pii_aggregation_alert(
                    conn=conn,
                    entity_id=entity_id,
                    entity_type=candidate.entity_type,
                    tenant_id=candidate.tenant_id,
                )
                _safe_publish_metric("PIIEntitiesRegistered", 1)
            _safe_publish_metric("NewEntitiesCreated", 1)
            _safe_publish_metric("AliasesAdded", 1)
            return {"action": "created", "entity_id": entity_id}
    except Exception:
        conn.rollback()
        raise
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        _safe_publish_metric("AuroraQueryMs", elapsed_ms)


def _find_existing_by_hash(*, cur: Any, canonical_hash: str, entity_type: str) -> dict[str, Any] | None:
    cur.execute(FIND_BY_HASH_SQL, (canonical_hash, entity_type))
    row = cur.fetchone()
    return row if isinstance(row, dict) else None


def _find_blocking_candidates(
    *, cur: Any, canonical_hash: str, entity_type: str
) -> list[dict[str, Any]]:
    cur.execute(BLOCKING_CANDIDATES_SQL, (entity_type, canonical_hash))
    rows = cur.fetchall() or []
    return [r for r in rows if isinstance(r, dict)]


def _pick_best_blocking_match(
    *, candidate: EntityCandidate, blocking_candidates: list[dict[str, Any]]
) -> dict[str, Any] | None:
    threshold = _get_match_threshold_default()
    best: dict[str, Any] | None = None
    best_score = 0.0
    for row in blocking_candidates:
        existing_form = str(row.get("canonical_value_text") or "")
        if not existing_form:
            continue
        score = calculate_match_score(
            candidate_form=candidate.normalized_form,
            existing_form=existing_form,
            entity_type=candidate.entity_type,
        )
        if score > best_score:
            best_score = score
            best = row
    if best and best_score >= threshold:
        return best
    return None


def _create_entity(*, cur: Any, candidate: EntityCandidate, encryption_key: str) -> str:
    entity_id = generate_entity_id(candidate.entity_type)
    canonical_hash = compute_canonical_hash(candidate.normalized_form)
    extraction_source = _normalize_extraction_source_for_db(candidate.extraction_source)
    mention_count = max(1, int(candidate.mention_count))

    if candidate.pii_flag:
        if not encryption_key:
            raise ValueError("PII encryption key is required for pii_flag=true entities")
        cur.execute(
            INSERT_PII_ENTITY_SQL,
            (
                entity_id,
                candidate.entity_type,
                candidate.normalized_form,
                encryption_key,
                canonical_hash,
                candidate.pii_category or None,
                extraction_source,
                float(candidate.confidence),
                mention_count,
            ),
        )
        return entity_id

    canonical_bytes = candidate.normalized_form.encode("utf-8")
    cur.execute(
        INSERT_ENTITY_SQL,
        (
            entity_id,
            candidate.entity_type,
            canonical_bytes,
            candidate.normalized_form,
            canonical_hash,
            False,
            None,
            extraction_source,
            float(candidate.confidence),
            mention_count,
        ),
    )
    return entity_id


def _add_alias(*, cur: Any, entity_id: str, candidate: EntityCandidate, encryption_key: str) -> None:
    alias_hash = compute_canonical_hash(candidate.normalized_form)
    alias_id = f"alias_{uuid.uuid4().hex[:24]}"
    alias_type = "surface_form"
    source_system = candidate.source

    if candidate.pii_flag:
        if not encryption_key:
            raise ValueError("PII encryption key is required for pii_flag=true aliases")
        cur.execute(
            INSERT_PII_ALIAS_SQL,
            (
                alias_id,
                entity_id,
                candidate.surface_form or candidate.normalized_form,
                encryption_key,
                alias_hash,
                alias_type,
                source_system,
                candidate.source_item_id,
                float(candidate.confidence),
            ),
        )
        return

    alias_bytes = (candidate.surface_form or candidate.normalized_form).encode("utf-8")
    alias_text = candidate.surface_form or candidate.normalized_form
    cur.execute(
        INSERT_ALIAS_SQL,
        (
            alias_id,
            entity_id,
            alias_bytes,
            alias_text,
            alias_hash,
            alias_type,
            source_system,
            candidate.source_item_id,
            float(candidate.confidence),
        ),
    )


def _update_entity_after_match(*, cur: Any, entity_id: str, candidate: EntityCandidate) -> None:
    cur.execute(
        UPDATE_MATCHED_ENTITY_SQL,
        (float(candidate.confidence), max(1, int(candidate.mention_count)), entity_id),
    )


def _insert_audit_log(*, cur: Any, entity_id: str, action: str, detail: dict[str, Any]) -> None:
    cur.execute(
        INSERT_AUDIT_LOG_SQL,
        (
            f"log_{uuid.uuid4().hex[:24]}",
            entity_id,
            action,
            "entityResolver",
            json.dumps(detail, ensure_ascii=False),
        ),
    )


def check_pii_aggregation_alert(
    *, conn: Any, entity_id: str, entity_type: str, tenant_id: str
) -> None:
    with conn.cursor() as cur:
        cur.execute(COUNT_ALIASES_SQL, (entity_id,))
        row = cur.fetchone()

    alias_count = 0
    if isinstance(row, dict):
        alias_count = int(row.get("alias_count") or 0)
    elif isinstance(row, (tuple, list)) and row:
        alias_count = int(row[0])
    if alias_count < PII_ALIAS_THRESHOLD:
        return

    topic_arn = os.environ.get("ALERT_TOPIC_ARN", "")
    if not topic_arn:
        return

    try:
        boto3.client("sns").publish(
            TopicArn=topic_arn,
            Subject=f"[AI Ready Ontology] PII aggregation alert: {entity_type}",
            Message=json.dumps(
                {
                    "alert_type": "pii_aggregation",
                    "entity_id": entity_id,
                    "entity_type": entity_type,
                    "alias_count": alias_count,
                    "threshold": PII_ALIAS_THRESHOLD,
                    "tenant_id": tenant_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            ),
        )
        _safe_publish_metric("PIIAggregationAlertFired", 1)
    except Exception as exc:  # pragma: no cover - alert should not break flow
        log_structured("WARN", "Failed to publish pii aggregation alert", error=str(exc))


def _record_lineage_for_resolution(
    candidate: EntityCandidate, result: dict[str, str]
) -> None:
    function_name = os.environ.get("LINEAGE_FUNCTION_NAME", "")
    if not function_name:
        return
    record_lineage_event(
        function_name=function_name,
        lineage_id=candidate.lineage_id or str(uuid.uuid4()),
        job_name="entityResolver",
        input_dataset=f"EntityResolutionQueue/{candidate.tenant_id}/{candidate.source_item_id}",
        output_dataset=f"Aurora/entity_master/{result['entity_id']}",
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


def _get_match_threshold_default() -> float:
    value = os.environ.get("ENTITY_MATCH_THRESHOLD", "0.85")
    try:
        return float(value)
    except ValueError:
        return 0.85


def _get_pii_encryption_key(tenant_id: str) -> str:
    raw = os.environ.get("PII_ENCRYPTION_KEY_PARAM", "").strip()
    if not raw:
        return ""
    if "{" in raw:
        param_name = raw.format(tenant_id=tenant_id)
    elif raw.startswith("/"):
        param_name = raw
    else:
        param_name = get_tenant_parameter_path(tenant_id, raw)
    return get_ssm_parameter(param_name, with_decryption=True)


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


def _ensure_schema_ready(conn: Any) -> None:
    global _migrations_checked
    if _migrations_checked:
        return

    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('ontology.entity_master')")
        row = cur.fetchone()
    exists = False
    if isinstance(row, (tuple, list)):
        exists = bool(row[0])
    elif isinstance(row, dict):
        exists = bool(next(iter(row.values()), None))

    if exists:
        _migrations_checked = True
        return

    migration_dir = Path("/var/task/db/migrations")
    files = sorted(migration_dir.glob("*.sql"))
    if not files:
        raise RuntimeError("No migration files found in Lambda package")

    for file in files:
        sql = file.read_text(encoding="utf-8")
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        log_structured("INFO", "Applied migration", file=file.name)

    _migrations_checked = True
