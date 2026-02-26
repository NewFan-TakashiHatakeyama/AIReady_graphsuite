from __future__ import annotations

import json

from src.handlers import entity_resolver


def _message(**overrides) -> dict:
    payload = {
        "message_type": "entity_resolution_request",
        "source": "document_analysis",
        "tenant_id": "tenant-1",
        "candidate_id": "cand-1",
        "source_item_id": "item-1",
        "surface_form": "田中太郎",
        "normalized_form": "タナカタロウ",
        "entity_type": "person",
        "pii_flag": True,
        "extraction_source": "governance+ner",
        "confidence": 0.95,
        "mention_count": 3,
        "language": "ja",
        "requested_at": "2026-02-25T00:00:00Z",
    }
    payload.update(overrides)
    return payload


def test_integrated_document_analysis_message_processed(monkeypatch) -> None:
    metrics = []
    monkeypatch.setattr(entity_resolver, "get_aurora_connection", lambda: object())
    monkeypatch.setattr(entity_resolver, "_ensure_schema_ready", lambda _conn: None)
    monkeypatch.setattr(entity_resolver, "_get_pii_encryption_key", lambda _tenant: "k")
    monkeypatch.setattr(
        entity_resolver,
        "_resolve_entity",
        lambda conn, candidate, key: {"action": "created", "entity_id": "ent-1"},
    )
    monkeypatch.setattr(
        entity_resolver, "_record_lineage_for_resolution", lambda candidate, result: None
    )
    monkeypatch.setattr(
        entity_resolver,
        "publish_metric",
        lambda name, value, **kwargs: metrics.append((name, value)),
    )
    result = entity_resolver.handler(
        {"Records": [{"body": json.dumps(_message())}]},
        None,
    )
    assert result == {"processed": 1, "skipped": 0, "errors": 0}
    assert ("EntityResolverProcessed", 1) in metrics


def test_pii_flag_true_is_preserved() -> None:
    candidate = entity_resolver._process_message(_message(pii_flag=True))
    assert candidate.pii_flag is True
    assert candidate.entity_type == "person"


def test_extraction_source_variants_supported() -> None:
    for source in (
        "governance+ner",
        "ner",
        "governance",
        "noun_chunk",
        "domain_dict",
        "connect_metadata",
    ):
        candidate = entity_resolver._process_message(_message(extraction_source=source))
        assert candidate.extraction_source == source


def test_backward_compatible_legacy_message_source() -> None:
    legacy_msg = _message(source="noun_extractor")
    legacy_msg.pop("extraction_source")
    candidate = entity_resolver._process_message(legacy_msg)
    assert candidate.extraction_source == "ner"
