from __future__ import annotations

import json
import pytest

from src.handlers import entity_resolver


def _candidate_message(**overrides) -> dict:
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
    monkeypatch.setattr(
        entity_resolver,
        "_resolve_entity",
        lambda candidate: {"action": "created", "entity_id": "ent-1"},
    )
    monkeypatch.setattr(
        entity_resolver, "_record_lineage_for_resolution", lambda candidate, result: None
    )
    monkeypatch.setattr(
        entity_resolver,
        "publish_metric",
        lambda name, value, **kwargs: metrics.append((name, value)),
    )
    body = {
        "message_type": "entity_resolution_document_request",
        "source": "document_analysis",
        "tenant_id": "tenant-1",
        "source_item_id": "item-1",
        "analysis_id": "an-1",
        "entity_candidates": [
            {
                "candidate_id": "cand-1",
                "surface_form": "田中太郎",
                "normalized_form": "タナカタロウ",
                "entity_type": "person",
                "pii_flag": True,
                "extraction_source": "governance+ner",
                "confidence": 0.95,
                "mention_count": 3,
                "language": "ja",
            }
        ],
    }
    result = entity_resolver.handler(
        {"Records": [{"body": json.dumps(body)}]},
        None,
    )
    assert result == {"processed": 1, "skipped": 0, "errors": 0}
    assert ("EntityResolverProcessed", 1) in metrics


def test_pii_flag_true_is_preserved() -> None:
    candidate = entity_resolver._process_message(_candidate_message(pii_flag=True))
    assert candidate.pii_flag is True
    assert candidate.entity_type == "person"


def test_extraction_source_variants_supported() -> None:
    for source in (
        "governance+ner",
        "ner",
        "governance",
        "connect_metadata",
    ):
        candidate = entity_resolver._process_message(
            _candidate_message(extraction_source=source)
        )
        assert candidate.extraction_source == source


def test_candidate_level_message_is_rejected() -> None:
    body = _candidate_message()
    with pytest.raises(ValueError):
        entity_resolver._expand_message_to_candidates(body)


def test_handler_skips_non_document_message(monkeypatch) -> None:
    monkeypatch.setattr(entity_resolver, "publish_metric", lambda *args, **kwargs: None)

    result = entity_resolver.handler(
        {"Records": [{"body": json.dumps(_candidate_message())}]},
        None,
    )
    assert result == {"processed": 0, "skipped": 1, "errors": 0}


def test_handler_invalid_json_body_is_skipped_not_error(monkeypatch) -> None:
    monkeypatch.setattr(entity_resolver, "publish_metric", lambda *args, **kwargs: None)

    result = entity_resolver.handler(
        {"Records": [{"body": "{invalid-json"}]},
        None,
    )
    assert result == {"processed": 0, "skipped": 1, "errors": 0}
