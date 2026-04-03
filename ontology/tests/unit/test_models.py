from __future__ import annotations

from src.models.entity_candidate import EntityCandidate
from src.models.lineage_event import LineageEvent


def test_entity_candidate_creation() -> None:
    candidate = EntityCandidate(
        candidate_id="c-1",
        tenant_id="tenant-1",
        source_item_id="item-1",
        surface_form="田中太郎",
        normalized_form="タナカタロウ",
        entity_type="person",
        pii_flag=True,
        extraction_source="ner",
        confidence=0.92,
        mention_count=2,
        context_snippet="...",
        ner_label="Person",
        language="ja",
        source_title="report.xlsx",
        extracted_at="2026-02-25T00:00:00Z",
    )
    assert candidate.entity_type == "person"
    assert candidate.pii_flag is True


def test_lineage_event_serialize_roundtrip() -> None:
    event = LineageEvent(
        tenant_id="tenant-1",
        lineage_id="lineage-1",
        event_type="COMPLETE",
        event_time="2026-02-25T00:00:00Z",
        job_namespace="ai-ready-ontology",
        job_name="schemaTransform",
        run_id="lineage-1",
        inputs=[{"name": "in"}],
        outputs=[{"name": "out"}],
        metadata={"a": 1},
        duration_ms=150,
        status="success",
        ttl=123,
    )
    item = event.to_dynamodb_item()
    restored = LineageEvent.from_dynamodb_item(item)
    assert restored.inputs[0]["name"] == "in"
    assert restored.outputs[0]["name"] == "out"
    assert restored.metadata["a"] == 1
