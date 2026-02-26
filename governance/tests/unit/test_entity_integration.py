from __future__ import annotations

from services.entity_integration import enqueue_entity_candidates, hash_pii, merge_pii_and_ner
from services.ner_pipeline import NERDetectionResult, NEREntity
from services.pii_detector import PIIDetectionResult, PIIEntity


def test_hash_pii():
    assert len(hash_pii("secret")) == 64


def test_merge_pii_and_ner_sets_pii_flag():
    pii = PIIDetectionResult(
        detected=True,
        details=[PIIEntity(type="PERSON_NAME_JA", start=0, end=4, score=0.9)],
    )
    ner = NERDetectionResult(
        entities=[NEREntity(text="田中", label="Person", start=0, end=4, confidence=0.85)],
        noun_chunks=[],
        language="ja",
    )
    merged = merge_pii_and_ner(pii, ner)
    assert len(merged) == 1
    assert merged[0]["pii_flag"] is True
    assert merged[0]["text"] != "田中"


def test_enqueue_entity_candidates(monkeypatch):
    captured = {}

    class _FakeSqs:
        def send_message(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setenv(
        "ENTITY_RESOLUTION_QUEUE_URL",
        "https://sqs.ap-northeast-1.amazonaws.com/123456789012/AIReadyOntology-EntityResolutionQueue.fifo",
    )
    monkeypatch.setattr("services.entity_integration._sqs_client", _FakeSqs())

    enqueue_entity_candidates(
        tenant_id="tenant-001",
        item_id="item-001",
        candidates=[{"text": "hash", "label": "Person", "start": 0, "end": 4, "pii_flag": True}],
    )
    assert captured["MessageGroupId"] == "tenant-001"
    assert "MessageDeduplicationId" in captured
