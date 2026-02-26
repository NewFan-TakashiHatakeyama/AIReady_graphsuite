from __future__ import annotations

from src.models.unified_metadata import UnifiedMetadata


def test_extended_fields_roundtrip() -> None:
    model = UnifiedMetadata(
        tenant_id="tenant-1",
        item_id="item-1",
        title="spec.docx",
        content_type="application/vnd.openxmlformats",
        author="alice",
        last_modified="2026-02-25T00:00:00Z",
        access_scope="organization",
        document_summary="要約です",
        summary_language="ja",
        topic_keywords=["設計", "要件"],
        embedding_ref="tenant-1/item-1",
        analysis_id="analysis-1",
        summary_generated_at="2026-02-25T01:00:00Z",
    )

    item = model.to_dynamodb_item()
    restored = UnifiedMetadata.from_dynamodb_item(item)

    assert restored.document_summary == "要約です"
    assert restored.summary_language == "ja"
    assert restored.topic_keywords == ["設計", "要件"]
    assert restored.embedding_ref == "tenant-1/item-1"
    assert restored.analysis_id == "analysis-1"
    assert restored.summary_generated_at == "2026-02-25T01:00:00Z"


def test_extended_fields_defaults_when_absent() -> None:
    restored = UnifiedMetadata.from_dynamodb_item(
        {
            "tenant_id": "tenant-1",
            "item_id": "item-1",
        }
    )
    assert restored.document_summary == ""
    assert restored.summary_language == ""
    assert restored.topic_keywords == []
    assert restored.embedding_ref == ""
    assert restored.analysis_id == ""
    assert restored.summary_generated_at == ""
