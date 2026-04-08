from __future__ import annotations

import copy

from src.handlers import schema_transform


class _Table:
    def __init__(self):
        self.items: dict[tuple[str, str], dict] = {}

    def put_item(self, Item):
        key = (Item["tenant_id"], Item["item_id"])
        self.items[key] = copy.deepcopy(Item)
        return {}

    def get_item(self, Key):
        key = (Key["tenant_id"], Key["item_id"])
        item = self.items.get(key)
        return {"Item": copy.deepcopy(item)} if item else {}


class _Dynamo:
    def __init__(self, mapping):
        self._mapping = mapping

    def Table(self, name):
        return self._mapping[name]


def _stream_record(*, event_name: str = "INSERT", new_image: dict | None = None) -> dict:
    return {
        "eventName": event_name,
        "eventSourceARN": "arn:aws:dynamodb:ap-northeast-1:123456789012:table/FileMetadata-tenant-1/stream/abc",
        "dynamodb": {
            "Keys": {
                "tenant_id": {"S": "tenant-1"},
                "item_id": {"S": "item-1"},
            },
            "NewImage": new_image or {},
        },
    }


def _default_new_image(**overrides) -> dict:
    image = {
        "tenant_id": {"S": "tenant-1"},
        "item_id": {"S": "item-1"},
        "name": {"S": "設計書.docx"},
        "mime_type": {
            "S": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        },
        "created_by": {"S": "alice"},
        "modified_at": {"S": "2026-02-20T00:00:00+00:00"},
        "sharing_scope": {"S": "organization"},
        "permissions": {"S": '{"roles":["reader"]}'},
        "sharepoint_ids": {"S": '{"site_id":"s-1","list_id":"l-1"}'},
        "drive_id": {"S": "d-1"},
        "web_url": {"S": "https://example.local/doc/1"},
        "size": {"N": "120"},
        "path": {"S": "/docs/設計書.docx"},
        "governance_flags": {"SS": ["GOV-002"]},
        "is_deleted": {"BOOL": False},
        "is_folder": {"BOOL": False},
    }
    image.update(overrides)
    return image


def _setup(monkeypatch):
    unified = _Table()
    doc_analysis = _Table()
    dyn = _Dynamo({"unified": unified, "analysis": doc_analysis})
    monkeypatch.setenv("UNIFIED_METADATA_TABLE", "unified")
    monkeypatch.setenv("DOCUMENT_ANALYSIS_TABLE", "analysis")
    monkeypatch.setenv("GOVERNANCE_FINDING_TABLE", "finding")
    monkeypatch.setenv("LINEAGE_FUNCTION_NAME", "lineage-recorder")
    monkeypatch.setattr(schema_transform, "_dynamodb_resource", dyn)
    monkeypatch.setattr(
        schema_transform,
        "get_document_analysis",
        lambda tenant_id, item_id: doc_analysis.get_item(
            Key={"tenant_id": tenant_id, "item_id": item_id}
        ).get("Item"),
    )

    lineage_calls = []
    metrics = []
    monkeypatch.setattr(
        schema_transform,
        "record_lineage_event",
        lambda **kwargs: lineage_calls.append(kwargs) or {"status": "ok"},
    )
    monkeypatch.setattr(
        schema_transform,
        "publish_metric",
        lambda name, value, **kwargs: metrics.append((name, value)),
    )
    return unified, doc_analysis, lineage_calls, metrics


def _governance_low(**overrides) -> dict:
    base = {
        "risk_level": "low",
        "pii_detected": False,
        "ai_eligible": True,
        "status": "open",
        "finding_id": "f-low",
        "classification": "internal",
    }
    base.update(overrides)
    return base


def test_ut_st_001_insert_normal_transform(monkeypatch) -> None:
    unified, _, lineage_calls, _ = _setup(monkeypatch)
    monkeypatch.setattr(
        schema_transform,
        "lookup_governance_finding",
        lambda **kwargs: _governance_low(),
    )

    result = schema_transform.handler(
        {"Records": [_stream_record(new_image=_default_new_image())]},
        context=None,
    )

    assert result == {"processed": 1, "errors": 0}
    item = unified.items[("tenant-1", "item-1")]
    assert item["title"] == "設計書.docx"
    assert item["classification"] == "internal"
    assert item["freshness_status"] in {"active", "aging", "stale"}
    assert lineage_calls[0]["job_name"] == "schemaTransform"


def test_ut_st_002_modify_updates_transform(monkeypatch) -> None:
    unified, _, _, _ = _setup(monkeypatch)
    monkeypatch.setattr(
        schema_transform,
        "lookup_governance_finding",
        lambda **kwargs: _governance_low(),
    )

    schema_transform.handler(
        {
            "Records": [
                _stream_record(
                    event_name="MODIFY",
                    new_image=_default_new_image(name={"S": "更新後.docx"}),
                )
            ]
        },
        context=None,
    )
    assert unified.items[("tenant-1", "item-1")]["title"] == "更新後.docx"


def test_ut_st_003_remove_event_delete(monkeypatch) -> None:
    unified, _, _, _ = _setup(monkeypatch)
    unified.put_item(Item={"tenant_id": "tenant-1", "item_id": "item-1", "title": "x"})

    schema_transform.handler({"Records": [_stream_record(event_name="REMOVE")]}, None)

    item = unified.items[("tenant-1", "item-1")]
    assert item["is_deleted"] is True
    assert item["ttl"] > 0


def test_ut_st_004_is_deleted_true_delete(monkeypatch) -> None:
    unified, _, _, _ = _setup(monkeypatch)
    schema_transform.handler(
        {
            "Records": [
                _stream_record(
                    new_image=_default_new_image(is_deleted={"BOOL": True}),
                )
            ]
        },
        None,
    )
    assert unified.items[("tenant-1", "item-1")]["is_deleted"] is True


def test_ut_st_005_is_folder_skip(monkeypatch) -> None:
    unified, _, lineage_calls, _ = _setup(monkeypatch)
    schema_transform.handler(
        {
            "Records": [
                _stream_record(
                    new_image=_default_new_image(is_folder={"BOOL": True}),
                )
            ]
        },
        None,
    )
    assert ("tenant-1", "item-1") not in unified.items
    assert lineage_calls == []


def test_ut_st_006_risk_high_forces_ai_ineligible(monkeypatch) -> None:
    unified, _, _, _ = _setup(monkeypatch)
    monkeypatch.setattr(
        schema_transform,
        "lookup_governance_finding",
        lambda **kwargs: {
            "risk_level": "high",
            "pii_detected": True,
            "ai_eligible": True,
            "status": "open",
            "finding_id": "f-1",
            "classification": "confidential",
        },
    )
    schema_transform.handler({"Records": [_stream_record(new_image=_default_new_image())]}, None)
    assert ("tenant-1", "item-1") not in unified.items


def test_ut_st_007_no_finding_defaults(monkeypatch) -> None:
    unified, _, _, _ = _setup(monkeypatch)
    monkeypatch.setattr(
        schema_transform,
        "lookup_governance_finding",
        lambda **kwargs: {
            **schema_transform.DEFAULT_GOVERNANCE_RESULT,
            "status": "open",
            "ai_eligible": True,
        },
    )
    schema_transform.handler(
        {"Records": [_stream_record(new_image=_default_new_image())]},
        None,
    )
    assert ("tenant-1", "item-1") in unified.items


def test_ut_st_008_finding_error_uses_defaults_closed_low_still_ingests_docx(monkeypatch) -> None:
    """Finding 取得失敗時は DEFAULT（closed/low）へ。closed+low はカタログ適格のため取り込める。"""
    unified, _, _, _ = _setup(monkeypatch)
    monkeypatch.setattr(
        schema_transform,
        "lookup_governance_finding",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("failed")),
    )
    schema_transform.handler(
        {"Records": [_stream_record(new_image=_default_new_image())]},
        None,
    )
    assert ("tenant-1", "item-1") in unified.items


def test_ut_st_009_risk_critical_forces_ai_ineligible(monkeypatch) -> None:
    unified, _, _, _ = _setup(monkeypatch)
    monkeypatch.setattr(
        schema_transform,
        "lookup_governance_finding",
        lambda **kwargs: {
            "risk_level": "critical",
            "pii_detected": True,
            "ai_eligible": True,
            "status": "open",
            "finding_id": "f-2",
            "classification": "top-secret",
        },
    )
    schema_transform.handler(
        {"Records": [_stream_record(new_image=_default_new_image())]},
        None,
    )
    assert ("tenant-1", "item-1") not in unified.items


def test_ut_st_010_freshness_status_calculated(monkeypatch) -> None:
    unified, _, _, _ = _setup(monkeypatch)
    monkeypatch.setattr(
        schema_transform,
        "lookup_governance_finding",
        lambda **kwargs: _governance_low(),
    )
    schema_transform.handler(
        {
            "Records": [
                _stream_record(
                    new_image=_default_new_image(
                        modified_at={"S": "2020-01-01T00:00:00+00:00"}
                    )
                )
            ]
        },
        None,
    )
    assert unified.items[("tenant-1", "item-1")]["freshness_status"] == "stale"


def test_ut_st_011_document_analysis_completed_enriched(monkeypatch) -> None:
    unified, doc_analysis, _, _ = _setup(monkeypatch)
    monkeypatch.setattr(
        schema_transform,
        "lookup_governance_finding",
        lambda **kwargs: _governance_low(),
    )
    doc_analysis.put_item(
        Item={
            "tenant_id": "tenant-1",
            "item_id": "item-1",
            "analysis_status": "completed",
            "document_summary": "summary text",
            "summary_language": "ja",
            "topic_keywords": ["設計", "要件"],
            "embedding_ref": "tenant-1/item-1",
            "analysis_id": "analysis-1",
            "summary_generated_at": "2026-02-25T00:00:00+00:00",
        }
    )
    schema_transform.handler(
        {"Records": [_stream_record(new_image=_default_new_image())]},
        None,
    )
    item = unified.items[("tenant-1", "item-1")]
    assert item["document_summary"] == "summary text"
    assert item["embedding_ref"] == "tenant-1/item-1"
    assert "document_profile" in str(item.get("extensions") or "")


def test_ut_st_012_png_is_excluded_even_when_risk_low(monkeypatch) -> None:
    unified, _, _, _ = _setup(monkeypatch)
    monkeypatch.setattr(schema_transform, "lookup_governance_finding", lambda **kwargs: _governance_low())
    schema_transform.handler(
        {
            "Records": [
                _stream_record(
                    new_image=_default_new_image(
                        name={"S": "diagram.png"},
                        mime_type={"S": "image/png"},
                        path={"S": "/images/diagram.png"},
                    )
                )
            ]
        },
        None,
    )
    assert ("tenant-1", "item-1") not in unified.items


def test_ut_st_013_closed_low_finding_allows_ontology_ingest(monkeypatch) -> None:
    """closed かつ低リスクは is_eligible_finding_status_for_ontology で適格（カタログ用途）。"""
    unified, _, _, _ = _setup(monkeypatch)
    monkeypatch.setattr(
        schema_transform,
        "lookup_governance_finding",
        lambda **kwargs: _governance_low(status="closed"),
    )
    schema_transform.handler(
        {"Records": [_stream_record(new_image=_default_new_image())]},
        None,
    )
    assert ("tenant-1", "item-1") in unified.items


def test_ut_st_014_completed_low_finding_allows_ontology_ingest(monkeypatch) -> None:
    """是正完了後の completed + low はカタログに upsert される。"""
    unified, _, _, _ = _setup(monkeypatch)
    monkeypatch.setattr(
        schema_transform,
        "lookup_governance_finding",
        lambda **kwargs: _governance_low(status="completed"),
    )
    schema_transform.handler(
        {"Records": [_stream_record(new_image=_default_new_image())]},
        None,
    )
    assert ("tenant-1", "item-1") in unified.items


def test_ut_st_015_in_progress_low_finding_allows_ontology_ingest(monkeypatch) -> None:
    """再スコア済み in_progress + low はストリーム競合時も取り込み可能にする。"""
    unified, _, _, _ = _setup(monkeypatch)
    monkeypatch.setattr(
        schema_transform,
        "lookup_governance_finding",
        lambda **kwargs: _governance_low(status="in_progress"),
    )
    schema_transform.handler(
        {"Records": [_stream_record(new_image=_default_new_image())]},
        None,
    )
    assert ("tenant-1", "item-1") in unified.items
