from __future__ import annotations

from src.handlers import entity_resolver
from src.models.entity_candidate import EntityCandidate


def test_process_document_message_passes_surface_and_context_to_llm_mapper(monkeypatch):
    captured = {}

    def _stub_mapper(label: str, *, surface_form: str = "", context_snippet: str = "") -> str:
        captured["label"] = label
        captured["surface_form"] = surface_form
        captured["context_snippet"] = context_snippet
        return "project"

    monkeypatch.setattr(entity_resolver, "_map_label_to_entity_type", _stub_mapper)
    message = {
        "message_type": "entity_resolution_document_request",
        "source": "document_analysis",
        "tenant_id": "tenant-1",
        "source_item_id": "item-1",
        "entity_candidates": [
            {
                "surface_form": "Project Atlas",
                "ner_label": "ORG",
                "context_snippet": "Project Atlas rollout in Q4.",
                "confidence": 0.8,
            }
        ],
    }

    expanded = entity_resolver._process_document_message(message)

    assert len(expanded) == 1
    assert expanded[0].entity_type == "project"
    assert captured["label"] == "ORG"
    assert captured["surface_form"] == "Project Atlas"
    assert captured["context_snippet"] == "Project Atlas rollout in Q4."


class _Table:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict] = {}

    def get_item(self, Key: dict) -> dict:
        item = self.items.get((Key["tenant_id"], Key["entity_id"]))
        if item is None:
            return {}
        return {"Item": dict(item)}

    def put_item(self, Item: dict) -> dict:
        self.items[(Item["tenant_id"], Item["entity_id"])] = dict(Item)
        return {}


class _Dynamo:
    def __init__(self, table: _Table) -> None:
        self._table = table

    def Table(self, name: str) -> _Table:
        assert name == "entity-master"
        return self._table


def _candidate(**overrides) -> EntityCandidate:
    c = EntityCandidate(
        candidate_id="cand-1",
        tenant_id="tenant-1",
        source_item_id="item-1",
        surface_form="田中太郎",
        normalized_form="タナカタロウ",
        entity_type="person",
        pii_flag=True,
        extraction_source="governance+ner",
        confidence=0.95,
        mention_count=3,
        context_snippet="",
        ner_label="",
        language="ja",
        source_title="",
        extracted_at="2026-02-25T00:00:00Z",
    )
    for key, value in overrides.items():
        setattr(c, key, value)
    return c


def test_resolve_entity_create_then_match(monkeypatch) -> None:
    table = _Table()
    monkeypatch.setenv("ENTITY_MASTER_TABLE", "entity-master")
    monkeypatch.setattr(entity_resolver, "_dynamodb_resource", _Dynamo(table))
    monkeypatch.setattr(entity_resolver, "publish_metric", lambda *args, **kwargs: None)

    candidate = _candidate()
    first = entity_resolver._resolve_entity(candidate)
    second = entity_resolver._resolve_entity(candidate)

    assert first["action"] == "created"
    assert second["action"] == "matched"
    saved = table.items[("tenant-1", first["entity_id"])]
    assert saved["mention_count"] == 6


def test_resolve_entity_without_projection_table_is_skipped(monkeypatch) -> None:
    monkeypatch.delenv("ENTITY_MASTER_TABLE", raising=False)
    result = entity_resolver._resolve_entity(_candidate())
    assert result["action"] == "skipped"


def test_stable_entity_id_is_deterministic() -> None:
    one = entity_resolver._stable_entity_id("person", "Alice")
    two = entity_resolver._stable_entity_id("person", "Alice")
    assert one == two
    assert one.startswith("person_")
