from __future__ import annotations

from src.shared import document_analysis_client


class _Table:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict] = {}

    def put_item(self, Item):  # noqa: N803
        self.items[(Item["tenant_id"], Item["item_id"])] = dict(Item)
        return {}

    def get_item(self, Key):  # noqa: N803
        item = self.items.get((Key["tenant_id"], Key["item_id"]))
        return {"Item": dict(item)} if item else {}


class _Dynamo:
    def __init__(self, table: _Table) -> None:
        self._table = table

    def Table(self, _name: str):  # noqa: N802
        return self._table


def _setup(monkeypatch):
    table = _Table()
    monkeypatch.setenv("DOCUMENT_ANALYSIS_TABLE", "analysis")
    monkeypatch.setattr(document_analysis_client, "_dynamodb_resource", _Dynamo(table))
    return table


def _setup_with_governance_env(monkeypatch):
    table = _Table()
    monkeypatch.delenv("DOCUMENT_ANALYSIS_TABLE", raising=False)
    monkeypatch.setenv("GOVERNANCE_DOCUMENT_ANALYSIS_TABLE_NAME", "analysis")
    monkeypatch.setattr(document_analysis_client, "_dynamodb_resource", _Dynamo(table))
    return table


def test_get_document_analysis_returns_item(monkeypatch) -> None:
    table = _setup(monkeypatch)
    table.put_item(
        Item={
            "tenant_id": "tenant-1",
            "item_id": "item-1",
            "analysis_status": "completed",
            "document_summary": "summary",
        }
    )

    result = document_analysis_client.get_document_analysis("tenant-1", "item-1")
    assert result is not None
    assert result["analysis_status"] == "completed"
    assert result["document_summary"] == "summary"


def test_get_document_analysis_returns_none_when_missing(monkeypatch) -> None:
    _setup(monkeypatch)
    result = document_analysis_client.get_document_analysis("tenant-1", "missing-item")
    assert result is None


def test_is_analysis_completed_status_matrix(monkeypatch) -> None:
    table = _setup(monkeypatch)
    table.put_item(
        Item={"tenant_id": "tenant-1", "item_id": "done", "analysis_status": "completed"}
    )
    table.put_item(
        Item={
            "tenant_id": "tenant-1",
            "item_id": "processing",
            "analysis_status": "processing",
        }
    )
    table.put_item(
        Item={"tenant_id": "tenant-1", "item_id": "failed", "analysis_status": "failed"}
    )

    assert document_analysis_client.is_analysis_completed("tenant-1", "done") is True
    assert (
        document_analysis_client.is_analysis_completed("tenant-1", "processing") is False
    )
    assert document_analysis_client.is_analysis_completed("tenant-1", "failed") is False
    assert document_analysis_client.is_analysis_completed("tenant-1", "missing") is False

    table.put_item(
        Item={
            "tenant_id": "tenant-1",
            "item_id": "legacy-summary-only",
            "summary": "done",
        }
    )
    assert document_analysis_client.is_analysis_completed("tenant-1", "legacy-summary-only") is True


def test_get_document_analysis_uses_governance_table_env(monkeypatch) -> None:
    table = _setup_with_governance_env(monkeypatch)
    table.put_item(
        Item={
            "tenant_id": "tenant-1",
            "item_id": "item-2",
            "analysis_status": "completed",
        }
    )
    result = document_analysis_client.get_document_analysis("tenant-1", "item-2")
    assert result is not None
    assert result["analysis_status"] == "completed"
