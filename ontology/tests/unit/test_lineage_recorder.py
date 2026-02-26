from __future__ import annotations

import json

from src.handlers import lineage_recorder


class _Table:
    def __init__(self):
        self.items: list[dict] = []

    def put_item(self, Item):
        self.items.append(Item)
        return {}


def test_lineage_recorder_success() -> None:
    table = _Table()
    metrics: list[tuple[str, float]] = []

    lineage_recorder._get_lineage_table = lambda: table  # type: ignore[assignment]
    lineage_recorder.publish_metric = (  # type: ignore[assignment]
        lambda name, value, **kwargs: metrics.append((name, value))
    )

    result = lineage_recorder.handler(
        {
            "lineage_id": "lineage-1",
            "tenant_id": "tenant-1",
            "job_name": "schemaTransform",
            "event_type": "COMPLETE",
            "input_dataset": "FileMetadata/tenant-1/item-1",
            "output_dataset": "UnifiedMetadata/tenant-1/item-1",
        },
        None,
    )

    assert result["statusCode"] == 200
    assert result["status"] == "recorded"
    assert len(table.items) == 1
    assert metrics == [("LineageEventsRecorded", 1)]
    assert json.loads(table.items[0]["inputs"])[0]["name"].startswith("FileMetadata")


def test_lineage_recorder_missing_required_field() -> None:
    result = lineage_recorder.handler(
        {
            "tenant_id": "tenant-1",
            "job_name": "schemaTransform",
            "event_type": "COMPLETE",
        },
        None,
    )
    assert result["statusCode"] == 400
    assert "Missing required field" in result["error"]


def test_lineage_recorder_invalid_event_type() -> None:
    result = lineage_recorder.handler(
        {
            "lineage_id": "lineage-1",
            "tenant_id": "tenant-1",
            "job_name": "schemaTransform",
            "event_type": "BROKEN",
        },
        None,
    )
    assert result["statusCode"] == 400
    assert "Invalid event_type" in result["error"]


def test_lineage_recorder_fail_event_emits_fail_metric() -> None:
    table = _Table()
    metrics: list[tuple[str, float]] = []

    lineage_recorder._get_lineage_table = lambda: table  # type: ignore[assignment]
    lineage_recorder.publish_metric = (  # type: ignore[assignment]
        lambda name, value, **kwargs: metrics.append((name, value))
    )

    result = lineage_recorder.handler(
        {
            "lineage_id": "lineage-2",
            "tenant_id": "tenant-1",
            "job_name": "schemaTransform",
            "event_type": "FAIL",
            "error_message": "some error",
        },
        None,
    )
    assert result["statusCode"] == 200
    assert ("LineageEventsRecorded", 1) in metrics
    assert ("LineageFailEvents", 1) in metrics
