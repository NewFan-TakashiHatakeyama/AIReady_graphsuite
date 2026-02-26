"""Lineage event model."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class LineageEvent:
    """OpenLineage-compatible lineage event stored in DynamoDB."""

    tenant_id: str
    lineage_id: str
    event_type: str
    event_time: str
    job_namespace: str
    job_name: str
    run_id: str
    inputs: list[dict[str, Any]] = field(default_factory=list)
    outputs: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0
    status: str = "success"
    error_message: str | None = None
    ttl: int = 0

    def to_dynamodb_item(self) -> dict[str, Any]:
        item = asdict(self)
        item["inputs"] = json.dumps(self.inputs, ensure_ascii=False)
        item["outputs"] = json.dumps(self.outputs, ensure_ascii=False)
        item["metadata"] = json.dumps(self.metadata, ensure_ascii=False)
        return item

    @classmethod
    def from_dynamodb_item(cls, item: dict[str, Any]) -> "LineageEvent":
        return cls(
            tenant_id=item["tenant_id"],
            lineage_id=item["lineage_id"],
            event_type=item["event_type"],
            event_time=item["event_time"],
            job_namespace=item.get("job_namespace", "ai-ready-ontology"),
            job_name=item["job_name"],
            run_id=item.get("run_id", item["lineage_id"]),
            inputs=_load_list(item.get("inputs")),
            outputs=_load_list(item.get("outputs")),
            metadata=_load_dict(item.get("metadata")),
            duration_ms=int(item.get("duration_ms", 0)),
            status=item.get("status", "success"),
            error_message=item.get("error_message"),
            ttl=int(item.get("ttl", 0)),
        )


def _load_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, str):
        return json.loads(value) if value else []
    if isinstance(value, list):
        return value
    return []


def _load_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return json.loads(value) if value else {}
    if isinstance(value, dict):
        return value
    return {}
