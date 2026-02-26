"""Unified metadata model."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class UnifiedMetadata:
    """Unified metadata record mapped from Connect FileMetadata."""

    tenant_id: str
    item_id: str
    title: str
    content_type: str
    author: str
    last_modified: str
    access_scope: str
    access_control: dict[str, Any] = field(default_factory=dict)
    classification: str = "unclassified"
    source_identifiers: dict[str, Any] = field(default_factory=dict)
    origin_url: str = ""
    size_bytes: int = 0
    hierarchy_path: str = ""
    extensions: dict[str, Any] = field(default_factory=dict)
    source: str = "microsoft365"
    lineage_id: str = ""
    risk_level: str = "none"
    pii_detected: bool = False
    ai_eligible: bool = True
    finding_id: str | None = None
    freshness_status: str = "active"
    schema_version: str = "1.0"
    transformed_at: str = ""
    document_summary: str = ""
    summary_language: str = ""
    topic_keywords: list[str] = field(default_factory=list)
    embedding_ref: str = ""
    analysis_id: str = ""
    summary_generated_at: str = ""
    is_deleted: bool = False
    deleted_at: str | None = None
    ttl: int | None = None

    def to_dynamodb_item(self) -> dict[str, Any]:
        """Serialize model to DynamoDB friendly item."""
        item = asdict(self)
        item["access_control"] = json.dumps(self.access_control, ensure_ascii=False)
        item["source_identifiers"] = json.dumps(
            self.source_identifiers, ensure_ascii=False
        )
        item["extensions"] = json.dumps(self.extensions, ensure_ascii=False)
        return item

    @classmethod
    def from_dynamodb_item(cls, item: dict[str, Any]) -> "UnifiedMetadata":
        """Create model from DynamoDB item."""
        return cls(
            tenant_id=item["tenant_id"],
            item_id=item["item_id"],
            title=item.get("title", ""),
            content_type=item.get("content_type", ""),
            author=item.get("author", ""),
            last_modified=item.get("last_modified", ""),
            access_scope=item.get("access_scope", "private"),
            access_control=_load_json_maybe(item.get("access_control", {})),
            classification=item.get("classification", "unclassified"),
            source_identifiers=_load_json_maybe(item.get("source_identifiers", {})),
            origin_url=item.get("origin_url", ""),
            size_bytes=int(item.get("size_bytes", 0)),
            hierarchy_path=item.get("hierarchy_path", ""),
            extensions=_load_json_maybe(item.get("extensions", {})),
            source=item.get("source", "microsoft365"),
            lineage_id=item.get("lineage_id", ""),
            risk_level=item.get("risk_level", "none"),
            pii_detected=bool(item.get("pii_detected", False)),
            ai_eligible=bool(item.get("ai_eligible", True)),
            finding_id=item.get("finding_id"),
            freshness_status=item.get("freshness_status", "active"),
            schema_version=item.get("schema_version", "1.0"),
            transformed_at=item.get("transformed_at", ""),
            document_summary=item.get("document_summary", ""),
            summary_language=item.get("summary_language", ""),
            topic_keywords=_load_str_list(item.get("topic_keywords")),
            embedding_ref=item.get("embedding_ref", ""),
            analysis_id=item.get("analysis_id", ""),
            summary_generated_at=item.get("summary_generated_at", ""),
            is_deleted=bool(item.get("is_deleted", False)),
            deleted_at=item.get("deleted_at"),
            ttl=int(item["ttl"]) if item.get("ttl") is not None else None,
        )


def _load_json_maybe(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return json.loads(value) if value else {}
    if isinstance(value, dict):
        return value
    return {}


def _load_str_list(value: Any) -> list[str]:
    if isinstance(value, str):
        loaded = json.loads(value) if value else []
        return [str(v) for v in loaded] if isinstance(loaded, list) else []
    if isinstance(value, list):
        return [str(v) for v in value]
    return []
