"""Client for reading Governance DocumentAnalysis records."""

from __future__ import annotations

import os
from typing import Any

import boto3

_dynamodb_resource = None


def _get_dynamodb_resource() -> Any:
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    return _dynamodb_resource


def _get_table() -> Any:
    table_name = os.environ.get("DOCUMENT_ANALYSIS_TABLE", "")
    if not table_name:
        raise ValueError("Environment variable 'DOCUMENT_ANALYSIS_TABLE' is required")
    return _get_dynamodb_resource().Table(table_name)


def get_document_analysis(tenant_id: str, item_id: str) -> dict[str, Any] | None:
    """Fetch a DocumentAnalysis record by tenant/item key."""
    response = _get_table().get_item(Key={"tenant_id": tenant_id, "item_id": item_id})
    item = response.get("Item")
    if not isinstance(item, dict):
        return None
    return item


def is_analysis_completed(tenant_id: str, item_id: str) -> bool:
    """Return True when analysis_status is completed."""
    record = get_document_analysis(tenant_id=tenant_id, item_id=item_id)
    if not record:
        return False
    status = str(record.get("analysis_status") or record.get("status") or "")
    return status == "completed"
