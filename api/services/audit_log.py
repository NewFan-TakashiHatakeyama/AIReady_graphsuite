"""Structured audit logging helpers with required trace fields."""

from __future__ import annotations

import json
from typing import Any


def build_audit_log(
    *,
    event: str,
    tenant_id: str,
    correlation_id: str,
    operator: str,
    attributes: dict[str, Any] | None = None,
) -> str:
    payload = {
        "event": event,
        "tenant_id": tenant_id,
        "correlation_id": correlation_id,
        "operator": operator,
    }
    if attributes:
        payload["attributes"] = attributes
    return json.dumps(payload, ensure_ascii=False, default=str)
