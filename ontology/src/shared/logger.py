"""Structured logging helpers."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any


def log_structured(level: str, message: str, **kwargs: Any) -> None:
    """Emit a JSON log entry with shared attributes."""
    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level.upper(),
        "function_name": os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown"),
        "tenant_id": kwargs.pop("tenant_id", "unknown"),
        "request_id": kwargs.pop("request_id", ""),
        "message": message,
        **kwargs,
    }
    print(json.dumps(log_entry, ensure_ascii=False))
