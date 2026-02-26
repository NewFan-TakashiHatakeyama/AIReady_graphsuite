"""Freshness evaluation logic."""

from __future__ import annotations

from datetime import datetime, timezone


FRESHNESS_THRESHOLDS = {
    "aging_days": 90,
    "stale_days": 365,
}

ACCESS_THRESHOLDS = {
    "dormant_days": 30,
    "abandoned_days": 180,
}

AI_FRESHNESS_MATRIX = {
    ("active", "active"): "recommended",
    ("active", "dormant"): "normal",
    ("active", "abandoned"): "review",
    ("aging", "active"): "normal",
    ("aging", "dormant"): "review",
    ("aging", "abandoned"): "stale_warning",
    ("stale", "active"): "review",
    ("stale", "dormant"): "stale_warning",
    ("stale", "abandoned"): "stale_warning",
}

FRESHNESS_SCORE_TABLE = {
    "recommended": 2.0,
    "normal": 1.0,
    "review": 0.5,
    "stale_warning": 0.1,
}


def _parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def calculate_freshness_status(last_modified: str) -> str:
    """Calculate content freshness status from modified datetime."""
    modified_dt = _parse_iso_datetime(last_modified)
    days = (datetime.now(timezone.utc) - modified_dt).days
    if days > FRESHNESS_THRESHOLDS["stale_days"]:
        return "stale"
    if days > FRESHNESS_THRESHOLDS["aging_days"]:
        return "aging"
    return "active"


def calculate_access_freshness(last_accessed: str | None) -> str:
    """Calculate access freshness from last accessed datetime."""
    if not last_accessed:
        return "abandoned"
    accessed_dt = _parse_iso_datetime(last_accessed)
    days = (datetime.now(timezone.utc) - accessed_dt).days
    if days > ACCESS_THRESHOLDS["abandoned_days"]:
        return "abandoned"
    if days > ACCESS_THRESHOLDS["dormant_days"]:
        return "dormant"
    return "active"


def calculate_ai_freshness(freshness_status: str, access_freshness: str) -> str:
    """Determine AI recommendation freshness from matrix."""
    return AI_FRESHNESS_MATRIX.get((freshness_status, access_freshness), "review")


def calculate_freshness_score(ai_freshness: str) -> float:
    """Map AI freshness label to numeric score."""
    return FRESHNESS_SCORE_TABLE.get(ai_freshness, 0.5)
