from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.shared.freshness import (
    calculate_access_freshness,
    calculate_ai_freshness,
    calculate_freshness_score,
    calculate_freshness_status,
)


def _iso_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def test_calculate_freshness_status_ranges() -> None:
    assert calculate_freshness_status(_iso_days_ago(30)) == "active"
    assert calculate_freshness_status(_iso_days_ago(120)) == "aging"
    assert calculate_freshness_status(_iso_days_ago(400)) == "stale"


def test_calculate_freshness_status_boundaries() -> None:
    assert calculate_freshness_status(_iso_days_ago(89)) == "active"
    assert calculate_freshness_status(_iso_days_ago(90)) == "active"
    assert calculate_freshness_status(_iso_days_ago(91)) == "aging"


def test_calculate_access_freshness_patterns() -> None:
    assert calculate_access_freshness(_iso_days_ago(5)) == "active"
    assert calculate_access_freshness(_iso_days_ago(60)) == "dormant"
    assert calculate_access_freshness(_iso_days_ago(220)) == "abandoned"
    assert calculate_access_freshness(None) == "abandoned"


def test_ai_freshness_all_patterns() -> None:
    freshness = ["active", "aging", "stale"]
    access = ["active", "dormant", "abandoned"]
    results = {calculate_ai_freshness(f, a) for f in freshness for a in access}
    assert "recommended" in results
    assert "normal" in results
    assert "review" in results
    assert "stale_warning" in results


def test_freshness_score_mapping() -> None:
    assert calculate_freshness_score("recommended") == 2.0
    assert calculate_freshness_score("stale_warning") == 0.1
