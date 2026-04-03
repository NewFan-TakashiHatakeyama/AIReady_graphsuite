from __future__ import annotations

from datetime import datetime, timedelta, timezone

import src.shared.freshness as freshness_module
from src.shared.freshness import calculate_freshness_status, infer_freshness_profile


def _iso_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


class _StubInferenceService:
    def infer_freshness(self, *, last_modified: str, last_accessed: str | None):
        del last_accessed
        if not str(last_modified or "").strip():
            return {"freshness_status": "aging"}
        return {"freshness_status": "active", "access_freshness": "dormant"}


def test_calculate_freshness_status_uses_infer_freshness(monkeypatch) -> None:
    monkeypatch.setattr(freshness_module, "OntologyInferenceService", lambda: _StubInferenceService())
    assert calculate_freshness_status(_iso_days_ago(30)) == "active"


def test_calculate_freshness_status_defaults_when_missing(monkeypatch) -> None:
    class _EmptyFreshness:
        def infer_freshness(self, *, last_modified: str, last_accessed: str | None):
            return {}

    monkeypatch.setattr(freshness_module, "OntologyInferenceService", lambda: _EmptyFreshness())
    assert calculate_freshness_status(_iso_days_ago(1)) == "aging"


def test_infer_freshness_profile_passes_through(monkeypatch) -> None:
    monkeypatch.setattr(freshness_module, "OntologyInferenceService", lambda: _StubInferenceService())
    profile = infer_freshness_profile(last_modified="2024-01-01T00:00:00+00:00", last_accessed=None)
    assert profile["freshness_status"] == "active"
