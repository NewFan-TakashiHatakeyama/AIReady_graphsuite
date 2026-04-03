"""LLM-driven freshness scoring helpers."""

from __future__ import annotations

from typing import Any

from src.shared.inference import OntologyInferenceService


def infer_freshness_profile(*, last_modified: str, last_accessed: str | None) -> dict[str, Any]:
    return OntologyInferenceService().infer_freshness(
        last_modified=last_modified,
        last_accessed=last_accessed,
    )


def calculate_freshness_status(last_modified: str) -> str:
    return str(
        infer_freshness_profile(last_modified=last_modified, last_accessed=None).get(
            "freshness_status", "aging"
        )
    )
