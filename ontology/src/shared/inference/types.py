"""Type models for ontology LLM inference."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class InferenceContext:
    """Structured input passed to model providers."""

    tenant_id: str
    item_id: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class InferenceResult:
    """Normalized inference output from model providers."""

    data: dict[str, Any]
    confidence: float
    provider: str
    model: str
    prompt_version: str

