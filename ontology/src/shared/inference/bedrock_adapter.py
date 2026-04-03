"""Bedrock-backed JSON inference adapter for ontology."""

from __future__ import annotations

import json
import os
import time
from decimal import Decimal
from typing import Any

import boto3
from botocore.config import Config

from src.shared.config import get_ontology_llm_settings
from src.shared.logger import log_structured
from src.shared.metrics import publish_metric
from src.shared.inference.types import InferenceContext, InferenceResult

DEFAULT_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"
DEFAULT_PROMPT_VERSION = "v1"


def _json_default_for_prompt(obj: object) -> float:
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)!r} is not JSON serializable")


class BedrockInferenceAdapter:
    """Small adapter that enforces JSON response contracts."""

    def __init__(self) -> None:
        self._client = None

    def infer_json(
        self,
        *,
        task: str,
        context: InferenceContext,
        schema_hint: dict[str, Any],
        system_prompt: str,
    ) -> InferenceResult | None:
        settings = get_ontology_llm_settings()
        provider = str(settings.get("provider") or "bedrock").strip().lower()
        model_id = str(settings.get("model_id") or DEFAULT_MODEL_ID).strip()
        prompt_version = str(settings.get("prompt_version") or DEFAULT_PROMPT_VERSION).strip()
        temperature = float(settings.get("temperature", 0.0))
        max_tokens = int(float(settings.get("max_tokens", 600)))
        max_retries = max(0, int(settings.get("max_retries", 2)))

        if provider != "bedrock":
            _safe_metric(
                "llm_inference_failure_count",
                1,
                task=task,
                provider=provider,
                prompt_version=prompt_version,
            )
            log_structured(
                "WARN",
                "Unsupported ontology LLM provider",
                task=task,
                provider=provider,
                tenant_id=context.tenant_id,
                item_id=context.item_id,
            )
            return None

        user_prompt = json.dumps(
            {
                "task": task,
                "prompt_version": prompt_version,
                "schema_hint": schema_hint,
                "input": context.payload,
                "requirements": [
                    "return strict JSON only",
                    "do not include markdown",
                    "include confidence (0.0-1.0)",
                ],
            },
            ensure_ascii=False,
            default=_json_default_for_prompt,
        )

        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "temperature": temperature,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        started_at = time.perf_counter()
        try:
            response = None
            last_error: Exception | None = None
            for _ in range(max_retries + 1):
                try:
                    response = self._bedrock_client().invoke_model(
                        modelId=model_id,
                        contentType="application/json",
                        accept="application/json",
                        body=json.dumps(body, ensure_ascii=False),
                    )
                    last_error = None
                    break
                except Exception as exc:
                    last_error = exc
            if response is None:
                if last_error is not None:
                    raise last_error
                raise RuntimeError("Bedrock invoke_model returned no response.")

            raw = json.loads(response["body"].read())
            text = _extract_response_text(raw)
            parsed = _parse_json_object(text)
            if parsed is None:
                _safe_metric(
                    "llm_inference_failure_count",
                    1,
                    task=task,
                    provider=provider,
                    prompt_version=prompt_version,
                )
                return None
            response_prompt_version = str(parsed.get("prompt_version") or "").strip()
            if response_prompt_version and response_prompt_version != prompt_version:
                _safe_metric(
                    "llm_prompt_version_mismatch_count",
                    1,
                    task=task,
                    provider=provider,
                    prompt_version=prompt_version,
                )
            confidence = _coerce_confidence(parsed.get("confidence"))
            _safe_metric(
                "llm_inference_success_count",
                1,
                task=task,
                provider=provider,
                prompt_version=prompt_version,
            )
            return InferenceResult(
                data=parsed,
                confidence=confidence,
                provider="bedrock",
                model=model_id,
                prompt_version=prompt_version,
            )
        except Exception as exc:  # pragma: no cover
            _safe_metric(
                "llm_inference_failure_count",
                1,
                task=task,
                provider=provider,
                prompt_version=prompt_version,
            )
            log_structured(
                "WARN",
                "Bedrock inference failed",
                task=task,
                tenant_id=context.tenant_id,
                item_id=context.item_id,
                error=str(exc),
            )
            return None
        finally:
            elapsed_ms = max(0.0, (time.perf_counter() - started_at) * 1000.0)
            _safe_metric(
                "llm_inference_latency_ms",
                elapsed_ms,
                unit="Milliseconds",
                task=task,
                provider=provider,
                prompt_version=prompt_version,
            )

    def _bedrock_client(self) -> Any:
        if self._client is None:
            settings = get_ontology_llm_settings()
            timeout_seconds = max(1.0, float(settings.get("timeout_seconds", 20.0)))
            retry_attempts = max(1, int(settings.get("max_retries", 2)) + 1)
            config = Config(
                connect_timeout=timeout_seconds,
                read_timeout=timeout_seconds,
                retries={"max_attempts": retry_attempts, "mode": "standard"},
            )
            self._client = boto3.client("bedrock-runtime", config=config)
        return self._client


def _extract_response_text(raw: dict[str, Any]) -> str:
    content = raw.get("content")
    if isinstance(content, list):
        return "".join(str(part.get("text") or "") for part in content if isinstance(part, dict))
    return str(raw.get("output_text") or "")


def _parse_json_object(text: str) -> dict[str, Any] | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        value = json.loads(raw[start : end + 1])
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def _coerce_confidence(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = 0.5
    return max(0.0, min(1.0, parsed))


def _get_float_env(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _safe_metric(
    metric_name: str,
    value: float,
    *,
    unit: str = "Count",
    task: str,
    provider: str,
    prompt_version: str,
) -> None:
    dimensions = [
        {"Name": "Task", "Value": str(task or "unknown")},
        {"Name": "Provider", "Value": str(provider or "unknown")},
        {"Name": "PromptVersion", "Value": str(prompt_version or "unknown")},
    ]
    try:
        publish_metric(metric_name, value, unit=unit, dimensions=dimensions)
    except Exception:
        return

