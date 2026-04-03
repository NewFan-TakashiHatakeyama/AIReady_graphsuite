"""LLM-based content signal analyzer for governance decisions."""

from __future__ import annotations

import json
from pathlib import Path
import time
from dataclasses import asdict, dataclass, field
from typing import Any

import boto3

from shared.config import get_env, get_env_bool
from shared.logger import get_logger
from shared.metrics import emit_count, emit_metric

logger = get_logger(__name__)

_bedrock_client = None

_SUPPORTED_SENSITIVITY_LEVELS = {"none", "low", "medium", "high", "critical"}
_SUPPORTED_EXPECTED_AUDIENCE = {
    "owner_only",
    "department_only",
    "internal_need_to_know",
    "organization",
    "external_allowed",
}
_DEFAULT_CATEGORY_CANDIDATES = [
    "payroll",
    "executive_confidential",
    "customer_list",
    "hr_evaluation",
    "legal_contract",
    "financial_statement_draft",
]
_CATEGORY_ALIASES = {
    # Legacy/variant aliases normalized to canonical category IDs.
    "personnel_evaluation": "hr_evaluation",
}
_DEFAULT_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"
_DEFAULT_PROMPT_VERSION = "governance-content-v1"
_CATEGORY_CATALOG_PATH = (
    Path(__file__).resolve().parent.parent / "resources" / "document_categories.txt"
)
_CATEGORY_CATALOG_CACHE: tuple[list[str], str] | None = None


@dataclass(frozen=True)
class ContentSignals:
    doc_sensitivity_level: str = "none"
    doc_categories: list[str] = field(default_factory=list)
    contains_pii: bool = False
    contains_secret: bool = False
    confidence: float = 0.0
    expected_audience: str = "internal_need_to_know"
    expected_department: str = "unknown"
    expected_department_confidence: float = 0.0
    justification: str = ""
    analysis_status: str = "failed"
    model_id: str = ""
    prompt_version: str = ""
    decision_source: str = "fallback"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _get_bedrock_client():
    global _bedrock_client
    if _bedrock_client is None:
        _bedrock_client = boto3.client("bedrock-runtime")
    return _bedrock_client


def _normalize_categories(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    supported_categories, _ = _load_category_catalog()
    supported_set = set(supported_categories)
    categories: list[str] = []
    seen: set[str] = set()
    for value in raw:
        category = str(value or "").strip().lower()
        category = _CATEGORY_ALIASES.get(category, category)
        if not category or category in seen:
            continue
        if category not in supported_set:
            continue
        seen.add(category)
        categories.append(category)
    return categories[:10]


def _normalize_sensitivity(value: Any) -> str:
    sensitivity = str(value or "").strip().lower()
    if sensitivity in _SUPPORTED_SENSITIVITY_LEVELS:
        return sensitivity
    return "none"


def _normalize_expected_audience(value: Any) -> str:
    audience = str(value or "").strip().lower()
    if audience in _SUPPORTED_EXPECTED_AUDIENCE:
        return audience
    return "internal_need_to_know"


def _normalize_expected_department(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return "unknown"
    return normalized[:120]


def _normalize_justification(value: Any) -> str:
    text = str(value or "").strip()
    return text[:500]


def _build_prompt(*, item_name: str, mime_type: str, text: str, source_metadata: dict[str, Any]) -> str:
    categories, category_catalog_text = _load_category_catalog()
    category_list = ", ".join(categories)
    text_sample = text[:4000]
    return (
        "You are a governance content classification assistant.\n"
        "Return only compact JSON with keys: doc_sensitivity_level, doc_categories, "
        "contains_pii, contains_secret, confidence, expected_audience, expected_department, "
        "expected_department_confidence, justification.\n"
        "doc_sensitivity_level must be one of [none, low, medium, high, critical].\n"
        f"doc_categories must use known candidates only: [{category_list}].\n"
        "Category catalog (authoritative):\n"
        f"{category_catalog_text}\n"
        "expected_audience must be one of "
        "[owner_only, department_only, internal_need_to_know, organization, external_allowed].\n"
        "expected_department should be a concise department/team name, or unknown.\n"
        "expected_department_confidence must be a number between 0.0 and 1.0.\n"
        "confidence must be a number between 0.0 and 1.0.\n"
        "Do not include markdown, comments, or additional keys.\n\n"
        f"item_name: {item_name}\n"
        f"mime_type: {mime_type}\n"
        f"source_metadata: {json.dumps(source_metadata, ensure_ascii=True, sort_keys=True)[:2000]}\n"
        f"text: {text_sample}\n"
    )


def _load_category_catalog() -> tuple[list[str], str]:
    global _CATEGORY_CATALOG_CACHE
    if _CATEGORY_CATALOG_CACHE is not None:
        return _CATEGORY_CATALOG_CACHE

    raw_path = str(
        get_env(
            "GOVERNANCE_CONTENT_CATEGORY_CATALOG_PATH",
            str(_CATEGORY_CATALOG_PATH),
        )
    ).strip()
    catalog_path = Path(raw_path)

    categories: list[str] = []
    lines_for_prompt: list[str] = []
    if catalog_path.exists():
        try:
            for raw_line in catalog_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = [token.strip() for token in line.split("\t")]
                category_id = str(parts[0] or "").lower()
                if not category_id:
                    continue
                category_id = _CATEGORY_ALIASES.get(category_id, category_id)
                if category_id in categories:
                    continue
                categories.append(category_id)
                lines_for_prompt.append(line)
        except Exception:
            logger.warning("failed to load category catalog, fallback to defaults", exc_info=True)

    if not categories:
        categories = list(_DEFAULT_CATEGORY_CANDIDATES)
        lines_for_prompt = [f"{category}\t(default)" for category in categories]

    _CATEGORY_CATALOG_CACHE = (categories, "\n".join(lines_for_prompt))
    return _CATEGORY_CATALOG_CACHE


def _extract_text(item_name: str, source_metadata: dict[str, Any], fallback_text: str | None) -> str:
    chunks: list[str] = []
    if item_name:
        chunks.append(item_name)
    if fallback_text:
        chunks.append(str(fallback_text))
    for key in (
        "text",
        "text_content",
        "extracted_text",
        "ocr_text",
        "content_preview",
        "summary",
        "description",
    ):
        value = source_metadata.get(key)
        if isinstance(value, str) and value.strip():
            chunks.append(value.strip())
    return "\n".join(chunks)[:6000]


def _failed_result(model_id: str, prompt_version: str, decision_source: str = "fallback") -> ContentSignals:
    return ContentSignals(
        doc_sensitivity_level="none",
        doc_categories=[],
        contains_pii=False,
        contains_secret=False,
        confidence=0.0,
        expected_audience="internal_need_to_know",
        expected_department="unknown",
        expected_department_confidence=0.0,
        justification="",
        analysis_status="failed",
        model_id=model_id,
        prompt_version=prompt_version,
        decision_source=decision_source,
    )


def analyze_content_signals(
    *,
    item_name: str,
    mime_type: str,
    source_metadata: dict[str, Any] | None = None,
    extracted_text: str | None = None,
) -> ContentSignals:
    """Analyze content signals using Bedrock Haiku with fail-safe fallback."""
    metadata = source_metadata if isinstance(source_metadata, dict) else {}
    model_id = get_env("GOVERNANCE_CONTENT_ANALYZER_MODEL_ID", _DEFAULT_MODEL_ID).strip() or _DEFAULT_MODEL_ID
    prompt_version = (
        get_env("GOVERNANCE_CONTENT_ANALYZER_PROMPT_VERSION", _DEFAULT_PROMPT_VERSION).strip()
        or _DEFAULT_PROMPT_VERSION
    )
    enabled = get_env_bool("GOVERNANCE_CONTENT_ANALYZER_ENABLED", default=True)
    if not enabled:
        emit_count("AIReadyGov.ContentAnalysis.Disabled")
        return _failed_result(model_id=model_id, prompt_version=prompt_version, decision_source="disabled")

    prepared_text = _extract_text(item_name=item_name, source_metadata=metadata, fallback_text=extracted_text)
    if not prepared_text.strip():
        emit_count("AIReadyGov.ContentAnalysis.EmptyInput")
        return _failed_result(model_id=model_id, prompt_version=prompt_version, decision_source="empty_input")

    prompt = _build_prompt(
        item_name=item_name,
        mime_type=mime_type,
        text=prepared_text,
        source_metadata=metadata,
    )
    start = time.perf_counter()
    try:
        low_conf_threshold = float(get_env("GOVERNANCE_CONTENT_CONFIDENCE_THRESHOLD", "0.7"))
    except Exception:
        low_conf_threshold = 0.7
    try:
        client = _get_bedrock_client()
        response = client.invoke_model(
            modelId=model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(
                {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 300,
                    "temperature": 0.0,
                    "messages": [{"role": "user", "content": prompt}],
                }
            ),
        )
        payload = json.loads(response["body"].read())
        content = str(((payload.get("content") or [{}])[0]).get("text") or "").strip()
        parsed = json.loads(content)
        result = ContentSignals(
            doc_sensitivity_level=_normalize_sensitivity(parsed.get("doc_sensitivity_level")),
            doc_categories=_normalize_categories(parsed.get("doc_categories")),
            contains_pii=bool(parsed.get("contains_pii", False)),
            contains_secret=bool(parsed.get("contains_secret", False)),
            confidence=max(0.0, min(1.0, float(parsed.get("confidence", 0.0)))),
            expected_audience=_normalize_expected_audience(parsed.get("expected_audience")),
            expected_department=_normalize_expected_department(parsed.get("expected_department", "unknown")),
            expected_department_confidence=max(
                0.0,
                min(1.0, float(parsed.get("expected_department_confidence", 0.0))),
            ),
            justification=_normalize_justification(parsed.get("justification")),
            analysis_status="success",
            model_id=model_id,
            prompt_version=prompt_version,
            decision_source="llm",
        )
        emit_count("AIReadyGov.ContentAnalysis.Success")
        emit_count("AIReadyGov.ExpectedAudience.InferSuccess")
        emit_metric("AIReadyGov.ContentAnalysis.Confidence", result.confidence, unit="None")
        if result.confidence < low_conf_threshold:
            emit_count("AIReadyGov.ContentAnalysis.LowConfidence")
            emit_count("AIReadyGov.ExpectedAudience.LowConfidence")
            result = ContentSignals(
                **{
                    **result.to_dict(),
                    "expected_audience": "internal_need_to_know",
                }
            )
        for category in result.doc_categories:
            emit_count("AIReadyGov.ContentAnalysis.Category", dimensions={"Category": category})
        return result
    except Exception:
        logger.warning("content signal analysis failed", exc_info=True)
        emit_count("AIReadyGov.ContentAnalysis.Failure")
        emit_count("AIReadyGov.ExpectedAudience.InferFail")
        return _failed_result(model_id=model_id, prompt_version=prompt_version, decision_source="fallback")
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        emit_metric("AIReadyGov.ContentAnalysis.DurationMs", elapsed_ms, unit="Milliseconds")
