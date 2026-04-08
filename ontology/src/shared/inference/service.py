"""Inference orchestration service for ontology pipelines."""

from __future__ import annotations

from typing import Any

from src.shared.inference.bedrock_adapter import BedrockInferenceAdapter
from src.shared.inference.types import InferenceContext
from src.shared.metrics import publish_metric


class OntologyInferenceService:
    """Single entry point for all LLM-driven ontology predictions."""

    def __init__(self, adapter: BedrockInferenceAdapter | None = None) -> None:
        self._adapter = adapter or BedrockInferenceAdapter()

    def infer_document_profile(self, unified_item: dict[str, Any]) -> dict[str, Any]:
        title = str(unified_item.get("title") or unified_item.get("name") or "")
        file_name = str(
            unified_item.get("file_name")
            or unified_item.get("name")
            or unified_item.get("title")
            or ""
        )
        extensions = unified_item.get("extensions")
        if not isinstance(extensions, dict):
            extensions = {}
        governance_signals = unified_item.get("governance_signals")
        if not isinstance(governance_signals, dict):
            governance_signals = {}
        content_analysis = governance_signals.get("content_analysis")
        if not isinstance(content_analysis, dict):
            content_analysis = {}
        extension_document_analysis = extensions.get("document_analysis")
        if isinstance(extension_document_analysis, dict):
            content_analysis = extension_document_analysis
        context = InferenceContext(
            tenant_id=str(unified_item.get("tenant_id") or ""),
            item_id=str(unified_item.get("item_id") or ""),
            payload={
                "title": title,
                "summary": str(unified_item.get("document_summary") or unified_item.get("summary") or ""),
                "file_name": file_name,
                "hierarchy_path": str(unified_item.get("hierarchy_path") or ""),
                "origin_url": str(unified_item.get("origin_url") or ""),
                "topic_keywords": _string_list(unified_item.get("topic_keywords")),
                "document_analysis": content_analysis,
                "governance": {
                    "classification": str(unified_item.get("classification") or ""),
                    "risk_level": str(unified_item.get("risk_level") or ""),
                    "ai_eligible": bool(unified_item.get("ai_eligible", False)),
                    "pii_detected": bool(unified_item.get("pii_detected", False)),
                    "finding_id": str(unified_item.get("finding_id") or ""),
                    "signals": governance_signals,
                },
                "source_author": str(unified_item.get("author") or ""),
                "source_project": str(unified_item.get("project") or ""),
            },
        )
        result = self._adapter.infer_json(
            task="infer_document_profile",
            context=context,
            schema_hint={
                "owner": "string",
                "project": "string",
                "topics": ["string"],
                "topic_categories": ["string"],
                "category_hierarchy": {"large": "string", "medium": "string", "small": "string"},
                "confidence": "float",
            },
            system_prompt=(
                "You classify enterprise documents for ontology indexing. "
                "Infer owner/project/topics/categories from content and metadata."
            ),
        )
        if result is None:
            _record_fallback_metric(task="infer_document_profile")
            profile = _default_document_profile(unified_item)
            profile["needs_review"] = True
            profile["inference_fallback"] = True
            return profile

        payload = result.data
        categories = _string_list(payload.get("topic_categories")) or ["general"]
        topics = _string_list(payload.get("topics"))
        hierarchy_raw = payload.get("category_hierarchy")
        hierarchy = hierarchy_raw if isinstance(hierarchy_raw, dict) else {}
        profile = {
            "owner": _clean_nonempty(payload.get("owner"), fallback="unknown"),
            "project": _clean_nonempty(payload.get("project"), fallback="general"),
            "topics": topics,
            "topic_categories": categories[:5],
            "category_hierarchy": {
                "large": _clean_nonempty(hierarchy.get("large"), fallback="general"),
                "medium": _clean_nonempty(hierarchy.get("medium"), fallback="general"),
                "small": _clean_nonempty(hierarchy.get("small"), fallback=categories[0]),
                "confidence": round(result.confidence, 3),
            },
            "confidence": round(result.confidence, 3),
            "llm_provider": result.provider,
            "llm_model": result.model,
            "prompt_version": result.prompt_version,
            "needs_review": result.confidence < 0.5,
            "inference_fallback": False,
        }
        return profile

    def infer_target_decision(self, *, file_name: str, risk_level: str, ai_eligible: bool, finding_status: str) -> bool:
        context = InferenceContext(
            tenant_id="",
            item_id=file_name,
            payload={
                "file_name": file_name,
                "risk_level": risk_level,
                "ai_eligible": ai_eligible,
                "finding_status": finding_status,
            },
        )
        result = self._adapter.infer_json(
            task="infer_ontology_target",
            context=context,
            schema_hint={"is_target": "boolean", "confidence": "float"},
            system_prompt=(
                "Decide if a document should be included in ontology processing for AI use. "
                "Prefer conservative exclusion for risky or unsupported data."
            ),
        )
        if result is None:
            _record_fallback_metric(task="infer_ontology_target")
            # Bedrock 未設定/失敗時: 低リスクかつ対象拡張子なら True（ai_eligible は見ない）。
            # 推論成功時は ai_eligible を payload に渡すが、最終判定は result.data["is_target"] のみ。
            from src.shared.ontology_target_policy import is_supported_extension

            rl = str(risk_level or "").strip().lower()
            if rl == "none":
                rl = "low"
            if rl != "low":
                return False
            return is_supported_extension(file_name)
        return bool(result.data.get("is_target", False))

    def infer_freshness(self, *, last_modified: str, last_accessed: str | None) -> dict[str, Any]:
        context = InferenceContext(
            tenant_id="",
            item_id="",
            payload={"last_modified": last_modified, "last_accessed": last_accessed or ""},
        )
        result = self._adapter.infer_json(
            task="infer_document_freshness",
            context=context,
            schema_hint={
                "freshness_status": "active|aging|stale",
                "access_freshness": "active|dormant|abandoned",
                "ai_freshness": "recommended|normal|review|stale_warning",
                "freshness_score": "float",
                "confidence": "float",
            },
            system_prompt=(
                "Classify freshness based on update and access timestamps for knowledge trustworthiness."
            ),
        )
        if result is None:
            _record_fallback_metric(task="infer_document_freshness")
            return {
                "freshness_status": "aging",
                "access_freshness": "dormant",
                "ai_freshness": "review",
                "freshness_score": 0.5,
                "needs_review": True,
                "inference_fallback": True,
            }
        data = result.data
        return {
            "freshness_status": _clean_nonempty(data.get("freshness_status"), fallback="aging"),
            "access_freshness": _clean_nonempty(data.get("access_freshness"), fallback="dormant"),
            "ai_freshness": _clean_nonempty(data.get("ai_freshness"), fallback="review"),
            "freshness_score": _clamped_float(data.get("freshness_score"), default=0.5),
            "needs_review": result.confidence < 0.5,
            "inference_fallback": False,
        }

    def infer_entity_type(self, *, surface_form: str, ner_label: str, context_snippet: str) -> str:
        context = InferenceContext(
            tenant_id="",
            item_id="",
            payload={
                "surface_form": surface_form,
                "ner_label": ner_label,
                "context_snippet": context_snippet,
            },
        )
        result = self._adapter.infer_json(
            task="infer_entity_type",
            context=context,
            schema_hint={"entity_type": "string", "confidence": "float"},
            system_prompt=(
                "Map named entities into ontology entity_type taxonomy: "
                "person, organization, location, date, metric, concept, project."
            ),
        )
        if result is None:
            _record_fallback_metric(task="infer_entity_type")
            return "concept"
        value = str(result.data.get("entity_type") or "").strip().lower()
        return value or "concept"


def _default_document_profile(unified_item: dict[str, Any]) -> dict[str, Any]:
    return {
        "owner": _clean_nonempty(unified_item.get("author"), fallback="unknown"),
        "project": _clean_nonempty(unified_item.get("project"), fallback="general"),
        "topics": _string_list(unified_item.get("topic_keywords"))[:3],
        "topic_categories": ["general"],
        "category_hierarchy": {
            "large": "general",
            "medium": "general",
            "small": "general",
            "confidence": 0.2,
        },
        "confidence": 0.2,
        "llm_provider": "",
        "llm_model": "",
        "prompt_version": "",
    }


def _string_list(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    return [str(v).strip() for v in raw if str(v).strip()]


def _clean_nonempty(value: Any, *, fallback: str) -> str:
    text = str(value or "").strip()
    return text or fallback


def _clamped_float(value: Any, *, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(0.0, min(1.0, parsed))


def _record_fallback_metric(*, task: str) -> None:
    try:
        publish_metric(
            "llm_fallback_count",
            1,
            dimensions=[{"Name": "Task", "Value": str(task or "unknown")}],
        )
    except Exception:
        return

