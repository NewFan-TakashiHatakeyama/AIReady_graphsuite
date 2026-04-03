"""doc-first document profile helpers (LLM-backed via OntologyInferenceService)."""

from __future__ import annotations

from typing import Any, TypedDict


class ScoreFeatures(TypedDict):
    top_score: float
    second_score: float
    margin: float
    evidence_count: int
    reason_count: int


class ProjectRunnerUp(TypedDict):
    value: str
    score: float


class CategoryRunnerUp(TypedDict):
    large: str
    medium: str
    small: str
    score: float


class ProjectProfile(TypedDict):
    value: str
    confidence: float
    is_new_project: bool
    reason_codes: list[str]
    needs_review: bool
    score_features: ScoreFeatures
    score_breakdown: dict[str, float]
    runner_up: ProjectRunnerUp | None


class CategoryHierarchy(TypedDict):
    large: str
    medium: str
    small: str
    confidence: float
    reason_codes: list[str]
    matched_keywords: list[str]
    needs_review: bool
    score_features: ScoreFeatures
    score_breakdown: dict[str, float]
    runner_up: CategoryRunnerUp | None


class DocumentProfile(TypedDict):
    owner: str
    project: str
    project_profile: ProjectProfile
    topics: list[str]
    topic_categories: list[str]
    category_hierarchy: CategoryHierarchy
    canonical_doc_id: str


def _inference_service():
    from src.shared.inference import OntologyInferenceService

    return OntologyInferenceService()


def _build_document_profile_from_inferred(
    unified_item: dict[str, Any],
    inferred: dict[str, Any],
    *,
    canonical_doc_id: str | None = None,
) -> DocumentProfile:
    item_id = str(unified_item.get("item_id") or "")
    owner = str(inferred.get("owner") or "unknown")
    project = str(inferred.get("project") or "general")
    topics = [str(v).strip() for v in inferred.get("topics", []) if str(v).strip()]
    topic_categories = [
        str(v).strip()
        for v in inferred.get("topic_categories", ["general"])
        if str(v).strip()
    ] or ["general"]
    hierarchy = inferred.get("category_hierarchy") if isinstance(inferred.get("category_hierarchy"), dict) else {}
    category_hierarchy: CategoryHierarchy = {
        "large": str(hierarchy.get("large") or "general"),
        "medium": str(hierarchy.get("medium") or "general"),
        "small": str(hierarchy.get("small") or topic_categories[0]),
        "confidence": float(hierarchy.get("confidence") or inferred.get("confidence") or 0.2),
        "reason_codes": ["llm_inference"],
        "matched_keywords": topic_categories[:8],
        "needs_review": float(inferred.get("confidence") or 0.2) < 0.6,
        "score_features": {
            "top_score": float(inferred.get("confidence") or 0.2),
            "second_score": 0.0,
            "margin": float(inferred.get("confidence") or 0.2),
            "evidence_count": len(topics) + len(topic_categories),
            "reason_count": 1,
        },
        "score_breakdown": {"llm": float(inferred.get("confidence") or 0.2)},
        "runner_up": None,
    }
    project_profile: ProjectProfile = {
        "value": project,
        "confidence": float(inferred.get("confidence") or 0.2),
        "is_new_project": False,
        "reason_codes": ["llm_inference"],
        "needs_review": float(inferred.get("confidence") or 0.2) < 0.6,
        "score_features": {
            "top_score": float(inferred.get("confidence") or 0.2),
            "second_score": 0.0,
            "margin": float(inferred.get("confidence") or 0.2),
            "evidence_count": len(topics) + len(topic_categories),
            "reason_count": 1,
        },
        "score_breakdown": {"llm": float(inferred.get("confidence") or 0.2)},
        "runner_up": None,
    }
    return {
        "owner": owner,
        "project": project,
        "project_profile": project_profile,
        "topics": topics,
        "topic_categories": topic_categories,
        "category_hierarchy": category_hierarchy,
        "canonical_doc_id": str(canonical_doc_id or item_id),
    }


def resolve_document_profile(
    unified_item: dict[str, Any],
    *,
    canonical_doc_id: str | None = None,
) -> DocumentProfile:
    inferred = _inference_service().infer_document_profile(unified_item)
    return _build_document_profile_from_inferred(
        unified_item,
        inferred,
        canonical_doc_id=canonical_doc_id,
    )


def apply_document_profile_fields(
    unified_item: dict[str, Any],
    *,
    canonical_doc_id: str | None = None,
) -> None:
    inferred = _inference_service().infer_document_profile(unified_item)
    profile = _build_document_profile_from_inferred(
        unified_item,
        inferred,
        canonical_doc_id=canonical_doc_id,
    )
    unified_item["owner"] = profile["owner"]
    unified_item["project"] = profile["project"]
    unified_item["topics"] = profile["topics"]
    unified_item["topic_categories"] = profile["topic_categories"]
    unified_item["category_hierarchy"] = profile["category_hierarchy"]
    unified_item["project_profile"] = profile["project_profile"]
    unified_item["canonical_doc_id"] = profile["canonical_doc_id"]
    extensions = dict(unified_item.get("extensions") or {})
    extensions["document_profile"] = profile
    # OntologyInferenceService は常に inference_fallback を返す。テスト用スタブ等で欠ける場合はメタを付与しない。
    if "inference_fallback" in inferred:
        fallback = bool(inferred.get("inference_fallback"))
        extensions["profile_inference_meta"] = {
            "inference_fallback": fallback,
            "needs_review": bool(inferred.get("needs_review")),
            "llm_used": not fallback,
            "source": "ingest",
        }
    unified_item["extensions"] = extensions


def infer_project_key(unified_item: dict[str, Any]) -> str:
    return str(resolve_document_profile(unified_item).get("project") or "general")


def infer_project_profile(unified_item: dict[str, Any]) -> ProjectProfile:
    return resolve_document_profile(unified_item)["project_profile"]


def infer_category_hierarchy(unified_item: dict[str, Any]) -> CategoryHierarchy:
    return resolve_document_profile(unified_item)["category_hierarchy"]


def infer_topics(
    unified_item: dict[str, Any],
    *,
    max_topics: int = 3,
) -> list[str]:
    topics = resolve_document_profile(unified_item)["topics"]
    return topics[:max_topics]


def map_topic_categories(
    topic_keywords: list[str],
    *,
    category_hierarchy: CategoryHierarchy | dict[str, Any] | None = None,
) -> list[str]:
    if category_hierarchy and isinstance(category_hierarchy, dict):
        small = str(category_hierarchy.get("small") or "").strip()
        if small:
            return [small]
    payload = {"topic_keywords": topic_keywords}
    return resolve_document_profile(payload)["topic_categories"]


def infer_profile_via_llm(
    *,
    item_id: str,
    title: str,
    summary: str,
    source_owner: str,
    source_project: str,
    topic_keywords: list[str],
    existing_profile: dict[str, Any],
    hierarchy_path: str,
    origin_url: str,
    use_llm: bool = True,
) -> dict[str, Any] | None:
    if not use_llm:
        return None
    profile = _inference_service().infer_document_profile(
        {
            "tenant_id": "",
            "item_id": item_id,
            "title": title,
            "document_summary": summary,
            "author": source_owner,
            "project": source_project,
            "topic_keywords": topic_keywords,
            "hierarchy_path": hierarchy_path,
            "origin_url": origin_url,
            "extensions": {"document_profile": existing_profile},
        }
    )
    return {
        "owner": str(profile.get("owner") or ""),
        "project": str(profile.get("project") or ""),
        "topics": [str(v).strip() for v in profile.get("topics", []) if str(v).strip()],
        "topic_categories": [str(v).strip() for v in profile.get("topic_categories", []) if str(v).strip()],
        "category_hierarchy": profile.get("category_hierarchy") if isinstance(profile.get("category_hierarchy"), dict) else {},
        "confidence": float(profile.get("confidence") or 0.0),
        "llm_provider": str(profile.get("llm_provider") or "bedrock"),
        "llm_model": str(profile.get("llm_model") or ""),
        "prompt_version": str(profile.get("prompt_version") or ""),
    }


__all__ = [
    "apply_document_profile_fields",
    "infer_category_hierarchy",
    "infer_profile_via_llm",
    "infer_project_key",
    "infer_project_profile",
    "infer_topics",
    "map_topic_categories",
    "resolve_document_profile",
]
