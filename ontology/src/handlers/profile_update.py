"""profileUpdate Lambda handler for ontology unified profile updates."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3

from src.shared.content_quality import calculate_content_quality_score
from src.shared.document_profile import (
    infer_category_hierarchy,
    infer_profile_via_llm,
    infer_project_profile,
    infer_topics,
    map_topic_categories,
)

_dynamodb_resource = None


def _resource():
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    return _dynamodb_resource


def _table():
    table_name = str(os.environ.get("UNIFIED_METADATA_TABLE") or "").strip()
    if not table_name:
        raise ValueError("Environment variable 'UNIFIED_METADATA_TABLE' is required")
    return _resource().Table(table_name)


def _as_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _convert_floats_to_decimal(value: Any) -> Any:
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_convert_floats_to_decimal(v) for v in value]
    if isinstance(value, dict):
        return {k: _convert_floats_to_decimal(v) for k, v in value.items()}
    return value


def _is_meaningful_owner(value: str) -> bool:
    normalized = _as_text(value).lower()
    return bool(normalized) and normalized not in {"unknown", "n/a", "none", "-"}


def _is_meaningful_project(value: str) -> bool:
    normalized = _as_text(value).lower()
    return bool(normalized) and normalized not in {"general", "unknown", "n/a", "none", "-"}


def _compute_quality(item: dict[str, Any], owner: str, project: str, topic_categories: list[str]) -> dict[str, float]:
    freshness_score = float(item.get("freshness_score") or 0.82)
    if item.get("duplicate_group_id"):
        uniqueness_score = 1.0 if bool(item.get("is_canonical_copy", False)) else 0.5
    else:
        uniqueness_score = 1.0
    relevance_score = float(item.get("relevance_score") or 0.4)
    content_quality_score = float(
        calculate_content_quality_score(
            freshness_score=freshness_score,
            uniqueness_score=uniqueness_score,
            relevance_score=relevance_score,
        )
    )
    location_score = (
        (1 if _is_meaningful_owner(owner) else 0)
        + (1 if _is_meaningful_project(project) else 0)
        + (1 if isinstance(topic_categories, list) and len(topic_categories) > 0 else 0)
    ) / 3.0
    return {
        "freshness_score": round(freshness_score, 3),
        "uniqueness_score": round(uniqueness_score, 3),
        "relevance_score": round(relevance_score, 3),
        "content_quality_score": round(content_quality_score, 3),
        "location_score": round(location_score, 3),
    }


def _extract_current_profile(item: dict[str, Any]) -> dict[str, Any]:
    extensions = item.get("extensions")
    if isinstance(extensions, str):
        import json

        try:
            extensions = json.loads(extensions)
        except Exception:
            extensions = {}
    if not isinstance(extensions, dict):
        extensions = {}
    profile = extensions.get("document_profile")
    if not isinstance(profile, dict):
        profile = {}
    return {
        "owner": _as_text(item.get("owner") or profile.get("owner")),
        "project": _as_text(item.get("project") or profile.get("project")),
        "topic_categories": (
            [str(v).strip() for v in (item.get("topic_categories") or profile.get("topic_categories") or []) if str(v).strip()]
            if isinstance(item.get("topic_categories") or profile.get("topic_categories") or [], list)
            else []
        ),
        "canonical_doc_id": _as_text(item.get("canonical_doc_id") or profile.get("canonical_doc_id")),
    }


def _run_ai_fill(item: dict[str, Any], *, use_llm: bool | None) -> dict[str, Any]:
    item_id = _as_text(item.get("item_id"))
    title = _as_text(item.get("title") or item_id)
    summary = _as_text(item.get("document_summary") or item.get("summary"))
    current = _extract_current_profile(item)
    source_owner = _as_text(item.get("author") or item.get("created_by") or item.get("owner"))

    source_project = _as_text(item.get("project") or item.get("site_name") or item.get("drive_name"))
    llm = infer_profile_via_llm(
        item_id=item_id,
        title=title,
        summary=summary,
        source_owner=source_owner,
        source_project=source_project,
        topic_keywords=[str(v).strip() for v in (item.get("topic_keywords") or []) if str(v).strip()]
        if isinstance(item.get("topic_keywords"), list)
        else [],
        existing_profile=current,
        hierarchy_path=_as_text(item.get("hierarchy_path")),
        origin_url=_as_text(item.get("origin_url")),
        use_llm=bool(use_llm) if use_llm is not None else True,
    )
    llm_owner = _as_text((llm or {}).get("owner"))
    llm_project = _as_text((llm or {}).get("project"))
    llm_topic_categories = (
        [str(v).strip() for v in (llm or {}).get("topic_categories", []) if str(v).strip()]
        if isinstance((llm or {}).get("topic_categories"), list)
        else []
    )
    llm_provider = _as_text((llm or {}).get("llm_provider"))
    llm_topics = (
        [str(v).strip() for v in (llm or {}).get("topics", []) if str(v).strip()]
        if isinstance((llm or {}).get("topics"), list)
        else []
    )
    llm_hierarchy = (llm or {}).get("category_hierarchy")
    if not isinstance(llm_hierarchy, dict):
        llm_hierarchy = {}

    owner = next(
        (v for v in (llm_owner, source_owner, current.get("owner", "")) if _is_meaningful_owner(str(v))),
        "unknown",
    )
    project = next(
        (v for v in (llm_project, source_project, current.get("project", "")) if _is_meaningful_project(str(v))),
        "general",
    )
    topics = llm_topics or [str(v).strip() for v in (item.get("topic_keywords") or []) if str(v).strip()][:3]
    topic_categories = llm_topic_categories or list(current.get("topic_categories") or [])
    if not topic_categories:
        topic_categories = ["general"]
    category_hierarchy = {
        "large": _as_text(llm_hierarchy.get("large")) or "general",
        "medium": _as_text(llm_hierarchy.get("medium")) or "general",
        "small": _as_text(llm_hierarchy.get("small")) or topic_categories[0],
        "confidence": float(llm_hierarchy.get("confidence") or (llm or {}).get("confidence") or 0.2),
    }

    canonical_doc_id = _as_text(current.get("canonical_doc_id")) or item_id
    quality = _compute_quality(item, owner, project, topic_categories)
    profile = {
        "owner": owner,
        "project": project,
        "topics": topics,
        "topic_categories": topic_categories,
        "category_hierarchy": category_hierarchy,
        "canonical_doc_id": canonical_doc_id,
        "llm_used": bool(llm is not None),
        "llm_provider": llm_provider,
    }
    return {"profile": profile, "quality": quality}


def _run_manual_update(
    item: dict[str, Any],
    *,
    owner: str | None,
    project: str | None,
    topic_categories: list[str] | None,
    canonical_doc_id: str | None,
) -> dict[str, Any]:
    item_id = _as_text(item.get("item_id"))
    current = _extract_current_profile(item)
    next_owner = _as_text(owner) or _as_text(current.get("owner")) or "unknown"
    next_project = _as_text(project) or _as_text(current.get("project")) or "general"
    next_categories = (
        [str(v).strip() for v in topic_categories if str(v).strip()]
        if isinstance(topic_categories, list)
        else list(current.get("topic_categories") or [])
    )
    next_canonical = _as_text(canonical_doc_id) or _as_text(current.get("canonical_doc_id")) or item_id
    category_hierarchy = infer_category_hierarchy(item)
    if _as_text(category_hierarchy.get("small")).lower() in {"", "general"} and next_categories:
        category_hierarchy["small"] = next_categories[0]
    quality = _compute_quality(item, next_owner, next_project, next_categories)
    profile = {
        "owner": next_owner,
        "project": next_project,
        "topics": list(next_categories),
        "topic_categories": next_categories,
        "category_hierarchy": category_hierarchy,
        "canonical_doc_id": next_canonical,
        "llm_used": False,
        "llm_provider": "",
    }
    return {"profile": profile, "quality": quality}


def _run_plan_state_sync(
    item: dict[str, Any],
    *,
    remediation_state: str,
    approved_profile: dict[str, Any] | None,
) -> dict[str, Any]:
    normalized_state = str(remediation_state or "").strip().lower()
    if normalized_state not in {"ai_proposed", "pending_approval", "approved", "executed"}:
        normalized_state = "ai_proposed"

    item_id = _as_text(item.get("item_id"))
    current = _extract_current_profile(item)
    profile = {
        "owner": _as_text(current.get("owner")) or "unknown",
        "project": _as_text(current.get("project")) or _as_text(infer_project_profile(item).get("value")) or "general",
        "topic_categories": list(current.get("topic_categories") or []),
        "canonical_doc_id": _as_text(current.get("canonical_doc_id")) or item_id,
    }
    if not profile["topic_categories"]:
        hierarchy = infer_category_hierarchy(item)
        topics = infer_topics(item)
        profile["topic_categories"] = map_topic_categories(topics, category_hierarchy=hierarchy)

    if isinstance(approved_profile, dict):
        profile["owner"] = _as_text(approved_profile.get("owner")) or profile["owner"]
        profile["project"] = _as_text(approved_profile.get("project")) or profile["project"]
        approved_topics = approved_profile.get("topic_categories")
        if isinstance(approved_topics, list) and approved_topics:
            profile["topic_categories"] = [str(v).strip() for v in approved_topics if str(v).strip()]
        profile["canonical_doc_id"] = _as_text(approved_profile.get("canonical_doc_id")) or profile["canonical_doc_id"]

    base_quality = float(item.get("content_quality_score") or 0.5)
    quality_boost = 0.10 if normalized_state == "approved" else 0.18 if normalized_state == "executed" else 0.0
    next_quality = round(max(0.05, min(1.0, base_quality + quality_boost)), 3)
    is_orphan = not (
        _is_meaningful_owner(profile["owner"])
        and _is_meaningful_project(profile["project"])
        and len(profile["topic_categories"]) > 0
    )
    updated = dict(item)
    updated["owner"] = profile["owner"]
    updated["project"] = profile["project"]
    updated["topic_categories"] = profile["topic_categories"]
    updated["canonical_doc_id"] = profile["canonical_doc_id"]
    updated["content_quality_score"] = next_quality
    updated["remediation_state"] = normalized_state
    updated["is_orphan"] = is_orphan
    updated["last_quality_scored_at"] = datetime.now(timezone.utc).isoformat()
    risk_level = _as_text(updated.get("risk_level")).lower()
    if normalized_state in {"approved", "executed"} and risk_level == "low":
        updated["ai_eligible"] = True
    extensions = updated.get("extensions")
    if not isinstance(extensions, dict):
        extensions = {}
    extensions["document_profile"] = profile
    updated["extensions"] = extensions
    return {"updated_item": updated}


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    del context
    action = _as_text(event.get("action")).lower()
    tenant_id = _as_text(event.get("tenant_id"))
    item_id = _as_text(event.get("item_id"))
    if not tenant_id or not item_id:
        return {"statusCode": 400, "body": {"error": "tenant_id and item_id are required"}}
    table = _table()
    response = table.get_item(Key={"tenant_id": tenant_id, "item_id": item_id})
    item = response.get("Item")
    if not isinstance(item, dict):
        return {"statusCode": 404, "body": {"error": f"Item not found: {item_id}"}}

    if action == "ai_fill":
        result = _run_ai_fill(item, use_llm=event.get("use_llm"))
    elif action == "manual_update":
        result = _run_manual_update(
            item,
            owner=event.get("owner"),
            project=event.get("project"),
            topic_categories=event.get("topic_categories"),
            canonical_doc_id=event.get("canonical_doc_id"),
        )
    elif action == "plan_state_sync":
        result = _run_plan_state_sync(
            item,
            remediation_state=str(event.get("remediation_state") or ""),
            approved_profile=event.get("approved_profile") if isinstance(event.get("approved_profile"), dict) else None,
        )
        table.put_item(Item=_convert_floats_to_decimal(result["updated_item"]))
        return {"statusCode": 200, "body": {"item_id": item_id, "status": "ok"}}
    else:
        return {"statusCode": 400, "body": {"error": f"Unsupported action: {action}"}}

    profile = result["profile"]
    quality = result["quality"]
    extensions = item.get("extensions")
    if isinstance(extensions, str):
        import json

        try:
            extensions = json.loads(extensions)
        except Exception:
            extensions = {}
    if not isinstance(extensions, dict):
        extensions = {}
    extensions["document_profile"] = profile
    prev_meta = extensions.get("profile_inference_meta")
    meta = dict(prev_meta) if isinstance(prev_meta, dict) else {}
    llm_used_flag = bool(profile.get("llm_used"))
    if action == "ai_fill":
        meta.update(
            {
                "inference_fallback": not llm_used_flag,
                "needs_review": not llm_used_flag,
                "llm_used": llm_used_flag,
                "source": "ai_fill",
            }
        )
    else:
        meta.update(
            {
                "inference_fallback": False,
                "needs_review": False,
                "llm_used": False,
                "source": "manual_update",
            }
        )
    extensions["profile_inference_meta"] = meta

    table.update_item(
        Key={"tenant_id": tenant_id, "item_id": item_id},
        UpdateExpression=(
            "SET #owner = :owner, #project = :project, "
            "topics = :raw_topics, topic_categories = :topic_categories, "
            "category_hierarchy = :category_hierarchy, canonical_doc_id = :canonical_doc_id, "
            "extensions = :extensions, content_quality_score = :content_quality_score"
        ),
        ExpressionAttributeNames={"#owner": "owner", "#project": "project"},
        ExpressionAttributeValues={
            ":owner": profile["owner"],
            ":project": profile["project"],
            ":raw_topics": profile["topics"],
            ":topic_categories": profile["topic_categories"],
            ":category_hierarchy": _convert_floats_to_decimal(profile["category_hierarchy"]),
            ":canonical_doc_id": profile["canonical_doc_id"],
            ":extensions": _convert_floats_to_decimal(extensions),
            ":content_quality_score": Decimal(str(quality["content_quality_score"])),
        },
    )

    return {
        "statusCode": 200,
        "body": {
            "item_id": item_id,
            "profile": profile,
            "quality": quality,
        },
    }

