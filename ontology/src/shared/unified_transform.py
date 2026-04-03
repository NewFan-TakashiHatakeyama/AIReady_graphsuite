"""FileMetadata -> UnifiedMetadata 変換の共通実装。"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from src.shared.document_profile import apply_document_profile_fields
from src.shared.freshness import calculate_freshness_status
from src.shared.json_normalizer import parse_json_dict, parse_string_list
from src.shared.runtime_common import to_int

TOKYO_TZ = ZoneInfo("Asia/Tokyo")


def _resolve_author_name(item: dict[str, Any]) -> str:
    """作成者名を優先順位付きで解決する。"""
    candidates = [
        item.get("created_by"),  # legacy key
        item.get("created_by_user_name"),
        item.get("created_by_user_email"),
        # SharePoint の一部イベントでは createdBy が欠けるため最終更新者へフォールバック
        item.get("modified_by_user_name"),
        item.get("modified_by_user_email"),
    ]
    for value in candidates:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return "unknown"


def build_unified_from_file_item(
    *,
    item: dict[str, Any],
    tenant_id: str,
    existing_unified: dict[str, Any] | None = None,
    governance_result: dict[str, Any] | None = None,
    source: str = "microsoft365",
    schema_version: str = "1.0",
    now_iso: str | None = None,
    lineage_id: str | None = None,
) -> dict[str, Any]:
    """FileMetadata 1件を UnifiedMetadata 辞書へ変換する。"""
    resolved_now_iso = str(now_iso or datetime.now(TOKYO_TZ).isoformat())
    governance = _resolve_governance(
        existing_unified=existing_unified,
        governance_result=governance_result,
    )
    transformed = {
        "tenant_id": tenant_id,
        "item_id": str(item.get("item_id") or ""),
        "title": str(item.get("name") or item.get("title") or ""),
        "content_type": str(item.get("mime_type") or "application/octet-stream"),
        "author": _resolve_author_name(item),
        "last_modified": str(item.get("modified_at") or resolved_now_iso),
        "access_scope": str(item.get("sharing_scope") or "private"),
        "access_control": parse_json_dict(item.get("permissions"), default={}),
        "classification": governance["classification"],
        "source_identifiers": _build_source_identifiers(item),
        "origin_url": str(item.get("web_url") or ""),
        "size_bytes": to_int(item.get("size"), default=0),
        "hierarchy_path": str(item.get("path") or ""),
        "extensions": _build_extensions(item),
        "source": source,
        "lineage_id": str(lineage_id or uuid.uuid4()),
        "risk_level": governance["risk_level"],
        "pii_detected": governance["pii_detected"],
        "ai_eligible": governance["ai_eligible"],
        "finding_id": governance["finding_id"],
        "governance_signals": governance.get("governance_signals", {}),
        "freshness_status": calculate_freshness_status(str(item.get("modified_at") or resolved_now_iso)),
        "schema_version": schema_version,
        "transformed_at": resolved_now_iso,
        "is_deleted": False,
    }
    apply_document_profile_fields(transformed)
    return transformed


def _resolve_governance(
    *,
    existing_unified: dict[str, Any] | None,
    governance_result: dict[str, Any] | None,
) -> dict[str, Any]:
    if governance_result is None:
        base = {
            "classification": "unclassified",
            "risk_level": "low",
            "pii_detected": False,
            "ai_eligible": False,
            "finding_id": None,
            "governance_signals": {},
        }
        if not existing_unified:
            return base
        base["classification"] = str(existing_unified.get("classification") or "unclassified")
        _prev_rl = str(existing_unified.get("risk_level") or "low").strip().lower()
        base["risk_level"] = "low" if _prev_rl == "none" else (_prev_rl or "low")
        base["pii_detected"] = bool(existing_unified.get("pii_detected", False))
        if "ai_eligible" in existing_unified:
            base["ai_eligible"] = bool(existing_unified["ai_eligible"])
        base["finding_id"] = existing_unified.get("finding_id")
        base["governance_signals"] = dict(existing_unified.get("governance_signals") or {})
        return base

    _gr = str(governance_result.get("risk_level") or "low").strip().lower()
    risk_level = "low" if _gr == "none" else (_gr or "low")
    ai_eligible = bool(governance_result.get("ai_eligible", False))
    if risk_level in {"high", "critical"}:
        ai_eligible = False
    return {
        "classification": str(governance_result.get("classification") or "unclassified"),
        "risk_level": risk_level,
        "pii_detected": bool(governance_result.get("pii_detected", False)),
        "ai_eligible": ai_eligible,
        "finding_id": governance_result.get("finding_id"),
        "governance_signals": {
            "content_signals": governance_result.get("content_signals") or {},
            "content_analysis": governance_result.get("content_analysis") or {},
            "exposure_vectors": governance_result.get("exposure_vectors") or [],
            "matched_guards": governance_result.get("matched_guards") or [],
            "decision_trace": governance_result.get("decision_trace") or [],
        },
    }


def _build_source_identifiers(source_item: dict[str, Any]) -> dict[str, Any]:
    identifiers = {"drive_id": source_item.get("drive_id")}
    identifiers.update(parse_json_dict(source_item.get("sharepoint_ids"), default={}))
    return {k: v for k, v in identifiers.items() if v is not None and v != ""}


def _build_extensions(source_item: dict[str, Any]) -> dict[str, Any]:
    return {
        "governance_flags": parse_string_list(
            source_item.get("governance_flags"),
            parse_json_string=False,
            fallback_single_string=True,
        ),
        "sharepoint_ids": parse_json_dict(source_item.get("sharepoint_ids"), default={}),
        "drive_id": source_item.get("drive_id"),
        "created_at": source_item.get("created_at"),
        "modified_by": source_item.get("modified_by"),
        "is_folder": bool(source_item.get("is_folder", False)),
        "synced_at": source_item.get("synced_at"),
    }
