"""Ontology API application service for MS-3 endpoints."""

from __future__ import annotations

import logging
import math
import os
import re
import json
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse
from collections import defaultdict
from decimal import Decimal
from typing import Any

from boto3.dynamodb.conditions import Attr, Key

from services.aws_clients import get_dynamodb_resource, get_lambda_client
from services.ontology_graph_repository import rebuild_ontology_graph_projection_from_unified
from services.repositories.audit_writer_repository import CommonAuditRepository
from services.runtime_config import load_aws_runtime_config, load_tenant_registry
from services.tenant_db_resolver import TenantDbResolver

_workspace_root = Path(__file__).resolve().parents[2]
_ontology_root = _workspace_root / "ontology"
if _ontology_root.exists() and str(_ontology_root) not in sys.path:
    sys.path.insert(0, str(_ontology_root))

try:
    from src.shared.document_profile import (
        infer_category_hierarchy as _shared_infer_category_hierarchy,
        infer_profile_via_llm as _shared_infer_profile_via_llm,
        infer_topics as _shared_infer_topics,
        map_topic_categories as _shared_map_topic_categories,
    )
except Exception:
    _shared_infer_category_hierarchy = None
    _shared_infer_profile_via_llm = None
    _shared_infer_topics = None
    _shared_map_topic_categories = None

_runtime_config = load_aws_runtime_config()
_dynamodb_resource = None
_lambda_client = None
_tenant_db_resolver = TenantDbResolver(_runtime_config)
_common_audit_repository = CommonAuditRepository()
_logger = logging.getLogger(__name__)
_ALLOWED_PROJECTION_PRESETS = {"strict", "standard", "relaxed"}
_PROJECTION_PRESET_OPTIONS: dict[str, dict[str, Any]] = {
    "strict": {
        "SIMILARITY_THRESHOLD": 0.86,
        "TOP_K_NEIGHBORS": 3,
        "ENABLE_SIMILARITY_TEXT_FALLBACK": False,
        "MENTIONS_AUTOPROMOTE_MIN_ALNUM_CHARS": 4,
        "MENTIONS_AUTOPROMOTE_MIN_CJK_CHARS": 2,
    },
    "standard": {
        "SIMILARITY_THRESHOLD": 0.78,
        "TOP_K_NEIGHBORS": 5,
        "ENABLE_SIMILARITY_TEXT_FALLBACK": True,
        "MENTIONS_AUTOPROMOTE_MIN_ALNUM_CHARS": 3,
        "MENTIONS_AUTOPROMOTE_MIN_CJK_CHARS": 2,
    },
    "relaxed": {
        "SIMILARITY_THRESHOLD": 0.68,
        "TOP_K_NEIGHBORS": 8,
        "ENABLE_SIMILARITY_TEXT_FALLBACK": True,
        "MENTIONS_AUTOPROMOTE_MIN_ALNUM_CHARS": 2,
        "MENTIONS_AUTOPROMOTE_MIN_CJK_CHARS": 1,
    },
}
_USER_SETTINGS_PROJECTION_PRESET_ACTION = "user_settings.projection_preset.update"
_USER_SETTINGS_UPDATE_ACTION = "user_settings.update"


class OntologyDataAccessError(RuntimeError):
    """Raised when tenant-scoped ontology data cannot be read from DynamoDB."""


def _resource():
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = get_dynamodb_resource(_runtime_config)
    return _dynamodb_resource


def _table(name: str):
    return _resource().Table(name)


def _lambda():
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = get_lambda_client(_runtime_config)
    return _lambda_client


def _require_ontology_tenant(tenant_id: str) -> str:
    normalized_tenant_id = str(tenant_id or "").strip()
    if not normalized_tenant_id:
        raise ValueError("tenant_id is required for ontology access.")
    if str(os.getenv("ONTOLOGY_REQUIRE_TENANT_REGISTRY", "false")).strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return normalized_tenant_id
    registry = load_tenant_registry()
    if normalized_tenant_id not in registry:
        raise ValueError(
            "Tenant is not registered for Ontology access. "
            f"tenant_id={normalized_tenant_id}"
        )
    return normalized_tenant_id


def _to_plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, list):
        return [_to_plain(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    return value


def _convert_floats_to_decimal(value: Any) -> Any:
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_convert_floats_to_decimal(v) for v in value]
    if isinstance(value, dict):
        return {k: _convert_floats_to_decimal(v) for k, v in value.items()}
    return value


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp_ratio(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _mean_or_default(values: list[float], default: float = 0.0) -> float:
    if not values:
        return float(default)
    return float(sum(values) / len(values))


def _feature_enabled(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _ontology_analysis_target_mode(default: str = "eligible_only") -> str:
    raw = str(os.getenv("ONTOLOGY_ANALYSIS_TARGET_MODE", default)).strip().lower()
    if raw in {"all_unified", "all", "full"}:
        return "all_unified"
    return "eligible_only"


def _parse_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _extract_document_profile(row: dict[str, Any]) -> dict[str, Any]:
    extensions = _parse_json_dict(row.get("extensions"))
    profile = _parse_json_dict(extensions.get("document_profile"))
    meta = _parse_json_dict(extensions.get("profile_inference_meta"))
    owner = _as_text(row.get("owner") or profile.get("owner"))
    project = _as_text(row.get("project") or profile.get("project"))
    canonical_doc_id = _as_text(row.get("canonical_doc_id") or profile.get("canonical_doc_id"))
    topics = row.get("topics") or profile.get("topics") or []
    if not isinstance(topics, list):
        topics = []
    topics = [str(v).strip() for v in topics if str(v).strip()]
    topic_categories = row.get("topic_categories") or profile.get("topic_categories") or []
    if not isinstance(topic_categories, list):
        topic_categories = []
    topic_categories = [str(v).strip() for v in topic_categories if str(v).strip()]
    category_hierarchy = row.get("category_hierarchy") or profile.get("category_hierarchy") or {}
    if not isinstance(category_hierarchy, dict):
        category_hierarchy = {}
    result: dict[str, Any] = {
        "owner": owner,
        "project": project,
        "topics": topics,
        "canonical_doc_id": canonical_doc_id,
        "topic_categories": topic_categories,
        "category_hierarchy": category_hierarchy,
    }
    fb = meta.get("inference_fallback")
    if fb is None:
        fb = profile.get("inference_fallback")
    if fb is not None:
        result["profile_inference_fallback"] = _as_bool(fb, False)
    nr = meta.get("needs_review")
    if nr is not None:
        result["profile_needs_review"] = _as_bool(nr, False)
    llm_u = meta.get("llm_used")
    if llm_u is None:
        llm_u = profile.get("llm_used")
    if llm_u is not None:
        result["profile_llm_used"] = _as_bool(llm_u, False)
    src = meta.get("source")
    if src is not None and str(src).strip():
        result["profile_inference_source"] = str(src).strip()
    return result


def _normalize_projection_preset(value: Any, default: str = "standard") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _ALLOWED_PROJECTION_PRESETS:
        return normalized
    return default


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _as_int(value: Any, default: int, *, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _parse_tenant_projection_presets(raw_value: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    raw = str(raw_value or "").strip()
    if not raw:
        return mapping
    for pair in re.split(r"[;,]", raw):
        entry = pair.strip()
        if not entry or ":" not in entry:
            continue
        tenant_key, preset_key = entry.split(":", 1)
        tenant_id = tenant_key.strip()
        preset = _normalize_projection_preset(preset_key, default="")
        if tenant_id and preset in _ALLOWED_PROJECTION_PRESETS:
            mapping[tenant_id] = preset
    return mapping


def _resolve_projection_preset(tenant_id: str, requested_preset: str | None) -> tuple[str, str]:
    if requested_preset:
        normalized_requested = _normalize_projection_preset(requested_preset, default="")
        if normalized_requested not in _ALLOWED_PROJECTION_PRESETS:
            raise ValueError(
                "Invalid projection preset. Choose one of: strict, standard, relaxed."
            )
        return normalized_requested, "request"

    tenant_mapping = _parse_tenant_projection_presets(
        os.getenv("ONTOLOGY_PROJECTION_PRESET_BY_TENANT", "")
    )
    mapped = tenant_mapping.get(tenant_id)
    if mapped in _ALLOWED_PROJECTION_PRESETS:
        return mapped, "tenant_mapping"

    env_default = _normalize_projection_preset(
        os.getenv("ONTOLOGY_PROJECTION_PRESET_DEFAULT", "standard"),
        default="standard",
    )
    return env_default, "default"


def _default_user_projection_settings(tenant_id: str) -> dict[str, Any]:
    default_preset, _ = _resolve_projection_preset(tenant_id, None)
    default_max_documents = _as_int(
        os.getenv("ONTOLOGY_USER_SETTINGS_MAX_DOCUMENTS_DEFAULT", 2000),
        2000,
        minimum=1,
        maximum=10000,
    )
    default_auto_refresh = _as_bool(
        os.getenv("ONTOLOGY_USER_SETTINGS_AUTO_REFRESH_DEFAULT", True),
        True,
    )
    return {
        "projection_preset": default_preset,
        "max_documents": default_max_documents,
        "auto_refresh": default_auto_refresh,
    }


def get_ontology_user_settings(tenant_id: str, username: str) -> dict[str, Any]:
    binding = _binding(tenant_id)
    normalized_tenant_id = binding.tenant_id
    normalized_username = _as_text(username) or "unknown"
    records = _common_audit_repository.list_recent(
        normalized_tenant_id,
        domain="ontology",
        limit=500,
        offset=0,
    )
    defaults = _default_user_projection_settings(normalized_tenant_id)
    for record in records:
        action = str(record.action)
        if action not in {_USER_SETTINGS_PROJECTION_PRESET_ACTION, _USER_SETTINGS_UPDATE_ACTION}:
            continue
        if str(record.actor or "").strip() != normalized_username:
            continue
        metadata = record.metadata()
        preset = _normalize_projection_preset(
            metadata.get("projection_preset"), default=defaults["projection_preset"]
        )
        max_documents = _as_int(
            metadata.get("max_documents", defaults["max_documents"]),
            defaults["max_documents"],
            minimum=1,
            maximum=10000,
        )
        auto_refresh = _as_bool(
            metadata.get("auto_refresh", defaults["auto_refresh"]),
            defaults["auto_refresh"],
        )
        return {
            "projection_preset": preset,
            "max_documents": max_documents,
            "auto_refresh": auto_refresh,
            "updated_at": str(record.occurred_at),
            "source": "user_setting",
        }
    return {
        **defaults,
        "updated_at": "",
        "source": "tenant_default",
    }


def update_ontology_user_projection_preset(
    tenant_id: str,
    username: str,
    *,
    projection_preset: str,
) -> dict[str, Any]:
    return update_ontology_user_settings(
        tenant_id=tenant_id,
        username=username,
        projection_preset=projection_preset,
    )


def update_ontology_user_settings(
    tenant_id: str,
    username: str,
    *,
    projection_preset: str | None = None,
    max_documents: int | None = None,
    auto_refresh: bool | None = None,
) -> dict[str, Any]:
    binding = _binding(tenant_id)
    normalized_tenant_id = binding.tenant_id
    normalized_username = _as_text(username) or "unknown"
    current = get_ontology_user_settings(normalized_tenant_id, normalized_username)

    next_preset = current["projection_preset"]
    if projection_preset is not None:
        normalized_preset = _normalize_projection_preset(projection_preset, default="")
        if normalized_preset not in _ALLOWED_PROJECTION_PRESETS:
            raise ValueError("Invalid projection preset. Choose one of: strict, standard, relaxed.")
        next_preset = normalized_preset

    next_max_documents = current["max_documents"]
    if max_documents is not None:
        next_max_documents = _as_int(max_documents, current["max_documents"], minimum=1, maximum=10000)

    next_auto_refresh = current["auto_refresh"]
    if auto_refresh is not None:
        next_auto_refresh = bool(auto_refresh)

    _common_audit_repository.append(
        tenant_id=normalized_tenant_id,
        domain="ontology",
        actor=normalized_username,
        action=_USER_SETTINGS_UPDATE_ACTION,
        target=normalized_username,
        correlation_id="",
        metadata={
            "projection_preset": next_preset,
            "max_documents": int(next_max_documents),
            "auto_refresh": bool(next_auto_refresh),
        },
    )
    return {
        "projection_preset": next_preset,
        "max_documents": int(next_max_documents),
        "auto_refresh": bool(next_auto_refresh),
        "updated_at": "",
        "source": "user_setting",
    }


def _binding(tenant_id: str):
    normalized_tenant_id = _require_ontology_tenant(tenant_id)
    return _tenant_db_resolver.resolve(normalized_tenant_id)


def _query_tenant_rows(table_name: str, tenant_id: str) -> list[dict[str, Any]]:
    normalized_tenant_id = _require_ontology_tenant(tenant_id)
    table = _table(table_name)
    rows: list[dict[str, Any]] = []
    try:
        response = table.query(KeyConditionExpression=Key("tenant_id").eq(normalized_tenant_id))
        rows.extend(response.get("Items", []))
        while response.get("LastEvaluatedKey"):
            response = table.query(
                KeyConditionExpression=Key("tenant_id").eq(normalized_tenant_id),
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            rows.extend(response.get("Items", []))
    except Exception as query_error:
        _logger.warning(
            "Ontology query failed; falling back to scan. table=%s tenant_id=%s error=%s",
            table_name,
            normalized_tenant_id,
            str(query_error),
            exc_info=True,
        )
        try:
            response = table.scan(FilterExpression=Attr("tenant_id").eq(normalized_tenant_id))
            rows.extend(response.get("Items", []))
            while response.get("LastEvaluatedKey"):
                response = table.scan(
                    FilterExpression=Attr("tenant_id").eq(normalized_tenant_id),
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                )
                rows.extend(response.get("Items", []))
        except Exception as scan_error:
            _logger.error(
                "Ontology scan fallback failed. table=%s tenant_id=%s error=%s",
                table_name,
                normalized_tenant_id,
                str(scan_error),
                exc_info=True,
            )
            raise OntologyDataAccessError(
                "Failed to read tenant-scoped ontology rows from DynamoDB. "
                f"table={table_name} tenant_id={normalized_tenant_id}"
            ) from scan_error
    return [_to_plain(row) for row in rows]


def get_ontology_overview(tenant_id: str) -> dict[str, Any]:
    binding = _binding(tenant_id)
    normalized_tenant_id = binding.tenant_id
    governance_document_analysis_table_name = getattr(
        binding, "governance_document_analysis_table_name", ""
    )
    unified = _query_tenant_rows(binding.ontology_unified_metadata_table_name, normalized_tenant_id)
    active_unified = [item for item in unified if not bool(item.get("is_deleted", False))]
    active_unified_item_ids = {
        _as_text(item.get("item_id"))
        for item in active_unified
        if _as_text(item.get("item_id"))
    }
    candidates = _query_tenant_rows(binding.ontology_entity_candidate_table_name, normalized_tenant_id)
    document_analysis_rows: list[dict[str, Any]] = []
    document_analysis_query_error = ""
    if governance_document_analysis_table_name:
        try:
            document_analysis_rows = _query_tenant_rows(
                governance_document_analysis_table_name,
                normalized_tenant_id,
            )
        except Exception as analysis_error:
            document_analysis_query_error = str(analysis_error)
    unresolved_candidates = sum(
        1 for row in candidates if str(row.get("status", "pending")).lower() == "pending"
    )
    stale_or_aging = sum(
        1
        for row in unified
        if str(row.get("freshness_status", "")).lower() in {"stale", "aging"}
    )
    profile_rows = [_extract_document_profile(row) for row in unified]
    owner_identified = sum(1 for profile in profile_rows if profile["owner"])
    project_identified = sum(1 for profile in profile_rows if profile["project"])
    topic_categorized = sum(1 for profile in profile_rows if profile["topic_categories"])
    canonicalized_documents = sum(1 for profile in profile_rows if profile["canonical_doc_id"])
    total = len(unified) or 1

    freshness_scores: list[float] = []
    uniqueness_scores: list[float] = []
    ontology_scores: list[float] = []
    base_scores: list[float] = []
    use_case_scores: list[float] = []
    freshness_validity_scores: list[float] = []
    canonicality_duplication_scores: list[float] = []
    stewardship_findability_scores: list[float] = []
    intent_coverage_scores: list[float] = []
    evidence_coverage_scores: list[float] = []
    freshness_fit_scores: list[float] = []
    benchmark_lite_scores: list[float] = []
    intent_counts: dict[str, int] = defaultdict(int)
    for item in unified:
        profile = _extract_document_profile(item)
        scores = _compute_quality_scores_from_item(
            item, profile["owner"], profile["project"], profile["topic_categories"]
        )
        freshness_scores.append(float(scores["freshness_score"]))
        uniqueness_scores.append(float(scores["uniqueness_score"]))
        ontology_score = item.get("ontology_score")
        if ontology_score is not None:
            ontology_scores.append(_clamp_ratio(_as_float(ontology_score)))
        base_score = item.get("base_ontology_score")
        if base_score is not None:
            base_scores.append(_clamp_ratio(_as_float(base_score)))
        use_case_score = item.get("use_case_readiness")
        if use_case_score is not None:
            use_case_scores.append(_clamp_ratio(_as_float(use_case_score)))
        fv = item.get("freshness_validity")
        if fv is not None:
            freshness_validity_scores.append(_clamp_ratio(_as_float(fv)))
        cd = item.get("canonicality_duplication")
        if cd is not None:
            canonicality_duplication_scores.append(_clamp_ratio(_as_float(cd)))
        sf = item.get("stewardship_findability")
        if sf is not None:
            stewardship_findability_scores.append(_clamp_ratio(_as_float(sf)))
        ic = item.get("intent_coverage")
        if ic is not None:
            intent_coverage_scores.append(_clamp_ratio(_as_float(ic)))
        ec = item.get("evidence_coverage")
        if ec is not None:
            evidence_coverage_scores.append(_clamp_ratio(_as_float(ec)))
        ff = item.get("freshness_fit")
        if ff is not None:
            freshness_fit_scores.append(_clamp_ratio(_as_float(ff)))
        bl = item.get("benchmark_lite")
        if bl is not None:
            benchmark_lite_scores.append(_clamp_ratio(_as_float(bl)))
        intent_tags = item.get("intent_tags")
        if isinstance(intent_tags, list):
            for intent_tag in intent_tags:
                tag = _as_text(intent_tag)
                if tag:
                    intent_counts[tag] += 1

    avg_freshness = sum(freshness_scores) / len(freshness_scores) if freshness_scores else 0.0
    avg_uniqueness = sum(uniqueness_scores) / len(uniqueness_scores) if uniqueness_scores else 0.0

    non_stale_ratio = max(0.0, 1.0 - stale_or_aging / total)
    freshness_signal = round(max(0.0, min(1.0, non_stale_ratio * 0.5 + min(1.0, avg_freshness) * 0.5)), 3)

    candidate_ratio = max(0.0, 1.0 - unresolved_candidates / max(1, len(candidates)))
    duplication_signal = round(max(0.0, min(1.0, candidate_ratio * 0.5 + avg_uniqueness * 0.5)), 3)

    meaningful_owner = sum(1 for p in profile_rows if _has_meaningful_owner(p["owner"]))
    meaningful_project = sum(1 for p in profile_rows if _has_meaningful_project(p["project"]))
    meaningful_topic = sum(1 for p in profile_rows if _has_meaningful_topic_categories(p["topic_categories"]))
    location_coverage = (meaningful_owner + meaningful_project + meaningful_topic) / (3.0 * total)
    location_signal = round(max(0.0, min(1.0, location_coverage)), 3)
    freshness_validity = round(
        _mean_or_default(freshness_validity_scores, default=freshness_signal), 3
    )
    canonicality_duplication = round(
        _mean_or_default(canonicality_duplication_scores, default=duplication_signal), 3
    )
    stewardship_findability = round(
        _mean_or_default(stewardship_findability_scores, default=location_signal), 3
    )
    base_ontology_score = round(
        _mean_or_default(
            base_scores,
            default=(
                0.35 * freshness_validity
                + 0.35 * canonicality_duplication
                + 0.30 * stewardship_findability
            ),
        ),
        3,
    )
    intent_coverage = round(_mean_or_default(intent_coverage_scores, default=0.0), 3)
    evidence_coverage = round(_mean_or_default(evidence_coverage_scores, default=0.0), 3)
    freshness_fit = round(_mean_or_default(freshness_fit_scores, default=0.0), 3)
    benchmark_lite = round(_mean_or_default(benchmark_lite_scores, default=0.0), 3)
    use_case_readiness = round(
        _mean_or_default(
            use_case_scores,
            default=(
                0.45 * intent_coverage
                + 0.30 * evidence_coverage
                + 0.15 * freshness_fit
                + 0.10 * benchmark_lite
            ),
        ),
        3,
    )
    ontology_score = round(
        _mean_or_default(
            ontology_scores,
            default=0.70 * base_ontology_score + 0.30 * use_case_readiness,
        ),
        3,
    )
    intent_breakdown = [
        {"intent_id": intent_id, "label": intent_id, "score": round(count / len(unified), 3)}
        for intent_id, count in sorted(intent_counts.items(), key=lambda x: x[1], reverse=True)
        if len(unified) > 0
    ]
    high_spread_entities = _compute_high_spread_entities(unified, candidates)
    latest_projection = _latest_projection_metrics(normalized_tenant_id)
    analysis_item_ids = {
        _as_text(item.get("item_id"))
        for item in document_analysis_rows
        if _as_text(item.get("item_id"))
    }
    matched_item_ids = active_unified_item_ids & analysis_item_ids
    unmatched_analysis_item_ids = analysis_item_ids - active_unified_item_ids
    coverage_denominator = len(active_unified_item_ids)
    analysis_coverage = (
        round(len(matched_item_ids) / coverage_denominator, 4)
        if coverage_denominator > 0
        else 0.0
    )
    return {
        "unified_document_count": len(unified),
        "active_unified_document_count": len(active_unified),
        "entity_candidate_count": len(candidates),
        "unresolved_candidates": unresolved_candidates,
        "stale_or_aging_documents": stale_or_aging,
        "owner_identified_documents": owner_identified,
        "project_identified_documents": project_identified,
        "topic_categorized_documents": topic_categorized,
        "canonicalized_documents": canonicalized_documents,
        "noun_resolution_enabled": _feature_enabled("ONTOLOGY_NOUN_RESOLUTION_ENABLED", False),
        "high_spread_entities": high_spread_entities,
        "signal_scores": {
            "freshness": freshness_signal,
            "duplication": duplication_signal,
            "location": location_signal,
            "overall": round((freshness_signal + duplication_signal + location_signal) / 3.0, 3),
        },
        "ontology_score": _clamp_ratio(ontology_score),
        "base_ontology_score": _clamp_ratio(base_ontology_score),
        "use_case_readiness": _clamp_ratio(use_case_readiness),
        "freshness_validity": _clamp_ratio(freshness_validity),
        "canonicality_duplication": _clamp_ratio(canonicality_duplication),
        "stewardship_findability": _clamp_ratio(stewardship_findability),
        "intent_coverage": _clamp_ratio(intent_coverage),
        "evidence_coverage": _clamp_ratio(evidence_coverage),
        "freshness_fit": _clamp_ratio(freshness_fit),
        "benchmark_lite": _clamp_ratio(benchmark_lite),
        "intent_breakdown": intent_breakdown,
        "document_analysis_contract": {
            "target_mode": _ontology_analysis_target_mode(),
            "description": (
                "DocumentAnalysis is stored for governance-eligible documents "
                "and may be a subset of UnifiedMetadata."
            ),
        },
        "document_analysis_metrics": {
            "table_name": governance_document_analysis_table_name,
            "analysis_total_count": len(document_analysis_rows),
            "matched_unified_count": len(matched_item_ids),
            "active_unified_total_count": len(active_unified_item_ids),
            "coverage_ratio": analysis_coverage,
            "unmatched_analysis_count": len(unmatched_analysis_item_ids),
            "query_error": document_analysis_query_error,
        },
        "projection_metrics": latest_projection,
    }


def _latest_projection_metrics(tenant_id: str) -> dict[str, Any]:
    records = _common_audit_repository.list_recent(
        tenant_id,
        domain="ontology",
        limit=200,
        offset=0,
    )
    for record in records:
        if str(record.action) != "graph_projection.refresh":
            continue
        metadata = record.metadata()
        return {
            "last_refresh_at": str(record.occurred_at),
            "projected_documents": int(metadata.get("projected_documents", 0) or 0),
            "contained_in_links": int(metadata.get("contained_in_links", 0) or 0),
            "mentions_links": int(metadata.get("mentions_links", 0) or 0),
            "similarity_links": int(metadata.get("similarity_links", 0) or 0),
            "skipped_similarity_docs": int(metadata.get("skipped_similarity_docs", 0) or 0),
            "text_fallback_vectors": int(metadata.get("text_fallback_vectors", 0) or 0),
            "auto_promoted_entities": int(metadata.get("auto_promoted_entities", 0) or 0),
            "projection_preset": str(metadata.get("projection_preset", "standard") or "standard"),
            "projection_preset_source": str(
                metadata.get("projection_preset_source", "default") or "default"
            ),
        }
    return {
        "last_refresh_at": "",
        "projected_documents": 0,
        "contained_in_links": 0,
        "mentions_links": 0,
        "similarity_links": 0,
        "skipped_similarity_docs": 0,
        "text_fallback_vectors": 0,
        "auto_promoted_entities": 0,
        "projection_preset": "standard",
        "projection_preset_source": "default",
    }


def list_ontology_unified_metadata(
    tenant_id: str,
    *,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    binding = _binding(tenant_id)
    normalized_tenant_id = binding.tenant_id
    rows = _query_tenant_rows(binding.ontology_unified_metadata_table_name, normalized_tenant_id)
    # Soft-deleted rows are kept for retention/TTL, but should not be listed in UI.
    rows = [item for item in rows if not bool(item.get("is_deleted", False))]
    normalized_rows: list[dict[str, Any]] = []
    for item in rows:
        profile = _extract_document_profile(item)
        enriched = dict(item)
        enriched["owner"] = profile["owner"]
        enriched["project"] = profile["project"]
        enriched["topics"] = profile["topics"]
        enriched["canonical_doc_id"] = profile["canonical_doc_id"]
        enriched["topic_categories"] = profile["topic_categories"]
        enriched["category_hierarchy"] = profile["category_hierarchy"]
        for k, v in profile.items():
            if str(k).startswith("profile_"):
                enriched[k] = v
        normalized_rows.append(enriched)
    normalized_rows.sort(
        key=lambda item: str(item.get("last_modified", item.get("updated_at", ""))),
        reverse=True,
    )
    bounded_limit = max(1, min(int(limit), 500))
    bounded_offset = max(0, int(offset))
    return {
        "rows": normalized_rows[bounded_offset : bounded_offset + bounded_limit],
        "pagination": {
            "limit": bounded_limit,
            "offset": bounded_offset,
            "total_count": len(normalized_rows),
        },
    }


def list_ontology_entity_master(
    tenant_id: str,
    *,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    binding = _binding(tenant_id)
    normalized_tenant_id = binding.tenant_id
    noun_enabled = _feature_enabled("ONTOLOGY_NOUN_RESOLUTION_ENABLED", False)
    table_name = binding.ontology_entity_master_table_name if noun_enabled else ""
    rows: list[dict[str, Any]]
    if table_name:
        rows = _query_tenant_rows(table_name, normalized_tenant_id)
    else:
        rows = []
    rows.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
    bounded_limit = max(1, min(int(limit), 500))
    bounded_offset = max(0, int(offset))
    return {
        "rows": rows[bounded_offset : bounded_offset + bounded_limit],
        "pagination": {
            "limit": bounded_limit,
            "offset": bounded_offset,
            "total_count": len(rows),
        },
        "source": "dynamodb_projection" if table_name else "feature_disabled",
        "noun_resolution_enabled": noun_enabled,
    }


def list_ontology_audit_logs(
    tenant_id: str,
    *,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    binding = _binding(tenant_id)
    normalized_tenant_id = binding.tenant_id
    table_name = binding.ontology_entity_audit_table_name
    if table_name:
        rows = _query_tenant_rows(table_name, normalized_tenant_id)
        rows.sort(key=lambda item: str(item.get("event_time", item.get("timestamp", ""))), reverse=True)
    else:
        rows = _query_tenant_rows(binding.ontology_lineage_event_table_name, normalized_tenant_id)
        rows = [
            {
                "tenant_id": row.get("tenant_id"),
                "event": row.get("event_type", "lineage"),
                "job_name": row.get("job_name", ""),
                "status": row.get("status", ""),
                "operator": row.get("operator", "system"),
                "correlation_id": row.get("correlation_id", ""),
                "timestamp": row.get("event_time", row.get("updated_at", "")),
                "source": "lineage_event",
            }
            for row in rows
        ]
        rows.sort(key=lambda item: str(item.get("timestamp", "")), reverse=True)

    bounded_limit = max(1, min(int(limit), 500))
    bounded_offset = max(0, int(offset))
    return {
        "rows": rows[bounded_offset : bounded_offset + bounded_limit],
        "pagination": {
            "limit": bounded_limit,
            "offset": bounded_offset,
            "total_count": len(rows),
        },
    }


def get_ontology_graph_by_item(
    tenant_id: str,
    *,
    item_id: str,
    file_name: str = "",
    max_nodes: int = 80,
) -> dict[str, Any]:
    binding = _binding(tenant_id)
    normalized_tenant_id = binding.tenant_id
    rows = _query_tenant_rows(binding.ontology_unified_metadata_table_name, normalized_tenant_id)
    normalized_item_id = _as_text(item_id).lower()
    normalized_file_name = _as_text(file_name).lower()

    matched_row: dict[str, Any] | None = None
    matched_by = "fallback"
    for row in rows:
        row_item_id = _as_text(row.get("item_id")).lower()
        if normalized_item_id and row_item_id == normalized_item_id:
            matched_row = row
            matched_by = "item_id"
            break
    if matched_row is None and normalized_file_name:
        for row in rows:
            title = _as_text(row.get("title")).lower()
            if normalized_file_name and normalized_file_name in title:
                matched_row = row
                matched_by = "file_name"
                break

    if matched_row is None:
        return {
            "nodes": [],
            "edges": [],
            "is_truncated": False,
            "start_node_id": None,
            "matched_by": matched_by,
        }

    item_suffix = _as_text(matched_row.get("item_id", item_id)) or _as_text(item_id)
    title = _as_text(matched_row.get("title")) or item_suffix or "document"
    source = _as_text(matched_row.get("source")) or "aws"
    freshness_status = _as_text(matched_row.get("freshness_status")) or "active"
    quality_score = float(matched_row.get("content_quality_score", 0.0) or 0.0)
    transformed_at = _as_text(matched_row.get("transformed_at")) or _as_text(
        matched_row.get("last_modified")
    )
    profile = _extract_document_profile(matched_row)
    start_node_id = f"doc:{_as_text(item_suffix) or title}"
    nodes: list[dict[str, Any]] = [
        {
            "id": start_node_id,
            "labels": [title],
            "properties": {
                "entity_type": "document",
                "item_id": _as_text(item_suffix),
                "file_name": title,
                "source": source,
                "freshnessStatus": freshness_status,
                "contentQualityScore": quality_score,
                "owner": profile["owner"],
                "project": profile["project"],
                "topic_categories": profile["topic_categories"],
                "canonical_doc_id": profile["canonical_doc_id"],
                "transformed_at": transformed_at,
            },
        }
    ]
    edges: list[dict[str, Any]] = []

    helper_fields = [
        ("owner", profile["owner"], "owned_by", "person"),
        ("project", profile["project"], "belongs_to_project", "project"),
        ("canonical", profile["canonical_doc_id"], "derived_from", "document"),
    ]
    for prefix, field_value, relation, entity_type in helper_fields:
        field_value = _as_text(field_value)
        if not field_value:
            continue
        node_id = f"{prefix}:{field_value}"
        nodes.append(
            {
                "id": node_id,
                "labels": [field_value],
                "properties": {
                    "entity_type": entity_type,
                    "resolution_status": "resolved",
                },
            }
        )
        edges.append(
            {
                "id": f"{start_node_id}->{node_id}",
                "source": start_node_id,
                "target": node_id,
                "type": relation,
                "properties": {"weight": 1.0, "keywords": relation},
            }
        )

    for category in profile["topic_categories"]:
        node_id = f"topic:{category}"
        nodes.append(
            {
                "id": node_id,
                "labels": [category],
                "properties": {"entity_type": "topic_category", "resolution_status": "resolved"},
            }
        )
        edges.append(
            {
                "id": f"{start_node_id}->{node_id}",
                "source": start_node_id,
                "target": node_id,
                "type": "categorized_as",
                "properties": {"weight": 1.0, "keywords": "topic_category"},
            }
        )

    max_allowed = max(1, min(int(max_nodes), 500))
    is_truncated = len(nodes) > max_allowed
    if is_truncated:
        nodes = nodes[:max_allowed]
        node_ids = {node["id"] for node in nodes}
        edges = [
            edge
            for edge in edges
            if edge["source"] in node_ids and edge["target"] in node_ids
        ]

    return {
        "nodes": nodes,
        "edges": edges,
        "is_truncated": is_truncated,
        "start_node_id": start_node_id,
        "matched_by": matched_by,
    }


def refresh_ontology_graph_projection(
    tenant_id: str,
    *,
    clear_existing: bool = True,
    max_documents: int = 2000,
    preset: str | None = None,
) -> dict[str, Any]:
    """AWS UnifiedMetadata を基に ontology グラフ投影を再構築する。"""
    binding = _binding(tenant_id)
    normalized_tenant_id = binding.tenant_id
    governance_document_analysis_table_name = getattr(
        binding, "governance_document_analysis_table_name", ""
    )
    rows = _query_tenant_rows(binding.ontology_unified_metadata_table_name, normalized_tenant_id)
    rows = [item for item in rows if not bool(item.get("is_deleted", False))]
    try:
        document_analysis_rows = (
            _query_tenant_rows(
                governance_document_analysis_table_name,
                normalized_tenant_id,
            )
            if governance_document_analysis_table_name
            else []
        )
    except Exception:
        # DocumentAnalysis projection is optional for refresh; keep rebuilding with Unified only.
        document_analysis_rows = []
    analysis_by_item_id = {
        _as_text(item.get("item_id")): item
        for item in document_analysis_rows
        if _as_text(item.get("item_id"))
    }
    for row in rows:
        item_id = _as_text(row.get("item_id"))
        if not item_id:
            continue
        analysis = analysis_by_item_id.get(item_id)
        if not analysis:
            continue
        if not _as_text(row.get("embedding_ref")):
            row["embedding_ref"] = _as_text(
                analysis.get("embedding_ref")
                or analysis.get("embedding_s3_key")
            )
        topic_keywords = row.get("topic_keywords")
        if not topic_keywords:
            row["topic_keywords"] = analysis.get("topic_keywords") or analysis.get("noun_chunks") or []
        if not _as_text(row.get("document_summary")):
            row["document_summary"] = _as_text(
                analysis.get("document_summary") or analysis.get("summary")
            )
    rows.sort(key=lambda item: str(item.get("last_modified", item.get("updated_at", ""))), reverse=True)
    bounded_max_documents = max(1, min(int(max_documents), 10000))
    projected_rows = rows[:bounded_max_documents]
    resolved_preset, preset_source = _resolve_projection_preset(normalized_tenant_id, preset)
    projection_options = dict(_PROJECTION_PRESET_OPTIONS[resolved_preset])
    result = rebuild_ontology_graph_projection_from_unified(
        tenant_id=normalized_tenant_id,
        unified_rows=projected_rows,
        clear_existing=bool(clear_existing),
        projection_options=projection_options,
    )
    return {
        "tenant_id": normalized_tenant_id,
        "clear_existing": bool(clear_existing),
        "max_documents": bounded_max_documents,
        "source_documents": len(projected_rows),
        "projection_preset": resolved_preset,
        "projection_preset_source": preset_source,
        "projection_options": projection_options,
        **result,
    }


# ---------------------------------------------------------------------------
# AI auto-fill & user manual edit for unified metadata profile fields
# ---------------------------------------------------------------------------

_PROJECT_MARKER_ONLY_RE = re.compile(
    r"^(?:project|projects|proj|pj|案件|プロジェクト)$", re.IGNORECASE
)
_PROJECT_PREFIX_RE = re.compile(
    r"^(?:project|projects|proj|pj|案件|プロジェクト)[\s_\-]+", re.IGNORECASE
)
_PROJECT_TOKEN_RE = re.compile(r"\b(project|projects|proj|pj)\b", re.IGNORECASE)
_PROJECT_IN_TEXT_RE = re.compile(
    r"(?:^|[\s_\-/\\])(project|projects|proj|pj|案件|プロジェクト)"
    r"[\s_\-]*([0-9A-Za-z一-龯ぁ-んァ-ヶ]+)",
    re.IGNORECASE,
)
_ID_LIKE_TOKEN_RE = re.compile(r"^[a-z0-9_-]{20,}$", re.IGNORECASE)
_ALPHANUM_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_-]{2,}")
_PROJECT_PATH_IGNORE_TOKENS = {
    "sites",
    "drives",
    "shared documents",
    "documents",
    "forms",
    "_layouts",
    "formserver",
    "root",
    "root:",
    "img",
    "image",
    "images",
    "tmp",
    "temp",
}
_GENERIC_PROJECT_TOKENS = {"general", "unknown", "n/a", "none", "-", "shared", "documents"}


def _has_meaningful_owner(owner: str) -> bool:
    normalized = (owner or "").strip().lower()
    return len(normalized) > 0 and normalized != "unknown"


def _is_project_id_like(project: str) -> bool:
    normalized = (project or "").strip()
    if not normalized:
        return False
    compact = normalized.replace(" ", "")
    if re.match(r"^b![A-Za-z0-9_-]{16,}$", compact):
        return True
    if re.match(r"^b![A-Za-z0-9_-]{16,}$", normalized):
        return True
    if re.match(r"^[0-9a-f]{8}-[0-9a-f-]{27,}$", normalized, re.IGNORECASE):
        return True
    if re.match(r"^ent-proj-[0-9a-z-]+$", normalized, re.IGNORECASE):
        return True
    return False


def _has_meaningful_project(project: str) -> bool:
    normalized = (project or "").strip().lower()
    if not normalized or normalized == "general":
        return False
    return not _is_project_id_like(project)


def _has_meaningful_topic_categories(topics: list) -> bool:
    if not isinstance(topics, list) or len(topics) == 0:
        return False
    return any(str(t).strip().lower() != "general" for t in topics)


def _compute_location_score(owner: str, project: str, topic_categories: list) -> float:
    score = 0
    if _has_meaningful_owner(owner):
        score += 1
    if _has_meaningful_project(project):
        score += 1
    if _has_meaningful_topic_categories(topic_categories):
        score += 1
    return round(score / 3.0, 2)


def _compute_document_location_completeness(owner: str, project: str, topic_categories: list) -> float:
    score = 0
    if _has_meaningful_owner(owner):
        score += 1
    if _has_meaningful_project(project):
        score += 1
    if _has_meaningful_topic_categories(topic_categories):
        score += 1
    return score / 3.0


def _resolve_candidate_item_id(candidate: dict[str, Any]) -> str:
    return _as_text(candidate.get("item_id") or candidate.get("source_item_id"))


def _resolve_candidate_entity_key(candidate: dict[str, Any]) -> str:
    return _as_text(
        candidate.get("resolved_entity_id")
        or candidate.get("entity_id")
        or candidate.get("canonical_value")
        or candidate.get("normalized_form")
        or candidate.get("surface_form")
    )


def _compute_high_spread_entities(
    unified_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
    *,
    low_location_threshold: float = 0.5,
) -> int:
    if not unified_rows or not candidate_rows:
        return 0

    profile_by_item_id: dict[str, dict[str, Any]] = {}
    for row in unified_rows:
        item_id = _as_text(row.get("item_id"))
        if not item_id:
            continue
        profile_by_item_id[item_id] = _extract_document_profile(row)

    total_documents = len(profile_by_item_id)
    if total_documents == 0:
        return 0

    # High-spread threshold scales with dataset size while keeping a small-sample floor.
    spread_doc_threshold = max(5, int(math.ceil(total_documents * 0.05)))
    entity_to_items: dict[str, set[str]] = defaultdict(set)
    for candidate in candidate_rows:
        entity_key = _resolve_candidate_entity_key(candidate)
        item_id = _resolve_candidate_item_id(candidate)
        if not entity_key or not item_id:
            continue
        if item_id not in profile_by_item_id:
            continue
        entity_to_items[entity_key].add(item_id)

    high_spread = 0
    for item_ids in entity_to_items.values():
        if len(item_ids) < spread_doc_threshold:
            continue
        location_scores: list[float] = []
        for item_id in item_ids:
            profile = profile_by_item_id[item_id]
            location_scores.append(
                _compute_document_location_completeness(
                    profile["owner"],
                    profile["project"],
                    profile["topic_categories"],
                )
            )
        if not location_scores:
            continue
        avg_location = sum(location_scores) / len(location_scores)
        if avg_location < low_location_threshold:
            high_spread += 1
    return high_spread


def _compute_quality_scores_from_item(
    item: dict[str, Any],
    owner: str,
    project: str,
    topic_categories: list,
) -> dict[str, Any]:
    """Compute quality scores using persisted DynamoDB fields when present.

    Falls back to heuristic calculation when stored scores are absent.
    """
    stored_freshness = item.get("freshness_score")
    if stored_freshness is not None:
        try:
            freshness_score = round(max(0.05, min(2.0, float(stored_freshness))), 3)
        except (TypeError, ValueError):
            freshness_score = None
    else:
        freshness_score = None

    if freshness_score is None:
        freshness_status = _as_text(item.get("freshness_status")) or "active"
        last_modified = _as_text(item.get("last_modified"))
        if last_modified:
            from datetime import datetime, timezone as _tz
            try:
                dt = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
                days = (datetime.now(_tz.utc) - dt).days
                if days > 365:
                    freshness_score = 0.1
                elif days > 90:
                    freshness_score = 0.5
                elif days > 30:
                    freshness_score = 1.0
                else:
                    freshness_score = 2.0
            except (ValueError, TypeError):
                freshness_score = 0.82 if freshness_status == "active" else 0.61 if freshness_status == "aging" else 0.32
        else:
            freshness_score = 0.82 if freshness_status == "active" else 0.61 if freshness_status == "aging" else 0.32

    duplicate_info_raw = item.get("duplicate_group_id")
    is_canonical = bool(item.get("is_canonical_copy", False))
    if duplicate_info_raw:
        uniqueness_score = 1.0 if is_canonical else 0.5
    else:
        uniqueness_score = 1.0

    relevance_score = _compute_document_relevance_score(item, owner, project, topic_categories)

    location_score = _compute_location_score(owner, project, topic_categories)
    content_quality_score = round(
        max(0.005, min(2.0, freshness_score * uniqueness_score * relevance_score)), 3
    )
    return {
        "freshness_score": round(freshness_score, 3),
        "uniqueness_score": round(uniqueness_score, 3),
        "relevance_score": round(relevance_score, 3),
        "content_quality_score": content_quality_score,
        "location_score": location_score,
    }


def _compute_document_relevance_score(
    item: dict[str, Any],
    owner: str,
    project: str,
    topic_categories: list,
) -> float:
    """Document relevance heuristic aligned with ontology content-quality scoring."""
    score = 0.1
    if owner and owner.lower() != "unknown":
        score += 0.25
    if project and project.lower() != "general" and not _is_project_id_like(project):
        score += 0.25
    if isinstance(topic_categories, list) and any(
        str(c).strip().lower() != "general" for c in topic_categories
    ):
        score += 0.2
    summary = _as_text(item.get("document_summary") or item.get("summary"))
    if summary:
        score += 0.15
    raw_kw = item.get("topic_keywords") or []
    if isinstance(raw_kw, list) and len([v for v in raw_kw if str(v).strip()]) > 0:
        score += 0.15
    return round(max(0.1, min(1.0, score)), 3)


def _is_meaningful_owner(owner: str) -> bool:
    normalized = _as_text(owner).lower()
    return bool(normalized) and normalized not in {"unknown", "n/a", "none", "-"}


def _is_meaningful_project(project: str) -> bool:
    normalized = _as_text(project).lower()
    if not normalized or normalized in {"general", "unknown", "n/a", "none", "-"}:
        return False
    if normalized.startswith("_"):
        return False
    if normalized in {"_layouts", "formserver", "sites", "drives"}:
        return False
    if re.search(r"\.[a-z0-9]{1,8}$", normalized):
        return False
    if "b!" in normalized:
        return False
    if re.fullmatch(r"[a-z0-9_-]{24,}", normalized):
        return False
    return not _is_project_id_like(project)


def _normalize_project_candidate(raw_value: Any) -> str:
    candidate = _as_text(raw_value).strip("/").strip("\\")
    if not candidate:
        return ""
    candidate = re.sub(r"\.[a-z0-9]{1,8}$", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"[_\-]+", " ", candidate)
    candidate = re.sub(r"\s+", " ", candidate).strip()
    if not candidate:
        return ""
    compact = re.sub(r"\s+", "", candidate).lower()
    if compact.startswith("b!") and len(compact) >= 18:
        return ""
    if _ID_LIKE_TOKEN_RE.fullmatch(compact):
        return ""
    normalized = candidate.lower()
    if normalized in _GENERIC_PROJECT_TOKENS:
        return ""
    if normalized.startswith("_"):
        return ""
    if any(token in _PROJECT_PATH_IGNORE_TOKENS for token in normalized.split()):
        return ""
    return candidate


def _extract_origin_path_segments(origin_url: str) -> list[tuple[str, bool]]:
    try:
        parsed = urlparse(origin_url)
    except Exception:
        return []
    segments = [segment.strip() for segment in unquote(parsed.path or "").split("/") if segment.strip()]
    if not segments:
        return []
    lower_segments = [segment.lower() for segment in segments]
    try:
        shared_idx = lower_segments.index("shared documents")
    except ValueError:
        shared_idx = -1
    return [(segment, shared_idx >= 0 and idx > shared_idx) for idx, segment in enumerate(segments)]


def _extract_project_from_origin_host(origin_url: str) -> str:
    try:
        parsed = urlparse(origin_url)
    except Exception:
        return ""
    host = _as_text(parsed.hostname).lower()
    if not host:
        return ""
    prefix = host.split(".")[0].strip()
    if not prefix:
        return ""
    candidate = re.sub(r"\d+$", "", prefix) or prefix
    if candidate in {"www", "localhost", "sharepoint"}:
        return ""
    if _ID_LIKE_TOKEN_RE.fullmatch(candidate):
        return ""
    return candidate


def _looks_like_filename(value: str) -> bool:
    return bool(re.search(r"\.[a-z0-9]{1,8}$", _as_text(value).lower()))


def _looks_like_project_segment(segment_norm: str) -> bool:
    if _PROJECT_MARKER_ONLY_RE.fullmatch(segment_norm):
        return True
    if _PROJECT_PREFIX_RE.match(segment_norm):
        return True
    if _PROJECT_TOKEN_RE.search(segment_norm):
        return True
    return "案件" in segment_norm or "プロジェクト" in segment_norm


def _infer_project_from_item_with_shared_rules(item: dict[str, Any]) -> str:
    candidates: list[str] = []

    source_identifiers = item.get("source_identifiers")
    if isinstance(source_identifiers, str):
        source_identifiers = _parse_json_dict(source_identifiers)
    if isinstance(source_identifiers, dict):
        for key in ("project_id", "project", "project_name", "site_name", "site_id", "drive_id"):
            value = _normalize_project_candidate(source_identifiers.get(key))
            if value:
                candidates.append(value)

    for key in ("site_name", "drive_name", "project"):
        value = _normalize_project_candidate(item.get(key))
        if value:
            candidates.append(value)

    origin_url = _as_text(item.get("origin_url"))
    if origin_url:
        host_candidate = _normalize_project_candidate(_extract_project_from_origin_host(origin_url))
        if host_candidate:
            candidates.append(host_candidate)
        for segment, from_shared in _extract_origin_path_segments(origin_url):
            normalized = _as_text(segment).lower()
            if not normalized or normalized in _PROJECT_PATH_IGNORE_TOKENS or normalized.startswith("_"):
                continue
            if _looks_like_filename(segment):
                if from_shared:
                    break
                continue
            normalized_candidate = _normalize_project_candidate(segment)
            if normalized_candidate:
                candidates.append(normalized_candidate)
                if from_shared:
                    break

    hierarchy_path = _as_text(item.get("hierarchy_path")).replace("\\", "/").strip("/")
    for segment in [s.strip() for s in hierarchy_path.split("/") if s.strip()]:
        segment_norm = _as_text(segment).lower()
        if _looks_like_project_segment(segment_norm):
            normalized = _normalize_project_candidate(segment)
            if normalized:
                candidates.append(normalized)

    title_nfkc = _as_text(item.get("title") or item.get("name"))
    for match in _PROJECT_IN_TEXT_RE.finditer(title_nfkc):
        normalized = _normalize_project_candidate(f"{match.group(1)} {match.group(2)}")
        if normalized:
            candidates.append(normalized)
    for token in _ALPHANUM_TOKEN_RE.findall(title_nfkc):
        normalized = _normalize_project_candidate(token)
        if normalized:
            candidates.append(normalized)

    for value in candidates:
        if _is_meaningful_project(value):
            return value
    return ""


def _extract_source_owner_from_item(item: dict[str, Any]) -> str:
    candidates: list[str] = []
    for key in (
        "author",
        "created_by",
        "created_by_user_name",
        "last_modified_by",
        "owner",
    ):
        value = _as_text(item.get(key))
        if value:
            candidates.append(value)

    modified_by = item.get("modified_by")
    if isinstance(modified_by, dict):
        for key in ("displayName", "userPrincipalName", "email", "id"):
            value = _as_text(modified_by.get(key))
            if value:
                candidates.append(value)

    origin_url = _as_text(item.get("origin_url"))
    if origin_url:
        try:
            host = _as_text(urlparse(origin_url).hostname)
            if host:
                org = host.split(".")[0].strip()
                if org:
                    candidates.append(org)
        except Exception:
            pass

    for value in candidates:
        if _is_meaningful_owner(value):
            return value
    return ""


def _extract_source_project_from_item(item: dict[str, Any]) -> str:
    return _infer_project_from_item_with_shared_rules(item)


def _infer_topic_categories_from_file_signals(
    *,
    title: str,
    origin_url: str,
    hierarchy_path: str,
) -> list[str]:
    text = f"{title} {origin_url} {hierarchy_path}".lower()
    extension = ""
    title_match = re.search(r"\.([a-z0-9]{1,8})$", title.lower().strip())
    if title_match:
        extension = title_match.group(1)

    if extension in {"png", "jpg", "jpeg", "gif", "webp", "bmp"} or any(
        token in text for token in ("/img/", "/image/", "/images/", "screenshot", "スクリーンショット")
    ):
        return ["image_evidence", "screenshot"]
    if any(token in text for token in ("webhook", "hook", "callback")):
        return ["webhook_event", "integration_test"]
    if extension in {"ppt", "pptx"}:
        return ["presentation_material"]
    if extension in {"xls", "xlsx", "csv"}:
        return ["data_sheet"]
    if extension in {"pdf"}:
        return ["reference_document"]
    return []


def _infer_topic_categories_from_text(
    *,
    title: str,
    summary: str,
    topic_keywords: list[str],
    existing_topics: list[str],
    origin_url: str = "",
    hierarchy_path: str = "",
) -> list[str]:
    base_topics = [str(v).strip() for v in topic_keywords if str(v).strip()]
    if not base_topics:
        signal_topics = _infer_topic_categories_from_file_signals(
            title=title,
            origin_url=origin_url,
            hierarchy_path=hierarchy_path,
        )
        if signal_topics:
            base_topics = signal_topics
    if not base_topics:
        text = f"{title} {summary}".strip().lower()
        tokens = re.findall(r"[0-9a-zA-Z一-龯ぁ-んァ-ヶー]{2,}", text)
        stopwords = {
            "document",
            "資料",
            "仕様",
            "general",
            "unknown",
            "trigger",
            "file",
            "txt",
            "docx",
            "pdf",
        }
        filtered_tokens: list[str] = []
        for token in tokens:
            if token in stopwords:
                continue
            if re.fullmatch(r"[a-z0-9_-]{20,}", token):
                continue
            if token in _GENERIC_PROJECT_TOKENS:
                continue
            filtered_tokens.append(token)
        base_topics = filtered_tokens[:3]
    if not base_topics:
        base_topics = [str(v).strip() for v in existing_topics if str(v).strip()]
    if not base_topics:
        return ["general"]
    deduped: list[str] = []
    seen: set[str] = set()
    for topic in base_topics:
        normalized = _as_text(topic).lower()
        if not normalized or normalized in seen or normalized in {"general", "unknown"}:
            continue
        seen.add(normalized)
        deduped.append(_as_text(topic))
        if len(deduped) >= 5:
            break
    return deduped or ["general"]


def _infer_category_bundle_from_item(
    item: dict[str, Any],
    *,
    fallback_topics: list[str],
) -> tuple[list[str], dict[str, Any]]:
    topics = [str(v).strip() for v in fallback_topics if str(v).strip()]
    category_hierarchy: dict[str, Any] = {
        "large": "general",
        "medium": "general",
        "small": "general",
        "confidence": 0.2,
        "reason_codes": ["fallback_general"],
        "matched_keywords": [],
        "needs_review": True,
        "score_features": {
            "top_score": 0.0,
            "second_score": 0.0,
            "margin": 0.0,
            "evidence_count": 0,
            "reason_count": 1,
        },
        "score_breakdown": {"fallback_general": 0.0},
        "runner_up": None,
    }

    if (
        _shared_infer_category_hierarchy is not None
        and _shared_infer_topics is not None
        and _shared_map_topic_categories is not None
    ):
        try:
            inferred_hierarchy = _shared_infer_category_hierarchy(item)
            inferred_topics = _shared_infer_topics(item, category_hierarchy=inferred_hierarchy)
            inferred_categories = _shared_map_topic_categories(
                inferred_topics,
                category_hierarchy=inferred_hierarchy,
            )
            if inferred_topics:
                topics = [str(v).strip() for v in inferred_topics if str(v).strip()]
            if isinstance(inferred_hierarchy, dict):
                category_hierarchy = dict(inferred_hierarchy)
            if inferred_categories:
                top_small = str(category_hierarchy.get("small") or "").strip().lower()
                if top_small in {"", "general"} and str(inferred_categories[0]).strip():
                    category_hierarchy["small"] = str(inferred_categories[0]).strip()
                return inferred_categories, category_hierarchy
        except Exception:
            pass

    if not topics:
        topics = ["general"]
    fallback_small = topics[0].strip().lower() if topics else "general"
    category_hierarchy = {
        **category_hierarchy,
        "small": fallback_small if fallback_small else "general",
    }
    return [fallback_small or "general"], category_hierarchy


def _infer_profile_via_llm(
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
    use_llm_override: bool | None = None,
) -> dict[str, Any] | None:
    if _shared_infer_profile_via_llm is None:
        return None
    if use_llm_override is None:
        use_llm_raw = str(os.getenv("ONTOLOGY_AI_FILL_USE_LLM", "true")).strip().lower()
        use_llm = use_llm_raw in {"1", "true", "yes", "on"}
    else:
        use_llm = bool(use_llm_override)
    return _shared_infer_profile_via_llm(
        item_id=item_id,
        title=title,
        summary=summary,
        source_owner=source_owner,
        source_project=source_project,
        topic_keywords=[str(v) for v in topic_keywords],
        existing_profile=_to_plain(existing_profile),
        hierarchy_path=hierarchy_path,
        origin_url=origin_url,
        use_llm=use_llm,
    )


def _ontology_profile_lambda_name() -> str:
    return (
        os.getenv("ONTOLOGY_PROFILE_UPDATE_LAMBDA_NAME")
        or "AIReadyOntology-profileUpdate"
    ).strip()


def _invoke_ontology_profile_lambda(payload: dict[str, Any]) -> dict[str, Any]:
    response = _lambda().invoke(
        FunctionName=_ontology_profile_lambda_name(),
        InvocationType="RequestResponse",
        Payload=json.dumps(payload, ensure_ascii=True).encode("utf-8"),
    )
    status_code = int(response.get("StatusCode", 500))
    if status_code < 200 or status_code >= 300:
        raise RuntimeError(f"ontology profile lambda invoke failed: status={status_code}")
    payload_stream = response.get("Payload")
    raw_payload = payload_stream.read().decode("utf-8") if payload_stream else "{}"
    parsed = json.loads(raw_payload or "{}")
    handler_status = int(parsed.get("statusCode", 500))
    body = parsed.get("body", {})
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            body = {"message": body}
    if not isinstance(body, dict):
        body = {"message": str(body)}
    if handler_status >= 400:
        raise RuntimeError(str(body.get("error") or body.get("message") or "profile update failed"))
    return body


def ai_fill_unified_metadata_profile(
    tenant_id: str,
    item_id: str,
    *,
    use_llm: bool | None = None,
) -> dict[str, Any]:
    """Invoke ontology Lambda to AI-fill unified metadata profile."""
    binding = _binding(tenant_id)
    return _invoke_ontology_profile_lambda(
        {
            "action": "ai_fill",
            "tenant_id": binding.tenant_id,
            "item_id": item_id,
            "use_llm": bool(use_llm) if use_llm is not None else None,
        }
    )


def update_unified_metadata_profile(
    tenant_id: str,
    item_id: str,
    *,
    owner: str | None = None,
    project: str | None = None,
    topic_categories: list[str] | None = None,
    canonical_doc_id: str | None = None,
) -> dict[str, Any]:
    """Invoke ontology Lambda for manual profile update."""
    binding = _binding(tenant_id)
    return _invoke_ontology_profile_lambda(
        {
            "action": "manual_update",
            "tenant_id": binding.tenant_id,
            "item_id": item_id,
            "owner": owner,
            "project": project,
            "topic_categories": topic_categories,
            "canonical_doc_id": canonical_doc_id,
        }
    )


def compute_quality_for_item(
    tenant_id: str,
    item_id: str,
) -> dict[str, Any]:
    """Compute quality scores for a single item (plan-independent)."""
    binding = _binding(tenant_id)
    normalized_tenant_id = binding.tenant_id
    table = _table(binding.ontology_unified_metadata_table_name)
    response = table.get_item(Key={"tenant_id": normalized_tenant_id, "item_id": item_id})
    item = response.get("Item")
    if not item:
        raise ValueError(f"Item not found: {item_id}")
    profile = _extract_document_profile(item)
    return {
        "item_id": item_id,
        **_compute_quality_scores_from_item(
            item,
            profile["owner"],
            profile["project"],
            profile["topic_categories"],
        ),
    }


def list_quality_for_all_items(
    tenant_id: str,
    *,
    limit: int = 500,
    offset: int = 0,
) -> dict[str, Any]:
    """List quality scores for all unified metadata rows (plan-independent)."""
    binding = _binding(tenant_id)
    normalized_tenant_id = binding.tenant_id
    rows = _query_tenant_rows(binding.ontology_unified_metadata_table_name, normalized_tenant_id)
    rows = [item for item in rows if not bool(item.get("is_deleted", False))]

    quality_rows: list[dict[str, Any]] = []
    for item in rows:
        profile = _extract_document_profile(item)
        freshness_status = _as_text(item.get("freshness_status")) or "active"
        scores = _compute_quality_scores_from_item(
            item,
            profile["owner"],
            profile["project"],
            profile["topic_categories"],
        )
        quality_rows.append({
            "item_id": _as_text(item.get("item_id")),
            "title": _as_text(item.get("title")),
            "freshness_status": freshness_status,
            "owner": profile["owner"],
            "project": profile["project"],
            "topic_categories": profile["topic_categories"],
            "canonical_doc_id": profile["canonical_doc_id"],
            **scores,
        })

    bounded_limit = max(1, min(int(limit), 500))
    bounded_offset = max(0, int(offset))
    return {
        "rows": quality_rows[bounded_offset: bounded_offset + bounded_limit],
        "pagination": {
            "limit": bounded_limit,
            "offset": bounded_offset,
            "total_count": len(quality_rows),
        },
    }


"""Ontology remediation helper functions removed."""
