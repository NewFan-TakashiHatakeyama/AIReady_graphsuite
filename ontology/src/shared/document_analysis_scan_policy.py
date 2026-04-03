"""DocumentAnalysis 連携モードの共通スキャン条件。"""

from __future__ import annotations

from typing import Any

from src.shared.ontology_target_policy import is_supported_extension


def evaluate_analysis_scan_skip_reason(unified_item: dict[str, Any]) -> str | None:
    """影分析/本番投入の共通除外条件を判定して skip 理由キーを返す。"""
    if unified_item.get("is_canonical_copy") is False:
        return "skipped_non_canonical"
    if bool(unified_item.get("is_orphan", False)):
        return "skipped_orphan"
    if str(unified_item.get("freshness_status") or "") == "stale":
        return "skipped_stale"
    if unified_item.get("ai_eligible") is not True:
        return "skipped_ineligible"
    if str(unified_item.get("risk_level") or "").strip().lower() != "low":
        return "skipped_risk_not_low"
    if not is_supported_extension(str(unified_item.get("title") or "")):
        return "skipped_unsupported_extension"
    return None
