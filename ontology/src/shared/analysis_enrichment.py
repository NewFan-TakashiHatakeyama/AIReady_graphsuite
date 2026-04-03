"""DocumentAnalysis 補完共通処理。"""

from __future__ import annotations

from typing import Any

from src.shared.json_normalizer import parse_string_list


def extract_completed_document_analysis_fields(
    analysis: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """DocumentAnalysis から Unified へ載せるフィールドを返す。

    明示的に ``analysis_status`` / ``status`` が ``completed`` のとき、または
    Governance ``save_document_analysis`` など **要約が永続化済みで status 未設定の旧行**では
    ``summary`` / ``document_summary`` があれば完了扱いする（下流のプロファイル推論が要約に依存するため）。
    ``failed`` / ``processing`` / ``pending`` / ``running`` は未完了として除外する。
    """
    if not analysis:
        return None
    status = str(analysis.get("analysis_status") or analysis.get("status") or "").strip().lower()
    summary_text = str(analysis.get("document_summary") or analysis.get("summary") or "").strip()

    if status == "failed":
        return None
    if status in ("processing", "pending", "running"):
        return None
    if status != "completed" and not summary_text:
        return None

    topic_keywords = analysis.get("topic_keywords")
    return {
        "document_summary": str(analysis.get("document_summary") or analysis.get("summary") or "")[:500],
        "summary_language": str(analysis.get("summary_language") or ""),
        "topic_keywords": [str(v) for v in topic_keywords] if isinstance(topic_keywords, list) else [],
        "embedding_ref": str(analysis.get("embedding_ref") or analysis.get("embedding_s3_key") or ""),
        "analysis_id": str(analysis.get("analysis_id") or ""),
        "summary_generated_at": str(
            analysis.get("summary_generated_at") or analysis.get("analyzed_at") or ""
        ),
    }


def apply_document_analysis_fields(unified_item: dict[str, Any], fields: dict[str, Any] | None) -> None:
    """抽出済み DocumentAnalysis フィールドを Unified 辞書へ反映する。"""
    if not fields:
        return
    unified_item["document_summary"] = str(fields.get("document_summary") or "")
    unified_item["summary_language"] = str(fields.get("summary_language") or "")
    unified_item["topic_keywords"] = parse_string_list(
        fields.get("topic_keywords"),
        parse_json_string=True,
        fallback_single_string=True,
    )
    unified_item["embedding_ref"] = str(fields.get("embedding_ref") or "")
    unified_item["analysis_id"] = str(fields.get("analysis_id") or "")
    unified_item["summary_generated_at"] = str(fields.get("summary_generated_at") or "")
