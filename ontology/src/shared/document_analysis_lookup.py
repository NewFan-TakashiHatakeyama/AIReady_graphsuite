"""DocumentAnalysis 参照ラッパー共通処理。"""

from __future__ import annotations

from typing import Any, Callable

from src.shared.analysis_enrichment import extract_completed_document_analysis_fields


def lookup_completed_document_analysis_fields(
    *,
    tenant_id: str,
    item_id: str,
    lookup: Callable[[str, str], dict[str, Any] | None],
    on_lookup_error: Callable[[Exception], None] | None = None,
    on_lookup_miss: Callable[[], None] | None = None,
) -> dict[str, Any] | None:
    """completed の DocumentAnalysis 補完フィールドを取得する。

    Notes:
        - 参照エラー時は on_lookup_error を呼んで None を返す。
        - レコード未存在/未完了時は on_lookup_miss を呼んで None を返す。
        - 呼び出し側でメトリクス送信有無を選べるようコールバック化している。
    """
    if not tenant_id or not item_id:
        return None
    try:
        analysis = lookup(tenant_id, item_id)
    except Exception as exc:
        if on_lookup_error is not None:
            on_lookup_error(exc)
        if on_lookup_miss is not None:
            on_lookup_miss()
        return None

    fields = extract_completed_document_analysis_fields(analysis)
    if not fields:
        if on_lookup_miss is not None:
            on_lookup_miss()
        return None
    return fields
