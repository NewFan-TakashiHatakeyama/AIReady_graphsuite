"""Governance の DocumentAnalysis 参照クライアント。"""

from __future__ import annotations

import os
from typing import Any

import boto3

_dynamodb_resource = None


def _get_dynamodb_resource() -> Any:
    """DynamoDB Resource を遅延初期化で返す。

    Args:
        なし。

    Returns:
        Any: 処理結果。

    Notes:
        同一コンテナ内の client 再生成コストを削減する。
    """
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    return _dynamodb_resource


def _get_table() -> Any:
    """DocumentAnalysis テーブルを環境変数から解決する。

    Args:
        なし。

    Returns:
        Any: 処理結果。

    Notes:
        環境変数未設定時は例外を送出して設定不備を明確化する。
    """
    # Prefer ontology-local key first, then shared governance/api key for compatibility.
    table_name = (
        os.environ.get("DOCUMENT_ANALYSIS_TABLE")
        or os.environ.get("GOVERNANCE_DOCUMENT_ANALYSIS_TABLE_NAME")
        or os.environ.get("DOCUMENT_ANALYSIS_TABLE_NAME")
        or ""
    )
    if not table_name:
        raise ValueError(
            "DocumentAnalysis table env is required "
            "(DOCUMENT_ANALYSIS_TABLE or GOVERNANCE_DOCUMENT_ANALYSIS_TABLE_NAME)"
        )
    return _get_dynamodb_resource().Table(table_name)


def get_document_analysis(tenant_id: str, item_id: str) -> dict[str, Any] | None:
    """tenant/item キーで DocumentAnalysis レコードを取得する。

    Args:
        tenant_id: 対象テナントID。
        item_id: 対象アイテムID。

    Returns:
        dict[str, Any] | None: 処理結果の辞書。

    Notes:
        レコード未存在や型不一致時は None を返す。
    """
    response = _get_table().get_item(Key={"tenant_id": tenant_id, "item_id": item_id})
    item = response.get("Item")
    if not isinstance(item, dict):
        return None
    return item


def is_analysis_completed(tenant_id: str, item_id: str) -> bool:
    """対象ドキュメントの分析完了可否を返す。

    Args:
        tenant_id: 対象テナントID。
        item_id: 対象アイテムID。

    Returns:
        bool: 判定結果。

    Notes:
        ``analysis_status`` / ``status`` が ``completed`` のとき True。
        旧行で status が空だが ``summary`` / ``document_summary`` がある場合も True
        （``extract_completed_document_analysis_fields`` と同じ判定）。
    """
    record = get_document_analysis(tenant_id=tenant_id, item_id=item_id)
    if not record:
        return False
    status = str(record.get("analysis_status") or record.get("status") or "").strip().lower()
    summary_text = str(record.get("document_summary") or record.get("summary") or "").strip()
    if status == "failed":
        return False
    if status in ("processing", "pending", "running"):
        return False
    if status == "completed":
        return True
    return bool(summary_text)
