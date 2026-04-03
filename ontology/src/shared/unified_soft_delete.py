"""UnifiedMetadata の論理削除共通処理。"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable


def build_soft_deleted_unified_item(
    *,
    tenant_id: str,
    item_id: str,
    existing: dict[str, Any] | None,
    now: datetime,
    ttl_days: int,
) -> dict[str, Any]:
    """論理削除属性を付与した UnifiedMetadata アイテムを生成する。"""
    ttl_epoch = int(now.timestamp()) + (ttl_days * 86400)
    item = dict(existing or {})
    item.update(
        {
            "tenant_id": tenant_id,
            "item_id": item_id,
            "is_deleted": True,
            "deleted_at": now.isoformat(),
            "ttl": ttl_epoch,
        }
    )
    return item


def execute_soft_delete_unified_item(
    *,
    table: Any,
    tenant_id: str,
    item_id: str,
    existing: dict[str, Any] | None,
    now: datetime,
    ttl_days: int,
    record_lineage: bool = False,
    lineage_callback: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """論理削除を実行し、必要な場合のみ lineage を記録する。"""
    item = build_soft_deleted_unified_item(
        tenant_id=tenant_id,
        item_id=item_id,
        existing=existing,
        now=now,
        ttl_days=ttl_days,
    )
    table.put_item(Item=item)
    if record_lineage and lineage_callback is not None:
        lineage_callback()
    return item
