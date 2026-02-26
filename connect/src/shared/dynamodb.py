"""T-018: DynamoDB ヘルパー

get/put/冪等チェック/Delta Token 管理を提供する。
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.types import TypeSerializer

from src.shared.config import get_config


def _resource():
    """DynamoDB resource（モジュールレベルで再利用）"""
    cfg = get_config()
    return boto3.resource("dynamodb", region_name=cfg.region)


# ==========================================
# JSON → DynamoDB 型変換ヘルパー
# ==========================================

def sanitize_for_dynamodb(obj: Any) -> Any:
    """DynamoDB に保存できない float → Decimal 変換、空文字列の除去など

    DynamoDB は float をサポートしないため Decimal に変換し、
    空文字列の属性値は None に変換する。
    """
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {
            k: sanitize_for_dynamodb(v)
            for k, v in obj.items()
            if v is not None and v != ""
        }
    if isinstance(obj, list):
        return [sanitize_for_dynamodb(item) for item in obj]
    return obj


# ==========================================
# FileMetadata テーブル操作
# ==========================================

def put_file_metadata(item: dict[str, Any]) -> None:
    """FileMetadata テーブルにアイテムを upsert する

    Args:
        item: 正規化済みのメタデータ dict
    """
    cfg = get_config()
    table = _resource().Table(cfg.file_metadata_table)
    sanitized = sanitize_for_dynamodb(item)
    table.put_item(Item=sanitized)


def get_file_metadata(drive_id: str, item_id: str) -> dict[str, Any] | None:
    """FileMetadata テーブルからアイテムを取得する

    Returns:
        メタデータ dict、存在しない場合は None
    """
    cfg = get_config()
    table = _resource().Table(cfg.file_metadata_table)
    resp = table.get_item(Key={"drive_id": drive_id, "item_id": item_id})
    return resp.get("Item")


# ==========================================
# 冪等チェック (IdempotencyKeys)
# ==========================================

def is_already_processed(event_id: str) -> bool:
    """指定された event_id が処理済みかチェックする

    Args:
        event_id: 通知イベントの一意識別子

    Returns:
        True: 処理済み (スキップすべき)
        False: 未処理 (処理を続行)
    """
    cfg = get_config()
    table = _resource().Table(cfg.idempotency_table)
    resp = table.get_item(Key={"event_id": event_id})
    return "Item" in resp


def mark_as_processed(event_id: str, tenant_id: str = "") -> None:
    """event_id を処理済みとして記録する

    TTL を設定し、7日後に自動削除される。

    Args:
        event_id: 通知イベントの一意識別子
        tenant_id: テナント識別子
    """
    cfg = get_config()
    table = _resource().Table(cfg.idempotency_table)
    ttl = int(time.time()) + cfg.idempotency_ttl_seconds
    table.put_item(
        Item={
            "event_id": event_id,
            "tenant_id": tenant_id or cfg.tenant_id,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "ttl": ttl,
        }
    )


# ==========================================
# DeltaTokens テーブル操作
# ==========================================

def get_delta_token(drive_id: str) -> str | None:
    """指定ドライブの Delta Token (deltaLink) を取得する

    Returns:
        deltaLink URL 文字列、未設定の場合は None
    """
    cfg = get_config()
    table = _resource().Table(cfg.delta_tokens_table)
    resp = table.get_item(Key={"drive_id": drive_id})
    item = resp.get("Item")
    return item.get("delta_link") if item else None


def save_delta_token(drive_id: str, delta_link: str) -> None:
    """指定ドライブの Delta Token を保存/更新する

    Args:
        drive_id: ドライブ ID
        delta_link: Delta Query から返された deltaLink URL
    """
    cfg = get_config()
    table = _resource().Table(cfg.delta_tokens_table)
    table.put_item(
        Item={
            "drive_id": drive_id,
            "delta_link": delta_link,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    )
