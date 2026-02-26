"""T-027/028/029: pull_file_metadata Lambda

SQS から通知メッセージを受け取り、以下を実行する:
1. 冪等チェック（処理済みならスキップ）
2. Delta Query で変更アイテム一覧を取得
3. 各アイテムの詳細 + 権限情報を取得（全フィールド）
4. 正規化して DynamoDB に保存
5. S3 に Raw Payload を保存
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3

from src.shared.config import get_config
from src.shared.logger import get_logger, log_with_context
from src.shared.dynamodb import (
    is_already_processed,
    mark_as_processed,
    put_file_metadata,
)
from src.connectors.m365.graph_client import GraphClient
from src.connectors.m365.delta import fetch_delta, fetch_item_detail, fetch_permissions
from src.connectors.m365.normalizer import (
    normalize_item,
    normalize_deleted_item,
)

# Lambda コールドスタート時に初期化
s3_client = boto3.client("s3", region_name=get_config().region)


def _save_raw_payload(
    bucket: str,
    tenant_id: str,
    item_id: str,
    item: dict[str, Any],
    permissions: list[dict[str, Any]],
) -> str:
    """S3 に Raw Payload を保存する

    キー形式: raw/{tenant_id}/{date}/{item_id}_{uuid}.json

    Returns:
        S3 オブジェクトキー
    """
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    uid = str(uuid.uuid4())[:8]
    key = f"raw/{tenant_id}/{date_str}/{item_id}_{uid}.json"

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(
            {"item": item, "permissions": permissions},
            ensure_ascii=False,
            default=str,
        ),
        ContentType="application/json",
    )
    return key


def _process_single_record(
    record: dict[str, Any],
    client: GraphClient,
    cfg: Any,
    logger: logging.Logger,
) -> dict[str, int]:
    """SQS レコード 1件を処理する

    Returns:
        {"processed": N, "skipped": N, "errors": N}
    """
    stats = {"processed": 0, "skipped": 0, "errors": 0}

    # SQS メッセージボディをパース
    body = json.loads(record["body"])

    drive_id = body.get("drive_id", "")
    tenant_id = body.get("tenant_id", cfg.tenant_id)
    change_type = body.get("change_type", "unknown")
    subscription_id = body.get("subscription_id", "")

    # 冪等キー: SQS messageId を使用（各通知で一意）
    # SQS の at-least-once 配信による重複のみスキップし、
    # 同一ドライブへの異なるイベントは個別に処理する
    message_id = record.get("messageId", "")
    event_id = message_id if message_id else f"{subscription_id}:{drive_id}:{change_type}"

    log_with_context(
        logger, logging.INFO,
        f"Processing notification: drive={drive_id}, type={change_type}",
        event_id=event_id,
        extra_data=body,
    )

    # 冪等チェック
    if is_already_processed(event_id):
        log_with_context(
            logger, logging.INFO,
            f"Already processed: {event_id} — skipping",
            event_id=event_id,
        )
        stats["skipped"] += 1
        return stats

    # Delta Query 実行（全変更アイテムを取得）
    log_with_context(
        logger, logging.INFO,
        f"Starting Delta Query for drive={drive_id}",
        event_id=event_id,
    )

    items = fetch_delta(client, drive_id)

    log_with_context(
        logger, logging.INFO,
        f"Delta Query returned {len(items)} items",
        event_id=event_id,
        extra_data={"item_count": len(items)},
    )

    # 各アイテムの詳細取得 + 正規化 + 保存
    for item in items:
        item_id = item.get("id", "")
        if not item_id:
            continue

        try:
            is_deleted = item.get("deleted") is not None

            if is_deleted:
                # 削除アイテムは最小限の正規化
                metadata = normalize_deleted_item(item, drive_id, tenant_id)
                permissions = []
            else:
                # 詳細取得（$expand=permissions で全フィールド + 権限を同時取得）
                try:
                    detail = fetch_item_detail(client, drive_id, item_id)
                    permissions = detail.get("permissions", [])
                except Exception as detail_err:
                    # 詳細取得失敗 → Delta の基本情報 + 別途 permissions 取得
                    log_with_context(
                        logger, logging.WARNING,
                        f"Failed to fetch item detail for {item_id}: {detail_err}. "
                        "Falling back to delta item + separate permissions.",
                        event_id=event_id,
                    )
                    detail = item
                    try:
                        permissions = fetch_permissions(client, drive_id, item_id)
                    except Exception:
                        permissions = []

                metadata = normalize_item(detail, permissions, drive_id, tenant_id)

            # DynamoDB に保存
            put_file_metadata(metadata)

            # S3 に Raw Payload を保存
            if cfg.raw_bucket:
                raw_item = detail if not is_deleted else item
                _save_raw_payload(
                    cfg.raw_bucket, tenant_id, item_id, raw_item, permissions
                )

            stats["processed"] += 1
            log_with_context(
                logger, logging.DEBUG,
                f"Saved metadata for item={item_id}, name={metadata.get('name', '')}",
                event_id=event_id,
            )

        except Exception as item_err:
            stats["errors"] += 1
            log_with_context(
                logger, logging.ERROR,
                f"Failed to process item {item_id}: {item_err}",
                event_id=event_id,
                exc_info=True,
            )

    # 処理済みマーク
    mark_as_processed(event_id, tenant_id)

    return stats


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """SQS Event Source Mapping から呼び出されるハンドラー

    BatchSize=10 で複数レコードが渡される可能性がある。
    """
    cfg = get_config()
    request_id = getattr(context, "aws_request_id", "local")
    logger = get_logger(__name__, tenant_id=cfg.tenant_id, request_id=request_id)

    records = event.get("Records", [])
    log_with_context(
        logger, logging.INFO,
        f"Received {len(records)} SQS records",
        extra_data={"record_count": len(records)},
    )

    # GraphClient を初期化（全レコードで共有）
    client = GraphClient.from_ssm()

    # まずトークンが有効か確認（PLACEHOLDER の場合は更新）
    if client._access_token == "PLACEHOLDER_WILL_BE_UPDATED":
        log_with_context(logger, logging.INFO, "Access token is placeholder — refreshing")
        client.refresh_and_store_token()

    total_stats = {"processed": 0, "skipped": 0, "errors": 0}
    failed_record_ids: list[str] = []

    for record in records:
        try:
            stats = _process_single_record(record, client, cfg, logger)
            for key in total_stats:
                total_stats[key] += stats[key]
        except Exception as e:
            log_with_context(
                logger, logging.ERROR,
                f"Failed to process SQS record: {e}",
                exc_info=True,
            )
            # Partial batch failure: 失敗したレコードの messageId を返す
            failed_record_ids.append(record.get("messageId", ""))

    log_with_context(
        logger, logging.INFO,
        f"Processing complete: {total_stats}",
        extra_data=total_stats,
    )

    # Partial batch failure response
    # 失敗したメッセージのみ SQS に戻す
    result: dict[str, Any] = {"batchItemFailures": []}
    for msg_id in failed_record_ids:
        result["batchItemFailures"].append({"itemIdentifier": msg_id})

    return result
