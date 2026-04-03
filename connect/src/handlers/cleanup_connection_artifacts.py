"""cleanupConnectionArtifacts Lambda handler.

Hard-delete modeで必要な Connect 関連アーティファクトを削除する。
"""

from __future__ import annotations

from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr, Key

from src.shared.config import get_config

_dynamodb_resource = None


def _resource():
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    return _dynamodb_resource


def _delete_file_metadata_rows_for_drive(drive_id: str) -> int:
    if not drive_id:
        return 0
    cfg = get_config()
    table = _resource().Table(cfg.file_metadata_table)
    deleted = 0
    last_key = None
    while True:
        kwargs: dict[str, Any] = {
            "KeyConditionExpression": Key("drive_id").eq(drive_id),
            "ProjectionExpression": "drive_id, item_id",
            "Limit": 200,
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        response = table.query(**kwargs)
        items = response.get("Items", [])
        if items:
            with table.batch_writer() as batch:
                for item in items:
                    batch.delete_item(Key={"drive_id": item["drive_id"], "item_id": item["item_id"]})
                    deleted += 1
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
    return deleted


def _delete_delta_token_for_drive(drive_id: str) -> int:
    if not drive_id:
        return 0
    cfg = get_config()
    table = _resource().Table(cfg.delta_tokens_table)
    existing = table.get_item(Key={"drive_id": drive_id}).get("Item")
    if not existing:
        return 0
    table.delete_item(Key={"drive_id": drive_id})
    return 1


def _delete_idempotency_keys_for_tenant(tenant_id: str) -> int:
    if not tenant_id:
        return 0
    cfg = get_config()
    table = _resource().Table(cfg.idempotency_table)
    deleted = 0
    last_key = None
    aliases = [tenant_id]
    lowered = tenant_id.lower()
    if lowered != tenant_id:
        aliases.append(lowered)
    while True:
        kwargs: dict[str, Any] = {
            "ProjectionExpression": "event_id, tenant_id",
            "Limit": 200,
            "FilterExpression": Attr("tenant_id").is_in(aliases) if len(aliases) > 1 else Attr("tenant_id").eq(tenant_id),
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        response = table.scan(**kwargs)
        items = response.get("Items", [])
        if items:
            with table.batch_writer() as batch:
                for item in items:
                    event_id = str(item.get("event_id") or "").strip()
                    if not event_id:
                        continue
                    batch.delete_item(Key={"event_id": event_id})
                    deleted += 1
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
    return deleted


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    del context
    tenant_id = str(event.get("tenant_id") or "").strip()
    drive_id = str(event.get("drive_id") or "").strip()
    if not tenant_id:
        return {"statusCode": 400, "body": {"error": "tenant_id is required"}}
    return {
        "statusCode": 200,
        "body": {
            "tenant_id": tenant_id,
            "drive_id": drive_id,
            "file_metadata_deleted": _delete_file_metadata_rows_for_drive(drive_id),
            "delta_tokens_deleted": _delete_delta_token_for_drive(drive_id),
            "idempotency_keys_deleted": _delete_idempotency_keys_for_tenant(tenant_id),
        },
    }

