"""cleanupConnectionArtifacts Lambda handler.

Hard-delete modeで必要な Connect 関連アーティファクトを削除する。

event:
  tenant_id (required)
  drive_id (optional) — 指定時、FileMetadata / DeltaTokens を削除
  purge_idempotency (optional) — true のときテナントの IdempotencyKeys を削除（force 削除で drive_id が空でも従来挙動を維持）
  connection_id (optional) — 指定時、その接続スコープの SSM パラメータと Secrets Manager の client_secret を削除
"""

from __future__ import annotations

import logging
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from src.shared.config import get_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
            "FilterExpression": Attr("tenant_id").is_in(aliases)
            if len(aliases) > 1
            else Attr("tenant_id").eq(tenant_id),
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


def _connection_ssm_prefix(tenant_id: str, connection_id: str) -> str:
    t = str(tenant_id or "").strip()
    c = str(connection_id or "").strip()
    return f"/aiready/connect/{t}/{c}/"


def _delete_ssm_parameters_under_prefix(prefix: str) -> int:
    """Delete all standard parameters whose names start with prefix (connection-scoped keys)."""
    if not prefix:
        return 0
    cfg = get_config()
    client = boto3.client("ssm", region_name=cfg.region)
    deleted = 0
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {
            "Path": prefix,
            "Recursive": True,
            "WithDecryption": False,
            "MaxResults": 10,
        }
        if token:
            kwargs["NextToken"] = token
        try:
            resp = client.get_parameters_by_path(**kwargs)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", "") or "")
            logger.warning("get_parameters_by_path failed path=%s code=%s", prefix, code)
            break
        for param in resp.get("Parameters", []):
            name = str(param.get("Name") or "").strip()
            if not name:
                continue
            try:
                client.delete_parameter(Name=name)
                deleted += 1
            except ClientError as exc:
                logger.warning(
                    "delete_parameter failed name=%s code=%s",
                    name,
                    exc.response.get("Error", {}).get("Code", ""),
                )
        token = resp.get("NextToken")
        if not token:
            break
    return deleted


def _delete_connect_client_secret_secret(tenant_id: str, connection_id: str) -> bool:
    """Remove Secrets Manager secret created at onboarding (same name as connect_service._upsert_secret)."""
    t = str(tenant_id or "").strip()
    c = str(connection_id or "").strip()
    if not t or not c:
        return False
    secret_id = f"/aiready/connect/{t}/{c}/client_secret"
    cfg = get_config()
    client = boto3.client("secretsmanager", region_name=cfg.region)
    try:
        client.describe_secret(SecretId=secret_id)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", "") or "")
        if code == "ResourceNotFoundException":
            return False
        logger.warning("describe_secret failed secret_id=%s code=%s", secret_id, code)
        return False
    try:
        client.delete_secret(SecretId=secret_id, ForceDeleteWithoutRecovery=True)
        return True
    except ClientError as exc:
        logger.warning(
            "delete_secret failed secret_id=%s code=%s",
            secret_id,
            exc.response.get("Error", {}).get("Code", ""),
        )
        return False


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    del context
    tenant_id = str(event.get("tenant_id") or "").strip()
    drive_id = str(event.get("drive_id") or "").strip()
    connection_id = str(event.get("connection_id") or "").strip()
    purge_idempotency = bool(event.get("purge_idempotency", False))
    if not tenant_id:
        return {"statusCode": 400, "body": {"error": "tenant_id is required"}}

    file_metadata_deleted = 0
    delta_tokens_deleted = 0
    idempotency_keys_deleted = 0
    if drive_id:
        file_metadata_deleted = _delete_file_metadata_rows_for_drive(drive_id)
        delta_tokens_deleted = _delete_delta_token_for_drive(drive_id)
    if drive_id or purge_idempotency:
        idempotency_keys_deleted = _delete_idempotency_keys_for_tenant(tenant_id)

    ssm_parameters_deleted = 0
    client_secret_secret_deleted = False
    if connection_id:
        prefix = _connection_ssm_prefix(tenant_id, connection_id)
        ssm_parameters_deleted = _delete_ssm_parameters_under_prefix(prefix)
        client_secret_secret_deleted = _delete_connect_client_secret_secret(tenant_id, connection_id)

    return {
        "statusCode": 200,
        "body": {
            "tenant_id": tenant_id,
            "drive_id": drive_id,
            "connection_id": connection_id,
            "file_metadata_deleted": file_metadata_deleted,
            "delta_tokens_deleted": delta_tokens_deleted,
            "idempotency_keys_deleted": idempotency_keys_deleted,
            "ssm_parameters_deleted": ssm_parameters_deleted,
            "client_secret_secret_deleted": client_secret_secret_deleted,
        },
    }
