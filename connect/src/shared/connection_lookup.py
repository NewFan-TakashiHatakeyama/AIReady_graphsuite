"""Connect Connections テーブルから subscription_id に紐づく connection_id を解決する。"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from src.shared.config import get_config

logger = logging.getLogger(__name__)

GSI_SUBSCRIPTION_ID = "GSI-SubscriptionId"


def lookup_connection_id_by_subscription(*, tenant_id: str, subscription_id: str) -> str:
    """Graph の subscriptionId に対応する connection_id を返す。

    Args:
        tenant_id: アプリテナント ID（DynamoDB の partition と一致）
        subscription_id: 通知ペイロードの subscriptionId

    Returns:
        見つかった connection_id。未設定・未検索時は空文字。
    """
    normalized_tenant = str(tenant_id or "").strip()
    normalized_sub = str(subscription_id or "").strip()
    cfg = get_config()
    table_name = str(cfg.connections_table or "").strip()
    if not normalized_tenant or not normalized_sub or not table_name:
        return ""

    table = boto3.resource("dynamodb", region_name=cfg.region).Table(table_name)
    try:
        resp = table.query(
            IndexName=GSI_SUBSCRIPTION_ID,
            KeyConditionExpression=Key("subscription_id").eq(normalized_sub),
            Limit=25,
        )
    except ClientError as exc:
        logger.warning(
            "Connections GSI query failed for subscription_id lookup: %s",
            exc.response.get("Error", {}).get("Code", ""),
        )
        return ""

    for item in resp.get("Items", []):
        if str(item.get("tenant_id") or "").strip() == normalized_tenant:
            found = str(item.get("connection_id") or "").strip()
            if found:
                return found
    return ""


def fetch_connection_item(*, tenant_id: str, connection_id: str) -> dict[str, Any] | None:
    """Return a single Connect connection row or None."""
    normalized_tenant = str(tenant_id or "").strip()
    normalized_conn = str(connection_id or "").strip()
    cfg = get_config()
    table_name = str(cfg.connections_table or "").strip()
    if not normalized_tenant or not normalized_conn or not table_name:
        return None

    table = boto3.resource("dynamodb", region_name=cfg.region).Table(table_name)
    try:
        resp = table.get_item(
            Key={"tenant_id": normalized_tenant, "connection_id": normalized_conn}
        )
    except ClientError as exc:
        logger.warning(
            "Connections get_item failed tenant_id=%s connection_id=%s code=%s",
            normalized_tenant,
            normalized_conn,
            exc.response.get("Error", {}).get("Code", ""),
        )
        return None
    item = resp.get("Item")
    if not item:
        return None
    return {str(k): _stringify_ddb_value(v) for k, v in item.items()}


def _stringify_ddb_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return str(int(value))
        return str(value)
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return ""
    return str(value)
