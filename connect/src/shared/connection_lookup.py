"""Connect Connections テーブルから subscription_id に紐づく connection_id を解決する。"""

from __future__ import annotations

import logging

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
