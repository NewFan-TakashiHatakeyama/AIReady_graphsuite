"""Connect connections repository backed by DynamoDB."""

from __future__ import annotations

import unicodedata
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from boto3.dynamodb.conditions import Key

from services.aws_clients import get_dynamodb_resource
from services.runtime_config import load_aws_runtime_config


def _normalize_subscription_id_for_gsi(value: str | None) -> str:
    """Align with SSM normalization: invisible/format chars must not yield a GSI key Dynamo rejects."""
    s = str(value if value is not None else "").strip()
    for ch in ("\ufeff", "\u200b", "\u200c", "\u200d", "\u2060"):
        s = s.replace(ch, "")
    s = "".join(c for c in s if unicodedata.category(c) != "Cf")
    return s.strip()


def _to_plain_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, list):
        return [_to_plain_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_plain_value(v) for k, v in value.items()}
    return value


class ConnectConnectionsRepository:
    def __init__(self, table_name: str):
        runtime_config = load_aws_runtime_config()
        if not runtime_config.aws_region:
            # Keep repository import-safe for test environments.
            runtime_config = replace(
                runtime_config,
                aws_region="ap-northeast-1",
            )
        self._table = get_dynamodb_resource(runtime_config).Table(table_name)

    def upsert_connection(
        self,
        *,
        tenant_id: str,
        connection_id: str,
        connection_name: str,
        site_id: str,
        drive_id: str,
        status: str,
        subscription_id: str = "",
        resource_type: str = "drive",
        resource_path: str = "",
        target_type: str = "drive",
        team_id: str = "",
        channel_id: str = "",
        chat_id: str = "",
    ) -> dict[str, Any]:
        now_iso = datetime.now(timezone.utc).isoformat()
        # GSI-SubscriptionId partition key cannot be an empty string; omit attribute until set.
        subscription_key = _normalize_subscription_id_for_gsi(subscription_id)
        item = {
            "tenant_id": tenant_id,
            "connection_id": connection_id,
            "connection_name": connection_name,
            "site_id": site_id,
            "drive_id": drive_id,
            "status": status,
            "resource_type": str(resource_type or "drive").strip() or "drive",
            "resource_path": str(resource_path or "").strip(),
            "target_type": str(target_type or "").strip(),
            "team_id": str(team_id or "").strip(),
            "channel_id": str(channel_id or "").strip(),
            "chat_id": str(chat_id or "").strip(),
            "updated_at": now_iso,
        }
        if subscription_key:
            item["subscription_id"] = subscription_key
        existing = self._table.get_item(
            Key={"tenant_id": tenant_id, "connection_id": connection_id}
        ).get("Item")
        if not existing:
            item["created_at"] = now_iso
        self._table.put_item(Item=item)
        return item

    def latest_connection_for_tenant(self, tenant_id: str) -> dict[str, Any] | None:
        response = self._table.query(
            IndexName="GSI-UpdatedAt",
            KeyConditionExpression=Key("tenant_id").eq(tenant_id),
            ScanIndexForward=False,
            Limit=1,
        )
        items = response.get("Items", [])
        if not items:
            return None
        return _to_plain_value(items[0])

    def list_connections_for_tenant(
        self,
        tenant_id: str,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        normalized_limit = max(1, min(int(limit), 500))
        response = self._table.query(
            IndexName="GSI-UpdatedAt",
            KeyConditionExpression=Key("tenant_id").eq(tenant_id),
            ScanIndexForward=False,
            Limit=normalized_limit,
        )
        return [_to_plain_value(item) for item in response.get("Items", [])]

    def update_connection_status(
        self,
        *,
        tenant_id: str,
        connection_id: str,
        status: str,
        cleanup_reason: str = "",
    ) -> dict[str, Any]:
        now_iso = datetime.now(timezone.utc).isoformat()
        expression_values: dict[str, Any] = {
            ":status": status,
            ":updated_at": now_iso,
        }
        update_expression = "SET #status = :status, updated_at = :updated_at"
        expression_names = {"#status": "status"}
        if cleanup_reason:
            expression_values[":cleanup_reason"] = cleanup_reason
            update_expression += ", cleanup_reason = :cleanup_reason"
        response = self._table.update_item(
            Key={"tenant_id": tenant_id, "connection_id": connection_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_names,
            ExpressionAttributeValues=expression_values,
            ReturnValues="ALL_NEW",
        )
        return _to_plain_value(response.get("Attributes", {}))

    def delete_connection(
        self,
        *,
        tenant_id: str,
        connection_id: str,
    ) -> bool:
        response = self._table.delete_item(
            Key={"tenant_id": tenant_id, "connection_id": connection_id},
            ReturnValues="ALL_OLD",
        )
        return bool(response.get("Attributes"))
