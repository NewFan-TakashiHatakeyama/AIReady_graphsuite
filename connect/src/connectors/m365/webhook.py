"""T-020: Webhook パース・検証

Microsoft Graph からの Webhook 通知をパース・検証する。
- validationToken への応答（サブスクリプション作成/更新時）
- clientState の検証（不正な通知の排除）
- 通知ペイロードの抽出
"""

from __future__ import annotations

import json
import logging
from typing import Any
from urllib.parse import unquote_plus

logger = logging.getLogger(__name__)


def _merged_query_params(event: dict[str, Any]) -> dict[str, str]:
    """ALB の queryStringParameters と multiValueQueryStringParameters を統合する。

    同一キーが複数ある場合は先頭値を採用する。Graph 検証で multiValue のみ渡るケースに対応する。
    """
    merged: dict[str, str] = {}
    single = event.get("queryStringParameters")
    if isinstance(single, dict):
        for key, value in single.items():
            if value is not None and str(value).strip():
                merged[key] = str(value)
    multi = event.get("multiValueQueryStringParameters")
    if isinstance(multi, dict):
        for key, values in multi.items():
            if key in merged:
                continue
            if not values:
                continue
            first = values[0]
            if first is not None and str(first).strip():
                merged[key] = str(first)
    return merged


def is_validation_request(event: dict[str, Any]) -> bool:
    """ALB イベントが Graph API のバリデーションリクエストか判定する

    サブスクリプション作成/更新時に Graph が POST ?validationToken=xxx を送信する。
    (Microsoft 公式ドキュメント: POST メソッドで validationToken クエリパラメータ付き)
    GET の場合も互換性のため受け付ける。

    Args:
        event: ALB Lambda イベント

    Returns:
        True: バリデーションリクエスト
    """
    query = _merged_query_params(event)
    return "validationToken" in query


def get_validation_token(event: dict[str, Any]) -> str:
    """バリデーションリクエストからトークンを取得する

    ALB は queryStringParameters を URL デコードしないため、
    明示的に unquote_plus でデコードして返す。

    Args:
        event: ALB Lambda イベント

    Returns:
        URL デコード済み validationToken の値
    """
    query = _merged_query_params(event)
    raw_token = query.get("validationToken", "")
    return unquote_plus(raw_token)


def normalized_request_path(event: dict[str, Any]) -> str:
    """ALB から渡る path が空文字のことがある。Graph 検証前の GET を 405 にしないため / とみなす。"""
    raw = str(event.get("path") or "").strip()
    return "/" if not raw else raw


def is_health_check(event: dict[str, Any]) -> bool:
    """ALB ヘルスチェック (GET /) か判定する

    Args:
        event: ALB Lambda イベント

    Returns:
        True: ヘルスチェック
    """
    method = event.get("httpMethod", "").upper()
    path = normalized_request_path(event)
    merged = _merged_query_params(event)
    return method == "GET" and path == "/" and "validationToken" not in merged


def verify_client_state(
    notification: dict[str, Any],
    expected_client_state: str,
) -> bool:
    """通知の clientState が期待値と一致するか検証する

    Args:
        notification: 通知オブジェクト（Graph の value[] 要素）
        expected_client_state: Parameter Store に保存した clientState

    Returns:
        True: 一致（正当な通知）
        False: 不一致（不正な通知の可能性）
    """
    actual = notification.get("clientState", "")
    if actual != expected_client_state:
        logger.warning(
            f"clientState mismatch: expected={expected_client_state[:8]}..., "
            f"actual={actual[:8]}..."
        )
        return False
    return True


def parse_notification_body(event: dict[str, Any]) -> list[dict[str, Any]]:
    """ALB イベントの body から通知オブジェクトのリストをパースする

    Graph API は以下の形式で通知を送信する:
    {
        "value": [
            {
                "subscriptionId": "...",
                "changeType": "updated",
                "resource": "drives/xxx/root",
                "resourceData": { ... },
                "clientState": "..."
            },
            ...
        ]
    }

    Args:
        event: ALB Lambda イベント

    Returns:
        通知オブジェクトのリスト
    """
    body = event.get("body", "")
    if not body:
        return []

    # ALB が Base64 エンコードしている場合
    if event.get("isBase64Encoded", False):
        import base64
        body = base64.b64decode(body).decode("utf-8")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        logger.error("Failed to parse notification body as JSON")
        return []

    notifications = payload.get("value", [])
    if not isinstance(notifications, list):
        logger.error(f"Unexpected 'value' type: {type(notifications)}")
        return []

    return notifications


def extract_resource_info(notification: dict[str, Any]) -> dict[str, str]:
    """通知オブジェクトからリソース情報を抽出する

    Args:
        notification: 通知オブジェクト

    Returns:
        {
            "subscription_id": "...",
            "change_type": "updated|created|deleted",
            "resource": "drives/xxx/root",
            "drive_id": "...",
            "item_id": "..." (resourceData から),
        }
    """
    resource_data = notification.get("resourceData", {})

    resource = str(notification.get("resource", "") or "").strip()
    drive_id = ""
    resource_type = "drive"
    team_id = ""
    channel_id = ""
    chat_id = ""
    message_id = ""
    if resource.startswith("drives/"):
        parts = resource.split("/")
        if len(parts) >= 2:
            drive_id = parts[1]
    elif resource.startswith("teams/"):
        resource_type = "message"
        parts = resource.split("/")
        if len(parts) >= 6 and parts[0] == "teams" and parts[2] == "channels":
            team_id = parts[1]
            channel_id = parts[3]
            if parts[4] == "messages":
                message_id = parts[5]
    elif resource.startswith("chats/"):
        resource_type = "message"
        parts = resource.split("/")
        if len(parts) >= 4 and parts[0] == "chats":
            chat_id = parts[1]
            if parts[2] == "messages":
                message_id = parts[3]
    else:
        resource_type = "unknown"

    item_id = str(resource_data.get("id") or "").strip()
    if resource_type == "message" and not item_id and message_id:
        item_id = message_id

    return {
        "subscription_id": notification.get("subscriptionId", ""),
        "change_type": notification.get("changeType", ""),
        "resource": resource,
        "resource_type": resource_type,
        "drive_id": drive_id,
        "item_id": item_id,
        "team_id": team_id,
        "channel_id": channel_id,
        "chat_id": chat_id,
        "message_id": message_id or item_id,
        "odata_type": resource_data.get("@odata.type", ""),
    }
