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
from urllib.parse import parse_qs, unquote_plus, urlparse

logger = logging.getLogger(__name__)


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
    query = event.get("queryStringParameters") or {}
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
    query = event.get("queryStringParameters") or {}
    raw_token = query.get("validationToken", "")
    return unquote_plus(raw_token)


def is_health_check(event: dict[str, Any]) -> bool:
    """ALB ヘルスチェック (GET /) か判定する

    Args:
        event: ALB Lambda イベント

    Returns:
        True: ヘルスチェック
    """
    method = event.get("httpMethod", "").upper()
    path = event.get("path", "/")
    query = event.get("queryStringParameters") or {}
    return method == "GET" and path == "/" and "validationToken" not in query


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

    # resource フィールドから drive_id を抽出
    # 例: "drives/b!USrprPl-yE6I8PBco0KG4sni6gP.../root"
    resource = notification.get("resource", "")
    drive_id = ""
    if resource.startswith("drives/"):
        parts = resource.split("/")
        if len(parts) >= 2:
            drive_id = parts[1]

    return {
        "subscription_id": notification.get("subscriptionId", ""),
        "change_type": notification.get("changeType", ""),
        "resource": resource,
        "drive_id": drive_id,
        "item_id": resource_data.get("id", ""),
        "odata_type": resource_data.get("@odata.type", ""),
    }
