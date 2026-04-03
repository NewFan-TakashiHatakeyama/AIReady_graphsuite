"""T-024: サブスクリプション初期化ハンドラー。

tenant/connection スコープの設定値を用いて Graph Webhook サブスクリプションを
作成し、発行された subscription_id を SSM に保存する。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from src.connectors.m365.graph_client import GraphClient
from src.shared.config import get_config
from src.shared.logger import get_logger, log_with_context
from src.shared.ssm import put_param, resolve_connect_param

SSM_SUBSCRIPTION_ID = "MSGraphSubscriptionId"


def _utc_graph_timestamp(days: int) -> str:
    """Graph API の `expirationDateTime` 形式で UTC 時刻を生成する。

    Args:
        days: 現在時刻に加算する日数

    Returns:
        Graph API 互換フォーマットの UTC 時刻文字列
    """
    expiry = datetime.now(timezone.utc) + timedelta(days=days)
    return expiry.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Graph Webhook サブスクリプションを初期化する。

    処理概要:
    1. tenant/connection ごとの設定値を解決
    2. Graph API へ `/subscriptions` を POST
    3. 取得した subscription_id を SSM に保存（スコープ + 互換キー）

    Args:
        event: Lambda 実行イベント
        context: Lambda コンテキスト

    Returns:
        初期化結果を含むレスポンス辞書
    """
    cfg = get_config()
    tenant_id = str(event.get("tenant_id") or cfg.tenant_id).strip() or cfg.tenant_id
    connection_id = str(event.get("connection_id") or "").strip()
    drive_id = str(event.get("drive_id") or "").strip()
    resource_type = str(event.get("resource_type") or "drive").strip().lower()
    requested_resource_path = str(event.get("resource_path") or "").strip()
    change_type = str(event.get("change_type") or "updated").strip() or "updated"
    team_id = str(event.get("team_id") or "").strip()
    channel_id = str(event.get("channel_id") or "").strip()
    chat_id = str(event.get("chat_id") or "").strip()
    notification_url = str(event.get("notification_url") or "").strip()
    client_state = str(event.get("client_state") or "").strip()
    request_id = getattr(context, "aws_request_id", "local")
    logger = get_logger(__name__, tenant_id=tenant_id, request_id=request_id)

    if not drive_id:
        drive_id = resolve_connect_param(
            "drive_id",
            tenant_id=tenant_id,
            connection_id=connection_id,
            decrypt=False,
            fallback_name=cfg.ssm_drive_id,
        )
    if not notification_url:
        notification_url = resolve_connect_param(
            "notification_url",
            tenant_id=tenant_id,
            connection_id=connection_id,
            decrypt=False,
            fallback_name="",
        ) or cfg.webhook_url
    if not client_state:
        client_state = resolve_connect_param(
            "client_state",
            tenant_id=tenant_id,
            connection_id=connection_id,
            decrypt=True,
            fallback_name=cfg.ssm_client_state,
        )

    if resource_type not in {"drive", "message"}:
        return {
            "statusCode": 400,
            "body": {
                "bootstrap_status": "failed",
                "bootstrap_error": "resource_type must be 'drive' or 'message'.",
            },
        }

    if resource_type == "drive" and not drive_id:
        return {
            "statusCode": 400,
            "body": {
                "bootstrap_status": "failed",
                "bootstrap_error": "drive_id is required for drive subscription.",
            },
        }

    if resource_type == "drive":
        resource_path = requested_resource_path or f"drives/{drive_id}/root"
    else:
        if requested_resource_path:
            resource_path = requested_resource_path
        elif team_id and channel_id:
            resource_path = f"teams/{team_id}/channels/{channel_id}/messages"
        elif chat_id:
            resource_path = f"chats/{chat_id}/messages"
        else:
            return {
                "statusCode": 400,
                "body": {
                    "bootstrap_status": "failed",
                    "bootstrap_error": (
                        "resource_path is required for message subscription "
                        "or provide team_id+channel_id / chat_id."
                    ),
                },
            }

    client = GraphClient.from_ssm(tenant_id=tenant_id, connection_id=connection_id)
    if not client._access_token or client._access_token == "PLACEHOLDER_WILL_BE_UPDATED":
        # 初回デプロイ直後はプレースホルダが入っていることがあるため更新する。
        client.refresh_and_store_token(tenant_id=tenant_id, connection_id=connection_id)

    try:
        response = client.graph_post(
            "/subscriptions",
            json_body={
                "changeType": change_type,
                "notificationUrl": notification_url,
                "resource": resource_path,
                "expirationDateTime": _utc_graph_timestamp(1 if resource_type == "message" else 2),
                "clientState": client_state,
            },
        )
        subscription_id = str(response.get("id") or "").strip()
        if not subscription_id:
            raise ValueError("Graph subscription response did not include id.")

        # 新スキーマ（tenant/connection）と従来キーの双方に保存して互換性を維持する。
        if connection_id:
            put_param(
                f"/aiready/connect/{tenant_id}/{connection_id}/subscription_id",
                subscription_id,
                param_type="String",
            )
            put_param(
                f"/aiready/connect/{tenant_id}/{connection_id}/resource_type",
                resource_type,
                param_type="String",
            )
            put_param(
                f"/aiready/connect/{tenant_id}/{connection_id}/resource_path",
                resource_path,
                param_type="String",
            )
            if team_id:
                put_param(
                    f"/aiready/connect/{tenant_id}/{connection_id}/team_id",
                    team_id,
                    param_type="String",
                )
            if channel_id:
                put_param(
                    f"/aiready/connect/{tenant_id}/{connection_id}/channel_id",
                    channel_id,
                    param_type="String",
                )
            if chat_id:
                put_param(
                    f"/aiready/connect/{tenant_id}/{connection_id}/chat_id",
                    chat_id,
                    param_type="String",
                )
        put_param(
            f"/aiready/connect/{tenant_id}/subscription_id",
            subscription_id,
            param_type="String",
        )
        put_param(SSM_SUBSCRIPTION_ID, subscription_id, param_type="String")

        log_with_context(
            logger,
            logging.INFO,
            "Connect init subscription succeeded.",
            extra_data={
                "tenant_id": tenant_id,
                "connection_id": connection_id,
                "subscription_id": subscription_id,
                "drive_id": drive_id,
                "resource_type": resource_type,
                "resource_path": resource_path,
                "change_type": change_type,
            },
        )
        return {
            "statusCode": 200,
            "body": {
                "bootstrap_status": "succeeded",
                "subscription_id": subscription_id,
            },
        }
    except Exception as exc:
        message = str(exc)
        log_with_context(
            logger,
            logging.ERROR,
            f"Connect init subscription failed: {message}",
            exc_info=True,
        )
        return {
            "statusCode": 500,
            "body": {
                "bootstrap_status": "failed",
                "bootstrap_error": message,
            },
        }

