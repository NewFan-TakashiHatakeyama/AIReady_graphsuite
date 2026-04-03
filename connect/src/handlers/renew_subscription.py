"""T-031: renew_subscription Lambda

EventBridge rate(1 day) で定期実行し、
Graph API のサブスクリプション有効期限を延長する。

サブスクリプションの最大有効期限は OneDrive/SharePoint で約 30 日間。
毎日更新することで失効を防ぐ。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from src.shared.config import get_config
from src.shared.logger import get_logger, log_with_context
from src.shared.ssm import resolve_connect_param
from src.connectors.m365.graph_client import GraphClient

# サブスクリプション ID は SSM に保存
SSM_SUBSCRIPTION_ID = "MSGraphSubscriptionId"


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """EventBridge から呼び出されるサブスクリプション更新ハンドラー。

    サブスクリプション失効を防ぐため、現在値を取得して
    Graph API の `/subscriptions/{id}` に対して有効期限更新を実施する。

    Args:
        event: Lambda 実行イベント
        context: Lambda コンテキスト

    Returns:
        更新結果を含むレスポンス辞書

    Raises:
        Exception: Graph API 呼び出しや設定解決に失敗した場合
    """
    cfg = get_config()
    tenant_id = str(event.get("tenant_id") or cfg.tenant_id).strip() or cfg.tenant_id
    connection_id = str(event.get("connection_id") or "").strip()
    request_id = getattr(context, "aws_request_id", "local")
    logger = get_logger(__name__, tenant_id=tenant_id, request_id=request_id)

    log_with_context(logger, logging.INFO, "Starting subscription renewal")

    try:
        # SSM からサブスクリプション ID を取得
        try:
            subscription_id = resolve_connect_param(
                "subscription_id",
                tenant_id=tenant_id,
                connection_id=connection_id,
                decrypt=False,
                fallback_name=SSM_SUBSCRIPTION_ID,
            )
        except Exception:
            log_with_context(
                logger, logging.WARNING,
                "No subscription ID found in SSM — skipping renewal. "
                "Invoke AIReadyConnect-initSubscription (or register subscription) first.",
            )
            return {"statusCode": 200, "body": "No subscription to renew"}

        if not subscription_id or subscription_id == "PLACEHOLDER":
            log_with_context(
                logger, logging.WARNING,
                "Subscription ID is placeholder — skipping renewal",
            )
            return {"statusCode": 200, "body": "No subscription to renew"}

        # GraphClient 初期化
        client = GraphClient.from_ssm(tenant_id=tenant_id, connection_id=connection_id)

        # 新しい有効期限: 現在から 2 日後。
        # Graph 側の更新失敗時に備え、短い間隔で継続更新する運用に寄せる。
        new_expiration = datetime.now(timezone.utc) + timedelta(days=2)
        expiration_str = new_expiration.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")

        # サブスクリプション更新 (PATCH)
        path = f"/subscriptions/{subscription_id}"
        result = client.graph_patch(
            path,
            json_body={"expirationDateTime": expiration_str},
        )

        actual_expiration = result.get("expirationDateTime", "")

        log_with_context(
            logger, logging.INFO,
            f"Subscription renewed: id={subscription_id}, "
            f"new expiration={actual_expiration}",
            extra_data={
                "subscription_id": subscription_id,
                "expiration": actual_expiration,
            },
        )

        return {
            "statusCode": 200,
            "body": f"Subscription renewed until {actual_expiration}",
        }

    except Exception as e:
        log_with_context(
            logger, logging.ERROR,
            f"Failed to renew subscription: {e}",
            exc_info=True,
        )
        raise
