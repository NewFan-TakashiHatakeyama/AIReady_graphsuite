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
from src.shared.ssm import get_param, put_param
from src.connectors.m365.graph_client import GraphClient

# サブスクリプション ID は SSM に保存
SSM_SUBSCRIPTION_ID = "MSGraphSubscriptionId"


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """EventBridge から呼び出されるサブスクリプション更新ハンドラー"""
    cfg = get_config()
    request_id = getattr(context, "aws_request_id", "local")
    logger = get_logger(__name__, tenant_id=cfg.tenant_id, request_id=request_id)

    log_with_context(logger, logging.INFO, "Starting subscription renewal")

    try:
        # SSM からサブスクリプション ID を取得
        try:
            subscription_id = get_param(SSM_SUBSCRIPTION_ID, decrypt=False)
        except Exception:
            log_with_context(
                logger, logging.WARNING,
                "No subscription ID found in SSM — skipping renewal. "
                "Run scripts/init_subscription.py first.",
            )
            return {"statusCode": 200, "body": "No subscription to renew"}

        if not subscription_id or subscription_id == "PLACEHOLDER":
            log_with_context(
                logger, logging.WARNING,
                "Subscription ID is placeholder — skipping renewal",
            )
            return {"statusCode": 200, "body": "No subscription to renew"}

        # GraphClient 初期化
        client = GraphClient.from_ssm()

        # 新しい有効期限: 現在から 2 日後 (余裕を持たせる)
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
