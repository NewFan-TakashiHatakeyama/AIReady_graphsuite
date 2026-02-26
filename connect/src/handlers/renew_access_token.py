"""T-025: renew_access_token Lambda

EventBridge rate(30 minutes) で定期実行し、
Azure AD から新しいアクセストークンを取得して SSM Parameter Store に保存する。
"""

from __future__ import annotations

import logging
from typing import Any

from src.shared.config import get_config
from src.shared.logger import get_logger, log_with_context
from src.connectors.m365.graph_client import GraphClient


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """EventBridge から呼び出されるトークン更新ハンドラー"""
    cfg = get_config()
    request_id = getattr(context, "aws_request_id", "local")
    logger = get_logger(__name__, tenant_id=cfg.tenant_id, request_id=request_id)

    log_with_context(logger, logging.INFO, "Starting access token renewal")

    try:
        # SSM から認証情報を読み込んで GraphClient を初期化
        client = GraphClient.from_ssm()

        # トークンを更新して SSM に保存
        token = client.refresh_and_store_token()

        log_with_context(
            logger, logging.INFO,
            f"Access token renewed successfully (token length: {len(token)})",
        )

        return {
            "statusCode": 200,
            "body": "Token renewed successfully",
        }

    except Exception as e:
        log_with_context(
            logger, logging.ERROR,
            f"Failed to renew access token: {e}",
            exc_info=True,
        )
        raise
