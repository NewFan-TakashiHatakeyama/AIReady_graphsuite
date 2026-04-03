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
    """EventBridge から呼び出されるトークン更新ハンドラー。

    tenant/connection スコープの認証情報を使って
    アクセストークンを再発行し、SSM に保存する。

    Args:
        event: Lambda 実行イベント
        context: Lambda コンテキスト

    Returns:
        更新結果を含むレスポンス辞書

    Raises:
        Exception: トークン更新または保存に失敗した場合
    """
    cfg = get_config()
    tenant_id = str(event.get("tenant_id") or cfg.tenant_id).strip() or cfg.tenant_id
    connection_id = str(event.get("connection_id") or "").strip()
    request_id = getattr(context, "aws_request_id", "local")
    logger = get_logger(__name__, tenant_id=tenant_id, request_id=request_id)

    log_with_context(logger, logging.INFO, "Starting access token renewal")

    try:
        # SSM から認証情報を読み込み GraphClient を初期化。
        # この時点では既存 token の有効性は問わず、更新処理で上書きする。
        client = GraphClient.from_ssm(tenant_id=tenant_id, connection_id=connection_id)

        # トークンを更新して SSM に保存
        token = client.refresh_and_store_token(tenant_id=tenant_id, connection_id=connection_id)

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
