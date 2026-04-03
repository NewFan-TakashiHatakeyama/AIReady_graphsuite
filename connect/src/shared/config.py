"""T-015: 設定管理 dataclass

Lambda 環境変数や SSM パラメータ名を一元管理する。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _default_chat_backfill_max() -> int:
    raw = os.getenv("CONNECT_CHAT_BACKFILL_MAX_MESSAGES", "500") or "500"
    try:
        return max(0, int(raw.strip()))
    except ValueError:
        return 500


@dataclass(frozen=True)
class Config:
    """Lambda 共通設定

    環境変数から自動読み込みし、未設定の場合はデフォルト値を使用する。
    """

    # ── AWS リソース ──
    region: str = field(
        default_factory=lambda: os.getenv("AWS_DEFAULT_REGION", "ap-northeast-1")
    )
    file_metadata_table: str = field(
        default_factory=lambda: os.getenv(
            "FILE_METADATA_TABLE", "AIReadyConnect-FileMetadata"
        )
    )
    message_metadata_table: str = field(
        default_factory=lambda: os.getenv(
            "MESSAGE_METADATA_TABLE", "AIReadyConnect-MessageMetadata"
        )
    )
    idempotency_table: str = field(
        default_factory=lambda: os.getenv(
            "IDEMPOTENCY_TABLE", "AIReadyConnect-IdempotencyKeys"
        )
    )
    delta_tokens_table: str = field(
        default_factory=lambda: os.getenv(
            "DELTA_TOKENS_TABLE", "AIReadyConnect-DeltaTokens"
        )
    )
    connections_table: str = field(
        default_factory=lambda: os.getenv("CONNECT_CONNECTIONS_TABLE", "")
    )
    notification_topic_arn: str = field(
        default_factory=lambda: os.getenv("NOTIFICATION_TOPIC_ARN", "")
    )
    raw_bucket: str = field(
        default_factory=lambda: os.getenv("RAW_BUCKET", "")
    )

    # ── テナント ──
    tenant_id: str = field(
        default_factory=lambda: os.getenv("TENANT_ID", "default")
    )

    # ── SSM パラメータ名 ──
    ssm_client_id: str = "MSGraphClientId"
    ssm_tenant_id: str = "MSGraphTenantId"
    ssm_client_secret: str = "MSGraphClientSecret"
    ssm_client_state: str = "MSGraphClientState"
    ssm_drive_id: str = "MSGraphDriveId"
    ssm_access_token: str = "MSGraphAccessToken"

    # ── Graph API ──
    graph_base_url: str = "https://graph.microsoft.com/v1.0"
    graph_token_url_template: str = (
        "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    )
    graph_scope: str = "https://graph.microsoft.com/.default"

    # ── Webhook ──
    webhook_url: str = field(
        default_factory=lambda: os.getenv(
            "WEBHOOK_URL", "https://webhook.graphsuite.jp"
        )
    )

    # ── タイムアウト・リトライ ──
    graph_api_timeout: int = 30
    graph_api_max_retries: int = 3
    graph_api_retry_backoff: float = 1.0

    # ── 冪等 TTL (秒) ──
    idempotency_ttl_seconds: int = 7 * 24 * 60 * 60  # 7 days

    # ── チャット履歴バックフィル（Lambda backfill_chat_messages） ──
    # 0 = 無制限（非推奨）。正の整数で最大取得件数を上限する。
    chat_backfill_max_messages: int = field(default_factory=_default_chat_backfill_max)

    def graph_token_url(self, azure_tenant_id: str) -> str:
        """Azure AD テナント ID を埋め込んだトークンエンドポイントを返す。

        Args:
            azure_tenant_id: Azure AD テナント ID

        Returns:
            トークンエンドポイント URL
        """
        return self.graph_token_url_template.format(tenant_id=azure_tenant_id)


# シングルトン的に利用
_config: Config | None = None


def get_config() -> Config:
    """Config のシングルトンインスタンスを取得する。

    Returns:
        共通設定インスタンス
    """
    global _config
    if _config is None:
        _config = Config()
    return _config
