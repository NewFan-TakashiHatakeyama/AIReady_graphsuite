"""T-015: Config dataclass — 環境変数を属性として参照可能にする

Lambda 環境変数や SSM パラメータ名を一元管理する。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


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

    def graph_token_url(self, azure_tenant_id: str) -> str:
        """Azure AD テナント ID を埋め込んだトークンエンドポイントを返す"""
        return self.graph_token_url_template.format(tenant_id=azure_tenant_id)


# シングルトン的に利用
_config: Config | None = None


def get_config() -> Config:
    """Config のシングルトンインスタンスを取得"""
    global _config
    if _config is None:
        _config = Config()
    return _config
