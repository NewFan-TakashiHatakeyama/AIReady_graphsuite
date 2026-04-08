"""T-016: SSM Parameter Store ヘルパー

get_param() / put_param() を提供。lru_cache で Lambda 実行内のキャッシュを行う。
"""

from __future__ import annotations

import unicodedata
from functools import lru_cache
from typing import Any

import boto3
from botocore.exceptions import ClientError

from src.shared.config import get_config
from src.shared.connect_secrets import get_connection_client_secret
from src.shared.connection_lookup import fetch_connection_item
from src.shared.customer_credentials import try_resolve_customer_graph_credentials


def _client():
    """SSM クライアントを生成する。

    Returns:
        boto3 の SSM クライアント
    """
    cfg = get_config()
    return boto3.client("ssm", region_name=cfg.region)


@lru_cache(maxsize=32)
def get_param(name: str, decrypt: bool = True) -> str:
    """Parameter Store からパラメータ値を取得する

    Args:
        name: パラメータ名
        decrypt: SecureString を復号するか（デフォルト True）

    Returns:
        パラメータの値

    Raises:
        botocore.exceptions.ClientError: パラメータが存在しない場合
    """
    resp = _client().get_parameter(Name=name, WithDecryption=decrypt)
    return resp["Parameter"]["Value"]


def get_param_optional(name: str, decrypt: bool = True) -> str:
    """Parameter Store から値を取得し、未存在時は空文字を返す。

    Args:
        name: パラメータ名
        decrypt: SecureString を復号するか

    Returns:
        取得した値。未存在時は空文字
    """
    try:
        return get_param(name, decrypt=decrypt)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"ParameterNotFound", "ValidationException"}:
            return ""
        raise


def _normalize_ssm_put_value(value: str | None) -> str:
    """PutParameter 用に値を正規化する。

    SSM の PutParameter は Value が空文字だと ValidationException になる。
    オプション項目（site_id など）は呼び出し元が空を渡すことがあるため、
    正規化後に空なら書き込まない（API 側 _normalize_ssm_parameter_value と同じ方針）。
    """
    raw = str(value if value is not None else "").strip()
    for ch in ("\ufeff", "\u200b", "\u200c", "\u200d", "\u2060"):
        raw = raw.replace(ch, "")
    raw = "".join(c for c in raw if unicodedata.category(c) != "Cf")
    return raw.strip()


def put_param(
    name: str,
    value: str,
    param_type: str = "SecureString",
    overwrite: bool = True,
) -> None:
    """Parameter Store にパラメータを登録（上書き可）

    Args:
        name: パラメータ名
        value: パラメータ値
        param_type: String | SecureString
        overwrite: 既存パラメータを上書きするか
    """
    normalized = _normalize_ssm_put_value(value)
    if not normalized:
        return
    _client().put_parameter(
        Name=name,
        Value=normalized,
        Type=param_type,
        Overwrite=overwrite,
    )
    # キャッシュを無効化（put した値を次回の get で正しく返すため）
    get_param.cache_clear()


def tenant_param_name(tenant_id: str, key: str) -> str:
    """テナント単位の SSM パラメータ名を構築する。

    Args:
        tenant_id: テナント識別子
        key: パラメータキー

    Returns:
        テナントスコープのパラメータ名
    """
    normalized_tenant_id = str(tenant_id or "").strip()
    return f"/aiready/connect/{normalized_tenant_id}/{key}"


def _connection_item_value_for_param(item: dict[str, str] | None, key: str) -> str:
    """Map SSM param keys to DynamoDB connection attributes."""
    if not item:
        return ""
    if key == "tenant_id":
        return str(item.get("graph_tenant_id") or "").strip()
    if key == "client_id":
        return str(item.get("graph_client_id") or "").strip()
    return str(item.get(key) or "").strip()


def tenant_connection_param_name(tenant_id: str, connection_id: str, key: str) -> str:
    """テナント + 接続単位の SSM パラメータ名を構築する。

    Args:
        tenant_id: テナント識別子
        connection_id: 接続識別子
        key: パラメータキー

    Returns:
        テナント + 接続スコープのパラメータ名
    """
    normalized_tenant_id = str(tenant_id or "").strip()
    normalized_connection_id = str(connection_id or "").strip()
    return f"/aiready/connect/{normalized_tenant_id}/{normalized_connection_id}/{key}"


def resolve_connect_param(
    key: str,
    *,
    tenant_id: str = "",
    connection_id: str = "",
    decrypt: bool = True,
    fallback_name: str = "",
    _connection_item: dict[str, str] | None = None,
) -> str:
    """接続スコープのパラメータを後方互換付きで解決する。

    解決順序:
    1. DynamoDB 接続行（CONNECT_ONBOARDING_OMIT_CONNECTION_SSM 運用時の主ソース）
    2. Secrets Manager の接続 client_secret（key == client_secret のみ）
    3. `/aiready/connect/{tenant_id}/{connection_id}/{key}`
    4. `/aiready/connect/{tenant_id}/{key}`
    5. `fallback_name`（従来キー）

    Args:
        key: 解決対象のパラメータキー
        tenant_id: テナント識別子
        connection_id: 接続識別子
        decrypt: SecureString を復号するか
        fallback_name: 最終フォールバックする従来キー名
        _connection_item: 同一呼び出し内で fetch 済みの行（任意）

    Returns:
        解決できたパラメータ値。未解決時は空文字
    """
    normalized_tenant_id = str(tenant_id or "").strip()
    normalized_connection_id = str(connection_id or "").strip()

    if key == "client_secret" and normalized_tenant_id and normalized_connection_id:
        from_sm = get_connection_client_secret(
            tenant_id=normalized_tenant_id, connection_id=normalized_connection_id
        )
        if from_sm:
            return from_sm

    row = _connection_item
    if row is None and normalized_tenant_id and normalized_connection_id:
        row = fetch_connection_item(
            tenant_id=normalized_tenant_id, connection_id=normalized_connection_id
        )
    if row:
        from_row = _connection_item_value_for_param(row, key)
        if from_row:
            return from_row

    if normalized_tenant_id and normalized_connection_id:
        scoped = get_param_optional(
            tenant_connection_param_name(normalized_tenant_id, normalized_connection_id, key),
            decrypt=decrypt,
        )
        if scoped:
            return scoped
    if normalized_tenant_id:
        tenant_scoped = get_param_optional(
            tenant_param_name(normalized_tenant_id, key),
            decrypt=decrypt,
        )
        if tenant_scoped:
            return tenant_scoped
    if fallback_name:
        return get_param_optional(fallback_name, decrypt=decrypt)
    return ""


def get_graph_credentials() -> dict[str, str]:
    """Graph API 認証に必要な全パラメータをまとめて取得

    Returns:
        {
            "client_id": ...,
            "tenant_id": ...,
            "client_secret": ...,
            "access_token": ...,
            "drive_id": ...,
            "client_state": ...,
        }
    """
    cfg = get_config()
    tenant_id = cfg.tenant_id
    connection_id = ""
    return {
        "client_id": resolve_connect_param(
            "client_id",
            tenant_id=tenant_id,
            connection_id=connection_id,
            decrypt=False,
            fallback_name=cfg.ssm_client_id,
        ),
        "tenant_id": resolve_connect_param(
            "tenant_id",
            tenant_id=tenant_id,
            connection_id=connection_id,
            decrypt=False,
            fallback_name=cfg.ssm_tenant_id,
        ),
        "client_secret": resolve_connect_param(
            "client_secret",
            tenant_id=tenant_id,
            connection_id=connection_id,
            decrypt=True,
            fallback_name=cfg.ssm_client_secret,
        ),
        "access_token": resolve_connect_param(
            "access_token",
            tenant_id=tenant_id,
            connection_id=connection_id,
            decrypt=True,
            fallback_name=cfg.ssm_access_token,
        ),
        "drive_id": resolve_connect_param(
            "drive_id",
            tenant_id=tenant_id,
            connection_id=connection_id,
            decrypt=False,
            fallback_name=cfg.ssm_drive_id,
        ),
        "client_state": resolve_connect_param(
            "client_state",
            tenant_id=tenant_id,
            connection_id=connection_id,
            decrypt=True,
            fallback_name=cfg.ssm_client_state,
        ),
    }


def get_graph_credentials_scoped(
    *,
    tenant_id: str,
    connection_id: str = "",
) -> dict[str, str]:
    """テナント/接続スコープを考慮して Graph 認証情報を取得する。

    既存環境との互換性を保つため、内部では `resolve_connect_param` を
    用いて段階的に解決する。

    Args:
        tenant_id: テナント識別子
        connection_id: 接続識別子

    Returns:
        Graph 認証関連パラメータの辞書
    """
    cfg = get_config()
    normalized_tenant_id = str(tenant_id or cfg.tenant_id).strip()
    normalized_connection_id = str(connection_id or "").strip()

    cust = try_resolve_customer_graph_credentials(tenant_id=normalized_tenant_id)
    if cust:
        azure_tenant_id, graph_client_id, graph_client_secret = cust
        row = (
            fetch_connection_item(
                tenant_id=normalized_tenant_id, connection_id=normalized_connection_id
            )
            if normalized_connection_id
            else None
        )

        def _resolve(
            k: str,
            *,
            decrypt: bool = True,
            fallback_name: str = "",
        ) -> str:
            return resolve_connect_param(
                k,
                tenant_id=normalized_tenant_id,
                connection_id=normalized_connection_id,
                decrypt=decrypt,
                fallback_name=fallback_name,
                _connection_item=row,
            )

        return {
            "client_id": graph_client_id,
            "tenant_id": azure_tenant_id,
            "client_secret": graph_client_secret,
            "access_token": _resolve("access_token", fallback_name=cfg.ssm_access_token),
            "drive_id": _resolve("drive_id", decrypt=False, fallback_name=cfg.ssm_drive_id),
            "client_state": _resolve("client_state", fallback_name=cfg.ssm_client_state),
            "notification_url": _resolve("notification_url", decrypt=False, fallback_name=""),
            "site_id": _resolve("site_id", decrypt=False, fallback_name=""),
        }

    row = (
        fetch_connection_item(
            tenant_id=normalized_tenant_id, connection_id=normalized_connection_id
        )
        if normalized_connection_id
        else None
    )

    def _resolve(
        k: str,
        *,
        decrypt: bool = True,
        fallback_name: str = "",
    ) -> str:
        return resolve_connect_param(
            k,
            tenant_id=normalized_tenant_id,
            connection_id=normalized_connection_id,
            decrypt=decrypt,
            fallback_name=fallback_name,
            _connection_item=row,
        )

    return {
        "client_id": _resolve("client_id", decrypt=False, fallback_name=cfg.ssm_client_id),
        "tenant_id": _resolve("tenant_id", decrypt=False, fallback_name=cfg.ssm_tenant_id),
        "client_secret": _resolve("client_secret", fallback_name=cfg.ssm_client_secret),
        "access_token": _resolve("access_token", fallback_name=cfg.ssm_access_token),
        "drive_id": _resolve("drive_id", decrypt=False, fallback_name=cfg.ssm_drive_id),
        "client_state": _resolve("client_state", fallback_name=cfg.ssm_client_state),
        "notification_url": _resolve("notification_url", decrypt=False, fallback_name=""),
        "site_id": _resolve("site_id", decrypt=False, fallback_name=""),
    }
