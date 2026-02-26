"""T-016: SSM Parameter Store ヘルパー

get_param() / put_param() を提供。lru_cache で Lambda 実行内のキャッシュを行う。
"""

from __future__ import annotations

from functools import lru_cache

import boto3

from src.shared.config import get_config


def _client():
    """SSM クライアント（モジュールレベルで再利用）"""
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
    _client().put_parameter(
        Name=name,
        Value=value,
        Type=param_type,
        Overwrite=overwrite,
    )
    # キャッシュを無効化（put した値を次回の get で正しく返すため）
    get_param.cache_clear()


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
    return {
        "client_id": get_param(cfg.ssm_client_id, decrypt=False),
        "tenant_id": get_param(cfg.ssm_tenant_id, decrypt=False),
        "client_secret": get_param(cfg.ssm_client_secret),
        "access_token": get_param(cfg.ssm_access_token),
        "drive_id": get_param(cfg.ssm_drive_id, decrypt=False),
        "client_state": get_param(cfg.ssm_client_state),
    }
