"""設定値取得ユーティリティ（SSMキャッシュ対応）。"""

from __future__ import annotations

import json
import os
import time
from typing import Any

import boto3
from botocore.client import BaseClient


_ssm_cache: dict[str, tuple[float, str]] = {}
DEFAULT_SSM_CACHE_TTL_SECONDS = 300


def get_ontology_llm_settings() -> dict[str, Any]:
    """LLM 推論実行設定を環境変数から取得する。"""
    return {
        "provider": get_env("ONTOLOGY_LLM_PROVIDER", "bedrock"),
        "model_id": get_env("ONTOLOGY_LLM_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0"),
        "prompt_version": get_env("ONTOLOGY_LLM_PROMPT_VERSION", "v1"),
        "temperature": _safe_float(get_env("ONTOLOGY_LLM_TEMPERATURE", "0.0"), 0.0),
        "max_tokens": int(_safe_float(get_env("ONTOLOGY_LLM_MAX_TOKENS", "600"), 600.0)),
        "timeout_seconds": _safe_float(get_env("ONTOLOGY_LLM_TIMEOUT_SECONDS", "20"), 20.0),
        "max_retries": int(_safe_float(get_env("ONTOLOGY_LLM_MAX_RETRIES", "2"), 2.0)),
    }


def get_env(name: str, default: str | None = None, required: bool = False) -> str:
    """環境変数を取得し、必要に応じて必須検証する。

    Args:
        name: 入力値。
        default: 入力値。
        required: 入力値。

    Returns:
        str: 処理結果。

    Notes:
        required=True で未設定なら例外を送出し、設定漏れを早期検出する。
    """
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return value or ""


def get_tenant_parameter_path(tenant_id: str, key: str) -> str:
    """tenant 単位の SSM パラメータパスを構築する。

    Args:
        tenant_id: 対象テナントID。
        key: 入力値。

    Returns:
        str: 処理結果。

    Notes:
        ontology 標準プレフィックス配下へ統一したパスを返す。
    """
    normalized_key = key.lstrip("/")
    return f"/ai-ready/ontology/{tenant_id}/{normalized_key}"


def get_ssm_parameter(
    name: str,
    *,
    with_decryption: bool = False,
    ttl_seconds: int = DEFAULT_SSM_CACHE_TTL_SECONDS,
    ssm_client: BaseClient | None = None,
) -> str:
    """SSM パラメータ値を TTL キャッシュ付きで取得する。

    Args:
        name: 入力値。
        with_decryption: 入力値。
        ttl_seconds: 入力値。
        ssm_client: 入力値。

    Returns:
        str: 処理結果。

    Notes:
        短時間の重複取得をメモリキャッシュで抑制する。
    """
    now = time.time()
    cache_entry = _ssm_cache.get(name)
    if cache_entry and now < cache_entry[0]:
        return cache_entry[1]

    client = ssm_client or boto3.client("ssm")
    response = client.get_parameter(Name=name, WithDecryption=with_decryption)
    value = response["Parameter"]["Value"]
    _ssm_cache[name] = (now + ttl_seconds, value)
    return value


def get_ssm_json_parameter(
    name: str,
    *,
    with_decryption: bool = False,
    ttl_seconds: int = DEFAULT_SSM_CACHE_TTL_SECONDS,
    ssm_client: BaseClient | None = None,
) -> dict[str, Any]:
    """SSM の JSON 文字列パラメータを辞書として取得する。

    Args:
        name: 入力値。
        with_decryption: 入力値。
        ttl_seconds: 入力値。
        ssm_client: 入力値。

    Returns:
        dict[str, Any]: 処理結果の辞書。

    Notes:
        get_ssm_parameter のキャッシュ経由で取得した値を JSON parse する。
    """
    return json.loads(
        get_ssm_parameter(
            name,
            with_decryption=with_decryption,
            ttl_seconds=ttl_seconds,
            ssm_client=ssm_client,
        )
    )


def clear_ssm_cache() -> None:
    """プロセス内 SSM キャッシュをクリアする。

    Args:
        なし。

    Returns:
        None: 戻り値なし。

    Notes:
        主にテスト時にキャッシュ依存を排除する目的で利用する。
    """
    _ssm_cache.clear()


def _safe_float(raw: str, default: float) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default
