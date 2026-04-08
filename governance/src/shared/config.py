"""設定取得ユーティリティ。

このモジュールは以下を提供する:
- 必須/任意の環境変数取得
- SSM Parameter Store からの設定取得（TTL キャッシュ付き）
- ガバナンス機能で利用する定数キー群
"""

import os
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

_ssm_client = None
_ssm_cache: dict[str, tuple[str, float]] = {}
SSM_CACHE_TTL_SECONDS = 300  # 5 分


def _get_ssm_client():
    """SSM クライアントを遅延初期化して返す。
    
    Args:
        なし。
    
    Returns:
        なし。
    
    Notes:
        なし。
    """
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client("ssm")
    return _ssm_client


def get_env(name: str, default: str | None = None) -> str:
    """環境変数を取得する。未設定かつデフォルトなしの場合は KeyError を送出。
    
    Args:
        name: 引数。
        default: 引数。
    
    Returns:
        戻り値。
    
    Notes:
        なし。
    """
    value = os.environ.get(name, default)
    if value is None:
        raise KeyError(f"Required environment variable '{name}' is not set")
    return value


def get_env_bool(name: str, default: bool = False) -> bool:
    """環境変数を bool として取得する。
    
    Args:
        name: 引数。
        default: 引数。
    
    Returns:
        戻り値。
    
    Notes:
        なし。
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def get_ssm_parameter(name: str, default: str | None = None) -> str:
    """SSM Parameter Store から値を取得する（キャッシュ付き）。
    
    Args:
        name: 引数。
        default: 引数。
    
    Returns:
        戻り値。
    
    Notes:
        なし。
    """
    now = time.time()

    if name in _ssm_cache:
        cached_value, cached_at = _ssm_cache[name]
        if now - cached_at < SSM_CACHE_TTL_SECONDS:
            return cached_value

    try:
        client = _get_ssm_client()
        response = client.get_parameter(Name=name)
        value = response["Parameter"]["Value"]
        _ssm_cache[name] = (value, now)
        return value
    except ClientError as e:
        if e.response["Error"]["Code"] == "ParameterNotFound":
            if default is not None:
                return default
        raise


def get_ssm_float(name: str, default: float | None = None) -> float:
    """SSM パラメータを float として取得する。
    
    Args:
        name: 引数。
        default: 引数。
    
    Returns:
        戻り値。
    
    Notes:
        なし。
    """
    str_default = str(default) if default is not None else None
    return float(get_ssm_parameter(name, str_default))


def get_ssm_int(name: str, default: int | None = None) -> int:
    """SSM パラメータを int として取得する。
    
    Args:
        name: 引数。
        default: 引数。
    
    Returns:
        戻り値。
    
    Notes:
        なし。
    """
    str_default = str(default) if default is not None else None
    return int(get_ssm_parameter(name, str_default))


def clear_ssm_cache():
    """テスト用: SSM キャッシュを全削除する。"""
    _ssm_cache.clear()


# ─── 定数: SSM パラメータパス ───
SSM_MAX_EXPOSURE_SCORE = "/aiready/governance/max_exposure_score"
SSM_PERMISSIONS_COUNT_THRESHOLD = "/aiready/governance/permissions_count_threshold"
SSM_RESCAN_INTERVAL_DAYS = "/aiready/governance/rescan_interval_days"
SSM_MAX_FILE_SIZE_BYTES = "/aiready/governance/max_file_size_bytes"
SSM_MAX_TEXT_LENGTH = "/aiready/governance/max_text_length"
SSM_IMPORTANCE_THRESHOLD = "/aiready/governance/importance_threshold"
SSM_IMPORTANCE_STALE_DAYS = "/aiready/governance/importance_stale_days"
SSM_CONTENT_CONFIDENCE_THRESHOLD = "/aiready/governance/content_confidence_threshold"

# ─── 定数: 互換維持の環境変数キー（Phase C で未使用） ───
ENV_DOCUMENT_ANALYSIS_TABLE_NAME = "DOCUMENT_ANALYSIS_TABLE_NAME"
ENV_VECTORS_BUCKET = "VECTORS_BUCKET"
ENV_ENTITY_RESOLUTION_QUEUE_URL = "ENTITY_RESOLUTION_QUEUE_URL"
ENV_GOV_CONTENT_ANALYZER_MODEL_ID = "GOVERNANCE_CONTENT_ANALYZER_MODEL_ID"
ENV_GOV_CONTENT_ANALYZER_PROMPT_VERSION = "GOVERNANCE_CONTENT_ANALYZER_PROMPT_VERSION"
ENV_GOV_CONTENT_CONFIDENCE_THRESHOLD = "GOVERNANCE_CONTENT_CONFIDENCE_THRESHOLD"
ENV_GOV_CONFIDENCE_FAILSAFE_IGNORE_PERMISSION_VECTORS = (
    "GOVERNANCE_CONFIDENCE_FAILSAFE_IGNORE_PERMISSION_VECTORS"
)
