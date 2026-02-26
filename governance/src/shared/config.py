"""環境変数の読み込み + SSM パラメータのキャッシュ付き取得"""

import os
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

_ssm_client = None
_ssm_cache: dict[str, tuple[str, float]] = {}
SSM_CACHE_TTL_SECONDS = 300  # 5 分


def _get_ssm_client():
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client("ssm")
    return _ssm_client


def get_env(name: str, default: str | None = None) -> str:
    """環境変数を取得する。未設定かつデフォルトなしの場合は KeyError を送出。"""
    value = os.environ.get(name, default)
    if value is None:
        raise KeyError(f"Required environment variable '{name}' is not set")
    return value


def get_env_bool(name: str, default: bool = False) -> bool:
    """環境変数を bool として取得する。"""
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def get_ssm_parameter(name: str, default: str | None = None) -> str:
    """SSM Parameter Store から値を取得する（キャッシュ付き）。"""
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
    """SSM パラメータを float として取得する。"""
    str_default = str(default) if default is not None else None
    return float(get_ssm_parameter(name, str_default))


def get_ssm_int(name: str, default: int | None = None) -> int:
    """SSM パラメータを int として取得する。"""
    str_default = str(default) if default is not None else None
    return int(get_ssm_parameter(name, str_default))


def clear_ssm_cache():
    """テスト用: SSM キャッシュをクリアする。"""
    _ssm_cache.clear()


# ─── 定数: SSM パラメータパス ───
SSM_RISK_SCORE_THRESHOLD = "/aiready/governance/risk_score_threshold"
SSM_MAX_EXPOSURE_SCORE = "/aiready/governance/max_exposure_score"
SSM_PERMISSIONS_COUNT_THRESHOLD = "/aiready/governance/permissions_count_threshold"
SSM_RESCAN_INTERVAL_DAYS = "/aiready/governance/rescan_interval_days"
SSM_MAX_FILE_SIZE_BYTES = "/aiready/governance/max_file_size_bytes"
SSM_MAX_TEXT_LENGTH = "/aiready/governance/max_text_length"
SSM_BATCH_SCORING_HOUR_UTC = "/aiready/governance/batch_scoring_hour_utc"

# ─── 定数: Phase 6.5 環境変数キー ───
ENV_DOCUMENT_ANALYSIS_TABLE_NAME = "DOCUMENT_ANALYSIS_TABLE_NAME"
ENV_VECTORS_BUCKET = "VECTORS_BUCKET"
ENV_ENTITY_RESOLUTION_QUEUE_URL = "ENTITY_RESOLUTION_QUEUE_URL"
ENV_DOCUMENT_ANALYSIS_ENABLED = "DOCUMENT_ANALYSIS_ENABLED"
