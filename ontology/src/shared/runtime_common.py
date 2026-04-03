"""ハンドラ横断で使う軽量ランタイム共通処理。"""

from __future__ import annotations

from typing import Any

import boto3
from botocore.exceptions import ClientError

from src.shared.logger import log_structured
from src.shared.metrics import publish_metric

_dynamodb_resource: Any | None = None


def get_dynamodb_resource() -> Any:
    """DynamoDB resource を遅延初期化で返す。"""
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    return _dynamodb_resource


def to_int(value: Any, *, default: int) -> int:
    """値を int へ安全変換する。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_publish_metric(metric_name: str, value: float, *, unit: str = "Count") -> None:
    """CloudWatch メトリクス送信失敗を握りつぶして継続する。"""
    try:
        publish_metric(metric_name, value, unit=unit)
    except ClientError as exc:
        log_structured(
            "WARN",
            "CloudWatch metric publish failed",
            metric_name=metric_name,
            value=value,
            error=str(exc),
        )
