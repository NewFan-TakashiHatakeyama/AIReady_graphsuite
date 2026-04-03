"""CloudWatchメトリクス送信ユーティリティ。"""

from __future__ import annotations

from typing import Any

import boto3
from botocore.client import BaseClient


NAMESPACE = "AIReadyOntology"


def publish_metric(
    metric_name: str,
    value: float,
    *,
    unit: str = "Count",
    dimensions: list[dict[str, str]] | None = None,
    namespace: str = NAMESPACE,
    cloudwatch_client: BaseClient | None = None,
) -> dict[str, Any]:
    """CloudWatch へ単一メトリクスを送信する。

    Args:
        metric_name: メトリクス名。
        value: 変換対象値。
        unit: 入力値。
        dimensions: 入力値。
        namespace: 入力値。
        cloudwatch_client: 入力値。

    Returns:
        dict[str, Any]: 処理結果の辞書。

    Notes:
        必要に応じて dimensions を付与し、Namespace 単位で記録する。
    """
    client = cloudwatch_client or boto3.client("cloudwatch")
    payload: dict[str, Any] = {
        "MetricName": metric_name,
        "Value": value,
        "Unit": unit,
    }
    if dimensions:
        payload["Dimensions"] = dimensions

    return client.put_metric_data(
        Namespace=namespace,
        MetricData=[payload],
    )
