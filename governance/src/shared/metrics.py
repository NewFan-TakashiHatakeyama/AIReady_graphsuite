"""CloudWatch メトリクス送信ヘルパー

Namespace: AIReadyGovernance
"""

from typing import Any

import boto3

_cw_client = None
NAMESPACE = "AIReadyGovernance"


def _get_cw_client():
    """CloudWatch クライアントを遅延初期化して返す。
    
    Args:
        なし。
    
    Returns:
        なし。
    
    Notes:
        なし。
    """
    global _cw_client
    if _cw_client is None:
        _cw_client = boto3.client("cloudwatch")
    return _cw_client


def emit_metric(
    metric_name: str,
    value: float,
    unit: str = "Count",
    dimensions: dict[str, str] | None = None,
) -> None:
    """CloudWatch にメトリクスを送信する。
    
    Args:
        metric_name: 引数。
        value: 引数。
        unit: 引数。
        dimensions: 引数。
    
    Returns:
        なし。
    
    Notes:
        なし。
    """
    cw_dimensions = []
    if dimensions:
        cw_dimensions = [{"Name": k, "Value": v} for k, v in dimensions.items()]

    try:
        client = _get_cw_client()
        client.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": unit,
                    "Dimensions": cw_dimensions,
                }
            ],
        )
    except Exception:
        # メトリクス送信失敗で本処理を止めない。
        pass


def emit_count(
    metric_name: str,
    count: int = 1,
    dimensions: dict[str, str] | None = None,
) -> None:
    """カウントメトリクスの送信ショートカット。
    
    Args:
        metric_name: 引数。
        count: 引数。
        dimensions: 引数。
    
    Returns:
        なし。
    
    Notes:
        なし。
    """
    emit_metric(metric_name, float(count), unit="Count", dimensions=dimensions)


def emit_duration(
    metric_name: str,
    duration_ms: float,
    dimensions: dict[str, str] | None = None,
) -> None:
    """ミリ秒メトリクスの送信ショートカット。
    
    Args:
        metric_name: 引数。
        duration_ms: 引数。
        dimensions: 引数。
    
    Returns:
        なし。
    
    Notes:
        なし。
    """
    emit_metric(metric_name, duration_ms, unit="Milliseconds", dimensions=dimensions)
