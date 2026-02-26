"""CloudWatch metrics helper."""

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
    """Publish a single CloudWatch metric value."""
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
