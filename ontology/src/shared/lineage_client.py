"""Helper for invoking lineageRecorder Lambda synchronously."""

from __future__ import annotations

import json
import os
import time
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from src.shared.logger import log_structured


_lambda_client: BaseClient | None = None


def _get_lambda_client() -> BaseClient:
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client("lambda")
    return _lambda_client


def record_lineage_event(
    function_name: str,
    lineage_id: str,
    job_name: str,
    input_dataset: str,
    output_dataset: str,
    *,
    event_type: str = "COMPLETE",
    metadata: dict[str, Any] | None = None,
    duration_ms: int | None = None,
    error_message: str | None = None,
    tenant_id: str | None = None,
    lambda_client: BaseClient | None = None,
) -> dict[str, Any] | None:
    """Invoke lineageRecorder and return response payload or None on failure."""
    tenant = tenant_id or os.environ.get("TENANT_ID", "unknown")
    payload: dict[str, Any] = {
        "lineage_id": lineage_id,
        "tenant_id": tenant,
        "job_name": job_name,
        "event_type": event_type,
        "input_dataset": input_dataset,
        "output_dataset": output_dataset,
    }
    if metadata is not None:
        payload["metadata"] = metadata
    if duration_ms is not None:
        payload["duration_ms"] = duration_ms
    if error_message:
        payload["error_message"] = error_message

    client = lambda_client or _get_lambda_client()
    try:
        started = time.monotonic()
        response = client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )
        elapsed_ms = int((time.monotonic() - started) * 1000)

        raw_payload = response["Payload"].read()
        if isinstance(raw_payload, bytes):
            raw_payload = raw_payload.decode("utf-8")
        decoded_payload = json.loads(raw_payload or "{}")

        if response.get("FunctionError"):
            log_structured(
                "WARN",
                "lineageRecorder invocation returned function error",
                tenant_id=tenant,
                function_name=function_name,
                elapsed_ms=elapsed_ms,
                response=decoded_payload,
            )
            return None

        return decoded_payload
    except ClientError as exc:
        log_structured(
            "WARN",
            "lineageRecorder invocation failed",
            tenant_id=tenant,
            function_name=function_name,
            error=str(exc),
        )
        return None
    except Exception as exc:  # pragma: no cover - defensive fallback
        log_structured(
            "WARN",
            "lineageRecorder invocation unexpected error",
            tenant_id=tenant,
            function_name=function_name,
            error=str(exc),
        )
        return None
