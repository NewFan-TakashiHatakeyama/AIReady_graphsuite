from __future__ import annotations

import json

from botocore.exceptions import ClientError

from src.shared.lineage_client import record_lineage_event


class _Payload:
    def __init__(self, data: dict):
        self._data = data

    def read(self) -> bytes:
        return json.dumps(self._data).encode("utf-8")


class _LambdaOk:
    def invoke(self, **kwargs):
        return {"Payload": _Payload({"status": "recorded"})}


class _LambdaFunctionError:
    def invoke(self, **kwargs):
        return {
            "FunctionError": "Unhandled",
            "Payload": _Payload({"error": "failed"}),
        }


class _LambdaClientError:
    def invoke(self, **kwargs):
        raise ClientError({"Error": {"Code": "500", "Message": "x"}}, "Invoke")


def test_record_lineage_event_success() -> None:
    result = record_lineage_event(
        function_name="lineageFn",
        lineage_id="l-1",
        job_name="schemaTransform",
        input_dataset="in",
        output_dataset="out",
        lambda_client=_LambdaOk(),
        tenant_id="tenant-1",
    )
    assert result is not None
    assert result["status"] == "recorded"


def test_record_lineage_event_function_error_returns_none() -> None:
    result = record_lineage_event(
        function_name="lineageFn",
        lineage_id="l-1",
        job_name="schemaTransform",
        input_dataset="in",
        output_dataset="out",
        lambda_client=_LambdaFunctionError(),
        tenant_id="tenant-1",
    )
    assert result is None


def test_record_lineage_event_client_error_returns_none() -> None:
    result = record_lineage_event(
        function_name="lineageFn",
        lineage_id="l-1",
        job_name="schemaTransform",
        input_dataset="in",
        output_dataset="out",
        lambda_client=_LambdaClientError(),
        tenant_id="tenant-1",
    )
    assert result is None
