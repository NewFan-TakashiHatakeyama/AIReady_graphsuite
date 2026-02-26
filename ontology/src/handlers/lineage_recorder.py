"""lineageRecorder Lambda handler."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import boto3

from src.models.lineage_event import LineageEvent
from src.shared.logger import log_structured
from src.shared.metrics import publish_metric

JOB_NAMESPACE = "ai-ready-ontology"
PRODUCER = "https://ai-ready.example.com/ontology"
SCHEMA_URL = (
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
)
TTL_DAYS = 90
VALID_EVENT_TYPES = {"START", "COMPLETE", "FAIL", "ABORT"}

JOB_DESCRIPTIONS = {
    "schemaTransform": "Connect FileMetadata to UnifiedMetadata",
    "entityResolver": "Resolve entities and register gold master",
    "batchReconciler": "Daily reconciliation and quality rescoring",
    "governanceIntegration": "Consume governance integrated analysis results",
}

_dynamodb_resource = None


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Handle synchronous lineage record request."""
    missing_field = _validate_required_fields(event)
    if missing_field:
        return {"statusCode": 400, "error": f"Missing required field: {missing_field}"}

    event_type = str(event["event_type"])
    if event_type not in VALID_EVENT_TYPES:
        return {"statusCode": 400, "error": f"Invalid event_type: {event_type}"}

    now_iso = datetime.now(timezone.utc).isoformat()
    run_event = _build_openlineage_event(
        lineage_id=str(event["lineage_id"]),
        tenant_id=str(event["tenant_id"]),
        job_name=str(event["job_name"]),
        event_type=event_type,
        event_time=now_iso,
        input_dataset=str(event.get("input_dataset", "")),
        output_dataset=str(event.get("output_dataset", "")),
        metadata=event.get("metadata"),
    )

    status = "failure" if event_type == "FAIL" else "success"
    if event_type == "ABORT":
        status = "skipped"

    ttl_epoch = int(datetime.now(timezone.utc).timestamp()) + (TTL_DAYS * 86400)
    model = LineageEvent(
        tenant_id=str(event["tenant_id"]),
        lineage_id=str(event["lineage_id"]),
        event_type=event_type,
        event_time=now_iso,
        job_namespace=JOB_NAMESPACE,
        job_name=str(event["job_name"]),
        run_id=str(event["lineage_id"]),
        inputs=run_event.get("inputs", []),
        outputs=run_event.get("outputs", []),
        metadata=event.get("metadata") or {},
        duration_ms=int(event.get("duration_ms", 0)),
        status=status,
        error_message=event.get("error_message"),
        ttl=ttl_epoch,
    )

    _get_lineage_table().put_item(Item=model.to_dynamodb_item())

    _safe_publish_metric("LineageEventsRecorded", 1)
    if event_type == "FAIL":
        _safe_publish_metric("LineageFailEvents", 1)

    return {
        "statusCode": 200,
        "lineage_id": model.lineage_id,
        "status": "recorded",
    }


def _get_dynamodb_resource() -> Any:
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    return _dynamodb_resource


def _get_lineage_table() -> Any:
    import os

    table_name = os.environ.get("LINEAGE_EVENT_TABLE", "")
    if not table_name:
        raise ValueError("Environment variable 'LINEAGE_EVENT_TABLE' is required")
    return _get_dynamodb_resource().Table(table_name)


def _validate_required_fields(event: dict[str, Any]) -> str | None:
    required = ("lineage_id", "tenant_id", "job_name", "event_type")
    for field in required:
        if field not in event:
            return field
    return None


def _build_openlineage_event(
    *,
    lineage_id: str,
    tenant_id: str,
    job_name: str,
    event_type: str,
    event_time: str,
    input_dataset: str,
    output_dataset: str,
    metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "eventType": event_type,
        "eventTime": event_time,
        "run": {
            "runId": lineage_id,
            "facets": {
                "processing_engine": {
                    "_producer": PRODUCER,
                    "_schemaURL": (
                        "https://openlineage.io/spec/facets/1-0-0/"
                        "ProcessingEngineRunFacet.json"
                    ),
                    "version": "1.0.0",
                    "name": "AWS Lambda",
                }
            },
        },
        "job": {
            "namespace": JOB_NAMESPACE,
            "name": job_name,
            "facets": {
                "documentation": {
                    "_producer": PRODUCER,
                    "_schemaURL": (
                        "https://openlineage.io/spec/facets/1-0-0/"
                        "DocumentationJobFacet.json"
                    ),
                    "description": JOB_DESCRIPTIONS.get(job_name, job_name),
                }
            },
        },
        "inputs": [],
        "outputs": [],
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
    }
    if input_dataset:
        event["inputs"] = [
            {"namespace": f"dynamodb://ai-ready/{tenant_id}", "name": input_dataset}
        ]
    if output_dataset:
        event["outputs"] = [
            {"namespace": f"dynamodb://ai-ready/{tenant_id}", "name": output_dataset}
        ]
    if metadata:
        event["run"]["facets"]["custom"] = metadata
    return event


def _safe_publish_metric(metric_name: str, value: float) -> None:
    try:
        publish_metric(metric_name, value)
    except Exception as exc:
        log_structured(
            "WARN",
            "CloudWatch metric publish failed in lineageRecorder",
            metric_name=metric_name,
            value=value,
            error=str(exc),
        )
