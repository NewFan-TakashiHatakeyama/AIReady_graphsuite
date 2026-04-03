"""lineageRecorder Lambda ハンドラ。

各処理ステップの開始/完了/失敗を OpenLineage 互換イベントとして保存する。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

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
TOKYO_TZ = ZoneInfo("Asia/Tokyo")

JOB_DESCRIPTIONS = {
    "schemaTransform": "Connect FileMetadata to UnifiedMetadata",
    "entityResolver": "Resolve entities and register gold master",
    "governanceIntegration": "Consume governance integrated analysis results",
}

_dynamodb_resource = None


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """系譜イベント要求を検証し、OpenLineage互換データとして保存する。

    Args:
        event: 系譜記録リクエスト。`lineage_id` / `tenant_id` / `job_name` /
            `event_type` などを含む辞書。
        context: Lambda 実行コンテキスト（本処理では未使用）。

    Returns:
        dict[str, Any]: 成功時は `statusCode=200` と記録ID、入力不備時は
            `statusCode=400` とエラー理由。

    Notes:
        event_type は START/COMPLETE/FAIL/ABORT のみ受け付ける。
        保存時には TTL を付与し、FAIL の場合は失敗メトリクスを追加記録する。
    """
    missing_field = _validate_required_fields(event)
    if missing_field:
        return {"statusCode": 400, "error": f"Missing required field: {missing_field}"}

    event_type = str(event["event_type"])
    if event_type not in VALID_EVENT_TYPES:
        return {"statusCode": 400, "error": f"Invalid event_type: {event_type}"}

    now_iso = datetime.now(TOKYO_TZ).isoformat()
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

    ttl_epoch = int(datetime.now(TOKYO_TZ).timestamp()) + (TTL_DAYS * 86400)
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
    """DynamoDB Resource を遅延初期化で取得する。

    Args:
        なし。

    Returns:
        Any: boto3 DynamoDB Resource。

    Notes:
        グローバルキャッシュを使い、同一コンテナでの再生成を避ける。
    """
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    return _dynamodb_resource


def _get_lineage_table() -> Any:
    """Lineage イベント保存先テーブルを取得する。

    Args:
        なし。

    Returns:
        Any: DynamoDB Table オブジェクト。

    Notes:
        `LINEAGE_EVENT_TABLE` が未設定の場合は例外を送出し、設定不備を明示する。
    """
    import os

    table_name = os.environ.get("LINEAGE_EVENT_TABLE", "")
    if not table_name:
        raise ValueError("Environment variable 'LINEAGE_EVENT_TABLE' is required")
    return _get_dynamodb_resource().Table(table_name)


def _validate_required_fields(event: dict[str, Any]) -> str | None:
    """必須フィールドの不足を検出する。

    Args:
        event: 検証対象イベント辞書。

    Returns:
        str | None: 最初に不足したフィールド名。問題なければ `None`。

    Notes:
        受信フォーマット不備を早期に弾き、下流処理での例外を減らす。
    """
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
    """OpenLineage RunEvent 互換のイベントペイロードを構築する。

    Args:
        lineage_id: 実行単位の識別子（runId）。
        tenant_id: データセット名前空間に反映するテナントID。
        job_name: ジョブ名（ファセット説明にも利用）。
        event_type: START/COMPLETE/FAIL/ABORT のイベント種別。
        event_time: イベント発生時刻（ISO8601）。
        input_dataset: 入力データセット識別子。
        output_dataset: 出力データセット識別子。
        metadata: 任意の追加実行情報（run facet の custom に格納）。

    Returns:
        dict[str, Any]: OpenLineage 仕様に沿ったイベント辞書。

    Notes:
        input/output が空文字の場合は datasets を空配列のまま保持する。
        metadata が与えられた場合のみ custom facet を付与する。
    """
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
    """CloudWatch メトリクスを安全に送信する。

    Args:
        metric_name: 送信するメトリクス名。
        value: メトリクス値。

    Returns:
        None: 戻り値なし。

    Notes:
        メトリクス送信失敗は WARN ログへ退避し、イベント記録処理は継続する。
    """
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
