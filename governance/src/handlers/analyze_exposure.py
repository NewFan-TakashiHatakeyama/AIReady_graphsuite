"""analyzeExposure — DynamoDB Streams トリガーによるリアルタイム Oversharing 検知

詳細設計 3.1–3.8 節準拠

トリガー: AIReadyConnect-FileMetadata の DynamoDB Streams (NEW_AND_OLD_IMAGES)
出力:
  - DynamoDB ExposureFinding テーブルへの Finding upsert
  - SQS SensitivityDetectionQueue への機微検知リクエスト
  - CloudWatch Metrics / Logs
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import boto3

from services.exposure_vectors import FileMetadata, extract_exposure_vectors
from services.finding_manager import (
    close_finding_if_exists,
    handle_item_deletion,
    upsert_finding,
)
from services.guard_matcher import match_guards
from services.scoring import (
    calculate_activity_score,
    calculate_exposure_score,
    calculate_preliminary_sensitivity,
    calculate_risk_score,
    classify_risk_level,
)
from shared.config import (
    SSM_RESCAN_INTERVAL_DAYS,
    SSM_RISK_SCORE_THRESHOLD,
    get_env,
    get_ssm_float,
    get_ssm_int,
)
from shared.dynamodb import deserialize_image
from shared.logger import get_logger
from shared.metrics import emit_count, emit_duration

logger = get_logger(__name__)

SCORING_RELEVANT_FIELDS = frozenset({
    "sharing_scope",
    "permissions",
    "permissions_count",
    "sensitivity_label",
    "is_deleted",
    "size",
    "item_name",
    "modified_at",
})

_sqs_client = None


def _get_sqs_client():
    global _sqs_client
    if _sqs_client is None:
        _sqs_client = boto3.client("sqs")
    return _sqs_client


def handler(event: dict, context: Any) -> dict:
    """DynamoDB Streams イベントのエントリーポイント。

    バッチサイズ最大10レコードを順次処理する。
    個別レコードの失敗はログ出力後に例外を再送出し、
    DynamoDB Streams のリトライ → DLQ フローに委ねる。
    """
    records = event.get("Records", [])
    processed = 0
    errors = 0

    for record in records:
        try:
            process_record(record)
            processed += 1
        except Exception:
            errors += 1
            logger.error(
                "Record processing failed: %s",
                record.get("eventID", ""),
                exc_info=True,
                extra={"extra_data": {
                    "event_id": record.get("eventID", ""),
                    "event_name": record.get("eventName", ""),
                }},
            )
            raise

    logger.info(f"Batch complete: {processed} processed, {errors} errors")
    return {"processed": processed, "errors": errors}


def process_record(record: dict) -> None:
    """1 レコードの処理フロー（詳細設計 3.3 準拠）。"""
    event_name = record.get("eventName", "")
    ddb = record.get("dynamodb", {})

    if event_name == "REMOVE":
        old_image = deserialize_image(ddb.get("OldImage"))
        handle_item_deletion(old_image)
        emit_count("AIReadyGov.FindingsClosed", dimensions={"Trigger": "REMOVE"})
        return

    new_image = deserialize_image(ddb.get("NewImage"))
    old_image = deserialize_image(ddb.get("OldImage"))

    if new_image.get("is_deleted", False):
        handle_item_deletion(new_image)
        emit_count("AIReadyGov.FindingsClosed", dimensions={"Trigger": "is_deleted"})
        return

    if old_image and not is_scoring_relevant_change(new_image, old_image):
        logger.debug("変更がスコアリングに影響しない: skip")
        return

    metadata = extract_metadata(new_image)

    exposure_result = calculate_exposure_score(metadata)
    sensitivity_result = calculate_preliminary_sensitivity(metadata)
    activity_score = calculate_activity_score(metadata)
    ai_amplification = 1.0

    risk_score = calculate_risk_score(
        exposure_result.score,
        sensitivity_result.score,
        activity_score,
        ai_amplification,
    )

    threshold = _get_risk_threshold()
    if risk_score < threshold:
        close_finding_if_exists(metadata.tenant_id, metadata.item_id, metadata.source)
        return

    matched_guards = match_guards(
        exposure_vectors=exposure_result.vectors,
        source=metadata.source,
    )

    finding = upsert_finding(
        tenant_id=metadata.tenant_id,
        item=metadata,
        exposure_result=exposure_result,
        sensitivity_result=sensitivity_result,
        activity_score=activity_score,
        ai_amplification=ai_amplification,
        risk_score=risk_score,
        matched_guards=matched_guards,
    )

    if should_enqueue_sensitivity_scan(finding, old_image):
        enqueue_sensitivity_scan(finding, metadata)
        emit_count("AIReadyGov.SQSEnqueued", dimensions={
            "TenantId": metadata.tenant_id,
        })


def is_scoring_relevant_change(new_image: dict, old_image: dict) -> bool:
    """MODIFY イベントでスコアリングに影響する変更があるか判定する。"""
    for field in SCORING_RELEVANT_FIELDS:
        new_val = new_image.get(field)
        old_val = old_image.get(field)
        if new_val != old_val:
            return True
    return False


def extract_metadata(image: dict) -> FileMetadata:
    """DynamoDB Streams の NewImage から FileMetadata DTO を構築する。"""
    return FileMetadata(
        tenant_id=str(image.get("tenant_id", "")),
        item_id=str(image.get("item_id", "")),
        source=str(image.get("source", "m365")),
        container_id=str(image.get("container_id", "")),
        container_name=str(image.get("container_name", "")),
        container_type=str(image.get("container_type", "")),
        item_name=str(image.get("item_name", "")),
        web_url=str(image.get("web_url", "")),
        sharing_scope=str(image.get("sharing_scope", "specific")),
        permissions=str(image.get("permissions", "{}")),
        permissions_count=int(image.get("permissions_count", 0)),
        sensitivity_label=image.get("sensitivity_label"),
        sensitivity_label_name=image.get("sensitivity_label_name"),
        mime_type=str(image.get("mime_type", "")),
        size=int(image.get("size", 0)),
        modified_at=image.get("modified_at"),
        is_deleted=bool(image.get("is_deleted", False)),
        raw_s3_key=str(image.get("raw_s3_key", "")),
        permissions_summary=image.get("permissions_summary"),
        source_metadata=image.get("source_metadata"),
    )


def should_enqueue_sensitivity_scan(
    finding: dict[str, Any],
    old_image: dict | None,
) -> bool:
    """detectSensitivity への SQS 送信が必要か判定する（詳細設計 3.6）。"""
    if finding.get("is_new", False):
        return True

    if old_image:
        old_modified = old_image.get("modified_at")
        new_modified = finding.get("modified_at")
        if new_modified and new_modified != old_modified:
            return True

    if finding.get("sensitivity_scan_at") is None:
        return True

    scan_at_str = finding.get("sensitivity_scan_at")
    if scan_at_str:
        try:
            scan_at = datetime.fromisoformat(scan_at_str.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            days_since = (now - scan_at).days
            rescan_interval = _get_rescan_interval()
            if days_since >= rescan_interval:
                return True
        except (ValueError, TypeError):
            return True

    return False


def enqueue_sensitivity_scan(finding: dict[str, Any], metadata: FileMetadata) -> None:
    """SQS に機微検知リクエストを送信する（詳細設計 3.7）。"""
    queue_url = get_env("SENSITIVITY_QUEUE_URL")
    now = datetime.now(timezone.utc).isoformat()

    message = {
        "finding_id": finding.get("finding_id", ""),
        "tenant_id": finding.get("tenant_id", ""),
        "source": metadata.source,
        "item_id": metadata.item_id,
        "item_name": metadata.item_name,
        "mime_type": metadata.mime_type,
        "size": metadata.size,
        "raw_s3_key": metadata.raw_s3_key,
        "raw_s3_bucket": get_env("RAW_PAYLOAD_BUCKET"),
        "enqueued_at": now,
        "trigger": "realtime",
    }

    client = _get_sqs_client()
    send_kwargs: dict[str, Any] = {
        "QueueUrl": queue_url,
        "MessageBody": json.dumps(message, ensure_ascii=False),
    }
    if queue_url.endswith(".fifo"):
        send_kwargs["MessageGroupId"] = metadata.tenant_id

    client.send_message(**send_kwargs)
    logger.info(f"SQS enqueued: {finding.get('finding_id', '')}")


# ─── SSM ヘルパー ───


def _get_risk_threshold() -> float:
    try:
        return get_ssm_float(SSM_RISK_SCORE_THRESHOLD, default=2.0)
    except Exception:
        return 2.0


def _get_rescan_interval() -> int:
    try:
        return get_ssm_int(SSM_RESCAN_INTERVAL_DAYS, default=7)
    except Exception:
        return 7
