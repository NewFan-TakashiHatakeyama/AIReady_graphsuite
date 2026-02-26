"""batchScoring — EventBridge 日次バッチによる全件再スコアリング + Finding 棚卸し + レポート生成

詳細設計 5.1–5.5 節準拠

トリガー: EventBridge rate(1 day) 05:00 UTC
処理:
  1. FileMetadata 全スキャン → 再スコアリング
  2. 孤立 Finding のクローズ（削除検知）
  3. 抑制期限切れ処理（acknowledged → open / closed）
  4. 未スキャンアイテムの SQS 投入
  5. 日次レポート生成 → S3
出力:
  - DynamoDB ExposureFinding テーブルの Finding upsert / close
  - SQS SensitivityDetectionQueue への再スキャンリクエスト
  - S3 日次レポート JSON
  - CloudWatch Metrics / Logs
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Generator

import boto3
from boto3.dynamodb.conditions import Attr, Key

from services.exposure_vectors import FileMetadata, extract_exposure_vectors
from services.finding_manager import (
    close_finding,
    close_finding_if_exists,
    generate_finding_id,
    get_finding,
    get_finding_by_item,
    query_findings_by_status,
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
from shared.dynamodb import float_to_decimal, get_table
from shared.logger import get_logger
from shared.metrics import emit_count, emit_duration

logger = get_logger(__name__)

SAFETY_MARGIN_MS = 60_000

_sqs_client = None
_s3_client = None
_finding_table = None
_connect_table = None


def _get_sqs_client():
    global _sqs_client
    if _sqs_client is None:
        _sqs_client = boto3.client("sqs")
    return _sqs_client


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _get_finding_table():
    global _finding_table
    if _finding_table is None:
        table_name = get_env("FINDING_TABLE_NAME")
        _finding_table = get_table(table_name)
    return _finding_table


def _get_connect_table():
    global _connect_table
    if _connect_table is None:
        table_name = get_env("CONNECT_TABLE_NAME", "AIReadyConnect-FileMetadata")
        _connect_table = get_table(table_name)
    return _connect_table


# ─── テスト用セッター ───


def set_finding_table(table):
    global _finding_table
    _finding_table = table


def set_connect_table(table):
    global _connect_table
    _connect_table = table


def set_sqs_client(client):
    global _sqs_client
    _sqs_client = client


def set_s3_client(client):
    global _s3_client
    _s3_client = client


# ─── バッチ統計 ───


@dataclass
class BatchStats:
    total_items_scanned: int = 0
    created: int = 0
    updated: int = 0
    closed: int = 0
    reopened: int = 0
    enqueued: int = 0
    errors: int = 0
    skipped: int = 0
    risk_distribution: dict[str, int] = field(
        default_factory=lambda: {"critical": 0, "high": 0, "medium": 0, "low": 0}
    )
    pii_summary: dict[str, int] = field(
        default_factory=lambda: {"files_with_pii": 0, "files_with_secrets": 0, "unscanned_files": 0}
    )
    exposure_vector_distribution: dict[str, int] = field(default_factory=dict)
    guard_match_distribution: dict[str, int] = field(default_factory=dict)
    suppression_summary: dict[str, int] = field(
        default_factory=lambda: {
            "total_acknowledged": 0,
            "expired_today": 0,
            "reopened_today": 0,
            "closed_after_expiry_today": 0,
        }
    )
    top_containers: dict[str, dict] = field(default_factory=dict)
    pii_type_counts: dict[str, int] = field(default_factory=dict)


# ─── ハンドラ ───


def handler(event: dict, context: Any) -> dict:
    """EventBridge トリガーのエントリーポイント。"""
    start_time = time.time()
    dims = {"Lambda": "batchScoring"}

    tenant_ids = _get_all_tenant_ids()
    logger.info(f"batchScoring started: {len(tenant_ids)} tenants")

    total_stats = BatchStats()
    for tenant_id in tenant_ids:
        try:
            stats = process_tenant(tenant_id, context)
            _merge_stats(total_stats, stats)
        except Exception:
            logger.error(f"Tenant processing failed: {tenant_id}", exc_info=True)
            total_stats.errors += 1

    elapsed_ms = (time.time() - start_time) * 1000
    emit_duration("AIReadyGov.BatchDurationMs", elapsed_ms, dimensions=dims)
    emit_count("AIReadyGov.BatchItemsProcessed", total_stats.total_items_scanned, dimensions=dims)

    logger.info(
        f"batchScoring complete: {total_stats.total_items_scanned} items, "
        f"{total_stats.created} created, {total_stats.updated} updated, "
        f"{total_stats.closed} closed, {total_stats.reopened} reopened, "
        f"{total_stats.errors} errors ({elapsed_ms:.0f}ms)"
    )

    return {
        "processed": total_stats.total_items_scanned,
        "created": total_stats.created,
        "updated": total_stats.updated,
        "closed": total_stats.closed,
        "errors": total_stats.errors,
    }


def process_tenant(tenant_id: str, context: Any) -> BatchStats:
    """テナント単位の処理（詳細設計 5.2）。"""
    stats = BatchStats()
    dims = {"TenantId": tenant_id, "Lambda": "batchScoring"}

    # Step 1: FileMetadata 全スキャン → 再スコアリング
    items = list(scan_file_metadata(tenant_id))
    item_ids = set()

    for item in items:
        remaining_ms = _get_remaining_ms(context)
        if remaining_ms < SAFETY_MARGIN_MS:
            logger.warning("Approaching timeout, stopping tenant processing")
            break

        try:
            process_item_batch(tenant_id, item, stats)
            item_ids.add(item.get("item_id", ""))
            stats.total_items_scanned += 1
        except Exception:
            logger.error(
                f"Item processing failed: {item.get('item_id', '')}",
                exc_info=True,
            )
            stats.errors += 1

    # Step 2: 孤立 Finding のクローズ
    close_orphaned_findings(tenant_id, item_ids, stats)

    # Step 3: 抑制期限切れの Finding を再評価
    process_expired_suppressions(tenant_id, items, stats)

    # Step 4: 未スキャンアイテムの SQS 投入
    enqueue_unscanned_items(tenant_id, stats)

    # Step 5: 日次レポート生成
    generate_daily_report(tenant_id, stats)

    # メトリクス
    if stats.created > 0:
        emit_count("AIReadyGov.FindingsCreated", stats.created, dimensions=dims)
    if stats.updated > 0:
        emit_count("AIReadyGov.FindingsUpdated", stats.updated, dimensions=dims)
    if stats.closed > 0:
        emit_count("AIReadyGov.FindingsClosed", stats.closed, dimensions=dims)
    if stats.reopened > 0:
        emit_count("AIReadyGov.FindingsReopened", stats.reopened, dimensions=dims)
    if stats.suppression_summary["expired_today"] > 0:
        emit_count(
            "AIReadyGov.SuppressionsExpired",
            stats.suppression_summary["expired_today"],
            dimensions=dims,
        )

    return stats


# ─── Step 1: 1 アイテムの再スコアリング ───


def process_item_batch(tenant_id: str, item: dict, stats: BatchStats) -> None:
    """1 アイテムの再スコアリング（詳細設計 5.2）。"""
    if item.get("is_deleted", False):
        close_finding_if_exists(tenant_id, item.get("item_id", ""), item.get("source", "m365"))
        stats.closed += 1
        return

    metadata = extract_metadata(item)
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

    existing_finding = get_finding_by_item(tenant_id, metadata.item_id)

    threshold = _get_risk_threshold()

    if risk_score >= threshold:
        # 既存 Finding がある場合、detectSensitivity のスコアを維持
        if existing_finding and existing_finding.get("sensitivity_scan_at"):
            sensitivity_result.score = float(existing_finding["sensitivity_score"])
            risk_score = calculate_risk_score(
                exposure_result.score,
                sensitivity_result.score,
                activity_score,
                ai_amplification,
            )

        matched_guards = match_guards(
            exposure_vectors=exposure_result.vectors,
            source=metadata.source,
        )

        finding = upsert_finding(
            tenant_id=tenant_id,
            item=metadata,
            exposure_result=exposure_result,
            sensitivity_result=sensitivity_result,
            activity_score=activity_score,
            ai_amplification=ai_amplification,
            risk_score=risk_score,
            matched_guards=matched_guards,
        )

        if finding.get("is_new", False):
            stats.created += 1
        else:
            stats.updated += 1

        # 統計更新
        risk_level = classify_risk_level(risk_score)
        if risk_level in stats.risk_distribution:
            stats.risk_distribution[risk_level] += 1

        for vec in exposure_result.vectors:
            stats.exposure_vector_distribution[vec] = (
                stats.exposure_vector_distribution.get(vec, 0) + 1
            )

        for guard in matched_guards:
            stats.guard_match_distribution[guard] = (
                stats.guard_match_distribution.get(guard, 0) + 1
            )

        _update_container_stats(stats, metadata, risk_score)
        _update_pii_stats(stats, existing_finding or finding)

    elif existing_finding and existing_finding.get("status") != "closed":
        close_finding(tenant_id, existing_finding["finding_id"])
        stats.closed += 1


# ─── Step 2: 孤立 Finding のクローズ ───


def close_orphaned_findings(
    tenant_id: str,
    active_item_ids: set[str],
    stats: BatchStats,
) -> None:
    """Finding テーブルにあるが FileMetadata に存在しないアイテムの Finding をクローズする。"""
    for status in ("new", "open"):
        findings = query_findings_by_status(tenant_id, status)
        for finding in findings:
            item_id = finding.get("item_id", "")
            if item_id and item_id not in active_item_ids:
                close_finding(tenant_id, finding["finding_id"])
                stats.closed += 1
                logger.info(f"Orphaned finding closed: {finding['finding_id']}")


# ─── Step 3: 抑制期限切れ処理 ───


def process_expired_suppressions(
    tenant_id: str,
    items: list[dict],
    stats: BatchStats,
) -> None:
    """acknowledged で suppress_until が期限切れの Finding を再評価する（詳細設計 5.3）。"""
    now = datetime.now(timezone.utc)
    acknowledged_findings = query_findings_by_status(tenant_id, "acknowledged")
    stats.suppression_summary["total_acknowledged"] = len(acknowledged_findings)

    items_by_id = {item.get("item_id", ""): item for item in items}

    for finding in acknowledged_findings:
        suppress_until_str = finding.get("suppress_until")
        if not suppress_until_str:
            continue

        try:
            suppress_until = datetime.fromisoformat(suppress_until_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue

        if suppress_until >= now:
            continue

        stats.suppression_summary["expired_today"] += 1

        item_id = finding.get("item_id", "")
        file_metadata = items_by_id.get(item_id)

        if file_metadata is None or file_metadata.get("is_deleted", False):
            close_finding(tenant_id, finding["finding_id"])
            stats.closed += 1
            stats.suppression_summary["closed_after_expiry_today"] += 1
            logger.info(f"Expired suppression closed (item deleted): {finding['finding_id']}")
            continue

        # 再スコアリング
        metadata = extract_metadata(file_metadata)
        exposure_result = calculate_exposure_score(metadata)
        activity_score = calculate_activity_score(metadata)

        sensitivity_score = float(finding.get("sensitivity_score", 1.0))
        ai_amplification = float(finding.get("ai_amplification", 1.0))

        new_risk_score = calculate_risk_score(
            exposure_result.score,
            sensitivity_score,
            activity_score,
            ai_amplification,
        )

        threshold = _get_risk_threshold()
        now_str = now.isoformat()
        table = _get_finding_table()

        if new_risk_score >= threshold:
            table.update_item(
                Key={"tenant_id": tenant_id, "finding_id": finding["finding_id"]},
                UpdateExpression="""
                    SET #st = :status,
                        suppress_until = :null_val,
                        risk_score = :rs,
                        risk_level = :rl,
                        exposure_score = :es,
                        activity_score = :as_score,
                        exposure_vectors = :ev,
                        last_evaluated_at = :now
                """,
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={
                    ":status": "open",
                    ":null_val": None,
                    ":rs": float_to_decimal(new_risk_score),
                    ":rl": classify_risk_level(new_risk_score),
                    ":es": float_to_decimal(exposure_result.score),
                    ":as_score": float_to_decimal(activity_score),
                    ":ev": exposure_result.vectors,
                    ":now": now_str,
                },
            )
            stats.reopened += 1
            stats.suppression_summary["reopened_today"] += 1
            logger.info(
                f"Expired suppression reopened: {finding['finding_id']}, "
                f"risk_score={new_risk_score}"
            )
        else:
            close_finding(tenant_id, finding["finding_id"])
            stats.closed += 1
            stats.suppression_summary["closed_after_expiry_today"] += 1
            logger.info(
                f"Expired suppression closed (risk resolved): {finding['finding_id']}, "
                f"risk_score={new_risk_score}"
            )


# ─── Step 4: 未スキャンアイテムの SQS 投入 ───


def enqueue_unscanned_items(tenant_id: str, stats: BatchStats) -> None:
    """sensitivity_scan_at がない / 再スキャン期限超過の Finding を SQS に投入する。"""
    now = datetime.now(timezone.utc)
    rescan_interval = _get_rescan_interval()

    for status in ("new", "open"):
        findings = query_findings_by_status(tenant_id, status)
        for finding in findings:
            should_enqueue = False

            scan_at_str = finding.get("sensitivity_scan_at")
            if scan_at_str is None:
                should_enqueue = True
                stats.pii_summary["unscanned_files"] += 1
            else:
                try:
                    scan_at = datetime.fromisoformat(scan_at_str.replace("Z", "+00:00"))
                    if (now - scan_at).days >= rescan_interval:
                        should_enqueue = True
                except (ValueError, TypeError):
                    should_enqueue = True

            if should_enqueue:
                try:
                    _enqueue_sensitivity_scan(finding, tenant_id)
                    stats.enqueued += 1
                except Exception:
                    logger.warning(
                        "SQS enqueue failed for finding %s",
                        finding.get("finding_id", ""),
                        exc_info=True,
                    )
                    stats.errors += 1


# ─── Step 5: 日次レポート生成 ───


def generate_daily_report(tenant_id: str, stats: BatchStats) -> None:
    """日次レポートを S3 に出力する（詳細設計 5.4）。"""
    now = datetime.now(timezone.utc)
    report_date = now.strftime("%Y-%m-%d")

    top_pii_types = sorted(
        [{"type": t, "count": c} for t, c in stats.pii_type_counts.items()],
        key=lambda x: x["count"],
        reverse=True,
    )[:10]

    top_containers = sorted(
        [
            {
                "container_id": cid,
                "container_name": cdata.get("name", ""),
                "finding_count": cdata.get("count", 0),
                "avg_risk_score": round(
                    cdata.get("total_risk", 0) / max(cdata.get("count", 1), 1), 2
                ),
                "max_risk_score": cdata.get("max_risk", 0),
            }
            for cid, cdata in stats.top_containers.items()
        ],
        key=lambda x: x["finding_count"],
        reverse=True,
    )[:20]

    total_findings = stats.created + stats.updated
    report = {
        "tenant_id": tenant_id,
        "report_date": report_date,
        "generated_at": now.isoformat(),
        "summary": {
            "total_items_scanned": stats.total_items_scanned,
            "total_findings": total_findings,
            "new_findings": stats.created,
            "closed_findings": stats.closed,
            "updated_findings": stats.updated,
            "errors": stats.errors,
        },
        "risk_distribution": stats.risk_distribution,
        "pii_summary": {
            "files_with_pii": stats.pii_summary["files_with_pii"],
            "files_with_secrets": stats.pii_summary["files_with_secrets"],
            "top_pii_types": top_pii_types,
            "unscanned_files": stats.pii_summary["unscanned_files"],
        },
        "top_containers": top_containers,
        "exposure_vector_distribution": stats.exposure_vector_distribution,
        "guard_match_distribution": stats.guard_match_distribution,
        "suppression_summary": stats.suppression_summary,
    }

    try:
        report_bucket = get_env("REPORT_BUCKET")
        s3_key = f"{tenant_id}/daily/{report_date}.json"
        client = _get_s3_client()
        client.put_object(
            Bucket=report_bucket,
            Key=s3_key,
            Body=json.dumps(report, ensure_ascii=False, default=_json_default),
            ContentType="application/json",
        )
        logger.info(f"Daily report uploaded: s3://{report_bucket}/{s3_key}")
    except Exception:
        logger.error(f"Failed to upload daily report for {tenant_id}", exc_info=True)
        stats.errors += 1


# ─── FileMetadata スキャン ───


def scan_file_metadata(tenant_id: str) -> Generator[dict, None, None]:
    """Connect の FileMetadata テーブルをページネーションでスキャンする。"""
    table = _get_connect_table()
    last_evaluated_key = None

    while True:
        scan_kwargs: dict[str, Any] = {
            "FilterExpression": Attr("tenant_id").eq(tenant_id),
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = table.scan(**scan_kwargs)

        for item in response.get("Items", []):
            yield item

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break


# ─── ヘルパー ───


def extract_metadata(item: dict) -> FileMetadata:
    """FileMetadata テーブルの dict から DTO を構築する。"""
    return FileMetadata(
        tenant_id=str(item.get("tenant_id", "")),
        item_id=str(item.get("item_id", "")),
        source=str(item.get("source", "m365")),
        container_id=str(item.get("container_id", "")),
        container_name=str(item.get("container_name", "")),
        container_type=str(item.get("container_type", "")),
        item_name=str(item.get("item_name", "")),
        web_url=str(item.get("web_url", "")),
        sharing_scope=str(item.get("sharing_scope", "specific")),
        permissions=str(item.get("permissions", "{}")),
        permissions_count=int(item.get("permissions_count", 0)),
        sensitivity_label=item.get("sensitivity_label"),
        sensitivity_label_name=item.get("sensitivity_label_name"),
        mime_type=str(item.get("mime_type", "")),
        size=int(item.get("size", 0)),
        modified_at=item.get("modified_at"),
        is_deleted=bool(item.get("is_deleted", False)),
        raw_s3_key=str(item.get("raw_s3_key", "")),
        permissions_summary=item.get("permissions_summary"),
        source_metadata=item.get("source_metadata"),
    )


def _get_all_tenant_ids() -> list[str]:
    """全テナント ID を取得する。

    FileMetadata テーブルからユニークな tenant_id を抽出する。
    大規模環境では SSM や DynamoDB のテナントマスタから取得する方式に変更する。
    """
    table = _get_connect_table()
    tenant_ids = set()

    last_evaluated_key = None
    while True:
        scan_kwargs: dict[str, Any] = {
            "ProjectionExpression": "tenant_id",
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = table.scan(**scan_kwargs)

        for item in response.get("Items", []):
            tid = item.get("tenant_id", "")
            if tid:
                tenant_ids.add(tid)

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    return sorted(tenant_ids)


def _enqueue_sensitivity_scan(finding: dict, tenant_id: str) -> None:
    """SQS に機微検知リクエストを投入する。"""
    queue_url = get_env("SENSITIVITY_QUEUE_URL")
    now = datetime.now(timezone.utc).isoformat()

    message = {
        "finding_id": finding.get("finding_id", ""),
        "tenant_id": tenant_id,
        "source": finding.get("source", "m365"),
        "item_id": finding.get("item_id", ""),
        "item_name": finding.get("item_name", ""),
        "mime_type": finding.get("mime_type", ""),
        "size": int(finding.get("size", 0)),
        "raw_s3_key": finding.get("raw_s3_key", ""),
        "raw_s3_bucket": get_env("RAW_PAYLOAD_BUCKET"),
        "enqueued_at": now,
        "trigger": "batch",
    }

    client = _get_sqs_client()
    send_kwargs: dict[str, Any] = {
        "QueueUrl": queue_url,
        "MessageBody": json.dumps(message, ensure_ascii=False),
    }
    if queue_url.endswith(".fifo"):
        send_kwargs["MessageGroupId"] = tenant_id

    client.send_message(**send_kwargs)


def _get_remaining_ms(context: Any) -> int:
    """Lambda context から残りミリ秒を取得する。"""
    if context is None:
        return 900_000
    try:
        return context.get_remaining_time_in_millis()
    except (AttributeError, TypeError):
        return 900_000


def _update_container_stats(stats: BatchStats, metadata: FileMetadata, risk_score: float) -> None:
    """コンテナ別統計を更新する。"""
    cid = metadata.container_id
    if not cid:
        return
    if cid not in stats.top_containers:
        stats.top_containers[cid] = {
            "name": metadata.container_name,
            "count": 0,
            "total_risk": 0.0,
            "max_risk": 0.0,
        }
    container = stats.top_containers[cid]
    container["count"] += 1
    container["total_risk"] += risk_score
    container["max_risk"] = max(container["max_risk"], risk_score)


def _update_pii_stats(stats: BatchStats, finding: dict) -> None:
    """PII 統計を更新する。"""
    if finding.get("pii_detected", False):
        stats.pii_summary["files_with_pii"] += 1
        for pii_type in finding.get("pii_types", []) or []:
            stats.pii_type_counts[pii_type] = stats.pii_type_counts.get(pii_type, 0) + 1
    if finding.get("secrets_detected", False):
        stats.pii_summary["files_with_secrets"] += 1
    if finding.get("sensitivity_scan_at") is None:
        stats.pii_summary["unscanned_files"] += 1


def _merge_stats(total: BatchStats, tenant: BatchStats) -> None:
    """テナント別統計をトータルにマージする。"""
    total.total_items_scanned += tenant.total_items_scanned
    total.created += tenant.created
    total.updated += tenant.updated
    total.closed += tenant.closed
    total.reopened += tenant.reopened
    total.enqueued += tenant.enqueued
    total.errors += tenant.errors


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


def _json_default(obj: Any) -> Any:
    """JSON シリアライズ用のデフォルトハンドラ。"""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
