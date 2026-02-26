"""Finding CRUD + ステータス遷移

詳細設計 7.1–7.4 節準拠
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from services.exposure_vectors import FileMetadata
from services.scoring import ExposureResult, SensitivityResult, classify_risk_level
from shared.config import get_env
from shared.dynamodb import float_to_decimal, get_table
from shared.logger import get_logger
from shared.metrics import emit_count

logger = get_logger(__name__)

# テーブル参照（Lambda 初期化時に解決）
_finding_table = None


def _get_finding_table():
    global _finding_table
    if _finding_table is None:
        table_name = get_env("FINDING_TABLE_NAME")
        _finding_table = get_table(table_name)
    return _finding_table


def set_finding_table(table):
    """テスト用: テーブルを差し替える。"""
    global _finding_table
    _finding_table = table


# ─── 7.1 Finding の一意性 ───


def generate_finding_id(tenant_id: str, source: str, item_id: str) -> str:
    """Finding ID の生成（決定的: 同一入力 → 同一 ID）。"""
    raw = f"{tenant_id}:{source}:{item_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


# ─── 7.2 Finding upsert ロジック ───


def upsert_finding(
    tenant_id: str,
    item: FileMetadata,
    exposure_result: ExposureResult,
    sensitivity_result: SensitivityResult,
    activity_score: float,
    ai_amplification: float,
    risk_score: float,
    matched_guards: list[str],
) -> dict[str, Any]:
    """Finding の作成 or 更新。"""
    table = _get_finding_table()
    now = datetime.now(timezone.utc).isoformat()
    finding_id = generate_finding_id(tenant_id, item.source, item.item_id)

    existing = get_finding(tenant_id, finding_id)

    if existing is None:
        finding = {
            "tenant_id": tenant_id,
            "finding_id": finding_id,
            "source": item.source,
            "container_id": item.container_id,
            "container_name": item.container_name,
            "container_type": item.container_type,
            "item_id": item.item_id,
            "item_name": item.item_name,
            "item_url": item.web_url,
            "risk_score": float_to_decimal(risk_score),
            "risk_level": classify_risk_level(risk_score),
            "exposure_score": float_to_decimal(exposure_result.score),
            "sensitivity_score": float_to_decimal(sensitivity_result.score),
            "activity_score": float_to_decimal(activity_score),
            "ai_amplification": float_to_decimal(ai_amplification),
            "exposure_vectors": exposure_result.vectors,
            "sharing_scope": item.sharing_scope,
            "permissions_summary": item.permissions_summary,
            "sensitivity_label": item.sensitivity_label_name,
            "pii_detected": False,
            "pii_types": None,
            "pii_count": 0,
            "pii_density": "none",
            "secrets_detected": False,
            "secret_types": None,
            "sensitivity_scan_at": None,
            "status": "new",
            "matched_guards": matched_guards,
            "detected_at": now,
            "last_evaluated_at": now,
            "remediated_at": None,
            "suppress_until": None,
            "acknowledged_reason": None,
            "acknowledged_by": None,
            "acknowledged_at": None,
            "evidence_s3_key": None,
            "source_metadata": item.source_metadata,
        }
        finding["is_new"] = True
        table.put_item(Item=finding)
        emit_count("AIReadyGov.FindingsCreated", dimensions={"TenantId": tenant_id})
        logger.info(f"Finding created: {finding_id}")
        return finding

    # acknowledged 状態の Finding は更新しない
    if existing.get("status") == "acknowledged":
        existing["is_new"] = False
        return existing

    # sensitivity_scan_at がある場合、detectSensitivity のスコアを維持
    final_sensitivity_score = sensitivity_result.score
    final_risk_score = risk_score
    if existing.get("sensitivity_scan_at"):
        final_sensitivity_score = float(existing["sensitivity_score"])
        final_risk_score = round(
            exposure_result.score * final_sensitivity_score * activity_score * ai_amplification, 2
        )

    new_status = "open" if existing.get("status") == "new" else existing.get("status", "open")

    table.update_item(
        Key={"tenant_id": tenant_id, "finding_id": finding_id},
        UpdateExpression="""
            SET risk_score = :rs,
                risk_level = :rl,
                exposure_score = :es,
                sensitivity_score = :ss,
                activity_score = :as_score,
                ai_amplification = :ca,
                exposure_vectors = :ev,
                sharing_scope = :scope,
                permissions_summary = :ps,
                sensitivity_label = :sl,
                matched_guards = :mg,
                last_evaluated_at = :now,
                #st = :status
        """,
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={
            ":rs": float_to_decimal(final_risk_score),
            ":rl": classify_risk_level(final_risk_score),
            ":es": float_to_decimal(exposure_result.score),
            ":ss": float_to_decimal(final_sensitivity_score),
            ":as_score": float_to_decimal(activity_score),
            ":ca": float_to_decimal(ai_amplification),
            ":ev": exposure_result.vectors,
            ":scope": item.sharing_scope,
            ":ps": item.permissions_summary,
            ":sl": item.sensitivity_label_name,
            ":mg": matched_guards,
            ":now": now,
            ":status": new_status,
        },
    )
    existing["is_new"] = False
    emit_count("AIReadyGov.FindingsUpdated", dimensions={"TenantId": tenant_id})
    logger.info(f"Finding updated: {finding_id}")
    return existing


# ─── 7.4 Finding の Closed 処理 ───


def close_finding(tenant_id: str, finding_id: str) -> None:
    """Finding を Closed 状態にする。"""
    table = _get_finding_table()
    now = datetime.now(timezone.utc).isoformat()
    try:
        table.update_item(
            Key={"tenant_id": tenant_id, "finding_id": finding_id},
            UpdateExpression="SET #st = :status, last_evaluated_at = :now",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":status": "closed",
                ":now": now,
            },
            ConditionExpression="attribute_exists(finding_id)",
        )
        emit_count("AIReadyGov.FindingsClosed", dimensions={"TenantId": tenant_id})
        logger.info(f"Finding closed: {finding_id}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            pass
        else:
            raise


def handle_item_deletion(image: dict[str, Any]) -> None:
    """アイテム削除時の Finding クローズ処理。"""
    tenant_id = image.get("tenant_id", "")
    item_id = image.get("item_id", "")
    source = image.get("source", "m365")
    if not tenant_id or not item_id:
        return
    finding_id = generate_finding_id(tenant_id, source, item_id)
    close_finding(tenant_id, finding_id)


# ─── 抑制（acknowledged）の登録 ───


def acknowledge_finding(
    tenant_id: str,
    finding_id: str,
    suppress_until: str,
    reason: str,
    acknowledged_by: str,
) -> dict[str, Any]:
    """Finding を acknowledged 状態にする。"""
    table = _get_finding_table()
    now = datetime.now(timezone.utc).isoformat()

    table.update_item(
        Key={"tenant_id": tenant_id, "finding_id": finding_id},
        UpdateExpression="""
            SET #st = :status,
                suppress_until = :suppress,
                acknowledged_reason = :reason,
                acknowledged_by = :by,
                acknowledged_at = :at,
                last_evaluated_at = :now
        """,
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={
            ":status": "acknowledged",
            ":suppress": suppress_until,
            ":reason": reason,
            ":by": acknowledged_by,
            ":at": now,
            ":now": now,
        },
    )
    logger.info(f"Finding acknowledged: {finding_id}")
    return {
        "finding_id": finding_id,
        "status": "acknowledged",
        "suppress_until": suppress_until,
    }


# ─── クエリ ───


def get_finding(tenant_id: str, finding_id: str) -> dict[str, Any] | None:
    """Finding を PK/SK で取得する。"""
    table = _get_finding_table()
    response = table.get_item(Key={"tenant_id": tenant_id, "finding_id": finding_id})
    return response.get("Item")


def get_finding_by_item(tenant_id: str, item_id: str) -> dict[str, Any] | None:
    """GSI-ItemFinding 経由で item_id から Finding を逆引きする。"""
    table = _get_finding_table()
    response = table.query(
        IndexName="GSI-ItemFinding",
        KeyConditionExpression=Key("item_id").eq(item_id) & Key("tenant_id").eq(tenant_id),
        Limit=1,
    )
    items = response.get("Items", [])
    return items[0] if items else None


def query_findings_by_status(tenant_id: str, status: str) -> list[dict[str, Any]]:
    """GSI-StatusFinding 経由で status 別の Finding 一覧を取得する。"""
    table = _get_finding_table()
    response = table.query(
        IndexName="GSI-StatusFinding",
        KeyConditionExpression=Key("tenant_id").eq(tenant_id) & Key("status").eq(status),
    )
    return response.get("Items", [])


def close_finding_if_exists(tenant_id: str, item_id: str, source: str = "m365") -> None:
    """item_id に対応する Finding があれば Closed にする。"""
    finding_id = generate_finding_id(tenant_id, source, item_id)
    close_finding(tenant_id, finding_id)
