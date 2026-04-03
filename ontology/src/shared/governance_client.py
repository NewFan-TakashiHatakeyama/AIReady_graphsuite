"""Governance Finding 参照ユーティリティ。"""

from __future__ import annotations

import hashlib
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from src.shared.logger import log_structured


DEFAULT_GOVERNANCE_RESULT = {
    "risk_level": "low",
    "pii_detected": False,
    "ai_eligible": False,
    "finding_id": None,
    "classification": "unclassified",
    "status": "closed",
    "content_signals": {},
    "content_analysis": {},
    "exposure_vectors": [],
    "matched_guards": [],
    "decision_trace": [],
}


def lookup_governance_finding(
    tenant_id: str,
    file_id: str,
    finding_table_name: str,
    *,
    dynamodb_resource: Any | None = None,
) -> dict[str, Any]:
    """Governance Finding を tenant/file 単位で検索する。

    Args:
        tenant_id: 対象テナントID。
        file_id: 入力値。
        finding_table_name: 入力値。
        dynamodb_resource: 入力値。

    Returns:
        dict[str, Any]: 処理結果の辞書。

    Notes:
        取得失敗時は安全な DEFAULT_GOVERNANCE_RESULT を返して処理継続する。
    """
    dynamodb = dynamodb_resource or boto3.resource("dynamodb")
    table = dynamodb.Table(finding_table_name)
    finding = None
    try:
        # 既定の finding_id 生成規則（tenant:source:item）で直接参照する。
        for source in ("m365", "microsoft365"):
            finding_id = _generate_finding_id(tenant_id=tenant_id, source=source, file_id=file_id)
            response = table.get_item(
                Key={
                    "tenant_id": tenant_id,
                    "finding_id": finding_id,
                }
            )
            finding = response.get("Item")
            if finding:
                break
        # 念のため、既存データ差異向けに item_id GSI でもフォールバック検索する。
        if finding is None:
            response = table.query(
                IndexName="GSI-ItemFinding",
                KeyConditionExpression=Key("item_id").eq(file_id)
                & Key("tenant_id").eq(tenant_id),
                ScanIndexForward=False,
                Limit=1,
            )
            items = response.get("Items", [])
            finding = items[0] if items else None
    except ClientError as exc:
        log_structured(
            "WARN",
            "Governance finding lookup failed",
            tenant_id=tenant_id,
            file_id=file_id,
            error=str(exc),
        )
        return dict(DEFAULT_GOVERNANCE_RESULT)
    except Exception as exc:  # pragma: no cover - defensive fallback
        log_structured(
            "WARN",
            "Governance finding lookup unexpected error",
            tenant_id=tenant_id,
            file_id=file_id,
            error=str(exc),
        )
        return dict(DEFAULT_GOVERNANCE_RESULT)

    if not finding:
        return dict(DEFAULT_GOVERNANCE_RESULT)

    result = dict(DEFAULT_GOVERNANCE_RESULT)
    rl = str(finding.get("risk_level", result["risk_level"]) or "").strip().lower()
    if rl == "none":
        rl = "low"
    result.update(
        {
            "risk_level": rl or result["risk_level"],
            "pii_detected": bool(finding.get("pii_detected", result["pii_detected"])),
            "finding_id": finding.get("finding_id"),
            "classification": finding.get("sensitivity_label", result["classification"]),
            "status": str(finding.get("status", result["status"]) or result["status"]),
            "content_signals": finding.get("content_signals") or {},
            "content_analysis": finding.get("content_analysis") or {},
            "exposure_vectors": finding.get("exposure_vectors") or [],
            "matched_guards": finding.get("matched_guards") or [],
            "decision_trace": finding.get("decision_trace") or [],
        }
    )
    if "ai_eligible" in finding:
        result["ai_eligible"] = bool(finding["ai_eligible"])
    elif result["risk_level"] == "critical":
        result["ai_eligible"] = False

    return result


def _generate_finding_id(*, tenant_id: str, source: str, file_id: str) -> str:
    raw = f"{tenant_id}:{source}:{file_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]
