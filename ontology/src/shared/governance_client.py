"""Governance finding lookup helper."""

from __future__ import annotations

from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from src.shared.logger import log_structured


DEFAULT_GOVERNANCE_RESULT = {
    "risk_level": "none",
    "pii_detected": False,
    "ai_eligible": True,
    "finding_id": None,
    "classification": "unclassified",
}


def lookup_governance_finding(
    tenant_id: str,
    file_id: str,
    finding_table_name: str,
    *,
    dynamodb_resource: Any | None = None,
) -> dict[str, Any]:
    """
    Lookup latest governance finding by file and return safe defaults on errors.
    """
    dynamodb = dynamodb_resource or boto3.resource("dynamodb")
    table = dynamodb.Table(finding_table_name)
    try:
        response = table.query(
            KeyConditionExpression=Key("tenant_id").eq(tenant_id)
            & Key("sk").begins_with(f"FILE#{file_id}"),
            ScanIndexForward=False,
            Limit=1,
        )
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

    items = response.get("Items", [])
    if not items:
        return dict(DEFAULT_GOVERNANCE_RESULT)

    finding = items[0]
    result = dict(DEFAULT_GOVERNANCE_RESULT)
    result.update(
        {
            "risk_level": finding.get("risk_level", result["risk_level"]),
            "pii_detected": bool(finding.get("pii_detected", result["pii_detected"])),
            "finding_id": finding.get("finding_id"),
            "classification": finding.get("sensitivity_label", result["classification"]),
        }
    )
    if "ai_eligible" in finding:
        result["ai_eligible"] = bool(finding["ai_eligible"])
    elif result["risk_level"] == "critical":
        result["ai_eligible"] = False

    return result
