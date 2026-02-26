from __future__ import annotations

from botocore.exceptions import ClientError

from src.shared.governance_client import lookup_governance_finding


class _Table:
    def __init__(self, items=None, should_raise=False):
        self.items = items or []
        self.should_raise = should_raise

    def query(self, **kwargs):
        if self.should_raise:
            raise ClientError({"Error": {"Code": "500", "Message": "x"}}, "Query")
        return {"Items": self.items}


class _Dynamo:
    def __init__(self, table):
        self._table = table

    def Table(self, table_name):
        return self._table


def test_lookup_governance_finding_hit() -> None:
    table = _Table(
        items=[
            {
                "risk_level": "high",
                "pii_detected": True,
                "ai_eligible": False,
                "finding_id": "f-1",
                "sensitivity_label": "confidential",
            }
        ]
    )
    result = lookup_governance_finding(
        tenant_id="tenant-1",
        file_id="item-1",
        finding_table_name="tbl",
        dynamodb_resource=_Dynamo(table),
    )
    assert result["risk_level"] == "high"
    assert result["pii_detected"] is True
    assert result["ai_eligible"] is False
    assert result["classification"] == "confidential"


def test_lookup_governance_finding_miss_default() -> None:
    result = lookup_governance_finding(
        tenant_id="tenant-1",
        file_id="item-1",
        finding_table_name="tbl",
        dynamodb_resource=_Dynamo(_Table(items=[])),
    )
    assert result["risk_level"] == "none"
    assert result["pii_detected"] is False
    assert result["ai_eligible"] is True


def test_lookup_governance_finding_error_default() -> None:
    result = lookup_governance_finding(
        tenant_id="tenant-1",
        file_id="item-1",
        finding_table_name="tbl",
        dynamodb_resource=_Dynamo(_Table(should_raise=True)),
    )
    assert result["risk_level"] == "none"
    assert result["pii_detected"] is False
