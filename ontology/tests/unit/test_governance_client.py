from __future__ import annotations

from botocore.exceptions import ClientError

from src.shared.governance_client import lookup_governance_finding


class _Table:
    def __init__(self, items=None, should_raise=False):
        self.items = items or []
        self.should_raise = should_raise

    def get_item(self, **kwargs):
        if self.should_raise:
            raise ClientError({"Error": {"Code": "500", "Message": "x"}}, "GetItem")
        key = kwargs.get("Key", {})
        tenant_id = key.get("tenant_id")
        finding_id = key.get("finding_id")
        for item in self.items:
            if item.get("tenant_id") == tenant_id and item.get("finding_id") == finding_id:
                return {"Item": item}
        return {}

    def query(self, **kwargs):
        if self.should_raise:
            raise ClientError({"Error": {"Code": "500", "Message": "x"}}, "Query")
        item_id = kwargs.get("ExpressionAttributeValues", {}).get(":item_id")
        tenant_id = kwargs.get("ExpressionAttributeValues", {}).get(":tenant_id")
        if item_id and tenant_id:
            filtered = [
                item
                for item in self.items
                if item.get("item_id") == item_id and item.get("tenant_id") == tenant_id
            ]
            return {"Items": filtered}
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
                "tenant_id": "tenant-1",
                "finding_id": "c9400e9c887f9bf6283511d3632fdb73",
                "item_id": "item-1",
                "risk_level": "high",
                "pii_detected": True,
                "ai_eligible": False,
                "status": "open",
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
    assert result["status"] == "open"
    assert result["classification"] == "confidential"


def test_lookup_governance_finding_hit_via_gsi_item_lookup() -> None:
    table = _Table(
        items=[
            {
                "tenant_id": "tenant-1",
                "finding_id": "legacy-finding-id",
                "item_id": "item-1",
                "risk_level": "low",
                "pii_detected": False,
                "ai_eligible": True,
                "status": "new",
                "sensitivity_label": "internal",
            }
        ]
    )
    result = lookup_governance_finding(
        tenant_id="tenant-1",
        file_id="item-1",
        finding_table_name="tbl",
        dynamodb_resource=_Dynamo(table),
    )
    assert result["risk_level"] == "low"
    assert result["ai_eligible"] is True
    assert result["status"] == "new"


def test_lookup_governance_finding_miss_default() -> None:
    result = lookup_governance_finding(
        tenant_id="tenant-1",
        file_id="item-1",
        finding_table_name="tbl",
        dynamodb_resource=_Dynamo(_Table(items=[])),
    )
    assert result["risk_level"] == "low"
    assert result["pii_detected"] is False
    assert result["ai_eligible"] is False
    assert result["status"] == "closed"


def test_lookup_governance_finding_error_default() -> None:
    result = lookup_governance_finding(
        tenant_id="tenant-1",
        file_id="item-1",
        finding_table_name="tbl",
        dynamodb_resource=_Dynamo(_Table(should_raise=True)),
    )
    assert result["risk_level"] == "low"
    assert result["pii_detected"] is False
    assert result["ai_eligible"] is False
    assert result["status"] == "closed"
