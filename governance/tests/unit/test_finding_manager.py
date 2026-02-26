"""Finding マネージャの単体テスト

moto で DynamoDB をモックし、Finding CRUD + ステータス遷移をテストする。
詳細設計 7.1–7.4 節準拠。
"""

import json
from decimal import Decimal

import boto3
import pytest
from moto import mock_aws

from services.exposure_vectors import FileMetadata
from services.finding_manager import (
    _get_finding_table,
    acknowledge_finding,
    close_finding,
    close_finding_if_exists,
    generate_finding_id,
    get_finding,
    get_finding_by_item,
    handle_item_deletion,
    query_findings_by_status,
    set_finding_table,
    upsert_finding,
)
from services.scoring import ExposureResult, SensitivityResult


@pytest.fixture
def dynamodb_table():
    """moto で DynamoDB テーブルを作成し、finding_manager に注入する。"""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
        table = dynamodb.create_table(
            TableName="AIReadyGov-ExposureFinding",
            KeySchema=[
                {"AttributeName": "tenant_id", "KeyType": "HASH"},
                {"AttributeName": "finding_id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "tenant_id", "AttributeType": "S"},
                {"AttributeName": "finding_id", "AttributeType": "S"},
                {"AttributeName": "item_id", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI-ItemFinding",
                    "KeySchema": [
                        {"AttributeName": "item_id", "KeyType": "HASH"},
                        {"AttributeName": "tenant_id", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "GSI-StatusFinding",
                    "KeySchema": [
                        {"AttributeName": "tenant_id", "KeyType": "HASH"},
                        {"AttributeName": "status", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(
            TableName="AIReadyGov-ExposureFinding"
        )
        set_finding_table(table)
        yield table
        set_finding_table(None)


def _make_metadata(**kwargs) -> FileMetadata:
    defaults = {
        "tenant_id": "t-001",
        "item_id": "item-001",
        "source": "m365",
        "container_id": "site-xyz",
        "container_name": "法務部門サイト",
        "container_type": "site",
        "item_name": "契約書_A社.docx",
        "web_url": "https://contoso.sharepoint.com/contract.docx",
        "sharing_scope": "organization",
    }
    defaults.update(kwargs)
    return FileMetadata(**defaults)


def _make_exposure_result(**kwargs):
    defaults = {"score": 3.0, "vectors": ["org_link"], "details": {"org_link": 3.0}}
    defaults.update(kwargs)
    return ExposureResult(**defaults)


def _make_sensitivity_result(**kwargs):
    defaults = {"score": 1.0, "factors": [], "is_preliminary": True}
    defaults.update(kwargs)
    return SensitivityResult(**defaults)


# ─── generate_finding_id ───


class TestGenerateFindingId:
    def test_deterministic(self):
        """同一入力 → 同一 ID"""
        id1 = generate_finding_id("t-001", "m365", "item-001")
        id2 = generate_finding_id("t-001", "m365", "item-001")
        assert id1 == id2

    def test_different_input_different_id(self):
        id1 = generate_finding_id("t-001", "m365", "item-001")
        id2 = generate_finding_id("t-001", "m365", "item-002")
        assert id1 != id2

    def test_id_length(self):
        id1 = generate_finding_id("t-001", "m365", "item-001")
        assert len(id1) == 32


# ─── upsert_finding: 新規作成 ───


class TestUpsertFindingNew:
    def test_create_new_finding(self, dynamodb_table):
        """新規 Finding 作成 → status = 'new'"""
        meta = _make_metadata()
        result = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            sensitivity_result=_make_sensitivity_result(),
            activity_score=1.5,
            ai_amplification=1.0,
            risk_score=4.5,
            matched_guards=["G3"],
        )
        assert result["is_new"] is True
        assert result["status"] == "new"

        finding = get_finding("t-001", result["finding_id"])
        assert finding is not None
        assert finding["status"] == "new"
        assert finding["item_name"] == "契約書_A社.docx"
        assert finding["matched_guards"] == ["G3"]


# ─── upsert_finding: 既存更新 ───


class TestUpsertFindingUpdate:
    def test_update_existing_finding(self, dynamodb_table):
        """既存 Finding 更新 → status 'new' → 'open'"""
        meta = _make_metadata()
        first = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            sensitivity_result=_make_sensitivity_result(),
            activity_score=1.5,
            ai_amplification=1.0,
            risk_score=4.5,
            matched_guards=["G3"],
        )
        assert first["status"] == "new"

        second = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(score=5.0, vectors=["public_link"]),
            sensitivity_result=_make_sensitivity_result(score=2.0),
            activity_score=2.0,
            ai_amplification=1.0,
            risk_score=20.0,
            matched_guards=["G3"],
        )

        finding = get_finding("t-001", first["finding_id"])
        assert finding["status"] == "open"

    def test_acknowledged_finding_not_updated(self, dynamodb_table):
        """acknowledged 状態の Finding は更新されない"""
        meta = _make_metadata()
        first = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            sensitivity_result=_make_sensitivity_result(),
            activity_score=1.5,
            ai_amplification=1.0,
            risk_score=4.5,
            matched_guards=["G3"],
        )

        acknowledge_finding(
            tenant_id="t-001",
            finding_id=first["finding_id"],
            suppress_until="2099-12-31T00:00:00Z",
            reason="x" * 50,
            acknowledged_by="admin",
        )

        result = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(score=8.0),
            sensitivity_result=_make_sensitivity_result(score=5.0),
            activity_score=2.0,
            ai_amplification=1.0,
            risk_score=80.0,
            matched_guards=["G3"],
        )

        finding = get_finding("t-001", first["finding_id"])
        assert finding["status"] == "acknowledged"


# ─── close_finding ───


class TestCloseFinding:
    def test_close_existing(self, dynamodb_table):
        meta = _make_metadata()
        result = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            sensitivity_result=_make_sensitivity_result(),
            activity_score=1.5,
            ai_amplification=1.0,
            risk_score=4.5,
            matched_guards=["G3"],
        )
        close_finding("t-001", result["finding_id"])

        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "closed"

    def test_close_nonexistent_no_error(self, dynamodb_table):
        """存在しない Finding の close はエラーにならない"""
        close_finding("t-001", "nonexistent-id")


# ─── handle_item_deletion ───


class TestHandleItemDeletion:
    def test_deletion_closes_finding(self, dynamodb_table):
        meta = _make_metadata()
        result = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            sensitivity_result=_make_sensitivity_result(),
            activity_score=1.5,
            ai_amplification=1.0,
            risk_score=4.5,
            matched_guards=["G3"],
        )

        handle_item_deletion({
            "tenant_id": "t-001",
            "item_id": "item-001",
            "source": "m365",
        })

        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "closed"


# ─── acknowledge_finding ───


class TestAcknowledgeFinding:
    def test_acknowledge(self, dynamodb_table):
        meta = _make_metadata()
        result = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            sensitivity_result=_make_sensitivity_result(),
            activity_score=1.5,
            ai_amplification=1.0,
            risk_score=4.5,
            matched_guards=["G3"],
        )

        ack_result = acknowledge_finding(
            tenant_id="t-001",
            finding_id=result["finding_id"],
            suppress_until="2026-06-01T00:00:00Z",
            reason="業務上の理由で一時的に過剰共有を許容。プロジェクト終了後に権限を整理予定。",
            acknowledged_by="admin@contoso.com",
        )
        assert ack_result["status"] == "acknowledged"

        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "acknowledged"
        assert finding["suppress_until"] == "2026-06-01T00:00:00Z"
        assert finding["acknowledged_by"] == "admin@contoso.com"


# ─── query_findings_by_status ───


class TestQueryByStatus:
    def test_query_acknowledged(self, dynamodb_table):
        meta = _make_metadata()
        result = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            sensitivity_result=_make_sensitivity_result(),
            activity_score=1.5,
            ai_amplification=1.0,
            risk_score=4.5,
            matched_guards=["G3"],
        )
        acknowledge_finding(
            tenant_id="t-001",
            finding_id=result["finding_id"],
            suppress_until="2099-12-31T00:00:00Z",
            reason="x" * 50,
            acknowledged_by="admin",
        )

        findings = query_findings_by_status("t-001", "acknowledged")
        assert len(findings) == 1
        assert findings[0]["finding_id"] == result["finding_id"]


# ─── get_finding_by_item ───


class TestGetFindingByItem:
    def test_get_by_item_id(self, dynamodb_table):
        meta = _make_metadata()
        result = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            sensitivity_result=_make_sensitivity_result(),
            activity_score=1.5,
            ai_amplification=1.0,
            risk_score=4.5,
            matched_guards=["G3"],
        )

        found = get_finding_by_item("t-001", "item-001")
        assert found is not None
        assert found["finding_id"] == result["finding_id"]

    def test_not_found(self, dynamodb_table):
        found = get_finding_by_item("t-001", "nonexistent")
        assert found is None


# ─── _get_finding_table 自動解決 ───


class TestGetFindingTableAuto:
    def test_auto_resolve_from_env(self, dynamodb_table, monkeypatch):
        """_get_finding_table が環境変数から自動解決する（Lines 32-33）"""
        monkeypatch.setenv("FINDING_TABLE_NAME", "AIReadyGov-ExposureFinding")
        set_finding_table(None)
        table = _get_finding_table()
        assert table.table_name == "AIReadyGov-ExposureFinding"
        set_finding_table(dynamodb_table)


# ─── upsert: sensitivity_scan_at 既存値の維持 ───


class TestUpsertSensitivityScanAt:
    def test_preserves_sensitivity_scan_at(self, dynamodb_table):
        """sensitivity_scan_at が既にある場合、既存の sensitivity_score を維持する（Lines 127-128）"""
        meta = _make_metadata()
        first = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(score=3.0),
            sensitivity_result=_make_sensitivity_result(score=2.0),
            activity_score=1.5,
            ai_amplification=1.0,
            risk_score=9.0,
            matched_guards=["G3"],
        )
        finding_id = first["finding_id"]

        dynamodb_table.update_item(
            Key={"tenant_id": "t-001", "finding_id": finding_id},
            UpdateExpression="SET sensitivity_scan_at = :ts, sensitivity_score = :ss",
            ExpressionAttributeValues={
                ":ts": "2026-01-15T00:00:00Z",
                ":ss": Decimal("4.0"),
            },
        )

        second = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(score=5.0),
            sensitivity_result=_make_sensitivity_result(score=1.0),
            activity_score=2.0,
            ai_amplification=1.0,
            risk_score=10.0,
            matched_guards=["G3"],
        )

        finding = get_finding("t-001", finding_id)
        assert float(finding["sensitivity_score"]) == 4.0
        expected_risk = round(5.0 * 4.0 * 2.0 * 1.0, 2)
        assert float(finding["risk_score"]) == expected_risk


# ─── close_finding: ClientError 再送出 ───


class TestCloseFindingError:
    def test_non_conditional_check_error_reraises(self, dynamodb_table):
        """ConditionalCheckFailedException 以外の ClientError は再送出する（Line 198）"""
        from unittest.mock import patch, MagicMock
        from botocore.exceptions import ClientError

        error_response = {"Error": {"Code": "ValidationException", "Message": "bad request"}}
        mock_table = MagicMock()
        mock_table.update_item.side_effect = ClientError(error_response, "UpdateItem")

        set_finding_table(mock_table)
        try:
            with pytest.raises(ClientError) as exc_info:
                close_finding("t-001", "some-finding-id")
            assert exc_info.value.response["Error"]["Code"] == "ValidationException"
        finally:
            set_finding_table(dynamodb_table)


# ─── handle_item_deletion: 空値のガード ───


class TestHandleItemDeletionEdge:
    def test_empty_tenant_id_returns_early(self, dynamodb_table):
        """tenant_id が空の場合、早期リターンする（Line 207）"""
        handle_item_deletion({"tenant_id": "", "item_id": "item-001"})

    def test_empty_item_id_returns_early(self, dynamodb_table):
        """item_id が空の場合、早期リターンする（Line 207）"""
        handle_item_deletion({"tenant_id": "t-001", "item_id": ""})

    def test_missing_keys_returns_early(self, dynamodb_table):
        handle_item_deletion({})


# ─── close_finding_if_exists ───


class TestCloseFindingIfExists:
    def test_closes_existing_finding(self, dynamodb_table):
        """Finding がある場合に Closed にする（Lines 292-293）"""
        meta = _make_metadata()
        result = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            sensitivity_result=_make_sensitivity_result(),
            activity_score=1.5,
            ai_amplification=1.0,
            risk_score=4.5,
            matched_guards=["G3"],
        )

        close_finding_if_exists("t-001", "item-001", source="m365")

        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "closed"

    def test_nonexistent_no_error(self, dynamodb_table):
        """Finding が存在しなくてもエラーにならない"""
        close_finding_if_exists("t-001", "nonexistent-item")
