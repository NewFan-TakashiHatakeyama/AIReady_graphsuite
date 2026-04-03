"""Finding マネージャの単体テスト（v1.2 workflow_status 対応）。

moto で DynamoDB をモックし、Finding CRUD + ステータス遷移をテストする。
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
    query_findings_by_workflow_status,
    set_finding_table,
    upsert_finding as upsert_finding_impl,
)
from services.scoring import ExposureResult


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
    defaults = {
        "score": 0.25,
        "vectors": ["org_link"],
        "details": {
            "audience_scope": "organization",
            "audience_scope_score": 0.55,
            "privilege_strength_score": 0.35,
            "permission_weighted_level": "comment",
            "permission_max_level": "edit",
            "permission_max_level_score": 0.60,
            "discoverability": "link_only",
            "discoverability_score": 0.35,
            "externality": "internal_only",
            "externality_score": 0.0,
            "reshare_capability": "limited",
            "reshare_capability_score": 0.50,
            "broken_inheritance_score": 0.0,
            "permission_outlier_score": 0.0,
        },
    }
    defaults.update(kwargs)
    return ExposureResult(**defaults)


def _risk_level_from_legacy_score(risk_score: float | None) -> str:
    score = float(risk_score or 0.0)
    if score >= 75:
        return "critical"
    if score >= 50:
        return "high"
    if score >= 25:
        return "medium"
    if score > 0:
        return "low"
    return "none"


def _upsert_finding(
    *,
    tenant_id: str,
    item: FileMetadata,
    exposure_result: ExposureResult,
    matched_guards: list[str],
    risk_level: str | None = None,
    risk_score: float | None = None,
    risk_type_counts: dict[str, int] | None = None,
    exposure_vector_counts: dict[str, int] | None = None,
    total_detected_risks: int | None = None,
    **kwargs,
):
    normalized_risk_level = str(risk_level or _risk_level_from_legacy_score(risk_score)).strip().lower()
    if total_detected_risks is None:
        total_detected_risks = {
            "critical": 8,
            "high": 5,
            "medium": 2,
            "low": 1,
        }.get(normalized_risk_level, 0)
    if exposure_vector_counts is None:
        exposure_vector_counts = {}
        for vector in exposure_result.vectors:
            key = str(vector).strip().lower()
            if not key:
                continue
            exposure_vector_counts[key] = exposure_vector_counts.get(key, 0) + 1
    if risk_type_counts is None:
        risk_type_counts = {"legacy_risk": 1} if int(total_detected_risks) > 0 else {}

    unsupported_keys = {
        "sensitivity_result",
        "activity_score",
        "ai_amplification",
        "raw_residual_risk",
        "sensitive_composite",
        "label_coverage_penalty",
        "label_accuracy_penalty",
        "secret_or_highrisk_penalty",
    }
    forwarded = {k: v for k, v in kwargs.items() if k not in unsupported_keys}
    return upsert_finding_impl(
        tenant_id=tenant_id,
        item=item,
        exposure_result=exposure_result,
        risk_level=normalized_risk_level,
        risk_type_counts=risk_type_counts,
        exposure_vector_counts=exposure_vector_counts,
        total_detected_risks=int(total_detected_risks),
        matched_guards=matched_guards,
        **forwarded,
    )


# ─── generate_finding_id ───


class TestGenerateFindingId:
    def test_deterministic(self):
        id1 = generate_finding_id("t-001", "m365", "item-001")
        id2 = generate_finding_id("t-001", "m365", "item-001")
        assert id1 == id2

    def test_different_input_different_id(self):
        id1 = generate_finding_id("t-001", "m365", "item-001")
        id2 = generate_finding_id("t-001", "m365", "item-002")
        assert id1 != id2

    def test_id_length(self):
        assert len(generate_finding_id("t-001", "m365", "item-001")) == 32


# ─── upsert_finding: 新規作成 ───


class TestUpsertFindingNew:
    def test_create_new_finding(self, dynamodb_table):
        meta = _make_metadata()
        result = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            risk_score=25.0,
            matched_guards=["G3"],
        )
        assert result["is_new"] is True
        assert result["status"] == "new"
        assert result["workflow_status"] == "new"
        assert result["exception_type"] == "none"

        finding = get_finding("t-001", result["finding_id"])
        assert finding is not None
        assert finding["item_name"] == "契約書_A社.docx"
        assert "risk_level" in finding

    def test_create_persists_composite_breakdown_fields(self, dynamodb_table):
        meta = _make_metadata()
        result = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            risk_score=42.5,
            raw_residual_risk=0.425,
            sensitive_composite=0.4123,
            label_coverage_penalty=0.25,
            label_accuracy_penalty=0.10,
            secret_or_highrisk_penalty=0.20,
            matched_guards=["G3"],
        )
        finding = get_finding("t-001", result["finding_id"])
        assert finding is not None
        assert finding["audience_scope"] == "organization"
        assert float(finding["audience_scope_score"]) == 0.55
        assert float(finding["privilege_strength_score"]) == 0.35
        assert finding["permission_weighted_level"] == "comment"
        assert finding["permission_max_level"] == "edit"
        assert float(finding["permission_max_level_score"]) == 0.60
        assert finding["discoverability"] == "link_only"
        assert float(finding["discoverability_score"]) == 0.35
        assert finding["externality"] == "internal_only"
        assert float(finding["externality_score"]) == 0.0
        assert finding["reshare_capability"] == "limited"
        assert float(finding["reshare_capability_score"]) == 0.50


# ─── upsert_finding: 既存更新 ───


class TestUpsertFindingUpdate:
    def test_update_existing_finding(self, dynamodb_table):
        meta = _make_metadata()
        first = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            risk_score=25.0,
            matched_guards=["G3"],
        )
        assert first["status"] == "new"

        second = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(score=0.6, vectors=["public_link"]),
            risk_score=60.0,
            matched_guards=["G3"],
        )

        finding = get_finding("t-001", first["finding_id"])
        assert finding["status"] == "open"

    def test_acknowledged_finding_still_updates_scores(self, dynamodb_table):
        """v1.2: workflow_status=acknowledged でもスコアは更新される。"""
        meta = _make_metadata()
        first = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            risk_score=25.0,
            matched_guards=["G3"],
        )

        acknowledge_finding(
            tenant_id="t-001",
            finding_id=first["finding_id"],
            suppress_until="2099-12-31T00:00:00Z",
            reason="x" * 50,
            acknowledged_by="admin",
        )

        result = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(score=0.8),
            risk_score=80.0,
            raw_residual_risk=0.80,
            matched_guards=["G3"],
        )

        finding = get_finding("t-001", first["finding_id"])
        # v1.2: acknowledged でもスコア更新される
        assert finding["risk_level"] == "critical"
        assert int(finding["total_detected_risks"]) == 8
        assert finding["workflow_status"] == "acknowledged"
        assert finding["exception_type"] == "temporary_accept"

    def test_closed_finding_reopens_on_upsert(self, dynamodb_table):
        meta = _make_metadata()
        created = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            risk_score=25.0,
            matched_guards=["G3"],
        )
        close_finding("t-001", created["finding_id"])

        _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(score=0.6, vectors=["public_link"]),
            risk_score=60.0,
            matched_guards=["G3"],
        )

        finding = get_finding("t-001", created["finding_id"])
        assert finding["status"] == "open"

    def test_update_overwrites_composite_breakdown_fields(self, dynamodb_table):
        meta = _make_metadata()
        created = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            risk_score=25.0,
            raw_residual_risk=0.25,
            sensitive_composite=0.20,
            label_coverage_penalty=0.0,
            label_accuracy_penalty=0.0,
            secret_or_highrisk_penalty=0.0,
            matched_guards=["G3"],
        )
        updated_exposure = _make_exposure_result(
            score=0.72,
            vectors=["public_link", "external_domain"],
            details={
                "audience_scope": "external_org",
                "audience_scope_score": 0.80,
                "privilege_strength_score": 0.75,
                "permission_weighted_level": "reshare",
                "permission_max_level": "manage",
                "permission_max_level_score": 1.00,
                "discoverability": "searchable",
                "discoverability_score": 0.70,
                "externality": "external_domain",
                "externality_score": 0.80,
                "reshare_capability": "admin",
                "reshare_capability_score": 1.00,
                "broken_inheritance_score": 1.0,
                "permission_outlier_score": 0.4,
            },
        )
        _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=updated_exposure,
            risk_score=78.0,
            raw_residual_risk=0.78,
            sensitive_composite=0.66,
            label_coverage_penalty=0.25,
            label_accuracy_penalty=0.25,
            secret_or_highrisk_penalty=0.35,
            matched_guards=["G3", "G9"],
        )
        finding = get_finding("t-001", created["finding_id"])
        assert finding is not None
        assert finding["audience_scope"] == "external_org"
        assert float(finding["audience_scope_score"]) == 0.80
        assert finding["permission_weighted_level"] == "reshare"
        assert finding["permission_max_level"] == "manage"
        assert finding["discoverability"] == "searchable"
        assert finding["externality"] == "external_domain"
        assert finding["reshare_capability"] == "admin"


# ─── close_finding ───


class TestCloseFinding:
    def test_close_existing(self, dynamodb_table):
        meta = _make_metadata()
        result = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            risk_score=25.0,
            matched_guards=["G3"],
        )
        close_finding("t-001", result["finding_id"])
        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "closed"

    def test_close_nonexistent_no_error(self, dynamodb_table):
        close_finding("t-001", "nonexistent-id")


# ─── handle_item_deletion ───


class TestHandleItemDeletion:
    def test_deletion_closes_finding(self, dynamodb_table):
        meta = _make_metadata()
        result = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            risk_score=25.0,
            matched_guards=["G3"],
        )
        handle_item_deletion({
            "tenant_id": "t-001",
            "item_id": "item-001",
            "source": "m365",
        })
        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "closed"


# ─── acknowledge_finding (v1.2 workflow_status) ───


class TestAcknowledgeFinding:
    def test_acknowledge_sets_workflow_status(self, dynamodb_table):
        meta = _make_metadata()
        result = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            risk_score=25.0,
            matched_guards=["G3"],
        )

        ack_result = acknowledge_finding(
            tenant_id="t-001",
            finding_id=result["finding_id"],
            suppress_until="2026-06-01T00:00:00Z",
            reason="業務上の理由で一時的に過剰共有を許容。プロジェクト終了後に権限を整理予定。",
            acknowledged_by="admin@contoso.com",
        )
        assert ack_result["workflow_status"] == "acknowledged"

        finding = get_finding("t-001", result["finding_id"])
        assert finding["workflow_status"] == "acknowledged"
        assert finding["exception_type"] == "temporary_accept"
        assert finding["exception_review_due_at"] == "2026-06-01T00:00:00Z"
        assert finding["suppress_until"] == "2026-06-01T00:00:00Z"


# ─── query_findings_by_workflow_status ───


class TestQueryByWorkflowStatus:
    def test_query_acknowledged(self, dynamodb_table):
        meta = _make_metadata()
        result = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            risk_score=25.0,
            matched_guards=["G3"],
        )
        acknowledge_finding(
            tenant_id="t-001",
            finding_id=result["finding_id"],
            suppress_until="2099-12-31T00:00:00Z",
            reason="x" * 50,
            acknowledged_by="admin",
        )

        findings = query_findings_by_workflow_status("t-001", "acknowledged")
        assert len(findings) == 1
        assert findings[0]["finding_id"] == result["finding_id"]


# ─── get_finding_by_item ───


class TestGetFindingByItem:
    def test_get_by_item_id(self, dynamodb_table):
        meta = _make_metadata()
        result = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            risk_score=25.0,
            matched_guards=["G3"],
        )
        found = get_finding_by_item("t-001", "item-001")
        assert found is not None
        assert found["finding_id"] == result["finding_id"]

    def test_not_found(self, dynamodb_table):
        assert get_finding_by_item("t-001", "nonexistent") is None


# ─── close_finding_if_exists ───


class TestCloseFindingIfExists:
    def test_closes_existing_finding(self, dynamodb_table):
        meta = _make_metadata()
        result = _upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            risk_score=25.0,
            matched_guards=["G3"],
        )
        close_finding_if_exists("t-001", "item-001", source="m365")
        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "closed"

    def test_nonexistent_no_error(self, dynamodb_table):
        close_finding_if_exists("t-001", "nonexistent-item")
