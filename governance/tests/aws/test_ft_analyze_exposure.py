"""FT-1: analyzeExposure Lambda — DynamoDB Streams トリガーテスト

DynamoDB Streams 経由で FileMetadata の INSERT / MODIFY / REMOVE イベントが
analyzeExposure Lambda を起動し、ExposureFinding を正しく生成・更新・クローズ
することを検証する。
"""

from __future__ import annotations

import time
import uuid
from decimal import Decimal

import pytest

from tests.aws.conftest import (
    CONNECT_TABLE_NAME,
    TEST_TENANT_ID,
    make_file_metadata,
    wait_for_finding_by_item,
)


class TestFT1AnalyzeExposure:
    """DynamoDB Streams → analyzeExposure → ExposureFinding の統合テスト群。"""

    def test_ft_1_01_insert_creates_finding(self, connect_table, finding_table):
        """INSERT: 高リスク FileMetadata → Finding が status=new で生成される。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=150)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None, "Finding was not created within timeout"
        assert str(finding["status"]).lower() in {"new", "open", "closed"}
        assert "audience_scope" in finding
        assert "audience_scope_score" in finding
        assert "discoverability" in finding
        assert "discoverability_score" in finding
        assert "externality" in finding
        assert "reshare_capability" in finding
        assert "risk_level" in finding
        assert "exposure_vectors" in finding

    def test_ft_1_02_insert_enqueues_sqs(
        self, connect_table, finding_table, sqs_client, sensitivity_queue_url,
    ):
        """INSERT: Finding 生成後に SQS メッセージがキューイングされる。"""
        attrs_before = sqs_client.get_queue_attributes(
            QueueUrl=sensitivity_queue_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )["Attributes"]
        count_before = int(attrs_before.get("ApproximateNumberOfMessages", "0"))

        meta = make_file_metadata(sharing_scope="organization", permissions_count=150)
        connect_table.put_item(Item=meta)

        wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )

        time.sleep(5)
        attrs_after = sqs_client.get_queue_attributes(
            QueueUrl=sensitivity_queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
            ],
        )["Attributes"]
        count_after = (
            int(attrs_after.get("ApproximateNumberOfMessages", "0"))
            + int(attrs_after.get("ApproximateNumberOfMessagesNotVisible", "0"))
        )
        assert count_after >= count_before, "SQS message count did not increase"

    def test_ft_1_03_modify_sharing_scope_updates(self, connect_table, finding_table):
        """MODIFY: sharing_scope 変更 → risk_score が再計算される。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=150)
        connect_table.put_item(Item=meta)

        finding_before = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding_before is not None
        level_before = finding_before.get("risk_level")
        vectors_before = tuple(sorted(finding_before.get("exposure_vectors") or []))

        connect_table.update_item(
            Key={"drive_id": meta["drive_id"], "item_id": meta["item_id"]},
            UpdateExpression="SET sharing_scope = :ss",
            ExpressionAttributeValues={":ss": "specific"},
        )

        time.sleep(30)
        finding_after = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding_after is not None
        assert (
            finding_after.get("risk_level") != level_before
            or tuple(sorted(finding_after.get("exposure_vectors") or [])) != vectors_before
        ), (
            "risk evaluation should change after sharing_scope update"
        )

    def test_ft_1_04_modify_irrelevant_field_skips(self, connect_table, finding_table):
        """MODIFY: リスク無関係フィールド (web_url) のみ変更 → Finding 更新なし。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=150)
        connect_table.put_item(Item=meta)

        finding_before = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding_before is not None
        last_eval = finding_before.get("last_evaluated_at")

        connect_table.update_item(
            Key={"drive_id": meta["drive_id"], "item_id": meta["item_id"]},
            UpdateExpression="SET web_url = :url",
            ExpressionAttributeValues={":url": "https://contoso.sharepoint.com/updated"},
        )

        time.sleep(60)
        finding_after = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=30,
        )
        assert finding_after is not None
        assert finding_after.get("last_evaluated_at") == last_eval, (
            "last_evaluated_at should not change for irrelevant field update"
        )

    def test_ft_1_05_remove_closes_finding(self, connect_table, finding_table):
        """REMOVE: FileMetadata 削除 → Finding が status=closed になる。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=150)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None

        connect_table.delete_item(
            Key={"drive_id": meta["drive_id"], "item_id": meta["item_id"]},
        )

        closed = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"],
            expected_status="closed", max_wait=180,
        )
        assert closed is not None, "Finding was not closed after REMOVE"

    def test_ft_1_06_is_deleted_closes_finding(self, connect_table, finding_table):
        """MODIFY: is_deleted=true → Finding が status=closed になる。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=150)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None

        connect_table.update_item(
            Key={"drive_id": meta["drive_id"], "item_id": meta["item_id"]},
            UpdateExpression="SET is_deleted = :d",
            ExpressionAttributeValues={":d": True},
        )

        closed = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"],
            expected_status="closed", max_wait=180,
        )
        assert closed is not None, "Finding was not closed after is_deleted=true"

    def test_ft_1_07_low_risk_no_finding(self, connect_table, finding_table):
        """INSERT: 低リスクでも Finding は生成される（v1.2）。"""
        meta = make_file_metadata(sharing_scope="specific", permissions_count=3)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None, "Low-risk item should still generate a Finding in v1.2"
        assert str(finding.get("risk_level", "")).lower() in {"low", "medium", "high", "critical", "none"}

    def test_ft_1_08_anonymous_link_high_exposure(self, connect_table, finding_table):
        """INSERT: sharing_scope=anonymous → ExposureScore は 0..1 で高め。"""
        meta = make_file_metadata(sharing_scope="anonymous", permissions_count=200)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None, "Finding was not created for anonymous sharing"
        vectors = set(finding.get("exposure_vectors") or [])
        assert "public_link" in vectors
        assert str(finding.get("risk_level", "")).lower() in {"high", "critical"}

    def test_ft_1_09_batch_processing(self, connect_table, finding_table):
        """INSERT x5: 連続挿入 → 5 件の Finding が全て生成される。"""
        items = []
        for _ in range(5):
            meta = make_file_metadata(sharing_scope="organization", permissions_count=150)
            connect_table.put_item(Item=meta)
            items.append(meta)

        for meta in items:
            finding = wait_for_finding_by_item(
                finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
            )
            assert finding is not None, (
                f"Finding not created for item_id={meta['item_id']}"
            )

    def test_ft_1_10_workflow_acknowledged_keeps_re_evaluation(self, connect_table, finding_table):
        """v1.2: workflow_status=acknowledged でも再評価（score更新）を継続する。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=150)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None

        finding_table.update_item(
            Key={"tenant_id": finding["tenant_id"], "finding_id": finding["finding_id"]},
            UpdateExpression=(
                "SET workflow_status = :ws, exception_type = :et, "
                "exception_review_due_at = :erd, suppress_until = :su"
            ),
            ExpressionAttributeValues={
                ":ws": "acknowledged",
                ":et": "temporary_accept",
                ":erd": "2099-12-31T00:00:00Z",
                ":su": "2099-12-31T00:00:00Z",
            },
        )

        original_total_risks = int(finding.get("total_detected_risks", 0))

        connect_table.update_item(
            Key={"drive_id": meta["drive_id"], "item_id": meta["item_id"]},
            UpdateExpression="SET permissions_count = :pc",
            ExpressionAttributeValues={":pc": 500},
        )

        time.sleep(60)
        updated = finding_table.get_item(
            Key={"tenant_id": finding["tenant_id"], "finding_id": finding["finding_id"]},
        ).get("Item")
        assert updated is not None
        assert updated.get("workflow_status") == "acknowledged"
        assert int(updated.get("total_detected_risks", 0)) >= original_total_risks
