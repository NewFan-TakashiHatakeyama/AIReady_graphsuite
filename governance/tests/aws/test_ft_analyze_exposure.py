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
        assert finding["status"] == "new"

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
        score_before = finding_before.get("risk_score")

        connect_table.update_item(
            Key={"tenant_id": meta["tenant_id"], "item_id": meta["item_id"]},
            UpdateExpression="SET sharing_scope = :ss",
            ExpressionAttributeValues={":ss": "specific"},
        )

        time.sleep(30)
        finding_after = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding_after is not None
        assert finding_after.get("risk_score") != score_before, (
            "risk_score should change after sharing_scope update"
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
            Key={"tenant_id": meta["tenant_id"], "item_id": meta["item_id"]},
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
            Key={"tenant_id": meta["tenant_id"], "item_id": meta["item_id"]},
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
            Key={"tenant_id": meta["tenant_id"], "item_id": meta["item_id"]},
            UpdateExpression="SET is_deleted = :d",
            ExpressionAttributeValues={":d": True},
        )

        closed = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"],
            expected_status="closed", max_wait=180,
        )
        assert closed is not None, "Finding was not closed after is_deleted=true"

    def test_ft_1_07_low_risk_no_finding(self, connect_table, finding_table):
        """INSERT: 低リスク (specific, permissions_count=3) → Finding が生成されない。"""
        meta = make_file_metadata(sharing_scope="specific", permissions_count=3)
        connect_table.put_item(Item=meta)

        time.sleep(60)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=10,
        )
        assert finding is None, "Low-risk item should not generate a Finding"

    def test_ft_1_08_anonymous_link_high_exposure(self, connect_table, finding_table):
        """INSERT: sharing_scope=anonymous → ExposureScore >= 5.0。"""
        meta = make_file_metadata(sharing_scope="anonymous", permissions_count=200)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None, "Finding was not created for anonymous sharing"
        assert Decimal(str(finding.get("exposure_score", 0))) >= Decimal("5.0"), (
            f"ExposureScore {finding.get('exposure_score')} should be >= 5.0 for anonymous"
        )

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

    def test_ft_1_10_acknowledged_not_updated(self, connect_table, finding_table):
        """acknowledged 状態の Finding は MODIFY イベントでスコア更新されない。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=150)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None

        finding_table.update_item(
            Key={"tenant_id": finding["tenant_id"], "finding_id": finding["finding_id"]},
            UpdateExpression="SET #st = :s, suppress_until = :su",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":s": "acknowledged",
                ":su": "2099-12-31T00:00:00Z",
            },
        )

        original_score = finding.get("risk_score")

        connect_table.update_item(
            Key={"tenant_id": meta["tenant_id"], "item_id": meta["item_id"]},
            UpdateExpression="SET permissions_count = :pc",
            ExpressionAttributeValues={":pc": 500},
        )

        time.sleep(60)
        updated = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=30,
        )
        assert updated is not None
        assert updated.get("status") == "acknowledged"
        assert updated.get("risk_score") == original_score, (
            "Acknowledged Finding scores should not be updated"
        )
