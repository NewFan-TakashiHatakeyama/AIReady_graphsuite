"""FT-3: batchScoring Lambda — バッチスコアリングテスト

batchScoring Lambda を直接 invoke し、全件再スコアリング・orphan クローズ・
サプレッション期限管理・レポート生成を検証する。
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from tests.aws.conftest import (
    BATCH_SCORING_FN,
    CONNECT_TABLE_NAME,
    FINDING_TABLE_NAME,
    RAW_PAYLOAD_BUCKET,
    REPORT_BUCKET,
    TEST_TENANT_ID,
    invoke_lambda,
    make_file_metadata,
    wait_for_finding,
    wait_for_finding_by_item,
)


def _generate_finding_id(tenant_id: str, source: str, item_id: str) -> str:
    return hashlib.sha256(f"{tenant_id}:{source}:{item_id}".encode()).hexdigest()[:32]


def _create_finding(finding_table, tenant_id: str, item_id: str, **overrides) -> dict:
    """テスト用 Finding を直接作成する。"""
    finding_id = _generate_finding_id(tenant_id, "m365", item_id)
    now = datetime.now(timezone.utc).isoformat()
    item = {
        "tenant_id": tenant_id,
        "finding_id": finding_id,
        "source": "m365",
        "item_id": item_id,
        "item_name": "test-file.txt",
        "container_id": "site-test-001",
        "container_name": "テスト部門サイト",
        "status": "new",
        "exposure_score": Decimal("6.0"),
        "risk_score": Decimal("6.0"),
        "created_at": now,
        "last_evaluated_at": now,
    }
    item.update(overrides)
    finding_table.put_item(Item=item)
    return item


class TestFT3BatchScoring:
    """batchScoring Lambda の統合テスト群。"""

    def test_ft_3_01_full_rescore(self, connect_table, finding_table, lambda_client):
        """10 件の FileMetadata → batchScoring → processed >= 10, errors=0。"""
        for _ in range(10):
            meta = make_file_metadata(sharing_scope="organization", permissions_count=100)
            connect_table.put_item(Item=meta)

        result = invoke_lambda(
            lambda_client, BATCH_SCORING_FN,
            {"tenant_id": TEST_TENANT_ID},
        )
        assert result["error"] is None, f"Lambda error: {result['body']}"
        body = result["body"]
        assert body.get("processed", 0) >= 10
        assert body.get("errors", -1) == 0

    def test_ft_3_02_finding_generation(self, connect_table, finding_table, lambda_client):
        """5 件の高リスク FileMetadata → batchScoring → 5 件の Finding 生成。"""
        items = []
        for _ in range(5):
            meta = make_file_metadata(
                sharing_scope="organization", permissions_count=200,
            )
            connect_table.put_item(Item=meta)
            items.append(meta)

        invoke_lambda(lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID})

        for meta in items:
            fid = _generate_finding_id(TEST_TENANT_ID, "m365", meta["item_id"])
            resp = finding_table.get_item(
                Key={"tenant_id": TEST_TENANT_ID, "finding_id": fid},
            )
            assert resp.get("Item") is not None, (
                f"Finding not created for item_id={meta['item_id']}"
            )

    def test_ft_3_03_orphan_finding_closed(self, finding_table, lambda_client):
        """FileMetadata なしの orphan Finding → batchScoring → status=closed。"""
        orphan_item_id = f"item-orphan-{uuid.uuid4().hex[:12]}"
        finding = _create_finding(finding_table, TEST_TENANT_ID, orphan_item_id)

        invoke_lambda(lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID})

        resp = finding_table.get_item(
            Key={"tenant_id": TEST_TENANT_ID, "finding_id": finding["finding_id"]},
        )
        item = resp.get("Item")
        assert item is not None
        assert item["status"] == "closed", "Orphan Finding should be closed"

    def test_ft_3_04_suppression_expired_opens(
        self, connect_table, finding_table, lambda_client,
    ):
        """suppress_until 過去 + 高リスク FileMetadata → batchScoring → status=open。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=200)
        connect_table.put_item(Item=meta)

        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        finding = _create_finding(
            finding_table, TEST_TENANT_ID, meta["item_id"],
            status="acknowledged", suppress_until=past,
        )

        invoke_lambda(lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID})

        resp = finding_table.get_item(
            Key={"tenant_id": TEST_TENANT_ID, "finding_id": finding["finding_id"]},
        )
        item = resp.get("Item")
        assert item is not None
        assert item["status"] == "open", (
            "Expired suppression with high-risk metadata should reopen Finding"
        )

    def test_ft_3_05_suppression_expired_closes(self, finding_table, lambda_client):
        """suppress_until 過去 + FileMetadata なし → batchScoring → status=closed。"""
        orphan_item_id = f"item-expired-{uuid.uuid4().hex[:12]}"
        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        finding = _create_finding(
            finding_table, TEST_TENANT_ID, orphan_item_id,
            status="acknowledged", suppress_until=past,
        )

        invoke_lambda(lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID})

        resp = finding_table.get_item(
            Key={"tenant_id": TEST_TENANT_ID, "finding_id": finding["finding_id"]},
        )
        item = resp.get("Item")
        assert item is not None
        assert item["status"] == "closed", (
            "Expired suppression without FileMetadata should close Finding"
        )

    def test_ft_3_06_acknowledged_not_expired_skipped(
        self, connect_table, finding_table, lambda_client,
    ):
        """suppress_until 未来の acknowledged → batchScoring → still acknowledged。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=200)
        connect_table.put_item(Item=meta)

        future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        finding = _create_finding(
            finding_table, TEST_TENANT_ID, meta["item_id"],
            status="acknowledged", suppress_until=future,
        )

        invoke_lambda(lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID})

        resp = finding_table.get_item(
            Key={"tenant_id": TEST_TENANT_ID, "finding_id": finding["finding_id"]},
        )
        item = resp.get("Item")
        assert item is not None
        assert item["status"] == "acknowledged", (
            "Non-expired suppression should remain acknowledged"
        )

    def test_ft_3_07_unscanned_enqueued(
        self, connect_table, finding_table, lambda_client, sqs_client, sensitivity_queue_url,
    ):
        """sensitivity_scan_at=None → batchScoring → SQS にエンキューされる。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=200)
        connect_table.put_item(Item=meta)
        _create_finding(
            finding_table, TEST_TENANT_ID, meta["item_id"],
            sensitivity_scan_at=None,
        )

        attrs_before = sqs_client.get_queue_attributes(
            QueueUrl=sensitivity_queue_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )["Attributes"]
        count_before = int(attrs_before.get("ApproximateNumberOfMessages", "0"))

        invoke_lambda(lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID})

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
        assert count_after > count_before, (
            "Unscanned Finding should be enqueued to SQS"
        )

    def test_ft_3_08_daily_report_s3(
        self, connect_table, finding_table, lambda_client, s3_client,
    ):
        """batchScoring → S3 にデイリーレポートが出力される。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=100)
        connect_table.put_item(Item=meta)

        invoke_lambda(lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID})

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"reports/{TEST_TENANT_ID}/daily/{today}.json"

        resp = s3_client.list_objects_v2(Bucket=REPORT_BUCKET, Prefix=key)
        contents = resp.get("Contents", [])
        assert len(contents) > 0, f"Daily report not found at s3://{REPORT_BUCKET}/{key}"

    def test_ft_3_09_report_structure(
        self, connect_table, finding_table, lambda_client, s3_client,
    ):
        """デイリーレポートの JSON 構造検証。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=100)
        connect_table.put_item(Item=meta)

        invoke_lambda(lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID})

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"reports/{TEST_TENANT_ID}/daily/{today}.json"

        obj = s3_client.get_object(Bucket=REPORT_BUCKET, Key=key)
        report = json.loads(obj["Body"].read().decode("utf-8"))

        required_fields = [
            "summary",
            "risk_distribution",
            "pii_summary",
            "top_containers",
            "exposure_vector_distribution",
            "guard_match_distribution",
            "suppression_summary",
        ]
        for field in required_fields:
            assert field in report, f"Report missing required field: {field}"

    def test_ft_3_10_deleted_item_closes_finding(
        self, connect_table, finding_table, lambda_client,
    ):
        """is_deleted=true の FileMetadata + 既存 Finding → batchScoring → closed。"""
        meta = make_file_metadata(
            sharing_scope="organization", permissions_count=200, is_deleted=True,
        )
        connect_table.put_item(Item=meta)
        finding = _create_finding(finding_table, TEST_TENANT_ID, meta["item_id"])

        invoke_lambda(lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID})

        resp = finding_table.get_item(
            Key={"tenant_id": TEST_TENANT_ID, "finding_id": finding["finding_id"]},
        )
        item = resp.get("Item")
        assert item is not None
        assert item["status"] == "closed", (
            "Finding for is_deleted=true item should be closed"
        )
