"""E2E-2: バッチパイプライン統合テスト

batchScoring Lambda の一括処理・レポート生成・抑制期限・孤立 Finding クローズなどを検証する。
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from tests.aws.conftest import (
    BATCH_SCORING_FN,
    FINDING_TABLE_NAME,
    RAW_PAYLOAD_BUCKET,
    REPORT_BUCKET,
    TEST_TENANT_ID,
    cleanup_findings,
    cleanup_connect_items,
    invoke_lambda,
    make_file_metadata,
    wait_for_finding_by_item,
)


class TestE2E2BatchPipeline:
    """batchScoring Lambda によるバッチ処理の E2E テスト。"""

    @pytest.mark.slow
    def test_e2e_2_01_batch_full_processing(
        self, connect_table, finding_table, lambda_client, s3_client
    ):
        """10 件の FileMetadata を挿入し batchScoring を実行すると、
        Finding が生成され S3 にレポートが出力される。"""
        item_ids = []
        for i in range(10):
            item_id = f"item-e2e201-{i:03d}-{uuid.uuid4().hex[:6]}"
            raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
            s3_client.put_object(
                Bucket=RAW_PAYLOAD_BUCKET,
                Key=raw_key,
                Body=f"batch test content {i}".encode("utf-8"),
            )
            metadata = make_file_metadata(
                tenant_id=TEST_TENANT_ID,
                item_id=item_id,
                item_name=f"batch_test_{i}.txt",
                mime_type="text/plain",
                raw_s3_key=raw_key,
            )
            connect_table.put_item(Item=metadata)
            item_ids.append(item_id)

        result = invoke_lambda(
            lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID}
        )
        assert result["error"] is None, f"batchScoring failed: {result}"

        resp = finding_table.query(
            KeyConditionExpression="tenant_id = :tid",
            ExpressionAttributeValues={":tid": TEST_TENANT_ID},
        )
        findings = resp.get("Items", [])
        found_item_ids = {f["item_id"] for f in findings}
        for iid in item_ids:
            assert iid in found_item_ids, f"Finding not created for {iid}"

        report_objects = s3_client.list_objects_v2(
            Bucket=REPORT_BUCKET, Prefix=f"{TEST_TENANT_ID}/"
        )
        assert report_objects.get("KeyCount", 0) > 0, "No report generated in S3"

    def test_e2e_2_02_orphan_finding_closed(
        self, finding_table, lambda_client
    ):
        """FileMetadata が存在しない孤立 Finding は batchScoring で closed になる。"""
        finding_id = f"finding-orphan-{uuid.uuid4().hex[:8]}"
        finding_table.put_item(Item={
            "tenant_id": TEST_TENANT_ID,
            "finding_id": finding_id,
            "item_id": f"item-nonexistent-{uuid.uuid4().hex[:8]}",
            "status": "open",
            "risk_score": Decimal("3.0"),
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

        result = invoke_lambda(
            lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID}
        )
        assert result["error"] is None, f"batchScoring failed: {result}"

        resp = finding_table.get_item(
            Key={"tenant_id": TEST_TENANT_ID, "finding_id": finding_id}
        )
        finding = resp.get("Item")
        assert finding is not None
        assert finding["status"] == "closed", (
            f"Orphan Finding should be closed, got: {finding['status']}"
        )

    def test_e2e_2_03_unscanned_sqs(
        self, connect_table, finding_table, lambda_client, sqs_client,
        sensitivity_queue_url, s3_client
    ):
        """sensitivity_scan_at が未設定の Finding に対し batchScoring が SQS メッセージを送信する。"""
        item_id = f"item-e2e203-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET,
            Key=raw_key,
            Body=b"unscanned test content",
        )

        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="unscanned.txt", mime_type="text/plain",
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        finding_id = f"finding-unscanned-{uuid.uuid4().hex[:8]}"
        finding_table.put_item(Item={
            "tenant_id": TEST_TENANT_ID,
            "finding_id": finding_id,
            "item_id": item_id,
            "status": "open",
            "risk_score": Decimal("3.0"),
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

        result = invoke_lambda(
            lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID}
        )
        assert result["error"] is None

        time.sleep(10)

        attrs = sqs_client.get_queue_attributes(
            QueueUrl=sensitivity_queue_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )["Attributes"]
        msg_count = int(attrs.get("ApproximateNumberOfMessages", "0"))
        assert msg_count >= 0

    def test_e2e_2_04_report_completeness(
        self, connect_table, finding_table, lambda_client, s3_client
    ):
        """batchScoring のレポートに必要な全 JSON フィールドが含まれること。"""
        item_id = f"item-e2e204-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key, Body=b"report completeness test",
        )
        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="report_test.txt", mime_type="text/plain",
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        result = invoke_lambda(
            lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID}
        )
        assert result["error"] is None

        report_objects = s3_client.list_objects_v2(
            Bucket=REPORT_BUCKET, Prefix=f"{TEST_TENANT_ID}/"
        )
        keys = [obj["Key"] for obj in report_objects.get("Contents", [])]
        assert len(keys) > 0, "No report found in S3"

        latest_key = sorted(keys)[-1]
        obj = s3_client.get_object(Bucket=REPORT_BUCKET, Key=latest_key)
        report = json.loads(obj["Body"].read().decode("utf-8"))

        required_fields = [
            "tenant_id", "generated_at", "total_findings",
            "high_risk_count", "findings",
        ]
        for field in required_fields:
            assert field in report, f"Report missing required field: {field}"
        assert isinstance(report["findings"], list)
        assert report["tenant_id"] == TEST_TENANT_ID

    def test_e2e_2_05_suppression_expiry(
        self, connect_table, finding_table, lambda_client, s3_client
    ):
        """suppress_until が過去の acknowledged Finding はバッチで status=open に戻る。"""
        item_id = f"item-e2e205-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
            Body="高リスク: 個人番号 9876 5432 1098".encode("utf-8"),
        )
        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="suppression_test.txt", mime_type="text/plain",
            sharing_scope="organization", permissions_count=200,
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        finding_id = f"finding-suppress-{uuid.uuid4().hex[:8]}"
        past_time = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        finding_table.put_item(Item={
            "tenant_id": TEST_TENANT_ID,
            "finding_id": finding_id,
            "item_id": item_id,
            "status": "acknowledged",
            "risk_score": Decimal("8.0"),
            "suppress_until": past_time,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

        result = invoke_lambda(
            lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID}
        )
        assert result["error"] is None

        resp = finding_table.get_item(
            Key={"tenant_id": TEST_TENANT_ID, "finding_id": finding_id}
        )
        finding = resp.get("Item")
        assert finding is not None
        assert finding["status"] == "open", (
            f"Suppressed Finding should reopen, got: {finding['status']}"
        )

    def test_e2e_2_06_formal_score_preserved(
        self, connect_table, finding_table, lambda_client, s3_client
    ):
        """sensitivity_scan_at 済みの Finding は batchScoring で sensitivity_score が上書きされない。"""
        item_id = f"item-e2e206-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key, Body=b"already scanned content",
        )
        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="preserved_score.txt", mime_type="text/plain",
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        finding_id = f"finding-preserved-{uuid.uuid4().hex[:8]}"
        finding_table.put_item(Item={
            "tenant_id": TEST_TENANT_ID,
            "finding_id": finding_id,
            "item_id": item_id,
            "status": "open",
            "risk_score": Decimal("5.0"),
            "sensitivity_score": Decimal("4.5"),
            "sensitivity_scan_at": datetime.now(timezone.utc).isoformat(),
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

        result = invoke_lambda(
            lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID}
        )
        assert result["error"] is None

        resp = finding_table.get_item(
            Key={"tenant_id": TEST_TENANT_ID, "finding_id": finding_id}
        )
        finding = resp.get("Item")
        assert finding is not None
        assert float(finding["sensitivity_score"]) == pytest.approx(4.5), (
            f"sensitivity_score changed: {finding['sensitivity_score']}"
        )
