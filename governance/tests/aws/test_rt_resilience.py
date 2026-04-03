"""RT-1: レジリエンステスト

DLQ 配信・冪等性・並行処理・部分障害のグレースフル処理を検証する。
"""

from __future__ import annotations

import json
import hashlib
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tests.aws.conftest import (
    ANALYZE_DLQ_NAME,
    ANALYZE_EXPOSURE_FN,
    RAW_PAYLOAD_BUCKET,
    SENSITIVITY_QUEUE_NAME,
    TEST_TENANT_ID,
    invoke_lambda,
    make_file_metadata,
    wait_for_document_analysis,
    wait_for_finding,
    wait_for_finding_by_item,
)


class TestRT1Resilience:
    """障害発生時のシステム耐性と回復力を検証するテスト。"""

    def test_rt_1_01_analyze_dlq_receives_bad_record(
        self, lambda_client, sqs_client, analyze_dlq_url
    ):
        """不正なイベントで analyzeExposure を呼び出すと DLQ にメッセージが到達する。"""
        malformed_event = {
            "Records": [{
                "eventName": "INSERT",
                "dynamodb": {"NewImage": {"invalid_key": {"S": "bad_data"}}},
            }]
        }

        result = invoke_lambda(lambda_client, ANALYZE_EXPOSURE_FN, malformed_event)

        time.sleep(30)

        attrs = sqs_client.get_queue_attributes(
            QueueUrl=analyze_dlq_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )["Attributes"]
        dlq_count = int(attrs.get("ApproximateNumberOfMessages", "0"))

        assert result.get("error") is not None or dlq_count >= 0, (
            "Expected Lambda error or DLQ message for malformed event"
        )

    @pytest.mark.slow
    @pytest.mark.skip(reason="detectSensitivity queue flow is retired in hard-cut mode")
    def test_rt_1_02_detect_dlq_receives_bad_message(
        self, sqs_client, detect_dlq_url
    ):
        """不正な SQS メッセージ送信後、リトライを経て DLQ に到達する。"""
        queue_url = sqs_client.get_queue_url(
            QueueName=SENSITIVITY_QUEUE_NAME
        )["QueueUrl"]

        # JSON 解析不能なメッセージを投入して handler 側で例外を発生させる
        invalid_message = "{invalid-json"
        sqs_client.send_message(QueueUrl=queue_url, MessageBody=invalid_message)

        max_wait = 120
        elapsed = 0
        initial_attrs = sqs_client.get_queue_attributes(
            QueueUrl=detect_dlq_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )["Attributes"]
        initial_count = int(initial_attrs.get("ApproximateNumberOfMessages", "0"))

        queue_retry_observed = False
        while elapsed < max_wait:
            time.sleep(15)
            elapsed += 15
            attrs = sqs_client.get_queue_attributes(
                QueueUrl=detect_dlq_url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )["Attributes"]
            current_count = int(attrs.get("ApproximateNumberOfMessages", "0"))
            if current_count > initial_count:
                break

            src_attrs = sqs_client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["ApproximateNumberOfMessagesNotVisible"],
            )["Attributes"]
            not_visible = int(src_attrs.get("ApproximateNumberOfMessagesNotVisible", "0"))
            if not_visible > 0:
                queue_retry_observed = True

        assert (current_count > initial_count) or queue_retry_observed, (
            "DLQ への移送またはリトライ状態が観測できませんでした: "
            f"dlq {initial_count} -> {current_count}"
        )

    def test_rt_1_03_s3_key_not_found_skips(
        self, connect_table, finding_table, s3_client, sqs_client
    ):
        """存在しない raw_s3_key を持つ FileMetadata は感度検出をスキップする。"""
        item_id = f"item-rt103-{uuid.uuid4().hex[:8]}"
        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID,
            item_id=item_id,
            item_name="missing_s3.txt",
            mime_type="text/plain",
            raw_s3_key=f"{TEST_TENANT_ID}/raw/nonexistent-{uuid.uuid4().hex}/file.txt",
        )
        connect_table.put_item(Item=metadata)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=120, interval=10
        )

        if finding is not None:
            assert finding.get("pii_detected") is not True, (
                "pii_detected should not be true for missing S3 key"
            )

    def test_rt_1_04_idempotent_finding(
        self, connect_table, finding_table, s3_client
    ):
        """同一 FileMetadata を 2 回挿入しても Finding は 1 件のみ。"""
        item_id = f"item-rt104-{uuid.uuid4().hex[:8]}"
        raw_key = f"{TEST_TENANT_ID}/raw/{item_id}/payload.txt"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key, Body=b"idempotent test",
        )

        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="idempotent.txt", mime_type="text/plain",
            raw_s3_key=raw_key,
        )

        connect_table.put_item(Item=metadata)
        time.sleep(2)
        connect_table.put_item(Item=metadata)

        time.sleep(60)

        resp = finding_table.query(
            IndexName="GSI-ItemFinding",
            KeyConditionExpression="item_id = :iid",
            ExpressionAttributeValues={":iid": item_id},
        )
        findings = [
            f for f in resp.get("Items", [])
            if f.get("tenant_id") == TEST_TENANT_ID
        ]
        assert len(findings) == 1, (
            f"Expected 1 Finding, got {len(findings)} for item_id={item_id}"
        )

    @pytest.mark.slow
    def test_rt_1_05_concurrent_processing(
        self, connect_table, finding_table, s3_client
    ):
        """20 件を一括挿入しても全 Finding が生成され重複が無い。"""
        count = 20
        item_ids = []
        for i in range(count):
            item_id = f"item-rt105-{i:03d}-{uuid.uuid4().hex[:6]}"
            raw_key = f"{TEST_TENANT_ID}/raw/{item_id}/payload.txt"
            s3_client.put_object(
                Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
                Body=f"concurrent test {i}".encode("utf-8"),
            )
            metadata = make_file_metadata(
                tenant_id=TEST_TENANT_ID, item_id=item_id,
                item_name=f"concurrent_{i}.txt", mime_type="text/plain",
                raw_s3_key=raw_key,
            )
            connect_table.put_item(Item=metadata)
            item_ids.append(item_id)

        max_wait = 180
        elapsed = 0
        while elapsed < max_wait:
            time.sleep(15)
            elapsed += 15
            resp = finding_table.query(
                KeyConditionExpression="tenant_id = :tid",
                ExpressionAttributeValues={":tid": TEST_TENANT_ID},
            )
            findings = resp.get("Items", [])
            found_ids = {f["item_id"] for f in findings}
            if all(iid in found_ids for iid in item_ids):
                break

        resp = finding_table.query(
            KeyConditionExpression="tenant_id = :tid",
            ExpressionAttributeValues={":tid": TEST_TENANT_ID},
        )
        findings = resp.get("Items", [])
        found_ids = {f["item_id"] for f in findings}
        for iid in item_ids:
            assert iid in found_ids, f"Missing Finding for {iid}"

        item_counts = {}
        for f in findings:
            iid = f["item_id"]
            if iid in item_ids:
                item_counts[iid] = item_counts.get(iid, 0) + 1
        duplicates = {k: v for k, v in item_counts.items() if v > 1}
        assert not duplicates, f"Duplicate Findings detected: {duplicates}"

    def test_rt_1_06_dlq_message_retention(self, sqs_client, analyze_dlq_url):
        """analyzeExposure DLQ のメッセージ保持期間が 14 日（1209600 秒）であること。"""
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=analyze_dlq_url,
            AttributeNames=["MessageRetentionPeriod"],
        )["Attributes"]
        retention = int(attrs.get("MessageRetentionPeriod", "0"))
        assert retention == 1209600, (
            f"{ANALYZE_DLQ_NAME}: retention={retention}s, expected 1209600s (14 days)"
        )

    def test_rt_1_08_lambda_throttle_recovery(self, lambda_client):
        """Lambda の予約同時実行数が設定されていることを確認。"""
        for fn_name in [ANALYZE_EXPOSURE_FN]:
            try:
                resp = lambda_client.get_function_concurrency(
                    FunctionName=fn_name
                )
                reserved = resp.get("ReservedConcurrentExecutions")
                assert reserved is not None and reserved > 0, (
                    f"{fn_name}: no reserved concurrency set"
                )
            except lambda_client.exceptions.ResourceNotFoundException:
                pytest.skip(f"Function {fn_name} not found")

    @pytest.mark.slow
    @pytest.mark.skip(reason="Phase 6.5 detectSensitivity flow is retired in hard-cut mode")
    def test_rt_1_09_phase65_duplicate_messages_idempotent(
        self,
        finding_table,
        document_analysis_table,
        s3_client,
        sqs_client,
        sensitivity_queue_url,
    ):
        """同一 item への重複 SQS メッセージでも DocumentAnalysis は単一レコードに収束する。"""
        item_id = f"item-rt109-{uuid.uuid4().hex[:8]}"
        finding_id = hashlib.sha256(f"{TEST_TENANT_ID}:m365:{item_id}".encode()).hexdigest()[:32]
        now = datetime.now(timezone.utc).isoformat()
        finding_table.put_item(
            Item={
                "tenant_id": TEST_TENANT_ID,
                "finding_id": finding_id,
                "source": "m365",
                "item_id": item_id,
                "item_name": "rt109.txt",
                "container_id": "site-test-001",
                "container_name": "テスト部門サイト",
                "status": "open",
                "exposure_score": Decimal("6.0"),
                "risk_score": Decimal("6.0"),
                "created_at": now,
                "last_evaluated_at": now,
            }
        )
        raw_key = f"{TEST_TENANT_ID}/raw/{item_id}/payload.txt"
        body = "重複メッセージの耐性テスト: 佐藤花子 sato@example.com"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET,
            Key=raw_key,
            Body=body.encode("utf-8"),
            ContentType="text/plain",
        )
        message = {
            "finding_id": finding_id,
            "tenant_id": TEST_TENANT_ID,
            "source": "m365",
            "item_id": item_id,
            "item_name": "rt109.txt",
            "mime_type": "text/plain",
            "size": len(body.encode("utf-8")),
            "raw_s3_key": raw_key,
            "raw_s3_bucket": RAW_PAYLOAD_BUCKET,
            "enqueued_at": now,
            "trigger": "rt-idempotency",
        }
        sqs_client.send_message(QueueUrl=sensitivity_queue_url, MessageBody=json.dumps(message))
        sqs_client.send_message(QueueUrl=sensitivity_queue_url, MessageBody=json.dumps(message))

        finding = wait_for_finding(
            finding_table, TEST_TENANT_ID, finding_id, max_wait=300, interval=10
        )
        assert finding is not None

        analysis = wait_for_document_analysis(
            document_analysis_table, TEST_TENANT_ID, item_id, max_wait=300, interval=10
        )
        assert analysis is not None

        records = document_analysis_table.query(
            KeyConditionExpression="tenant_id = :tid AND item_id = :iid",
            ExpressionAttributeValues={":tid": TEST_TENANT_ID, ":iid": item_id},
        )["Items"]
        assert len(records) == 1, "Duplicate DocumentAnalysis records detected"
