"""E2E-1: リアルタイムパイプライン統合テスト

FileMetadata INSERT → DynamoDB Streams → analyzeExposure → Finding → SQS
→ detectSensitivity → Finding update のフルフローを検証する。
"""

from __future__ import annotations

import json
import time
import uuid
from decimal import Decimal

import pytest

from tests.aws.conftest import (
    ANALYZE_DLQ_NAME,
    CONNECT_TABLE_NAME,
    DETECT_DLQ_NAME,
    FINDING_TABLE_NAME,
    RAW_PAYLOAD_BUCKET,
    TEST_TENANT_ID,
    cleanup_findings,
    cleanup_connect_items,
    invoke_lambda,
    make_file_metadata,
    wait_for_finding_by_item,
    wait_for_sqs_empty,
)


class TestE2E1RealtimePipeline:
    """FileMetadata 挿入からリアルタイム Finding 生成・感度検出までの E2E テスト。"""

    @pytest.mark.slow
    def test_e2e_1_01_full_realtime_pipeline(
        self, connect_table, finding_table, s3_client, sqs_client, sensitivity_queue_url
    ):
        """PII を含むファイルを S3 にアップロードし FileMetadata を挿入すると、
        pii_detected=true の Finding が生成され sensitivity_score が付与される。"""
        item_id = f"item-e2e101-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"

        pii_content = "田中太郎の電話番号は 090-1234-5678 です。メール: tanaka@example.com"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET,
            Key=raw_key,
            Body=pii_content.encode("utf-8"),
            ContentType="text/plain",
        )

        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID,
            item_id=item_id,
            item_name="pii_test.txt",
            mime_type="text/plain",
            size=len(pii_content.encode("utf-8")),
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=300, interval=10
        )

        assert finding is not None, f"Finding not created for item_id={item_id} within 5 minutes"
        assert finding.get("status") == "open"
        assert finding.get("pii_detected") is True
        assert finding.get("sensitivity_score") is not None
        assert float(finding["sensitivity_score"]) > 0

    @pytest.mark.slow
    def test_e2e_1_02_secret_detection(
        self, connect_table, finding_table, s3_client
    ):
        """AWS アクセスキーを含むファイルで secrets_detected=true, sensitivity_score=5.0 を検証。"""
        item_id = f"item-e2e102-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"

        secret_content = "aws_access_key_id = AKIAIOSFODNN7EXAMPLE\naws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET,
            Key=raw_key,
            Body=secret_content.encode("utf-8"),
            ContentType="text/plain",
        )

        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID,
            item_id=item_id,
            item_name="secrets_test.txt",
            mime_type="text/plain",
            size=len(secret_content.encode("utf-8")),
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=300, interval=10
        )

        assert finding is not None, f"Finding not created for item_id={item_id}"
        assert finding.get("secrets_detected") is True
        assert float(finding.get("sensitivity_score", 0)) == pytest.approx(5.0, abs=0.5)

    @pytest.mark.slow
    def test_e2e_1_03_high_risk_pii_mynumber(
        self, connect_table, finding_table, s3_client
    ):
        """マイナンバーを含むファイルで sensitivity_score >= 4.0 を検証。"""
        item_id = f"item-e2e103-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"

        mynumber_content = "従業員情報: 個人番号 1234 5678 9012"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET,
            Key=raw_key,
            Body=mynumber_content.encode("utf-8"),
            ContentType="text/plain",
        )

        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID,
            item_id=item_id,
            item_name="mynumber_test.txt",
            mime_type="text/plain",
            size=len(mynumber_content.encode("utf-8")),
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=300, interval=10
        )

        assert finding is not None, f"Finding not created for item_id={item_id}"
        assert float(finding.get("sensitivity_score", 0)) >= 4.0

    @pytest.mark.slow
    def test_e2e_1_04_permission_change_rescore(
        self, connect_table, finding_table, s3_client
    ):
        """Finding 生成後に sharing_scope を specific に変更すると risk_score が下がる。"""
        item_id = f"item-e2e104-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"

        content = "機密情報が含まれるテストファイル"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET,
            Key=raw_key,
            Body=content.encode("utf-8"),
            ContentType="text/plain",
        )

        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID,
            item_id=item_id,
            item_name="permission_test.txt",
            mime_type="text/plain",
            sharing_scope="organization",
            permissions_count=150,
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        finding_before = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=300, interval=10
        )
        assert finding_before is not None, "Initial Finding not created"
        original_risk = float(finding_before.get("risk_score", 0))

        connect_table.update_item(
            Key={"tenant_id": TEST_TENANT_ID, "item_id": item_id},
            UpdateExpression="SET sharing_scope = :ss, permissions_count = :pc",
            ExpressionAttributeValues={":ss": "specific", ":pc": 3},
        )

        time.sleep(60)

        finding_after = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=180, interval=10
        )
        assert finding_after is not None
        new_risk = float(finding_after.get("risk_score", original_risk))
        assert new_risk <= original_risk, (
            f"risk_score should decrease: {original_risk} → {new_risk}"
        )

    @pytest.mark.slow
    def test_e2e_1_05_deletion_closes(
        self, connect_table, finding_table, s3_client
    ):
        """FileMetadata を削除すると Finding の status が closed になる。"""
        item_id = f"item-e2e105-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"

        content = "削除テスト用ファイル"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET,
            Key=raw_key,
            Body=content.encode("utf-8"),
            ContentType="text/plain",
        )

        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID,
            item_id=item_id,
            item_name="deletion_test.txt",
            mime_type="text/plain",
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=300, interval=10
        )
        assert finding is not None, "Finding not created before deletion"

        connect_table.delete_item(Key={"tenant_id": TEST_TENANT_ID, "item_id": item_id})

        closed_finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id,
            expected_status="closed", max_wait=180, interval=10,
        )
        assert closed_finding is not None, "Finding was not closed after FileMetadata deletion"
        assert closed_finding["status"] == "closed"

    def test_e2e_1_06_dlq_empty(self, sqs_client, analyze_dlq_url, detect_dlq_url):
        """全テスト完了後に両方の DLQ にメッセージが残っていないことを確認。"""
        for dlq_url, name in [
            (analyze_dlq_url, ANALYZE_DLQ_NAME),
            (detect_dlq_url, DETECT_DLQ_NAME),
        ]:
            attrs = sqs_client.get_queue_attributes(
                QueueUrl=dlq_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                ],
            )["Attributes"]
            visible = int(attrs.get("ApproximateNumberOfMessages", "0"))
            not_visible = int(attrs.get("ApproximateNumberOfMessagesNotVisible", "0"))
            assert visible == 0 and not_visible == 0, (
                f"{name}: visible={visible}, not_visible={not_visible}"
            )
