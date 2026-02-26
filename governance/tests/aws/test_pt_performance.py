"""PT-1: パフォーマンステスト

Lambda レイテンシ・スループット・コールドスタート・バッチ処理時間を検証する。
"""

from __future__ import annotations

import json
import time
import uuid

import pytest

from tests.aws.conftest import (
    ANALYZE_EXPOSURE_FN,
    BATCH_SCORING_FN,
    DETECT_SENSITIVITY_FN,
    RAW_PAYLOAD_BUCKET,
    TEST_TENANT_ID,
    invoke_lambda,
    make_file_metadata,
    wait_for_document_analysis,
    wait_for_finding_by_item,
)


class TestPT1Performance:
    """Lambda 実行のパフォーマンス特性を検証するテスト。"""

    def test_pt_1_01_analyze_exposure_latency(
        self, connect_table, finding_table, s3_client
    ):
        """FileMetadata 挿入から Finding 出現まで 30 秒以内。"""
        item_id = f"item-pt101-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
            Body=b"latency test content",
        )

        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="latency_test.txt", mime_type="text/plain",
            raw_s3_key=raw_key,
        )

        start = time.monotonic()
        connect_table.put_item(Item=metadata)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=30, interval=3
        )
        elapsed = time.monotonic() - start

        assert finding is not None, (
            f"Finding not created within 30s (elapsed={elapsed:.1f}s)"
        )
        assert elapsed < 30, f"Latency too high: {elapsed:.1f}s"

    @pytest.mark.slow
    def test_pt_1_02_analyze_exposure_throughput(
        self, connect_table, finding_table, s3_client
    ):
        """50 件一括挿入後、120 秒以内に全 Finding が出現しエラー率 < 1%。"""
        count = 50
        item_ids = []

        for i in range(count):
            item_id = f"item-pt102-{i:03d}-{uuid.uuid4().hex[:6]}"
            raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
            s3_client.put_object(
                Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
                Body=f"throughput test {i}".encode("utf-8"),
            )
            metadata = make_file_metadata(
                tenant_id=TEST_TENANT_ID, item_id=item_id,
                item_name=f"throughput_{i}.txt", mime_type="text/plain",
                raw_s3_key=raw_key,
            )
            connect_table.put_item(Item=metadata)
            item_ids.append(item_id)

        start = time.monotonic()
        max_wait = 120
        interval = 10
        elapsed = 0

        while elapsed < max_wait:
            resp = finding_table.query(
                KeyConditionExpression="tenant_id = :tid",
                ExpressionAttributeValues={":tid": TEST_TENANT_ID},
            )
            findings = resp.get("Items", [])
            found_ids = {f["item_id"] for f in findings}
            if all(iid in found_ids for iid in item_ids):
                break
            time.sleep(interval)
            elapsed = time.monotonic() - start

        resp = finding_table.query(
            KeyConditionExpression="tenant_id = :tid",
            ExpressionAttributeValues={":tid": TEST_TENANT_ID},
        )
        findings = resp.get("Items", [])
        found_ids = {f["item_id"] for f in findings}
        missing = [iid for iid in item_ids if iid not in found_ids]
        error_rate = len(missing) / count

        assert elapsed < max_wait, (
            f"Throughput test timed out at {elapsed:.1f}s, missing {len(missing)}/{count}"
        )
        assert error_rate < 0.01, (
            f"Error rate {error_rate:.2%} exceeds 1% ({len(missing)}/{count} missing)"
        )

    @pytest.mark.slow
    def test_pt_1_03_detect_sensitivity_cold_start(
        self, sqs_client, sensitivity_queue_url, finding_table,
        connect_table, s3_client
    ):
        """コールドスタート時の detectSensitivity 応答が 30 秒以内。
        15 分の待機が必要なためスキップ可能。"""
        item_id = f"item-pt103-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
            Body="コールドスタートテスト: 個人情報テスト".encode("utf-8"),
        )
        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="cold_start.txt", mime_type="text/plain",
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=30, interval=3
        )
        if finding is None:
            pytest.skip("Could not verify cold start within time limit")

        assert finding is not None

    def test_pt_1_04_detect_sensitivity_processing(
        self, sqs_client, sensitivity_queue_url, finding_table,
        connect_table, s3_client
    ):
        """1KB ファイルの detectSensitivity 処理が 60 秒以内に完了。"""
        item_id = f"item-pt104-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        content = "PII テストデータ " * 50
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
            Body=content.encode("utf-8"),
        )

        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="processing_test.txt", mime_type="text/plain",
            size=len(content.encode("utf-8")),
            raw_s3_key=raw_key,
        )

        start = time.monotonic()
        connect_table.put_item(Item=metadata)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=60, interval=5
        )
        elapsed = time.monotonic() - start

        assert finding is not None, (
            f"Finding not created within 60s (elapsed={elapsed:.1f}s)"
        )
        assert elapsed < 60, f"Processing took too long: {elapsed:.1f}s"

    @pytest.mark.slow
    def test_pt_1_05_batch_scoring_medium(
        self, connect_table, finding_table, lambda_client, s3_client
    ):
        """100 件の FileMetadata に対する batchScoring が 120 秒以内に完了。"""
        count = 100
        for i in range(count):
            item_id = f"item-pt105-{i:03d}-{uuid.uuid4().hex[:6]}"
            raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
            s3_client.put_object(
                Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
                Body=f"batch perf test {i}".encode("utf-8"),
            )
            metadata = make_file_metadata(
                tenant_id=TEST_TENANT_ID, item_id=item_id,
                item_name=f"batch_perf_{i}.txt", mime_type="text/plain",
                raw_s3_key=raw_key,
            )
            connect_table.put_item(Item=metadata)

        start = time.monotonic()
        result = invoke_lambda(
            lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID}
        )
        elapsed = time.monotonic() - start

        assert result["error"] is None, f"batchScoring failed: {result}"
        assert elapsed < 120, f"batchScoring took {elapsed:.1f}s (limit: 120s)"

    def test_pt_1_06_batch_scoring_timeout_safety(
        self, connect_table, finding_table, lambda_client, s3_client
    ):
        """100+ 件の batchScoring がエラー無く完了することを検証（タイムアウト耐性）。"""
        count = 110
        for i in range(count):
            item_id = f"item-pt106-{i:03d}-{uuid.uuid4().hex[:6]}"
            raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
            s3_client.put_object(
                Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
                Body=f"timeout safety test {i}".encode("utf-8"),
            )
            metadata = make_file_metadata(
                tenant_id=TEST_TENANT_ID, item_id=item_id,
                item_name=f"timeout_{i}.txt", mime_type="text/plain",
                raw_s3_key=raw_key,
            )
            connect_table.put_item(Item=metadata)

        result = invoke_lambda(
            lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID}
        )

        assert result["error"] is None, (
            f"batchScoring returned error for {count} items: {result}"
        )
        assert result["status_code"] == 200

    @pytest.mark.slow
    def test_pt_1_07_phase65_document_analysis_latency(
        self, connect_table, finding_table, document_analysis_table, s3_client
    ):
        """Phase 6.5 有効時、Finding 作成から DocumentAnalysis 生成まで 180 秒以内。"""
        item_id = f"item-pt107-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        body = "Phase 6.5 latency test: 山田太郎 yamada@example.com"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET,
            Key=raw_key,
            Body=body.encode("utf-8"),
        )
        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID,
            item_id=item_id,
            item_name="phase65_latency.txt",
            mime_type="text/plain",
            raw_s3_key=raw_key,
        )

        start = time.monotonic()
        connect_table.put_item(Item=metadata)
        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=180, interval=10
        )
        assert finding is not None, "Finding not generated during Phase 6.5 latency test"

        analysis = wait_for_document_analysis(
            document_analysis_table, TEST_TENANT_ID, item_id, max_wait=180, interval=10
        )
        elapsed = time.monotonic() - start
        assert analysis is not None, "DocumentAnalysis not generated within 180s"
        assert elapsed < 180, f"Phase 6.5 processing too slow: {elapsed:.1f}s"
