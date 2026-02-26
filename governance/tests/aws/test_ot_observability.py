"""OT-1: オブザーバビリティテスト

CloudWatch アラーム・カスタムメトリクス・構造化ログ・エラートレーサビリティを検証する。
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone, timedelta

import pytest

from tests.aws.conftest import (
    ALARM_NAMES,
    ANALYZE_EXPOSURE_FN,
    BATCH_SCORING_FN,
    DETECT_SENSITIVITY_FN,
    RAW_PAYLOAD_BUCKET,
    TEST_TENANT_ID,
    invoke_lambda,
    make_file_metadata,
    wait_for_finding_by_item,
)

CW_NAMESPACE = "AIReadyGovernance"


def _sum_metric_values(cloudwatch_client, metric_name: str, lookback_minutes: int = 60) -> float:
    """対象メトリクスの全ディメンション系列を合算して返す。"""
    metrics = []
    next_token = None
    while True:
        kwargs = {"Namespace": CW_NAMESPACE, "MetricName": metric_name}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = cloudwatch_client.list_metrics(**kwargs)
        metrics.extend(resp.get("Metrics", []))
        next_token = resp.get("NextToken")
        if not next_token:
            break

    if not metrics:
        return 0.0

    now = datetime.now(timezone.utc)
    queries = []
    for i, metric in enumerate(metrics[:100]):
        queries.append({
            "Id": f"m{i}",
            "MetricStat": {
                "Metric": metric,
                "Period": 300,
                "Stat": "Sum",
            },
            "ReturnData": True,
        })

    data = cloudwatch_client.get_metric_data(
        MetricDataQueries=queries,
        StartTime=now - timedelta(minutes=lookback_minutes),
        EndTime=now,
    )
    total = 0.0
    for result in data.get("MetricDataResults", []):
        total += sum(result.get("Values", []))
    return total


class TestOT1Observability:
    """CloudWatch による監視・可観測性設定を検証するテスト。"""

    def test_ot_1_01_analyze_dlq_alarm_exists(self, cloudwatch_client):
        """analyzeExposure DLQ アラームが存在し正しく設定されている。"""
        alarm_name = "AIReadyGov-analyzeExposure-DLQ-NotEmpty"
        resp = cloudwatch_client.describe_alarms(AlarmNames=[alarm_name])
        alarms = resp.get("MetricAlarms", [])
        assert len(alarms) == 1, f"Alarm '{alarm_name}' not found"
        alarm = alarms[0]
        assert alarm["MetricName"] == "ApproximateNumberOfMessagesVisible"
        assert alarm["ComparisonOperator"] in (
            "GreaterThanThreshold", "GreaterThanOrEqualToThreshold"
        )

    def test_ot_1_02_detect_dlq_alarm_exists(self, cloudwatch_client):
        """detectSensitivity DLQ アラームが存在し正しく設定されている。"""
        alarm_name = "AIReadyGov-detectSensitivity-DLQ-NotEmpty"
        resp = cloudwatch_client.describe_alarms(AlarmNames=[alarm_name])
        alarms = resp.get("MetricAlarms", [])
        assert len(alarms) == 1, f"Alarm '{alarm_name}' not found"
        alarm = alarms[0]
        assert alarm["MetricName"] == "ApproximateNumberOfMessagesVisible"

    def test_ot_1_03_batch_duration_alarm_exists(self, cloudwatch_client):
        """batchScoring Duration アラームの閾値が 840000ms であること。"""
        alarm_name = "AIReadyGov-batchScoring-Duration-High"
        resp = cloudwatch_client.describe_alarms(AlarmNames=[alarm_name])
        alarms = resp.get("MetricAlarms", [])
        assert len(alarms) == 1, f"Alarm '{alarm_name}' not found"
        alarm = alarms[0]
        assert alarm["Threshold"] == 840000, (
            f"Expected threshold 840000, got {alarm['Threshold']}"
        )

    @pytest.mark.slow
    def test_ot_1_04_findings_created_metric(
        self, connect_table, finding_table, s3_client, cloudwatch_client
    ):
        """FileMetadata 挿入後に AIReadyGov.FindingsCreated メトリクスが記録される。"""
        item_id = f"item-ot104-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
            Body=b"metric test findings created",
        )
        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="metric_findings.txt", mime_type="text/plain",
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=120, interval=10
        )

        time.sleep(60)

        total = _sum_metric_values(cloudwatch_client, "AIReadyGov.FindingsCreated", 60)
        assert total > 0, "FindingsCreated metric not recorded in last 30 minutes"

    @pytest.mark.slow
    def test_ot_1_05_pii_detected_metric(
        self, connect_table, finding_table, s3_client, cloudwatch_client
    ):
        """PII 検出後に AIReadyGov.PIIDetected メトリクスが記録される。"""
        item_id = f"item-ot105-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
            Body="個人番号 1111 2222 3333".encode("utf-8"),
        )
        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="metric_pii.txt", mime_type="text/plain",
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=300, interval=10
        )
        assert finding is not None

        time.sleep(60)

        total = _sum_metric_values(cloudwatch_client, "AIReadyGov.PIIDetected", 60)
        assert total > 0, "PIIDetected metric not recorded in last 30 minutes"

    def test_ot_1_06_batch_items_processed_metric(
        self, connect_table, lambda_client, s3_client, cloudwatch_client
    ):
        """batchScoring 実行後に AIReadyGov.BatchItemsProcessed メトリクスが記録される。"""
        item_id = f"item-ot106-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
            Body=b"batch metric test",
        )
        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="metric_batch.txt", mime_type="text/plain",
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        result = invoke_lambda(
            lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID}
        )
        assert result["error"] is None

        time.sleep(60)

        total = _sum_metric_values(cloudwatch_client, "AIReadyGov.BatchItemsProcessed", 60)
        assert total > 0, "BatchItemsProcessed metric not recorded in last 30 minutes"

    def test_ot_1_07_structured_log_output(self, logs_client):
        """analyzeExposure のログが JSON 構造化出力であること。"""
        log_group = f"/aws/lambda/{ANALYZE_EXPOSURE_FN}"
        try:
            now_ms = int(time.time() * 1000)
            events_resp = logs_client.filter_log_events(
                logGroupName=log_group,
                startTime=now_ms - (60 * 60 * 1000),
                endTime=now_ms,
                limit=200,
            )
            events = events_resp.get("events", [])
            if not events:
                pytest.skip("No recent analyzeExposure logs found")

            json_found = False
            for event in events:
                message = event["message"].strip()
                if message.startswith("{"):
                    try:
                        parsed = json.loads(message)
                        json_found = True
                        break
                    except json.JSONDecodeError:
                        continue

            assert json_found, (
                "No JSON-formatted log entry found in recent analyzeExposure logs"
            )
        except logs_client.exceptions.ResourceNotFoundException:
            pytest.skip(f"Log group {log_group} not found")

    def test_ot_1_08_error_log_traceability(self, lambda_client, logs_client):
        """不正ペイロードでの Lambda 呼び出し後、エラーログに tenant_id またはエラーメッセージが含まれる。"""
        bad_payload = {
            "Records": [{
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {
                        "tenant_id": {"S": TEST_TENANT_ID},
                        "item_id": {"S": "bad-item-traceability"},
                    }
                },
            }]
        }
        invoke_lambda(lambda_client, ANALYZE_EXPOSURE_FN, bad_payload)

        time.sleep(15)

        log_group = f"/aws/lambda/{ANALYZE_EXPOSURE_FN}"
        try:
            now_ms = int(time.time() * 1000)
            resp = logs_client.filter_log_events(
                logGroupName=log_group,
                startTime=now_ms - (5 * 60 * 1000),
                endTime=now_ms,
                filterPattern="ERROR",
                limit=10,
            )
            events = resp.get("events", [])

            if not events:
                resp = logs_client.filter_log_events(
                    logGroupName=log_group,
                    startTime=now_ms - (5 * 60 * 1000),
                    endTime=now_ms,
                    filterPattern="error",
                    limit=10,
                )
                events = resp.get("events", [])

            has_context = any(
                TEST_TENANT_ID in e["message"] or "error" in e["message"].lower()
                for e in events
            )
            assert has_context or len(events) > 0, (
                "No error logs with traceability context found"
            )
        except logs_client.exceptions.ResourceNotFoundException:
            pytest.skip(f"Log group {log_group} not found")

    def test_ot_1_09_detect_sensitivity_phase65_logs(self, logs_client):
        """detectSensitivity のログに Phase 6.5 実行痕跡が出力される。"""
        log_group = f"/aws/lambda/{DETECT_SENSITIVITY_FN}"
        try:
            now_ms = int(time.time() * 1000)
            resp = logs_client.filter_log_events(
                logGroupName=log_group,
                startTime=now_ms - (60 * 60 * 1000),
                endTime=now_ms,
                filterPattern="Sensitivity scan complete",
                limit=20,
            )
            events = resp.get("events", [])
            if not events:
                pytest.skip("No detectSensitivity completion logs found in last 60 minutes")

            has_context = any(
                "sensitivity_score" in e["message"] and "pii_detected" in e["message"]
                for e in events
            )
            assert has_context, "detectSensitivity log is missing expected structured context"
        except logs_client.exceptions.ResourceNotFoundException:
            pytest.skip(f"Log group {log_group} not found")
