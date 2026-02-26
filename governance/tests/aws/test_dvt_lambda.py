"""DVT-2: Lambda 関数デプロイ検証テスト

analyzeExposure / detectSensitivity / batchScoring の各 Lambda が
設計通りの構成・トリガー・環境変数でデプロイされていることを実 AWS 環境で検証する。
"""

from __future__ import annotations

import pytest

from tests.aws.conftest import (
    ANALYZE_EXPOSURE_FN,
    BATCH_SCORING_FN,
    BATCH_SCORING_RULE,
    DETECT_SENSITIVITY_FN,
    DOCUMENT_ANALYSIS_TABLE_NAME,
    FINDING_TABLE_NAME,
    VECTORS_BUCKET,
)

pytestmark = pytest.mark.aws


class TestDVT2Lambda:
    """DVT-2: Lambda 関数デプロイ検証（12 テストケース）"""

    # ── analyzeExposure ──────────────────────────────────

    def test_dvt_2_01_analyze_exposure_exists(self, lambda_client):
        """analyzeExposure Lambda が Active であること"""
        resp = lambda_client.get_function(FunctionName=ANALYZE_EXPOSURE_FN)
        assert resp["Configuration"]["State"] == "Active"

    def test_dvt_2_02_analyze_exposure_runtime(self, lambda_client):
        """ランタイムが python3.12"""
        resp = lambda_client.get_function(FunctionName=ANALYZE_EXPOSURE_FN)
        assert resp["Configuration"]["Runtime"] == "python3.12"

    def test_dvt_2_03_analyze_exposure_memory_timeout(self, lambda_client):
        """メモリ 512MB、タイムアウト 60 秒"""
        cfg = lambda_client.get_function(FunctionName=ANALYZE_EXPOSURE_FN)["Configuration"]
        assert cfg["MemorySize"] == 512
        assert cfg["Timeout"] == 60

    def test_dvt_2_04_analyze_exposure_env_vars(self, lambda_client):
        """FINDING_TABLE_NAME, SENSITIVITY_QUEUE_URL, LOG_LEVEL が設定済み"""
        cfg = lambda_client.get_function(FunctionName=ANALYZE_EXPOSURE_FN)["Configuration"]
        env = cfg["Environment"]["Variables"]
        assert env["FINDING_TABLE_NAME"] == FINDING_TABLE_NAME
        assert "SENSITIVITY_QUEUE_URL" in env
        assert "LOG_LEVEL" in env

    def test_dvt_2_05_analyze_exposure_event_source(self, lambda_client):
        """DynamoDB Streams トリガー（BatchSize=10）が有効"""
        resp = lambda_client.list_event_source_mappings(FunctionName=ANALYZE_EXPOSURE_FN)
        dynamodb_mappings = [
            m for m in resp["EventSourceMappings"]
            if "dynamodb" in m["EventSourceArn"]
        ]
        assert len(dynamodb_mappings) >= 1

        mapping = dynamodb_mappings[0]
        assert mapping["State"] == "Enabled"
        assert mapping["BatchSize"] == 10

    def test_dvt_2_06_analyze_exposure_concurrency(self, lambda_client):
        """Reserved Concurrency = 50"""
        resp = lambda_client.get_function_concurrency(FunctionName=ANALYZE_EXPOSURE_FN)
        assert resp["ReservedConcurrentExecutions"] == 50

    # ── detectSensitivity ────────────────────────────────

    def test_dvt_2_07_detect_sensitivity_exists(self, lambda_client):
        """detectSensitivity が Docker Lambda（PackageType=Image）であること"""
        resp = lambda_client.get_function(FunctionName=DETECT_SENSITIVITY_FN)
        assert resp["Configuration"]["PackageType"] == "Image"

    def test_dvt_2_08_detect_sensitivity_memory_timeout(self, lambda_client):
        """メモリ 4096MB、タイムアウト 600 秒"""
        cfg = lambda_client.get_function(FunctionName=DETECT_SENSITIVITY_FN)["Configuration"]
        assert cfg["MemorySize"] == 4096
        assert cfg["Timeout"] == 600

    def test_dvt_2_09_detect_sensitivity_ephemeral(self, lambda_client):
        """エフェメラルストレージ = 1024MB"""
        cfg = lambda_client.get_function(FunctionName=DETECT_SENSITIVITY_FN)["Configuration"]
        assert cfg["EphemeralStorage"]["Size"] == 1024

    def test_dvt_2_10_detect_sensitivity_env_vars_phase65(self, lambda_client):
        """Phase 6.5 の環境変数が設定されていること"""
        cfg = lambda_client.get_function(FunctionName=DETECT_SENSITIVITY_FN)["Configuration"]
        env = cfg["Environment"]["Variables"]
        assert env["DOCUMENT_ANALYSIS_TABLE_NAME"] == DOCUMENT_ANALYSIS_TABLE_NAME
        assert env["VECTORS_BUCKET"] == VECTORS_BUCKET
        assert "ENTITY_RESOLUTION_QUEUE_URL" in env
        assert env.get("DOCUMENT_ANALYSIS_ENABLED", "").lower() in {"1", "true", "yes", "on"}

    def test_dvt_2_11_detect_sensitivity_sqs_trigger(self, lambda_client):
        """SQS トリガー（BatchSize=1）が有効"""
        resp = lambda_client.list_event_source_mappings(FunctionName=DETECT_SENSITIVITY_FN)
        sqs_mappings = [
            m for m in resp["EventSourceMappings"]
            if "sqs" in m["EventSourceArn"]
        ]
        assert len(sqs_mappings) >= 1

        mapping = sqs_mappings[0]
        assert mapping["State"] == "Enabled"
        assert mapping["BatchSize"] == 1

    # ── batchScoring ─────────────────────────────────────

    def test_dvt_2_12_batch_scoring_exists(self, lambda_client):
        """batchScoring Lambda が Active であること"""
        resp = lambda_client.get_function(FunctionName=BATCH_SCORING_FN)
        assert resp["Configuration"]["State"] == "Active"

    def test_dvt_2_13_batch_scoring_eventbridge_rule(self, events_client):
        """EventBridge ルール: 日次 05:00 UTC, ENABLED"""
        resp = events_client.describe_rule(Name=BATCH_SCORING_RULE)
        assert resp["State"] == "ENABLED"
        assert resp["ScheduleExpression"] in {
            "cron(0 5 * * ? *)",
            "cron(0 5 ? * * *)",
        }
