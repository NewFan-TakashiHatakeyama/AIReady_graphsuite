"""DVT-2: Lambda 関数デプロイ検証テスト。

現行 hard-cut 後の契約に合わせ、active Lambda（analyze/remediate）と
廃止済み detectSensitivity / batchScoring の非存在を実 AWS で検証する。
"""

from __future__ import annotations

import pytest

from botocore.exceptions import ClientError

from tests.aws.conftest import (
    ANALYZE_EXPOSURE_FN,
    DETECT_SENSITIVITY_FN,
    CONNECT_TABLE_NAME,
    FINDING_TABLE_NAME,
    REMEDIATE_FINDING_FN,
)

pytestmark = pytest.mark.aws


class TestDVT2Lambda:
    """DVT-2: Lambda 関数デプロイ検証（analyze / remediate と廃止 Lambda の非存在）"""

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
        """Hard-cut後の analyzeExposure 必須 env が設定済み"""
        cfg = lambda_client.get_function(FunctionName=ANALYZE_EXPOSURE_FN)["Configuration"]
        env = cfg["Environment"]["Variables"]
        assert env["FINDING_TABLE_NAME"] == FINDING_TABLE_NAME
        assert "POLICY_SCOPE_TABLE_NAME" in env
        assert "RAW_PAYLOAD_BUCKET" in env
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
        """detectSensitivity は hard-cut で廃止済み（関数が存在しない）。"""
        with pytest.raises(ClientError) as exc_info:
            lambda_client.get_function(FunctionName=DETECT_SENSITIVITY_FN)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_dvt_2_08_detect_sensitivity_memory_timeout(self, lambda_client):
        """detectSensitivity は hard-cut 後にリソース非存在。"""
        with pytest.raises(ClientError) as exc_info:
            lambda_client.get_function(FunctionName=DETECT_SENSITIVITY_FN)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_dvt_2_09_detect_sensitivity_ephemeral(self, lambda_client):
        """detectSensitivity は hard-cut 後にリソース非存在。"""
        with pytest.raises(ClientError) as exc_info:
            lambda_client.get_function(FunctionName=DETECT_SENSITIVITY_FN)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_dvt_2_10_detect_sensitivity_env_vars_ontology_extensions(self, lambda_client):
        """detectSensitivity は hard-cut 後にリソース非存在。"""
        with pytest.raises(ClientError) as exc_info:
            lambda_client.get_function(FunctionName=DETECT_SENSITIVITY_FN)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_dvt_2_11_detect_sensitivity_sqs_trigger(self, lambda_client):
        """detectSensitivity は hard-cut 後に関数自体が存在しない。"""
        with pytest.raises(ClientError) as exc_info:
            lambda_client.get_function(FunctionName=DETECT_SENSITIVITY_FN)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── batchScoring（廃止）──────────────────────────────

    def test_dvt_2_12_batch_scoring_removed(self, lambda_client):
        """batchScoring Lambda は廃止済み（関数が存在しない）。"""
        with pytest.raises(ClientError) as exc_info:
            lambda_client.get_function(FunctionName="AIReadyGov-batchScoring")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ── remediateFinding ─────────────────────────────────

    def test_dvt_2_14_remediate_finding_exists(self, lambda_client):
        """remediateFinding Lambda が Active であること"""
        resp = lambda_client.get_function(FunctionName=REMEDIATE_FINDING_FN)
        assert resp["Configuration"]["State"] == "Active"

    def test_dvt_2_15_remediate_finding_runtime_and_limits(self, lambda_client):
        """python3.12 / メモリ 512MB / タイムアウト 120 秒"""
        cfg = lambda_client.get_function(FunctionName=REMEDIATE_FINDING_FN)["Configuration"]
        assert cfg["Runtime"] == "python3.12"
        assert cfg["MemorySize"] == 512
        assert cfg["Timeout"] == 120

    def test_dvt_2_16_remediate_finding_env_vars(self, lambda_client):
        """Finding / Connect テーブル名が環境変数に設定されていること"""
        cfg = lambda_client.get_function(FunctionName=REMEDIATE_FINDING_FN)["Configuration"]
        env = cfg.get("Environment") or {}
        variables = env.get("Variables") or {}
        assert variables.get("FINDING_TABLE_NAME") == FINDING_TABLE_NAME
        assert variables.get("CONNECT_TABLE_NAME") == CONNECT_TABLE_NAME
