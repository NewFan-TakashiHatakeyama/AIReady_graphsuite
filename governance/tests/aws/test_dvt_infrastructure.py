"""DVT-1: インフラリソース検証テスト

CDK でデプロイされた DynamoDB / SQS / S3 / SSM が設計通りに
構成されていることを実 AWS 環境で検証する。
"""

from __future__ import annotations

import json

import pytest
from botocore.exceptions import ClientError

from tests.aws.conftest import (
    ANALYZE_DLQ_NAME,
    DOCUMENT_ANALYSIS_TABLE_NAME,
    DETECT_DLQ_NAME,
    ENTITY_RESOLUTION_QUEUE_PARAM,
    FINDING_TABLE_NAME,
    GOVERNANCE_DASHBOARD_NAME,
    SENSITIVITY_QUEUE_NAME,
    SSM_PARAMETERS,
    VECTORS_BUCKET,
)

pytestmark = pytest.mark.aws


class TestDVT1Infrastructure:
    """DVT-1: インフラリソース検証（18 テストケース）"""

    # ── DynamoDB ──────────────────────────────────────────

    def test_dvt_1_01_finding_table_exists(self, dynamodb_client):
        """ExposureFinding テーブルが ACTIVE であること"""
        resp = dynamodb_client.describe_table(TableName=FINDING_TABLE_NAME)
        assert resp["Table"]["TableStatus"] == "ACTIVE"

    def test_dvt_1_02_finding_table_key_schema(self, dynamodb_client):
        """PK=tenant_id(S), SK=finding_id(S)"""
        resp = dynamodb_client.describe_table(TableName=FINDING_TABLE_NAME)
        key_schema = {ks["AttributeName"]: ks["KeyType"] for ks in resp["Table"]["KeySchema"]}
        assert key_schema == {"tenant_id": "HASH", "finding_id": "RANGE"}

        attr_defs = {ad["AttributeName"]: ad["AttributeType"] for ad in resp["Table"]["AttributeDefinitions"]}
        assert attr_defs["tenant_id"] == "S"
        assert attr_defs["finding_id"] == "S"

    def test_dvt_1_03_finding_table_billing_mode(self, dynamodb_client):
        """オンデマンド課金（PAY_PER_REQUEST）"""
        resp = dynamodb_client.describe_table(TableName=FINDING_TABLE_NAME)
        billing = resp["Table"].get("BillingModeSummary", {}).get("BillingMode")
        assert billing == "PAY_PER_REQUEST"

    def test_dvt_1_04_gsi_item_finding(self, dynamodb_client):
        """GSI-ItemFinding: PK=item_id, Projection=ALL"""
        resp = dynamodb_client.describe_table(TableName=FINDING_TABLE_NAME)
        gsi_map = {g["IndexName"]: g for g in resp["Table"].get("GlobalSecondaryIndexes", [])}
        gsi = gsi_map["GSI-ItemFinding"]

        key_schema = {ks["AttributeName"]: ks["KeyType"] for ks in gsi["KeySchema"]}
        assert key_schema["item_id"] == "HASH"
        assert gsi["Projection"]["ProjectionType"] == "ALL"

    def test_dvt_1_05_gsi_status_finding(self, dynamodb_client):
        """GSI-StatusFinding: PK=tenant_id, SK=status"""
        resp = dynamodb_client.describe_table(TableName=FINDING_TABLE_NAME)
        gsi_map = {g["IndexName"]: g for g in resp["Table"].get("GlobalSecondaryIndexes", [])}
        gsi = gsi_map["GSI-StatusFinding"]

        key_schema = {ks["AttributeName"]: ks["KeyType"] for ks in gsi["KeySchema"]}
        assert key_schema["tenant_id"] == "HASH"
        assert key_schema["status"] == "RANGE"

    def test_dvt_1_06_pitr_enabled(self, dynamodb_client):
        """ポイントインタイムリカバリが有効"""
        resp = dynamodb_client.describe_continuous_backups(TableName=FINDING_TABLE_NAME)
        pitr = resp["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
        assert pitr["PointInTimeRecoveryStatus"] == "ENABLED"

    def test_dvt_1_06b_governance_phase9_tables_exist(self, dynamodb_client):
        """Phase 9 追加テーブル（PolicyScope/AuditLog）が ACTIVE。"""
        for table_name in ("AIReadyGov-PolicyScope", "AIReadyGov-AuditLog"):
            resp = dynamodb_client.describe_table(TableName=table_name)
            assert resp["Table"]["TableStatus"] == "ACTIVE"

    def test_dvt_1_06c_governance_phase9_tables_schema(self, dynamodb_client):
        """Phase 9 追加テーブルの PK/SK が設計どおり。"""
        expected = {
            "AIReadyGov-PolicyScope": {"tenant_id": "HASH", "policy_id": "RANGE"},
            "AIReadyGov-AuditLog": {"tenant_id": "HASH", "audit_id": "RANGE"},
        }
        for table_name, expected_schema in expected.items():
            resp = dynamodb_client.describe_table(TableName=table_name)
            key_schema = {
                ks["AttributeName"]: ks["KeyType"] for ks in resp["Table"]["KeySchema"]
            }
            assert key_schema == expected_schema

    # ── SQS ──────────────────────────────────────────────

    def test_dvt_1_07_sensitivity_queue_removed(self, sqs_client):
        """hard-cut 後は SensitivityDetectionQueue が存在しないこと。"""
        with pytest.raises(ClientError) as exc_info:
            sqs_client.get_queue_url(QueueName=SENSITIVITY_QUEUE_NAME)
        assert exc_info.value.response["Error"]["Code"] in {"AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"}

    def test_dvt_1_08_sensitivity_queue_redrive_removed(self, sqs_client):
        """hard-cut 後は detectSensitivity 系キュー設定検証を行わない。"""
        with pytest.raises(ClientError) as exc_info:
            sqs_client.get_queue_url(QueueName=SENSITIVITY_QUEUE_NAME)
        assert exc_info.value.response["Error"]["Code"] in {"AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"}

    def test_dvt_1_09_sensitivity_queue_dlq_redrive_removed(self, sqs_client):
        """hard-cut 後は detectSensitivity の Redrive 設定対象が存在しない。"""
        with pytest.raises(ClientError) as exc_info:
            sqs_client.get_queue_url(QueueName=SENSITIVITY_QUEUE_NAME)
        assert exc_info.value.response["Error"]["Code"] in {"AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"}

    def test_dvt_1_10_analyze_dlq_exists(self, sqs_client, analyze_dlq_url):
        """analyzeExposure DLQ が存在し保持期間 14 日"""
        assert ANALYZE_DLQ_NAME in analyze_dlq_url
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=analyze_dlq_url,
            AttributeNames=["MessageRetentionPeriod"],
        )["Attributes"]
        assert attrs["MessageRetentionPeriod"] == str(14 * 86400)

    def test_dvt_1_11_detect_dlq_removed(self, sqs_client):
        """hard-cut 後は detectSensitivity DLQ が存在しないこと。"""
        with pytest.raises(ClientError) as exc_info:
            sqs_client.get_queue_url(QueueName=DETECT_DLQ_NAME)
        assert exc_info.value.response["Error"]["Code"] in {"AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"}

    # ── S3 ──────────────────────────────────────────────
    # 日次レポート用バケットは batchScoring 廃止に伴い CDK から削除済み

    def test_dvt_1_16_document_analysis_table_exists(self, dynamodb_client):
        """DocumentAnalysis テーブルは有効時のみ ACTIVE を確認する。"""
        try:
            resp = dynamodb_client.describe_table(TableName=DOCUMENT_ANALYSIS_TABLE_NAME)
        except dynamodb_client.exceptions.ResourceNotFoundException:
            pytest.skip("DocumentAnalysis table is not deployed in current environment")
        assert resp["Table"]["TableStatus"] == "ACTIVE"

    def test_dvt_1_17_document_analysis_schema_and_ttl(self, dynamodb_client):
        """DocumentAnalysis の PK/SK/TTL/GSI は有効時のみ検証する。"""
        try:
            resp = dynamodb_client.describe_table(TableName=DOCUMENT_ANALYSIS_TABLE_NAME)
        except dynamodb_client.exceptions.ResourceNotFoundException:
            pytest.skip("DocumentAnalysis table is not deployed in current environment")
        key_schema = {ks["AttributeName"]: ks["KeyType"] for ks in resp["Table"]["KeySchema"]}
        assert key_schema == {"tenant_id": "HASH", "item_id": "RANGE"}

        ttl = dynamodb_client.describe_time_to_live(
            TableName=DOCUMENT_ANALYSIS_TABLE_NAME
        )["TimeToLiveDescription"]
        assert ttl["AttributeName"] == "ttl"
        assert ttl["TimeToLiveStatus"] in ("ENABLED", "ENABLING")

        gsi_map = {g["IndexName"]: g for g in resp["Table"].get("GlobalSecondaryIndexes", [])}
        assert "GSI-AnalyzedAt" in gsi_map

    def test_dvt_1_18_vectors_bucket_security_and_lifecycle(self, s3_client):
        """Vectors バケット有効時のみ暗号化・公開ブロック・ライフサイクルを確認。"""
        try:
            s3_client.head_bucket(Bucket=VECTORS_BUCKET)
        except Exception:
            pytest.skip("Vectors bucket is not deployed in current environment")
        encryption = s3_client.get_bucket_encryption(Bucket=VECTORS_BUCKET)
        algo = encryption["ServerSideEncryptionConfiguration"]["Rules"][0][
            "ApplyServerSideEncryptionByDefault"
        ]["SSEAlgorithm"]
        assert algo == "AES256"

        pab = s3_client.get_public_access_block(Bucket=VECTORS_BUCKET)[
            "PublicAccessBlockConfiguration"
        ]
        assert pab["BlockPublicAcls"] is True
        assert pab["BlockPublicPolicy"] is True

        rules = s3_client.get_bucket_lifecycle_configuration(Bucket=VECTORS_BUCKET)["Rules"]
        deep_archive_rule = [r for r in rules if r["Status"] == "Enabled"]
        assert len(deep_archive_rule) >= 1
        transitions = deep_archive_rule[0].get("Transitions", [])
        assert any(
            t.get("StorageClass") == "DEEP_ARCHIVE" and t.get("Days") == 365
            for t in transitions
        )

    # ── SSM パラメータ ──────────────────────────────────

    def test_dvt_1_20_ssm_max_exposure_score(self, ssm_client):
        """max_exposure_score = 10.0"""
        resp = ssm_client.get_parameter(Name="/aiready/governance/max_exposure_score")
        assert resp["Parameter"]["Value"] == "10.0"

    def test_dvt_1_21_ssm_all_parameters_exist(self, ssm_client):
        """定義済みガバナンス SSM パラメータが存在しデフォルト値が設定されていること"""
        for path, expected_value in SSM_PARAMETERS.items():
            resp = ssm_client.get_parameter(Name=path)
            assert resp["Parameter"]["Value"] == expected_value, (
                f"{path}: expected={expected_value}, got={resp['Parameter']['Value']}"
            )

    def test_dvt_1_22_entity_resolution_queue_param_exists(self, ssm_client):
        """Ontology EntityResolutionQueue URL の SSM パラメータが存在すること"""
        resp = ssm_client.get_parameter(Name=ENTITY_RESOLUTION_QUEUE_PARAM)
        value = resp["Parameter"]["Value"]
        assert "EntityResolutionQueue.fifo" in value

    def test_dvt_1_23_ontology_report_params_exist(self, ssm_client):
        """Ontology report bucket/prefix の SSM パラメータが存在すること（BatchReconciler 廃止後は任意）"""
        report_bucket_param = "/ai-ready/ontology/report_bucket"
        report_prefix_param = "/ai-ready/ontology/report_prefix"
        try:
            bucket_value = ssm_client.get_parameter(Name=report_bucket_param)["Parameter"]["Value"]
            prefix_value = ssm_client.get_parameter(Name=report_prefix_param)["Parameter"]["Value"]
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ParameterNotFound":
                pytest.skip(
                    "Ontology report SSM parameters not configured (optional after batch reconciler removal)"
                )
            raise
        assert str(bucket_value).strip() != ""
        assert str(prefix_value).strip() != ""

    def test_dvt_1_24_governance_operations_dashboard_exists(self, cloudwatch_client):
        """T-064: Governance Operations ダッシュボードが存在すること。"""
        resp = cloudwatch_client.get_dashboard(DashboardName=GOVERNANCE_DASHBOARD_NAME)
        body = resp.get("DashboardBody", "")
        assert str(body).strip() != ""
