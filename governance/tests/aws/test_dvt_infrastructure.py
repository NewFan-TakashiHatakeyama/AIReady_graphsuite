"""DVT-1: インフラリソース検証テスト

CDK でデプロイされた DynamoDB / SQS / S3 / SSM が設計通りに
構成されていることを実 AWS 環境で検証する。
"""

from __future__ import annotations

import json

import pytest

from tests.aws.conftest import (
    ANALYZE_DLQ_NAME,
    DOCUMENT_ANALYSIS_TABLE_NAME,
    DETECT_DLQ_NAME,
    ENTITY_RESOLUTION_QUEUE_PARAM,
    FINDING_TABLE_NAME,
    REPORT_BUCKET,
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

    # ── SQS ──────────────────────────────────────────────

    def test_dvt_1_07_sensitivity_queue_exists(self, sensitivity_queue_url):
        """SensitivityDetectionQueue の URL が取得できること"""
        assert SENSITIVITY_QUEUE_NAME in sensitivity_queue_url

    def test_dvt_1_08_sensitivity_queue_visibility_timeout(self, sqs_client, sensitivity_queue_url):
        """可視性タイムアウト = 660 秒"""
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=sensitivity_queue_url,
            AttributeNames=["VisibilityTimeout"],
        )["Attributes"]
        assert attrs["VisibilityTimeout"] == "660"

    def test_dvt_1_09_sensitivity_queue_dlq_redrive(self, sqs_client, sensitivity_queue_url):
        """DLQ リドライブポリシー maxReceiveCount=3"""
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=sensitivity_queue_url,
            AttributeNames=["RedrivePolicy"],
        )["Attributes"]
        policy = json.loads(attrs["RedrivePolicy"])
        assert int(policy["maxReceiveCount"]) == 3

    def test_dvt_1_10_analyze_dlq_exists(self, sqs_client, analyze_dlq_url):
        """analyzeExposure DLQ が存在し保持期間 14 日"""
        assert ANALYZE_DLQ_NAME in analyze_dlq_url
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=analyze_dlq_url,
            AttributeNames=["MessageRetentionPeriod"],
        )["Attributes"]
        assert attrs["MessageRetentionPeriod"] == str(14 * 86400)

    def test_dvt_1_11_detect_dlq_exists(self, sqs_client, detect_dlq_url):
        """detectSensitivity DLQ が存在し保持期間 14 日"""
        assert DETECT_DLQ_NAME in detect_dlq_url
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=detect_dlq_url,
            AttributeNames=["MessageRetentionPeriod"],
        )["Attributes"]
        assert attrs["MessageRetentionPeriod"] == str(14 * 86400)

    # ── S3 ──────────────────────────────────────────────

    def test_dvt_1_12_report_bucket_exists(self, s3_client):
        """レポートバケットが存在すること"""
        s3_client.head_bucket(Bucket=REPORT_BUCKET)

    def test_dvt_1_13_report_bucket_encryption(self, s3_client):
        """AES256 サーバーサイド暗号化"""
        resp = s3_client.get_bucket_encryption(Bucket=REPORT_BUCKET)
        rules = resp["ServerSideEncryptionConfiguration"]["Rules"]
        sse = rules[0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
        assert sse == "AES256"

    def test_dvt_1_14_report_bucket_public_access_blocked(self, s3_client):
        """パブリックアクセスが全ブロック"""
        resp = s3_client.get_public_access_block(Bucket=REPORT_BUCKET)
        cfg = resp["PublicAccessBlockConfiguration"]
        assert cfg["BlockPublicAcls"] is True
        assert cfg["BlockPublicPolicy"] is True
        assert cfg["IgnorePublicAcls"] is True
        assert cfg["RestrictPublicBuckets"] is True

    def test_dvt_1_15_report_bucket_lifecycle(self, s3_client):
        """Glacier @90 日（Expiration は設定されていれば 365 日）"""
        resp = s3_client.get_bucket_lifecycle_configuration(Bucket=REPORT_BUCKET)
        rules = resp["Rules"]
        active_rules = [r for r in rules if r["Status"] == "Enabled"]
        assert len(active_rules) >= 1

        rule = active_rules[0]
        transitions = rule.get("Transitions", [])
        glacier = [t for t in transitions if t["StorageClass"] == "GLACIER"]
        assert len(glacier) >= 1
        assert glacier[0]["Days"] == 90

        expiration = rule.get("Expiration", {})
        if expiration:
            assert expiration.get("Days") == 365

    def test_dvt_1_16_document_analysis_table_exists(self, dynamodb_client):
        """DocumentAnalysis テーブルが ACTIVE であること"""
        resp = dynamodb_client.describe_table(TableName=DOCUMENT_ANALYSIS_TABLE_NAME)
        assert resp["Table"]["TableStatus"] == "ACTIVE"

    def test_dvt_1_17_document_analysis_schema_and_ttl(self, dynamodb_client):
        """DocumentAnalysis の PK/SK/TTL/GSI が設計どおりであること"""
        resp = dynamodb_client.describe_table(TableName=DOCUMENT_ANALYSIS_TABLE_NAME)
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
        """Vectors バケットの暗号化・公開ブロック・DeepArchive ライフサイクルを確認"""
        s3_client.head_bucket(Bucket=VECTORS_BUCKET)
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

    def test_dvt_1_19_ssm_risk_score_threshold(self, ssm_client):
        """risk_score_threshold = 2.0"""
        resp = ssm_client.get_parameter(Name="/aiready/governance/risk_score_threshold")
        assert resp["Parameter"]["Value"] == "2.0"

    def test_dvt_1_20_ssm_max_exposure_score(self, ssm_client):
        """max_exposure_score = 10.0"""
        resp = ssm_client.get_parameter(Name="/aiready/governance/max_exposure_score")
        assert resp["Parameter"]["Value"] == "10.0"

    def test_dvt_1_21_ssm_all_parameters_exist(self, ssm_client):
        """全 7 パラメータが存在しデフォルト値が設定されていること"""
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
