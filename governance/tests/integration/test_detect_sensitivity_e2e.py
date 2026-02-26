"""detectSensitivity 結合テスト

moto を使って AWS リソースをモックし、SQS メッセージ投入 →
detectSensitivity 処理 → Finding 更新の全フローを検証する。

Presidio / GiNZA は mock_aws 環境では利用できないため、
pii_detector / secret_detector をパッチして結合フローに集中する。
"""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from services.pii_detector import PIIDetectionResult, PIIEntity
from services.secret_detector import SecretDetectionResult, SecretEntity


@pytest.fixture
def e2e_resources():
    """DynamoDB + S3 + SQS をセットアップ"""
    with mock_aws():
        region = "ap-northeast-1"
        dynamodb = boto3.resource("dynamodb", region_name=region)
        s3_client = boto3.client("s3", region_name=region)
        sqs_client = boto3.client("sqs", region_name=region)

        # DynamoDB
        table = dynamodb.create_table(
            TableName="AIReadyGov-ExposureFinding",
            KeySchema=[
                {"AttributeName": "tenant_id", "KeyType": "HASH"},
                {"AttributeName": "finding_id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "tenant_id", "AttributeType": "S"},
                {"AttributeName": "finding_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(
            TableName="AIReadyGov-ExposureFinding"
        )

        # S3
        bucket_name = "aireadyconnect-raw-payload-123456789012"
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )

        # SQS
        queue_response = sqs_client.create_queue(
            QueueName="AIReadyGov-SensitivityDetectionQueue"
        )
        queue_url = queue_response["QueueUrl"]

        yield {
            "table": table,
            "s3_client": s3_client,
            "sqs_client": sqs_client,
            "bucket_name": bucket_name,
            "queue_url": queue_url,
            "dynamodb": dynamodb,
        }


def _seed_finding(table, **overrides):
    item = {
        "tenant_id": "tenant-001",
        "finding_id": "finding-e2e-001",
        "source": "m365",
        "item_id": "item-e2e-001",
        "item_name": "顧客リスト.xlsx",
        "item_url": "https://contoso.sharepoint.com/sites/sales/顧客リスト.xlsx",
        "exposure_score": Decimal("5.0"),
        "sensitivity_score": Decimal("2.0"),
        "activity_score": Decimal("2.0"),
        "ai_amplification": Decimal("1.0"),
        "risk_score": Decimal("20.0"),
        "risk_level": "high",
        "status": "new",
        "exposure_vectors": ["public_link"],
        "sharing_scope": "anonymous",
        "matched_guards": ["G3"],
        "pii_detected": False,
        "pii_types": None,
        "pii_count": 0,
        "pii_density": "none",
        "secrets_detected": False,
        "secret_types": None,
        "sensitivity_scan_at": None,
        "detected_at": "2026-02-10T08:30:00Z",
        "last_evaluated_at": "2026-02-10T08:30:00Z",
    }
    item.update(overrides)
    table.put_item(Item=item)
    return item


class TestDetectSensitivityE2E:
    """detectSensitivity の結合テストシナリオ。"""

    @patch("handlers.detect_sensitivity.detect_pii")
    @patch("handlers.detect_sensitivity.detect_secrets")
    def test_scenario_pii_detection_flow(
        self, mock_secrets, mock_pii, e2e_resources
    ):
        """シナリオ 1: PII 入りファイル → Finding 更新"""
        from handlers.detect_sensitivity import handler, set_finding_table, set_s3_client

        table = e2e_resources["table"]
        s3_client = e2e_resources["s3_client"]
        bucket = e2e_resources["bucket_name"]
        set_finding_table(table)
        set_s3_client(s3_client)

        _seed_finding(table)
        s3_client.put_object(
            Bucket=bucket,
            Key="raw/tenant-001/item-e2e-001/data.xlsx",
            Body=b"Name,Phone\nTanaka,090-1234-5678\n",
        )

        mock_pii.return_value = PIIDetectionResult(
            detected=True,
            types=["PERSON", "PHONE_NUMBER"],
            count=2,
            density="low",
            high_risk_detected=False,
            details=[
                PIIEntity(type="PERSON", start=0, end=6, score=0.85),
                PIIEntity(type="PHONE_NUMBER", start=7, end=20, score=0.9),
            ],
        )
        mock_secrets.return_value = SecretDetectionResult()

        event = {
            "Records": [
                {
                    "messageId": "e2e-msg-001",
                    "body": json.dumps({
                        "finding_id": "finding-e2e-001",
                        "tenant_id": "tenant-001",
                        "source": "m365",
                        "item_id": "item-e2e-001",
                        "item_name": "顧客リスト.xlsx",
                        "mime_type": "text/plain",
                        "size": 50,
                        "raw_s3_key": "raw/tenant-001/item-e2e-001/data.xlsx",
                        "raw_s3_bucket": bucket,
                        "enqueued_at": "2026-02-10T08:31:00Z",
                        "trigger": "realtime",
                    }),
                }
            ]
        }

        result = handler(event, None)
        assert result["processed"] == 1
        assert result["errors"] == 0

        finding = table.get_item(
            Key={"tenant_id": "tenant-001", "finding_id": "finding-e2e-001"}
        )["Item"]
        assert finding["pii_detected"] is True
        assert finding["pii_count"] == 2
        assert finding["pii_density"] == "low"
        assert finding["secrets_detected"] is False
        assert finding.get("sensitivity_scan_at") is not None
        assert float(finding["sensitivity_score"]) == 2.5

    @patch("handlers.detect_sensitivity.detect_pii")
    @patch("handlers.detect_sensitivity.detect_secrets")
    def test_scenario_secret_detection_flow(
        self, mock_secrets, mock_pii, e2e_resources
    ):
        """シナリオ 2: Secret 入りファイル → sensitivity_score = 5.0"""
        from handlers.detect_sensitivity import handler, set_finding_table, set_s3_client

        table = e2e_resources["table"]
        s3_client = e2e_resources["s3_client"]
        bucket = e2e_resources["bucket_name"]
        set_finding_table(table)
        set_s3_client(s3_client)

        _seed_finding(table, finding_id="finding-e2e-002", item_id="item-e2e-002")
        s3_client.put_object(
            Bucket=bucket,
            Key="raw/tenant-001/item-e2e-002/data.txt",
            Body=b"AKIAIOSFODNN7EXAMPLE\npassword=secret123!",
        )

        mock_pii.return_value = PIIDetectionResult()
        mock_secrets.return_value = SecretDetectionResult(
            detected=True,
            types=["aws_access_key", "generic_password"],
            count=2,
            details=[
                SecretEntity(type="aws_access_key", start=0, end=20),
                SecretEntity(type="generic_password", start=21, end=40),
            ],
        )

        event = {
            "Records": [
                {
                    "messageId": "e2e-msg-002",
                    "body": json.dumps({
                        "finding_id": "finding-e2e-002",
                        "tenant_id": "tenant-001",
                        "source": "m365",
                        "item_id": "item-e2e-002",
                        "item_name": "config.txt",
                        "mime_type": "text/plain",
                        "size": 40,
                        "raw_s3_key": "raw/tenant-001/item-e2e-002/data.txt",
                        "raw_s3_bucket": bucket,
                        "enqueued_at": "2026-02-10T08:32:00Z",
                        "trigger": "realtime",
                    }),
                }
            ]
        }

        result = handler(event, None)
        assert result["processed"] == 1

        finding = table.get_item(
            Key={"tenant_id": "tenant-001", "finding_id": "finding-e2e-002"}
        )["Item"]
        assert finding["secrets_detected"] is True
        assert float(finding["sensitivity_score"]) == 5.0

    def test_scenario_file_too_large_skip(self, e2e_resources):
        """シナリオ 3: ファイルサイズ超過 → スキップ"""
        from handlers.detect_sensitivity import handler, set_finding_table

        table = e2e_resources["table"]
        set_finding_table(table)
        _seed_finding(table, finding_id="finding-e2e-003")

        event = {
            "Records": [
                {
                    "messageId": "e2e-msg-003",
                    "body": json.dumps({
                        "finding_id": "finding-e2e-003",
                        "tenant_id": "tenant-001",
                        "source": "m365",
                        "item_id": "item-e2e-003",
                        "item_name": "huge_file.xlsx",
                        "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "size": 100_000_000,
                        "raw_s3_key": "raw/tenant-001/item-e2e-003/data.xlsx",
                        "raw_s3_bucket": e2e_resources["bucket_name"],
                        "enqueued_at": "2026-02-10T08:33:00Z",
                        "trigger": "realtime",
                    }),
                }
            ]
        }

        result = handler(event, None)
        assert result["processed"] == 1

        finding = table.get_item(
            Key={"tenant_id": "tenant-001", "finding_id": "finding-e2e-003"}
        )["Item"]
        assert finding["pii_detected"] is False
        assert finding.get("sensitivity_scan_at") is not None

    def test_scenario_unsupported_format_skip(self, e2e_resources):
        """シナリオ 4: 未対応形式 → スキップ"""
        from handlers.detect_sensitivity import handler, set_finding_table

        table = e2e_resources["table"]
        set_finding_table(table)
        _seed_finding(table, finding_id="finding-e2e-004")

        event = {
            "Records": [
                {
                    "messageId": "e2e-msg-004",
                    "body": json.dumps({
                        "finding_id": "finding-e2e-004",
                        "tenant_id": "tenant-001",
                        "source": "m365",
                        "item_id": "item-e2e-004",
                        "item_name": "photo.jpg",
                        "mime_type": "image/jpeg",
                        "size": 1000,
                        "raw_s3_key": "raw/tenant-001/item-e2e-004/photo.jpg",
                        "raw_s3_bucket": e2e_resources["bucket_name"],
                        "enqueued_at": "2026-02-10T08:34:00Z",
                        "trigger": "realtime",
                    }),
                }
            ]
        }

        result = handler(event, None)
        assert result["processed"] == 1
