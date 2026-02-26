"""detect_sensitivity ハンドラ単体テスト

moto で DynamoDB / S3 をモックし、detectSensitivity の処理フローを検証する。
Presidio / GiNZA は利用せず、pii_detector / secret_detector をモックパッチして
ハンドラのルーティング・分岐ロジックに集中する。
"""

from __future__ import annotations

import io
import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

from services.pii_detector import PIIDetectionResult, PIIEntity
from services.secret_detector import SecretDetectionResult, SecretEntity


# ─── fixtures ───


@pytest.fixture
def aws_resources():
    """DynamoDB テーブル + S3 バケットを moto でセットアップする。"""
    with mock_aws():
        region = "ap-northeast-1"
        dynamodb = boto3.resource("dynamodb", region_name=region)
        s3_client = boto3.client("s3", region_name=region)

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

        bucket_name = "aireadyconnect-raw-payload-123456789012"
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )

        yield {
            "table": table,
            "s3_client": s3_client,
            "bucket_name": bucket_name,
            "dynamodb": dynamodb,
        }


def _create_finding(table, tenant_id="t-001", finding_id="f-001", **overrides):
    """テスト用 Finding を DynamoDB に挿入する。"""
    item = {
        "tenant_id": tenant_id,
        "finding_id": finding_id,
        "source": "m365",
        "item_id": "item-001",
        "item_name": "test.docx",
        "exposure_score": Decimal("5.0"),
        "sensitivity_score": Decimal("1.0"),
        "activity_score": Decimal("2.0"),
        "ai_amplification": Decimal("1.0"),
        "risk_score": Decimal("10.0"),
        "risk_level": "medium",
        "status": "new",
        "exposure_vectors": ["public_link"],
        "matched_guards": ["G3"],
        "pii_detected": False,
        "pii_count": 0,
        "pii_density": "none",
        "secrets_detected": False,
        "sensitivity_scan_at": None,
    }
    item.update(overrides)
    table.put_item(Item=item)
    return item


def _upload_s3_file(s3_client, bucket, key, content):
    """テスト用ファイルを S3 にアップロードする。"""
    s3_client.put_object(Bucket=bucket, Key=key, Body=content)


def _build_sqs_event(message: dict) -> dict:
    """SQS イベントを組み立てる。"""
    return {
        "Records": [
            {
                "messageId": "msg-001",
                "body": json.dumps(message),
            }
        ]
    }


def _base_message(**overrides) -> dict:
    """基本 SQS メッセージを生成する。"""
    msg = {
        "finding_id": "f-001",
        "tenant_id": "t-001",
        "source": "m365",
        "item_id": "item-001",
        "item_name": "test.txt",
        "mime_type": "text/plain",
        "size": 100,
        "raw_s3_key": "raw/t-001/item-001/data.txt",
        "raw_s3_bucket": "aireadyconnect-raw-payload-123456789012",
        "enqueued_at": "2026-02-10T08:31:00Z",
        "trigger": "realtime",
    }
    msg.update(overrides)
    return msg


# ─── handler テスト ───


class TestProcessSensitivityScan:
    """process_sensitivity_scan の各分岐をテストする。"""

    def test_file_too_large_skips(self, aws_resources):
        """ファイルサイズ超過 → スキップ"""
        from handlers.detect_sensitivity import process_sensitivity_scan, set_finding_table

        table = aws_resources["table"]
        set_finding_table(table)
        _create_finding(table)

        message = _base_message(size=999_999_999)
        process_sensitivity_scan(message)

        finding = table.get_item(Key={"tenant_id": "t-001", "finding_id": "f-001"})["Item"]
        assert finding.get("sensitivity_scan_at") is not None
        assert finding["pii_detected"] is False

    def test_unsupported_format_skips(self, aws_resources):
        """未対応形式 → スキップ"""
        from handlers.detect_sensitivity import process_sensitivity_scan, set_finding_table

        table = aws_resources["table"]
        set_finding_table(table)
        _create_finding(table)

        message = _base_message(mime_type="application/octet-stream")
        process_sensitivity_scan(message)

        finding = table.get_item(Key={"tenant_id": "t-001", "finding_id": "f-001"})["Item"]
        assert finding.get("sensitivity_scan_at") is not None

    def test_empty_text_skips(self, aws_resources):
        """空テキスト → スキップ"""
        from handlers.detect_sensitivity import (
            process_sensitivity_scan,
            set_finding_table,
            set_s3_client,
        )

        table = aws_resources["table"]
        s3_client = aws_resources["s3_client"]
        bucket = aws_resources["bucket_name"]
        set_finding_table(table)
        set_s3_client(s3_client)
        _create_finding(table)
        _upload_s3_file(s3_client, bucket, "raw/t-001/item-001/data.txt", b"   ")

        message = _base_message()
        process_sensitivity_scan(message)

        finding = table.get_item(Key={"tenant_id": "t-001", "finding_id": "f-001"})["Item"]
        assert finding.get("sensitivity_scan_at") is not None

    @patch("handlers.detect_sensitivity.detect_pii")
    @patch("handlers.detect_sensitivity.detect_secrets")
    def test_normal_scan_updates_finding(
        self, mock_detect_secrets, mock_detect_pii, aws_resources
    ):
        """正常スキャン → PII 検出 → Finding 更新"""
        from handlers.detect_sensitivity import (
            process_sensitivity_scan,
            set_finding_table,
            set_s3_client,
        )

        table = aws_resources["table"]
        s3_client = aws_resources["s3_client"]
        bucket = aws_resources["bucket_name"]
        set_finding_table(table)
        set_s3_client(s3_client)
        _create_finding(table)
        _upload_s3_file(
            s3_client, bucket, "raw/t-001/item-001/data.txt",
            "田中太郎 の電話番号は 03-1234-5678 です。メール: tanaka@example.com".encode("utf-8"),
        )

        mock_detect_pii.return_value = PIIDetectionResult(
            detected=True,
            types=["PERSON_NAME_JA", "PHONE_NUMBER", "EMAIL_ADDRESS"],
            count=3,
            density="low",
            high_risk_detected=False,
            details=[
                PIIEntity(type="PERSON_NAME_JA", start=0, end=4, score=0.85),
                PIIEntity(type="PHONE_NUMBER", start=10, end=22, score=0.9),
                PIIEntity(type="EMAIL_ADDRESS", start=30, end=50, score=0.95),
            ],
        )
        mock_detect_secrets.return_value = SecretDetectionResult(
            detected=False, types=[], count=0, details=[]
        )

        message = _base_message()
        process_sensitivity_scan(message)

        finding = table.get_item(Key={"tenant_id": "t-001", "finding_id": "f-001"})["Item"]
        assert finding["pii_detected"] is True
        assert finding["pii_count"] == 3
        assert finding["pii_density"] == "low"
        assert finding["secrets_detected"] is False
        assert finding.get("sensitivity_scan_at") is not None
        assert float(finding["sensitivity_score"]) == 2.5
        assert float(finding["risk_score"]) == 25.0

    @patch("handlers.detect_sensitivity.detect_pii")
    @patch("handlers.detect_sensitivity.detect_secrets")
    def test_secret_detection_updates_score(
        self, mock_detect_secrets, mock_detect_pii, aws_resources
    ):
        """Secret 検出 → sensitivity_score = 5.0"""
        from handlers.detect_sensitivity import (
            process_sensitivity_scan,
            set_finding_table,
            set_s3_client,
        )

        table = aws_resources["table"]
        s3_client = aws_resources["s3_client"]
        bucket = aws_resources["bucket_name"]
        set_finding_table(table)
        set_s3_client(s3_client)
        _create_finding(table)
        _upload_s3_file(
            s3_client, bucket, "raw/t-001/item-001/data.txt",
            b"password = MySecretPassword123",
        )

        mock_detect_pii.return_value = PIIDetectionResult()
        mock_detect_secrets.return_value = SecretDetectionResult(
            detected=True,
            types=["generic_password"],
            count=1,
            details=[SecretEntity(type="generic_password", start=0, end=30)],
        )

        message = _base_message()
        process_sensitivity_scan(message)

        finding = table.get_item(Key={"tenant_id": "t-001", "finding_id": "f-001"})["Item"]
        assert finding["secrets_detected"] is True
        assert float(finding["sensitivity_score"]) == 5.0
        assert float(finding["risk_score"]) == 50.0

    @patch("handlers.detect_sensitivity.detect_pii")
    @patch("handlers.detect_sensitivity.detect_secrets")
    def test_high_risk_pii_updates_score(
        self, mock_detect_secrets, mock_detect_pii, aws_resources
    ):
        """高リスク PII（マイナンバー）検出 → sensitivity_score = 4.0"""
        from handlers.detect_sensitivity import (
            process_sensitivity_scan,
            set_finding_table,
            set_s3_client,
        )

        table = aws_resources["table"]
        s3_client = aws_resources["s3_client"]
        bucket = aws_resources["bucket_name"]
        set_finding_table(table)
        set_s3_client(s3_client)
        _create_finding(table)
        _upload_s3_file(
            s3_client, bucket, "raw/t-001/item-001/data.txt",
            "マイナンバー: 1234 5678 9012".encode("utf-8"),
        )

        mock_detect_pii.return_value = PIIDetectionResult(
            detected=True,
            types=["MY_NUMBER"],
            count=1,
            density="low",
            high_risk_detected=True,
            details=[PIIEntity(type="MY_NUMBER", start=0, end=12, score=0.95)],
        )
        mock_detect_secrets.return_value = SecretDetectionResult()

        message = _base_message()
        process_sensitivity_scan(message)

        finding = table.get_item(Key={"tenant_id": "t-001", "finding_id": "f-001"})["Item"]
        assert finding["pii_detected"] is True
        assert float(finding["sensitivity_score"]) == 4.0
        assert float(finding["risk_score"]) == 40.0


    @patch("handlers.detect_sensitivity.detect_pii")
    @patch("handlers.detect_sensitivity.detect_secrets")
    def test_pii_and_secret_combined_secret_takes_priority(
        self, mock_detect_secrets, mock_detect_pii, aws_resources
    ):
        """B6: PII + Secret 同時検出 → Secret 優先で sensitivity_score = 5.0"""
        from handlers.detect_sensitivity import (
            process_sensitivity_scan,
            set_finding_table,
            set_s3_client,
        )

        table = aws_resources["table"]
        s3_client = aws_resources["s3_client"]
        bucket = aws_resources["bucket_name"]
        set_finding_table(table)
        set_s3_client(s3_client)
        _create_finding(table)
        _upload_s3_file(
            s3_client, bucket, "raw/t-001/item-001/data.txt",
            "マイナンバー: 1234 5678 9012\npassword=MySecret123".encode("utf-8"),
        )

        mock_detect_pii.return_value = PIIDetectionResult(
            detected=True,
            types=["MY_NUMBER"],
            count=1,
            density="low",
            high_risk_detected=True,
            details=[PIIEntity(type="MY_NUMBER", start=0, end=12, score=0.95)],
        )
        mock_detect_secrets.return_value = SecretDetectionResult(
            detected=True,
            types=["generic_password"],
            count=1,
            details=[SecretEntity(type="generic_password", start=30, end=50)],
        )

        message = _base_message()
        process_sensitivity_scan(message)

        finding = table.get_item(Key={"tenant_id": "t-001", "finding_id": "f-001"})["Item"]
        assert finding["pii_detected"] is True
        assert finding["secrets_detected"] is True
        assert float(finding["sensitivity_score"]) == 5.0
        assert float(finding["risk_score"]) == 50.0

    def test_s3_download_failure_skips(self, aws_resources):
        """B7: S3 ダウンロード失敗（キー不存在）→ スキップ"""
        from handlers.detect_sensitivity import (
            process_sensitivity_scan,
            set_finding_table,
            set_s3_client,
        )

        table = aws_resources["table"]
        s3_client = aws_resources["s3_client"]
        set_finding_table(table)
        set_s3_client(s3_client)
        _create_finding(table)

        message = _base_message(raw_s3_key="raw/t-001/item-001/nonexistent.txt")
        process_sensitivity_scan(message)

        finding = table.get_item(Key={"tenant_id": "t-001", "finding_id": "f-001"})["Item"]
        assert finding.get("sensitivity_scan_at") is not None
        assert finding["pii_detected"] is False
        assert finding["secrets_detected"] is False

    @patch("handlers.detect_sensitivity.detect_pii")
    @patch("handlers.detect_sensitivity.detect_secrets")
    @patch("handlers.detect_sensitivity._run_document_analysis_extensions")
    def test_document_analysis_extension_runs_when_enabled(
        self, mock_ext, mock_detect_secrets, mock_detect_pii, aws_resources, monkeypatch
    ):
        """DOCUMENT_ANALYSIS_ENABLED=true で拡張フローが呼ばれる。"""
        from handlers.detect_sensitivity import (
            process_sensitivity_scan,
            set_finding_table,
            set_s3_client,
        )

        monkeypatch.setenv("DOCUMENT_ANALYSIS_ENABLED", "true")

        table = aws_resources["table"]
        s3_client = aws_resources["s3_client"]
        bucket = aws_resources["bucket_name"]
        set_finding_table(table)
        set_s3_client(s3_client)
        _create_finding(table)
        _upload_s3_file(
            s3_client, bucket, "raw/t-001/item-001/data.txt",
            "田中太郎の連絡先".encode("utf-8"),
        )

        mock_detect_pii.return_value = PIIDetectionResult(
            detected=True,
            types=["PERSON_NAME_JA"],
            count=1,
            density="low",
            high_risk_detected=False,
            details=[PIIEntity(type="PERSON_NAME_JA", start=0, end=4, score=0.85)],
        )
        mock_detect_secrets.return_value = SecretDetectionResult()

        message = _base_message()
        process_sensitivity_scan(message)

        mock_ext.assert_called_once()

    @patch("handlers.detect_sensitivity.detect_pii")
    @patch("handlers.detect_sensitivity.detect_secrets")
    @patch("services.finding_manager.close_finding")
    def test_auto_close_finding_below_threshold(
        self, mock_close, mock_detect_secrets, mock_detect_pii, aws_resources
    ):
        """B9: RiskScore < 閾値(2.0) → close_finding が呼ばれる"""
        from handlers.detect_sensitivity import (
            process_sensitivity_scan,
            set_finding_table,
            set_s3_client,
        )

        table = aws_resources["table"]
        s3_client = aws_resources["s3_client"]
        bucket = aws_resources["bucket_name"]
        set_finding_table(table)
        set_s3_client(s3_client)
        _create_finding(
            table,
            exposure_score=Decimal("0.5"),
            activity_score=Decimal("1.0"),
            ai_amplification=Decimal("1.0"),
            risk_score=Decimal("0.5"),
            risk_level="none",
        )
        _upload_s3_file(
            s3_client, bucket, "raw/t-001/item-001/data.txt",
            "田中太郎の連絡先".encode("utf-8"),
        )

        mock_detect_pii.return_value = PIIDetectionResult(
            detected=True,
            types=["PERSON_NAME_JA"],
            count=1,
            density="low",
            high_risk_detected=False,
            details=[PIIEntity(type="PERSON_NAME_JA", start=0, end=4, score=0.85)],
        )
        mock_detect_secrets.return_value = SecretDetectionResult()

        message = _base_message()
        process_sensitivity_scan(message)

        finding = table.get_item(Key={"tenant_id": "t-001", "finding_id": "f-001"})["Item"]
        assert float(finding["sensitivity_score"]) == 2.5
        assert float(finding["risk_score"]) == 1.25
        mock_close.assert_called_once_with("t-001", "f-001")


class TestHandler:
    """handler 関数のルーティングをテストする。"""

    @patch("handlers.detect_sensitivity.process_sensitivity_scan")
    def test_handler_routes_sqs_records(self, mock_process):
        from handlers.detect_sensitivity import handler

        message = _base_message()
        event = _build_sqs_event(message)
        result = handler(event, None)
        assert result["processed"] == 1
        assert result["errors"] == 0
        mock_process.assert_called_once()

    @patch("handlers.detect_sensitivity.process_sensitivity_scan")
    def test_handler_raises_on_error(self, mock_process):
        from handlers.detect_sensitivity import handler

        mock_process.side_effect = Exception("scan failed")
        message = _base_message()
        event = _build_sqs_event(message)
        with pytest.raises(Exception, match="scan failed"):
            handler(event, None)
