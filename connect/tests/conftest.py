"""共通テストフィクスチャ — pytest + moto

T-036: テスト基盤セットアップ
- AWS サービスモック (DynamoDB, S3, SSM, SNS, SQS)
- テスト用環境変数
- fixture JSON 読み込みヘルパー
"""

import json
import os
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# ── テスト用定数 ──
TEST_REGION = "ap-northeast-1"
TEST_TENANT_ID = "test-tenant"
TEST_DRIVE_ID = "b!test-drive-id-12345"
TEST_CLIENT_STATE = "test-client-state-secret"
TEST_TOPIC_ARN = f"arn:aws:sns:{TEST_REGION}:123456789012:AIReadyConnect-NotificationTopic"
TEST_QUEUE_URL = f"https://sqs.{TEST_REGION}.amazonaws.com/123456789012/AIReadyConnect-NotificationQueue"
TEST_RAW_BUCKET = "aireadyconnect-raw-payload-test"


@pytest.fixture
def fixtures_dir():
    """テスト用 fixtures ディレクトリのパス"""
    return FIXTURES_DIR


@pytest.fixture
def load_fixture():
    """JSON fixture ファイルを読み込むヘルパー"""
    def _load(filename: str) -> dict:
        filepath = FIXTURES_DIR / filename
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    return _load


@pytest.fixture(autouse=True)
def env_setup(monkeypatch):
    """テスト用の環境変数を設定"""
    monkeypatch.setenv("AWS_DEFAULT_REGION", TEST_REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("FILE_METADATA_TABLE", "AIReadyConnect-FileMetadata")
    monkeypatch.setenv("IDEMPOTENCY_TABLE", "AIReadyConnect-IdempotencyKeys")
    monkeypatch.setenv("DELTA_TOKENS_TABLE", "AIReadyConnect-DeltaTokens")
    monkeypatch.setenv("NOTIFICATION_TOPIC_ARN", TEST_TOPIC_ARN)
    monkeypatch.setenv("RAW_BUCKET", TEST_RAW_BUCKET)
    monkeypatch.setenv("TENANT_ID", TEST_TENANT_ID)

    # Config シングルトンをリセット
    import src.shared.config as config_mod
    config_mod._config = None
    yield
    config_mod._config = None


@pytest.fixture
def aws_mock():
    """moto で AWS サービスをモックする"""
    with mock_aws():
        # DynamoDB テーブル作成
        ddb = boto3.resource("dynamodb", region_name=TEST_REGION)

        ddb.create_table(
            TableName="AIReadyConnect-FileMetadata",
            KeySchema=[
                {"AttributeName": "drive_id", "KeyType": "HASH"},
                {"AttributeName": "item_id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "drive_id", "AttributeType": "S"},
                {"AttributeName": "item_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        ddb.create_table(
            TableName="AIReadyConnect-IdempotencyKeys",
            KeySchema=[
                {"AttributeName": "event_id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "event_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        ddb.create_table(
            TableName="AIReadyConnect-DeltaTokens",
            KeySchema=[
                {"AttributeName": "drive_id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "drive_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        # S3 バケット作成
        s3 = boto3.client("s3", region_name=TEST_REGION)
        s3.create_bucket(
            Bucket=TEST_RAW_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": TEST_REGION},
        )

        # SNS Topic
        sns = boto3.client("sns", region_name=TEST_REGION)
        sns.create_topic(Name="AIReadyConnect-NotificationTopic")

        # SQS Queue
        sqs = boto3.client("sqs", region_name=TEST_REGION)
        sqs.create_queue(QueueName="AIReadyConnect-NotificationQueue")

        yield {
            "ddb": ddb,
            "s3": s3,
            "sns": sns,
            "sqs": sqs,
            "region": TEST_REGION,
        }