"""AWS 実環境テスト用共通設定

Phase 8: AWS デプロイ検証テスト基盤
moto は使用しない — 実際の AWS リソースに対してテストを実行する。
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Generator

import boto3
import pytest

# ─── AWS 環境定数 ───
AWS_REGION = "ap-northeast-1"
AWS_ACCOUNT_ID = "565699611973"

# ─── リソース名 ───
FINDING_TABLE_NAME = "AIReadyGov-ExposureFinding"
DOCUMENT_ANALYSIS_TABLE_NAME = "AIReadyGov-DocumentAnalysis"
CONNECT_TABLE_NAME = "AIReadyConnect-FileMetadata"
SENSITIVITY_QUEUE_NAME = "AIReadyGov-SensitivityDetectionQueue"
ANALYZE_DLQ_NAME = "AIReadyGov-analyzeExposure-DLQ"
DETECT_DLQ_NAME = "AIReadyGov-detectSensitivity-DLQ"
REPORT_BUCKET = f"aireadygov-reports-{AWS_ACCOUNT_ID}"
RAW_PAYLOAD_BUCKET = f"aireadyconnect-raw-payload-{AWS_ACCOUNT_ID}"
VECTORS_BUCKET = f"aiready-{AWS_ACCOUNT_ID}-vectors"
ENTITY_RESOLUTION_QUEUE_PARAM = "/aiready/ontology/entity_resolution_queue_url"

# ─── Lambda 関数名 ───
ANALYZE_EXPOSURE_FN = "AIReadyGov-analyzeExposure"
DETECT_SENSITIVITY_FN = "AIReadyGov-detectSensitivity"
BATCH_SCORING_FN = "AIReadyGov-batchScoring"

# ─── EventBridge ───
BATCH_SCORING_RULE = "AIReadyGov-batchScoring-daily"

# ─── SSM パラメータ ───
SSM_PARAMETERS = {
    "/aiready/governance/risk_score_threshold": "2.0",
    "/aiready/governance/max_exposure_score": "10.0",
    "/aiready/governance/permissions_count_threshold": "50",
    "/aiready/governance/rescan_interval_days": "7",
    "/aiready/governance/max_file_size_bytes": "52428800",
    "/aiready/governance/max_text_length": "500000",
    "/aiready/governance/batch_scoring_hour_utc": "5",
}

# ─── テスト専用テナント ───
TEST_TENANT_ID = "test-tenant-dvt-001"
TEST_TENANT_ID_2 = "test-tenant-dvt-002"

# ─── CloudWatch アラーム ───
ALARM_NAMES = [
    "AIReadyGov-analyzeExposure-DLQ-NotEmpty",
    "AIReadyGov-detectSensitivity-DLQ-NotEmpty",
    "AIReadyGov-analyzeExposure-ErrorRate-High",
    "AIReadyGov-detectSensitivity-ErrorRate-High",
    "AIReadyGov-batchScoring-Duration-High",
]


# ─── AWS クライアントファクトリ ───


@pytest.fixture(scope="session")
def aws_region() -> str:
    return AWS_REGION


@pytest.fixture(scope="session")
def dynamodb_resource():
    return boto3.resource("dynamodb", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def dynamodb_client():
    return boto3.client("dynamodb", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def sqs_client():
    return boto3.client("sqs", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def lambda_client():
    return boto3.client("lambda", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def cloudwatch_client():
    return boto3.client("cloudwatch", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def logs_client():
    return boto3.client("logs", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def ssm_client():
    return boto3.client("ssm", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def events_client():
    return boto3.client("events", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def iam_client():
    return boto3.client("iam", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def ecr_client():
    return boto3.client("ecr", region_name=AWS_REGION)


# ─── テーブルフィクスチャ ───


@pytest.fixture(scope="session")
def finding_table(dynamodb_resource):
    return dynamodb_resource.Table(FINDING_TABLE_NAME)


@pytest.fixture(scope="session")
def document_analysis_table(dynamodb_resource):
    return dynamodb_resource.Table(DOCUMENT_ANALYSIS_TABLE_NAME)


@pytest.fixture(scope="session")
def connect_table(dynamodb_resource):
    return dynamodb_resource.Table(CONNECT_TABLE_NAME)


# ─── SQS URL 解決 ───


@pytest.fixture(scope="session")
def sensitivity_queue_url(sqs_client) -> str:
    resp = sqs_client.get_queue_url(QueueName=SENSITIVITY_QUEUE_NAME)
    return resp["QueueUrl"]


@pytest.fixture(scope="session")
def analyze_dlq_url(sqs_client) -> str:
    resp = sqs_client.get_queue_url(QueueName=ANALYZE_DLQ_NAME)
    return resp["QueueUrl"]


@pytest.fixture(scope="session")
def detect_dlq_url(sqs_client) -> str:
    resp = sqs_client.get_queue_url(QueueName=DETECT_DLQ_NAME)
    return resp["QueueUrl"]


@pytest.fixture(scope="session")
def entity_resolution_queue_url(ssm_client) -> str:
    resp = ssm_client.get_parameter(Name=ENTITY_RESOLUTION_QUEUE_PARAM)
    return resp["Parameter"]["Value"]


# ─── テストデータ生成ヘルパー ───


def make_file_metadata(
    tenant_id: str = TEST_TENANT_ID,
    item_id: str | None = None,
    *,
    source: str = "m365",
    drive_id: str | None = None,
    container_id: str = "site-test-001",
    container_name: str = "テスト部門サイト",
    container_type: str = "site",
    item_name: str = "テストファイル.docx",
    web_url: str = "https://contoso.sharepoint.com/test",
    sharing_scope: str = "organization",
    permissions: str = '{"entries": []}',
    permissions_count: int = 150,
    sensitivity_label: str | None = None,
    mime_type: str = "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    size: int = 2048000,
    is_deleted: bool = False,
    raw_s3_key: str | None = None,
) -> dict[str, Any]:
    if item_id is None:
        item_id = f"item-{uuid.uuid4().hex[:12]}"
    if drive_id is None:
        # AIReadyConnect-FileMetadata の PK は drive_id のため必須
        drive_id = f"drive-{tenant_id}"
    if raw_s3_key is None:
        raw_s3_key = f"raw/{tenant_id}/{item_id}/2026-02-23.json"
    now = datetime.now(timezone.utc).isoformat()
    return {
        "tenant_id": tenant_id,
        "drive_id": drive_id,
        "item_id": item_id,
        "source": source,
        "container_id": container_id,
        "container_name": container_name,
        "container_type": container_type,
        "item_name": item_name,
        "web_url": web_url,
        "sharing_scope": sharing_scope,
        "permissions": permissions,
        "permissions_count": permissions_count,
        "sensitivity_label": json.dumps({"id": "lbl-001", "name": sensitivity_label})
        if sensitivity_label
        else None,
        "mime_type": mime_type,
        "size": size,
        "modified_at": now,
        "is_deleted": is_deleted,
        "raw_s3_key": raw_s3_key,
    }


# ─── ポーリングヘルパー ───


def wait_for_finding(
    finding_table,
    tenant_id: str,
    finding_id: str,
    expected_field: str | None = None,
    expected_value: Any = None,
    max_wait: int = 300,
    interval: int = 10,
) -> dict | None:
    """Finding が生成/更新されるまでポーリングする。"""
    elapsed = 0
    while elapsed < max_wait:
        resp = finding_table.get_item(Key={"tenant_id": tenant_id, "finding_id": finding_id})
        item = resp.get("Item")
        if item:
            if expected_field is None:
                return item
            if item.get(expected_field) == expected_value:
                return item
        time.sleep(interval)
        elapsed += interval
    return None


def wait_for_finding_scan_completed(
    finding_table,
    tenant_id: str,
    finding_id: str,
    max_wait: int = 300,
    interval: int = 10,
) -> dict | None:
    """Finding の sensitivity_scan_at が設定されるまで待機する。"""
    elapsed = 0
    while elapsed < max_wait:
        resp = finding_table.get_item(Key={"tenant_id": tenant_id, "finding_id": finding_id})
        item = resp.get("Item")
        if item and item.get("sensitivity_scan_at"):
            return item
        time.sleep(interval)
        elapsed += interval
    return None


def wait_for_finding_by_item(
    finding_table,
    tenant_id: str,
    item_id: str,
    expected_status: str | None = None,
    max_wait: int = 300,
    interval: int = 10,
) -> dict | None:
    """item_id で Finding を GSI 検索し、期待ステータスになるまでポーリングする。"""
    elapsed = 0
    while elapsed < max_wait:
        resp = finding_table.query(
            IndexName="GSI-ItemFinding",
            KeyConditionExpression="item_id = :iid",
            ExpressionAttributeValues={":iid": item_id},
        )
        items = resp.get("Items", [])
        for item in items:
            if item.get("tenant_id") == tenant_id:
                if expected_status is None:
                    return item
                if item.get("status") == expected_status:
                    return item
        time.sleep(interval)
        elapsed += interval
    return None


def wait_for_sqs_empty(sqs_client, queue_url: str, max_wait: int = 120, interval: int = 10):
    """SQS キューが空になるまでポーリングする。"""
    elapsed = 0
    while elapsed < max_wait:
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        visible = int(attrs.get("ApproximateNumberOfMessages", "0"))
        not_visible = int(attrs.get("ApproximateNumberOfMessagesNotVisible", "0"))
        if visible == 0 and not_visible == 0:
            return True
        time.sleep(interval)
        elapsed += interval
    return False


def wait_for_document_analysis(
    document_analysis_table,
    tenant_id: str,
    item_id: str,
    expected_field: str | None = None,
    expected_value: Any = None,
    max_wait: int = 300,
    interval: int = 10,
) -> dict | None:
    """DocumentAnalysis レコードが作成/更新されるまでポーリングする。"""
    elapsed = 0
    while elapsed < max_wait:
        resp = document_analysis_table.get_item(
            Key={"tenant_id": tenant_id, "item_id": item_id}
        )
        item = resp.get("Item")
        if item:
            if expected_field is None:
                return item
            if item.get(expected_field) == expected_value:
                return item
        time.sleep(interval)
        elapsed += interval
    return None


def invoke_lambda(lambda_client, function_name: str, payload: dict | None = None) -> dict:
    """Lambda を同期 invoke して結果を返す。"""
    resp = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload or {}),
    )
    body = json.loads(resp["Payload"].read())
    return {"status_code": resp["StatusCode"], "body": body, "error": resp.get("FunctionError")}


# ─── クリーンアップ ───


def cleanup_findings(finding_table, tenant_id: str):
    """指定テナントの Finding を全削除する。"""
    resp = finding_table.query(
        KeyConditionExpression="tenant_id = :tid",
        ExpressionAttributeValues={":tid": tenant_id},
        ProjectionExpression="tenant_id, finding_id",
    )
    with finding_table.batch_writer() as batch:
        for item in resp.get("Items", []):
            batch.delete_item(Key={"tenant_id": item["tenant_id"], "finding_id": item["finding_id"]})


def cleanup_connect_items(connect_table, tenant_id: str):
    """指定テナントの FileMetadata を全削除する。"""
    try:
        resp = connect_table.query(
            IndexName="GSI-ModifiedAt",
            KeyConditionExpression="tenant_id = :tid",
            ExpressionAttributeValues={":tid": tenant_id},
            ProjectionExpression="drive_id, item_id",
        )
    except Exception:
        return
    with connect_table.batch_writer() as batch:
        for item in resp.get("Items", []):
            batch.delete_item(Key={"drive_id": item["drive_id"], "item_id": item["item_id"]})


def cleanup_document_analysis_items(document_analysis_table, tenant_id: str):
    """指定テナントの DocumentAnalysis を全削除する。"""
    try:
        resp = document_analysis_table.query(
            KeyConditionExpression="tenant_id = :tid",
            ExpressionAttributeValues={":tid": tenant_id},
            ProjectionExpression="tenant_id, item_id",
        )
    except Exception:
        return
    with document_analysis_table.batch_writer() as batch:
        for item in resp.get("Items", []):
            batch.delete_item(Key={"tenant_id": item["tenant_id"], "item_id": item["item_id"]})


def cleanup_s3_prefix(s3_client, bucket: str, prefix: str):
    """指定プレフィックスの S3 オブジェクトを全削除する。"""
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects = page.get("Contents", [])
            if objects:
                s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
                )
    except Exception:
        return


@pytest.fixture(autouse=True)
def cleanup_after_test(finding_table, document_analysis_table, connect_table, s3_client):
    """各テスト後にテストデータをクリーンアップする。"""
    yield
    for tid in [TEST_TENANT_ID, TEST_TENANT_ID_2]:
        cleanup_findings(finding_table, tid)
        cleanup_document_analysis_items(document_analysis_table, tid)
        cleanup_connect_items(connect_table, tid)
    cleanup_s3_prefix(s3_client, REPORT_BUCKET, f"{TEST_TENANT_ID}/")
    cleanup_s3_prefix(s3_client, REPORT_BUCKET, f"{TEST_TENANT_ID_2}/")
    cleanup_s3_prefix(s3_client, RAW_PAYLOAD_BUCKET, f"raw/{TEST_TENANT_ID}/")
    cleanup_s3_prefix(s3_client, RAW_PAYLOAD_BUCKET, f"raw/{TEST_TENANT_ID_2}/")
    cleanup_s3_prefix(s3_client, VECTORS_BUCKET, f"vectors/{TEST_TENANT_ID}/")
    cleanup_s3_prefix(s3_client, VECTORS_BUCKET, f"vectors/{TEST_TENANT_ID_2}/")
