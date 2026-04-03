"""AWS テストデータ生成スクリプト

テスト実行前に必要なシードデータを各 AWS リソースに投入する。
単独実行: python -m tests.aws.test_data.generate_test_data
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import boto3

AWS_REGION = "ap-northeast-1"

FINDING_TABLE_NAME = "AIReadyGov-ExposureFinding"
CONNECT_TABLE_NAME = "AIReadyConnect-FileMetadata"
RAW_PAYLOAD_BUCKET = "aireadyconnect-raw-payload"

TEST_TENANT_ID = "test-tenant-dvt-001"
TEST_TENANT_ID_2 = "test-tenant-dvt-002"

TEMPLATES_DIR = Path(__file__).parent / "metadata_templates"
FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _get_resources():
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    s3 = boto3.client("s3", region_name=AWS_REGION)
    return {
        "finding_table": dynamodb.Table(FINDING_TABLE_NAME),
        "connect_table": dynamodb.Table(CONNECT_TABLE_NAME),
        "s3": s3,
    }


def generate_file_metadata_records(connect_table, tenant_id: str, count: int = 10):
    """FileMetadata テーブルにテストレコードを投入する。"""
    now = datetime.now(timezone.utc).isoformat()
    items = []
    with connect_table.batch_writer() as batch:
        for i in range(count):
            item_id = f"item-seed-{uuid.uuid4().hex[:12]}"
            record = {
                "tenant_id": tenant_id,
                "item_id": item_id,
                "source": "m365",
                "container_id": f"site-seed-{i % 3:03d}",
                "container_name": f"テスト部門サイト{i % 3}",
                "container_type": "site",
                "item_name": f"テストファイル_{i:03d}.docx",
                "web_url": f"https://contoso.sharepoint.com/test/{item_id}",
                "sharing_scope": ["organization", "anonymous", "specific"][i % 3],
                "permissions": json.dumps({"entries": []}),
                "permissions_count": [150, 200, 5][i % 3],
                "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "size": 2048000,
                "modified_at": now,
                "is_deleted": False,
                "raw_s3_key": f"{tenant_id}/raw/{item_id}/2026-02-23.json",
            }
            batch.put_item(Item=record)
            items.append(record)
    return items


def generate_seed_findings(finding_table, tenant_id: str, items: list[dict]):
    """Finding テーブルにシードレコードを投入する。"""
    import hashlib

    now = datetime.now(timezone.utc).isoformat()
    with finding_table.batch_writer() as batch:
        for item in items:
            if item["sharing_scope"] == "specific":
                continue
            raw = f"{tenant_id}:{item['source']}:{item['item_id']}"
            finding_id = hashlib.sha256(raw.encode()).hexdigest()[:32]
            batch.put_item(Item={
                "tenant_id": tenant_id,
                "finding_id": finding_id,
                "source": item["source"],
                "item_id": item["item_id"],
                "item_name": item["item_name"],
                "container_id": item["container_id"],
                "container_name": item["container_name"],
                "container_type": item["container_type"],
                "status": "new",
                "exposure_score": Decimal("5.0"),
                "sensitivity_score": Decimal("1.0"),
                "activity_score": Decimal("1.0"),
                "risk_score": Decimal("5.0"),
                "matched_guards": ["G3"],
                "created_at": now,
                "last_evaluated_at": now,
            })


def upload_test_files(s3_client, tenant_id: str):
    """S3 にテスト用ファイルをアップロードする。"""
    test_files = {
        "pii_english.txt": b"John Smith john@example.com 555-123-4567",
        "pii_japanese.txt": "田中太郎 tanaka@example.com 090-1234-5678 個人番号 1234 5678 9012".encode("utf-8"),
        "secret_aws.txt": b"aws_access_key_id = AKIAIOSFODNN7EXAMPLE",
        "safe_document.txt": b"This is a safe document with no PII or secrets.",
    }

    for filename, content in test_files.items():
        key = f"{tenant_id}/raw/fixtures/{filename}"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET,
            Key=key,
            Body=content,
            ContentType="text/plain",
        )
        print(f"  Uploaded s3://{RAW_PAYLOAD_BUCKET}/{key}")

    for fixture_file in FIXTURES_DIR.glob("*"):
        if fixture_file.is_file():
            key = f"{tenant_id}/raw/fixtures/{fixture_file.name}"
            s3_client.put_object(
                Bucket=RAW_PAYLOAD_BUCKET,
                Key=key,
                Body=fixture_file.read_bytes(),
            )
            print(f"  Uploaded s3://{RAW_PAYLOAD_BUCKET}/{key}")


def main():
    print("=== AWS テストデータ生成開始 ===")
    resources = _get_resources()

    for tenant_id in [TEST_TENANT_ID, TEST_TENANT_ID_2]:
        print(f"\n--- テナント: {tenant_id} ---")

        print("  FileMetadata レコード生成...")
        items = generate_file_metadata_records(resources["connect_table"], tenant_id, count=10)
        print(f"  → {len(items)} 件投入完了")

        print("  Finding シードデータ生成...")
        generate_seed_findings(resources["finding_table"], tenant_id, items)
        print("  → シード Finding 投入完了")

        print("  S3 テストファイルアップロード...")
        upload_test_files(resources["s3"], tenant_id)

    print("\n=== テストデータ生成完了 ===")


if __name__ == "__main__":
    main()
