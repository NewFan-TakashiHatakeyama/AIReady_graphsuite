"""batchScoring 結合テスト（T-024）

moto で AWS リソース一式を構築し、FileMetadata に 100 件のテストデータを投入後、
batchScoring を手動実行して以下を検証する:

  1. Finding が正しく生成・更新・クローズされる
  2. S3 にレポートが出力される
  3. 抑制期限切れの Finding が再評価される
  4. 削除済みアイテムの孤立 Finding がクローズされる
  5. 未スキャンアイテムが SQS に投入される
  6. マルチテナント処理が正常に動作する

詳細設計 11.2 節準拠。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

from handlers.batch_scoring import (
    handler,
    set_connect_table,
    set_finding_table,
    set_s3_client,
    set_sqs_client,
)
from services.finding_manager import (
    generate_finding_id,
    get_finding,
    set_finding_table as fm_set_finding_table,
)


# ─── Fixtures ───


@pytest.fixture
def e2e_resources():
    """E2E テスト用に全 AWS リソースを構築する。"""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")

        finding_table = dynamodb.create_table(
            TableName="AIReadyGov-ExposureFinding",
            KeySchema=[
                {"AttributeName": "tenant_id", "KeyType": "HASH"},
                {"AttributeName": "finding_id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "tenant_id", "AttributeType": "S"},
                {"AttributeName": "finding_id", "AttributeType": "S"},
                {"AttributeName": "item_id", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI-ItemFinding",
                    "KeySchema": [
                        {"AttributeName": "item_id", "KeyType": "HASH"},
                        {"AttributeName": "tenant_id", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "GSI-StatusFinding",
                    "KeySchema": [
                        {"AttributeName": "tenant_id", "KeyType": "HASH"},
                        {"AttributeName": "status", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        connect_table = dynamodb.create_table(
            TableName="AIReadyConnect-FileMetadata",
            KeySchema=[
                {"AttributeName": "tenant_id", "KeyType": "HASH"},
                {"AttributeName": "item_id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "tenant_id", "AttributeType": "S"},
                {"AttributeName": "item_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        sqs = boto3.client("sqs", region_name="ap-northeast-1")
        queue = sqs.create_queue(QueueName="AIReadyGov-SensitivityDetectionQueue")
        queue_url = queue["QueueUrl"]

        s3 = boto3.client("s3", region_name="ap-northeast-1")
        s3.create_bucket(
            Bucket="aireadygov-reports-123456789012",
            CreateBucketConfiguration={"LocationConstraint": "ap-northeast-1"},
        )

        set_finding_table(finding_table)
        set_connect_table(connect_table)
        set_sqs_client(sqs)
        set_s3_client(s3)
        fm_set_finding_table(finding_table)

        yield {
            "finding_table": finding_table,
            "connect_table": connect_table,
            "sqs": sqs,
            "s3": s3,
            "queue_url": queue_url,
        }

        set_finding_table(None)
        set_connect_table(None)
        set_sqs_client(None)
        set_s3_client(None)
        fm_set_finding_table(None)


def _make_context(remaining_ms: int = 900_000) -> MagicMock:
    ctx = MagicMock()
    ctx.get_remaining_time_in_millis.return_value = remaining_ms
    return ctx


def _insert_items(connect_table, count: int, tenant_id: str = "tenant-001") -> list[dict]:
    """connect_table に count 件のテストデータを投入する。"""
    now = datetime.now(timezone.utc)
    items = []
    sharing_scopes = ["anonymous", "organization", "specific"]
    item_names = [
        "給与一覧.xlsx", "契約書_A社.docx", "readme.txt",
        "人事評価.xlsx", "budget.pptx", "顧客リスト.csv",
        "meeting_notes.txt", "パスワード一覧.xlsx", "general.pdf",
        "report.docx",
    ]

    for i in range(count):
        scope = sharing_scopes[i % len(sharing_scopes)]
        name = item_names[i % len(item_names)]
        days_ago = (i * 3) % 120
        modified = (now - timedelta(days=days_ago)).isoformat()

        item = {
            "tenant_id": tenant_id,
            "item_id": f"item-{i:04d}",
            "source": "m365",
            "container_id": f"site-{i // 10:02d}",
            "container_name": f"部門サイト {i // 10}",
            "container_type": "site",
            "item_name": name,
            "web_url": f"https://contoso.sharepoint.com/{name}",
            "sharing_scope": scope,
            "permissions": "{}",
            "permissions_count": 10 + i,
            "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "size": 1024 * (i + 1),
            "modified_at": modified,
            "is_deleted": False,
            "raw_s3_key": f"raw/{tenant_id}/item-{i:04d}/data.json",
        }
        connect_table.put_item(Item=item)
        items.append(item)

    return items


def _insert_finding(
    finding_table,
    tenant_id: str = "tenant-001",
    item_id: str = "item-0001",
    source: str = "m365",
    status: str = "open",
    risk_score: float = 6.0,
    suppress_until: str | None = None,
    sensitivity_scan_at: str | None = None,
    **extra,
) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    finding_id = generate_finding_id(tenant_id, source, item_id)

    finding: dict[str, Any] = {
        "tenant_id": tenant_id,
        "finding_id": finding_id,
        "source": source,
        "item_id": item_id,
        "item_name": "test.docx",
        "item_url": "https://contoso.sharepoint.com/test.docx",
        "container_id": "site-01",
        "container_name": "テストサイト",
        "container_type": "site",
        "risk_score": Decimal(str(risk_score)),
        "risk_level": "medium",
        "exposure_score": Decimal("3.0"),
        "sensitivity_score": Decimal("1.0"),
        "activity_score": Decimal("2.0"),
        "ai_amplification": Decimal("1.0"),
        "exposure_vectors": ["org_link"],
        "sharing_scope": "organization",
        "permissions_summary": None,
        "sensitivity_label": None,
        "pii_detected": False,
        "pii_types": None,
        "pii_count": 0,
        "pii_density": "none",
        "secrets_detected": False,
        "secret_types": None,
        "sensitivity_scan_at": sensitivity_scan_at,
        "status": status,
        "matched_guards": ["G3"],
        "detected_at": now,
        "last_evaluated_at": now,
        "remediated_at": None,
        "suppress_until": suppress_until,
        "acknowledged_reason": extra.get("acknowledged_reason"),
        "acknowledged_by": extra.get("acknowledged_by"),
        "acknowledged_at": extra.get("acknowledged_at"),
        "evidence_s3_key": None,
        "source_metadata": None,
    }
    finding.update({k: v for k, v in extra.items() if k not in finding})
    finding_table.put_item(Item=finding)
    return finding


# ─── E2E テスト ───


class TestBatchScoringE2E:
    """batchScoring の E2E テスト。"""

    def test_100_items_finding_generation_and_report(self, e2e_resources):
        """シナリオ 1: 100 件のアイテム → Finding 生成 + レポート出力。"""
        connect_table = e2e_resources["connect_table"]
        _insert_items(connect_table, count=100)

        ctx = _make_context()
        result = handler({}, ctx)

        assert result["processed"] == 100
        assert result["errors"] == 0
        assert result["created"] > 0

        # S3 にレポートが出力されていることを確認
        s3 = e2e_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        obj = s3.get_object(
            Bucket="aireadygov-reports-123456789012",
            Key=f"tenant-001/daily/{today}.json",
        )
        report = json.loads(obj["Body"].read())

        assert report["tenant_id"] == "tenant-001"
        assert report["summary"]["total_items_scanned"] == 100
        assert report["summary"]["new_findings"] > 0
        assert report["summary"]["errors"] == 0

        # リスク分布が正しいこと
        total_risk_findings = sum(report["risk_distribution"].values())
        assert total_risk_findings == report["summary"]["new_findings"]

    def test_orphan_finding_closed(self, e2e_resources):
        """シナリオ 2: Finding はあるが FileMetadata にないアイテム → Closed。"""
        connect_table = e2e_resources["connect_table"]
        finding_table = e2e_resources["finding_table"]

        # FileMetadata には item-0001 のみ
        _insert_items(connect_table, count=1)

        # Finding テーブルに item-orphan を追加（FileMetadata にない）
        _insert_finding(finding_table, item_id="item-orphan", status="open")

        ctx = _make_context()
        result = handler({}, ctx)

        finding_id = generate_finding_id("tenant-001", "m365", "item-orphan")
        finding = get_finding("tenant-001", finding_id)
        assert finding["status"] == "closed"
        assert result["closed"] >= 1

    def test_suppression_expired_risk_remains(self, e2e_resources):
        """シナリオ 3: 抑制期限切れ + リスク残存 → open に戻る。"""
        connect_table = e2e_resources["connect_table"]
        finding_table = e2e_resources["finding_table"]

        # 高リスクアイテムを FileMetadata に追加
        connect_table.put_item(Item={
            "tenant_id": "tenant-001",
            "item_id": "item-suppressed",
            "source": "m365",
            "container_id": "site-01",
            "container_name": "法務部門",
            "container_type": "site",
            "item_name": "給与一覧.xlsx",
            "web_url": "https://contoso.sharepoint.com/salary.xlsx",
            "sharing_scope": "anonymous",
            "permissions": "{}",
            "permissions_count": 10,
            "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "size": 1024,
            "modified_at": datetime.now(timezone.utc).isoformat(),
            "is_deleted": False,
            "raw_s3_key": "raw/tenant-001/item-suppressed/data.json",
        })

        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        _insert_finding(
            finding_table,
            item_id="item-suppressed",
            status="acknowledged",
            suppress_until=yesterday,
            sensitivity_score=2.0,
            acknowledged_reason="テスト用の抑制理由",
            acknowledged_by="admin@example.com",
            acknowledged_at=(datetime.now(timezone.utc) - timedelta(days=30)).isoformat(),
        )

        ctx = _make_context()
        handler({}, ctx)

        finding_id = generate_finding_id("tenant-001", "m365", "item-suppressed")
        finding = get_finding("tenant-001", finding_id)
        assert finding["status"] == "open"
        assert finding.get("suppress_until") is None

    def test_suppression_expired_risk_resolved(self, e2e_resources):
        """シナリオ 4: 抑制期限切れ + リスク解消 → closed。"""
        connect_table = e2e_resources["connect_table"]
        finding_table = e2e_resources["finding_table"]

        old_date = (datetime.now(timezone.utc) - timedelta(days=200)).isoformat()
        connect_table.put_item(Item={
            "tenant_id": "tenant-001",
            "item_id": "item-resolved",
            "source": "m365",
            "container_id": "site-01",
            "container_name": "一般サイト",
            "container_type": "site",
            "item_name": "readme.txt",
            "web_url": "https://contoso.sharepoint.com/readme.txt",
            "sharing_scope": "specific",
            "permissions": "{}",
            "permissions_count": 5,
            "mime_type": "text/plain",
            "size": 256,
            "modified_at": old_date,
            "is_deleted": False,
            "raw_s3_key": "raw/tenant-001/item-resolved/data.json",
        })

        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        _insert_finding(
            finding_table,
            item_id="item-resolved",
            status="acknowledged",
            suppress_until=yesterday,
            sensitivity_score=1.0,
        )

        ctx = _make_context()
        handler({}, ctx)

        finding_id = generate_finding_id("tenant-001", "m365", "item-resolved")
        finding = get_finding("tenant-001", finding_id)
        assert finding["status"] == "closed"

    def test_unscanned_items_enqueued_to_sqs(self, e2e_resources):
        """シナリオ 5: sensitivity_scan_at がない Finding → SQS 投入。"""
        connect_table = e2e_resources["connect_table"]
        _insert_items(connect_table, count=5)

        ctx = _make_context()
        handler({}, ctx)

        sqs = e2e_resources["sqs"]
        queue_url = e2e_resources["queue_url"]
        messages = []
        while True:
            resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
            batch = resp.get("Messages", [])
            if not batch:
                break
            messages.extend(batch)
            for msg in batch:
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

        assert len(messages) > 0

        for msg in messages:
            body = json.loads(msg["Body"])
            assert "finding_id" in body
            assert "tenant_id" in body
            assert body["trigger"] == "batch"

    def test_multi_tenant_processing(self, e2e_resources):
        """シナリオ 6: 複数テナントの処理。"""
        connect_table = e2e_resources["connect_table"]
        _insert_items(connect_table, count=5, tenant_id="tenant-A")
        _insert_items(connect_table, count=3, tenant_id="tenant-B")

        ctx = _make_context()
        result = handler({}, ctx)

        assert result["processed"] == 8
        assert result["errors"] == 0

        # 各テナントのレポートが出力されている
        s3 = e2e_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        for tid in ("tenant-A", "tenant-B"):
            obj = s3.get_object(
                Bucket="aireadygov-reports-123456789012",
                Key=f"{tid}/daily/{today}.json",
            )
            report = json.loads(obj["Body"].read())
            assert report["tenant_id"] == tid

    def test_deleted_items_in_file_metadata(self, e2e_resources):
        """シナリオ 7: is_deleted=true のアイテムの Finding がクローズされる。"""
        connect_table = e2e_resources["connect_table"]
        finding_table = e2e_resources["finding_table"]

        connect_table.put_item(Item={
            "tenant_id": "tenant-001",
            "item_id": "item-deleted",
            "source": "m365",
            "container_id": "site-01",
            "container_name": "テストサイト",
            "container_type": "site",
            "item_name": "old_file.docx",
            "web_url": "https://contoso.sharepoint.com/old.docx",
            "sharing_scope": "anonymous",
            "permissions": "{}",
            "permissions_count": 10,
            "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "size": 1024,
            "modified_at": datetime.now(timezone.utc).isoformat(),
            "is_deleted": True,
            "raw_s3_key": "raw/tenant-001/item-deleted/data.json",
        })

        _insert_finding(finding_table, item_id="item-deleted", status="open")

        ctx = _make_context()
        result = handler({}, ctx)

        finding_id = generate_finding_id("tenant-001", "m365", "item-deleted")
        finding = get_finding("tenant-001", finding_id)
        assert finding["status"] == "closed"

    def test_report_contains_exposure_and_guard_distribution(self, e2e_resources):
        """シナリオ 8: レポートに exposure_vector / guard_match の分布が含まれる。"""
        connect_table = e2e_resources["connect_table"]
        _insert_items(connect_table, count=20)

        ctx = _make_context()
        handler({}, ctx)

        s3 = e2e_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        obj = s3.get_object(
            Bucket="aireadygov-reports-123456789012",
            Key=f"tenant-001/daily/{today}.json",
        )
        report = json.loads(obj["Body"].read())

        assert isinstance(report["exposure_vector_distribution"], dict)
        assert isinstance(report["guard_match_distribution"], dict)
        assert isinstance(report["suppression_summary"], dict)

    def test_report_top_containers(self, e2e_resources):
        """シナリオ 9: レポートの top_containers が正しくソートされている。"""
        connect_table = e2e_resources["connect_table"]
        _insert_items(connect_table, count=30)

        ctx = _make_context()
        handler({}, ctx)

        s3 = e2e_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        obj = s3.get_object(
            Bucket="aireadygov-reports-123456789012",
            Key=f"tenant-001/daily/{today}.json",
        )
        report = json.loads(obj["Body"].read())

        containers = report["top_containers"]
        if len(containers) > 1:
            counts = [c["finding_count"] for c in containers]
            assert counts == sorted(counts, reverse=True)

    def test_acknowledged_not_expired_skipped(self, e2e_resources):
        """シナリオ 10: 期限内の acknowledged Finding は変更されない。"""
        connect_table = e2e_resources["connect_table"]
        finding_table = e2e_resources["finding_table"]

        connect_table.put_item(Item={
            "tenant_id": "tenant-001",
            "item_id": "item-ack",
            "source": "m365",
            "container_id": "site-01",
            "container_name": "テストサイト",
            "container_type": "site",
            "item_name": "acknowledged.docx",
            "web_url": "https://contoso.sharepoint.com/ack.docx",
            "sharing_scope": "anonymous",
            "permissions": "{}",
            "permissions_count": 10,
            "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "size": 1024,
            "modified_at": datetime.now(timezone.utc).isoformat(),
            "is_deleted": False,
            "raw_s3_key": "raw/tenant-001/item-ack/data.json",
        })

        future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        _insert_finding(
            finding_table,
            item_id="item-ack",
            status="acknowledged",
            suppress_until=future,
        )

        ctx = _make_context()
        handler({}, ctx)

        finding_id = generate_finding_id("tenant-001", "m365", "item-ack")
        finding = get_finding("tenant-001", finding_id)
        assert finding["status"] == "acknowledged"
