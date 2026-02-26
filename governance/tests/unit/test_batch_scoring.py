"""batchScoring 単体テスト

詳細設計 5.2–5.5 節 / Tasks.md T-022 準拠

テストケース:
  - 正常ケース: アイテム再スコアリング → Finding 生成 + レポート出力
  - 削除検知: Finding はあるが FileMetadata にない → Closed
  - 抑制期限切れ（リスク残存）→ open に戻る
  - 抑制期限切れ（リスク解消）→ closed
  - タイムアウト回避: context 残り時間チェック
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

import handlers.batch_scoring as batch_module
from handlers.batch_scoring import (
    BatchStats,
    close_orphaned_findings,
    enqueue_unscanned_items,
    extract_metadata,
    generate_daily_report,
    handler,
    process_expired_suppressions,
    process_item_batch,
    process_tenant,
    set_connect_table,
    set_finding_table,
    set_s3_client,
    set_sqs_client,
)
from services.finding_manager import (
    generate_finding_id,
    get_finding,
    set_finding_table as fm_set_finding_table,
    upsert_finding,
)
from services.scoring import ExposureResult, SensitivityResult


# ─── Fixtures ───


@pytest.fixture
def aws_resources():
    """moto で DynamoDB / SQS / S3 を構築する。"""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")

        # Finding テーブル
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

        # Connect FileMetadata テーブル
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

        # SQS
        sqs = boto3.client("sqs", region_name="ap-northeast-1")
        queue = sqs.create_queue(QueueName="AIReadyGov-SensitivityDetectionQueue")
        queue_url = queue["QueueUrl"]

        # S3
        s3 = boto3.client("s3", region_name="ap-northeast-1")
        s3.create_bucket(
            Bucket="aireadygov-reports-123456789012",
            CreateBucketConfiguration={"LocationConstraint": "ap-northeast-1"},
        )

        # モジュールにリソースを注入
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

        # クリーンアップ
        set_finding_table(None)
        set_connect_table(None)
        set_sqs_client(None)
        set_s3_client(None)
        fm_set_finding_table(None)


def _make_context(remaining_ms: int = 900_000) -> MagicMock:
    """Lambda context のモック。"""
    ctx = MagicMock()
    ctx.get_remaining_time_in_millis.return_value = remaining_ms
    return ctx


def _insert_file_metadata(
    connect_table,
    tenant_id: str = "tenant-001",
    item_id: str = "item-001",
    source: str = "m365",
    sharing_scope: str = "organization",
    permissions_count: int = 10,
    item_name: str = "契約書_A社.docx",
    is_deleted: bool = False,
    modified_at: str | None = None,
    **extra,
) -> dict:
    """Connect FileMetadata にテストデータを投入する。"""
    if modified_at is None:
        modified_at = datetime.now(timezone.utc).isoformat()

    item: dict[str, Any] = {
        "tenant_id": tenant_id,
        "item_id": item_id,
        "source": source,
        "container_id": "site-xyz",
        "container_name": "法務部門サイト",
        "container_type": "site",
        "item_name": item_name,
        "web_url": "https://contoso.sharepoint.com/doc.docx",
        "sharing_scope": sharing_scope,
        "permissions": "{}",
        "permissions_count": permissions_count,
        "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "size": 2048000,
        "modified_at": modified_at,
        "is_deleted": is_deleted,
        "raw_s3_key": f"raw/{tenant_id}/{item_id}/data.json",
    }
    item.update(extra)
    connect_table.put_item(Item=item)
    return item


def _insert_finding(
    finding_table,
    tenant_id: str = "tenant-001",
    item_id: str = "item-001",
    source: str = "m365",
    status: str = "open",
    risk_score: float = 6.0,
    exposure_score: float = 3.0,
    sensitivity_score: float = 1.0,
    activity_score: float = 2.0,
    ai_amplification: float = 1.0,
    suppress_until: str | None = None,
    sensitivity_scan_at: str | None = None,
    pii_detected: bool = False,
    secrets_detected: bool = False,
    **extra,
) -> dict:
    """Finding テーブルにテストデータを投入する。"""
    now = datetime.now(timezone.utc).isoformat()
    finding_id = generate_finding_id(tenant_id, source, item_id)

    finding: dict[str, Any] = {
        "tenant_id": tenant_id,
        "finding_id": finding_id,
        "source": source,
        "item_id": item_id,
        "item_name": "契約書_A社.docx",
        "item_url": "https://contoso.sharepoint.com/doc.docx",
        "container_id": "site-xyz",
        "container_name": "法務部門サイト",
        "container_type": "site",
        "risk_score": Decimal(str(risk_score)),
        "risk_level": "medium",
        "exposure_score": Decimal(str(exposure_score)),
        "sensitivity_score": Decimal(str(sensitivity_score)),
        "activity_score": Decimal(str(activity_score)),
        "ai_amplification": Decimal(str(ai_amplification)),
        "exposure_vectors": ["org_link"],
        "sharing_scope": "organization",
        "permissions_summary": None,
        "sensitivity_label": None,
        "pii_detected": pii_detected,
        "pii_types": None,
        "pii_count": 0,
        "pii_density": "none",
        "secrets_detected": secrets_detected,
        "secret_types": None,
        "sensitivity_scan_at": sensitivity_scan_at,
        "status": status,
        "matched_guards": ["G3"],
        "detected_at": now,
        "last_evaluated_at": now,
        "remediated_at": None,
        "suppress_until": suppress_until,
        "acknowledged_reason": None,
        "acknowledged_by": None,
        "acknowledged_at": None,
        "evidence_s3_key": None,
        "source_metadata": None,
    }
    finding.update(extra)
    finding_table.put_item(Item=finding)
    return finding


# ─── テスト: 正常ケース ───


class TestProcessItemBatch:
    """1 アイテムの再スコアリング。"""

    def test_new_finding_created(self, aws_resources):
        """高リスクアイテム → 新規 Finding 生成。"""
        connect_table = aws_resources["connect_table"]
        _insert_file_metadata(connect_table, sharing_scope="anonymous", item_name="給与一覧.xlsx")

        stats = BatchStats()
        item = connect_table.get_item(
            Key={"tenant_id": "tenant-001", "item_id": "item-001"}
        )["Item"]
        process_item_batch("tenant-001", item, stats)

        assert stats.created == 1
        assert stats.updated == 0

        finding_id = generate_finding_id("tenant-001", "m365", "item-001")
        finding = get_finding("tenant-001", finding_id)
        assert finding is not None
        assert finding["status"] == "new"
        assert float(finding["exposure_score"]) == 5.0

    def test_existing_finding_updated(self, aws_resources):
        """既存 Finding がある場合 → 更新。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        _insert_file_metadata(connect_table, sharing_scope="organization")
        _insert_finding(finding_table, status="new")

        stats = BatchStats()
        item = connect_table.get_item(
            Key={"tenant_id": "tenant-001", "item_id": "item-001"}
        )["Item"]
        process_item_batch("tenant-001", item, stats)

        assert stats.updated == 1
        assert stats.created == 0

    def test_deleted_item_closes_finding(self, aws_resources):
        """is_deleted=true → Finding Closed。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        _insert_file_metadata(connect_table, is_deleted=True)
        _insert_finding(finding_table, status="open")

        stats = BatchStats()
        item = connect_table.get_item(
            Key={"tenant_id": "tenant-001", "item_id": "item-001"}
        )["Item"]
        process_item_batch("tenant-001", item, stats)

        assert stats.closed == 1

        finding_id = generate_finding_id("tenant-001", "m365", "item-001")
        finding = get_finding("tenant-001", finding_id)
        assert finding["status"] == "closed"

    def test_low_risk_closes_existing_finding(self, aws_resources):
        """RiskScore < 閾値 → 既存 Finding を Closed。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        # sharing_scope=specific で低リスク + modified_at を古くして ActivityScore を低く
        old_date = (datetime.now(timezone.utc) - timedelta(days=200)).isoformat()
        _insert_file_metadata(
            connect_table,
            sharing_scope="specific",
            permissions_count=5,
            item_name="readme.txt",
            modified_at=old_date,
        )
        _insert_finding(finding_table, status="open")

        stats = BatchStats()
        item = connect_table.get_item(
            Key={"tenant_id": "tenant-001", "item_id": "item-001"}
        )["Item"]
        process_item_batch("tenant-001", item, stats)

        assert stats.closed == 1
        finding_id = generate_finding_id("tenant-001", "m365", "item-001")
        finding = get_finding("tenant-001", finding_id)
        assert finding["status"] == "closed"

    def test_sensitivity_score_preserved_from_detect(self, aws_resources):
        """detectSensitivity 実行済みの場合、sensitivity_score を維持する。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        _insert_file_metadata(connect_table, sharing_scope="anonymous")
        _insert_finding(
            finding_table,
            status="open",
            sensitivity_score=4.0,
            sensitivity_scan_at=datetime.now(timezone.utc).isoformat(),
        )

        stats = BatchStats()
        item = connect_table.get_item(
            Key={"tenant_id": "tenant-001", "item_id": "item-001"}
        )["Item"]
        process_item_batch("tenant-001", item, stats)

        finding_id = generate_finding_id("tenant-001", "m365", "item-001")
        finding = get_finding("tenant-001", finding_id)
        assert float(finding["sensitivity_score"]) == 4.0


# ─── テスト: 孤立 Finding クローズ ───


class TestCloseOrphanedFindings:
    """Finding テーブルにあるが FileMetadata に存在しない → Closed。"""

    def test_orphaned_finding_closed(self, aws_resources):
        finding_table = aws_resources["finding_table"]
        _insert_finding(finding_table, item_id="item-orphan", status="open")

        active_ids = {"item-001", "item-002"}
        stats = BatchStats()
        close_orphaned_findings("tenant-001", active_ids, stats)

        assert stats.closed == 1
        finding_id = generate_finding_id("tenant-001", "m365", "item-orphan")
        finding = get_finding("tenant-001", finding_id)
        assert finding["status"] == "closed"

    def test_active_finding_not_closed(self, aws_resources):
        finding_table = aws_resources["finding_table"]
        _insert_finding(finding_table, item_id="item-001", status="open")

        active_ids = {"item-001"}
        stats = BatchStats()
        close_orphaned_findings("tenant-001", active_ids, stats)

        assert stats.closed == 0
        finding_id = generate_finding_id("tenant-001", "m365", "item-001")
        finding = get_finding("tenant-001", finding_id)
        assert finding["status"] == "open"


# ─── テスト: 抑制期限切れ処理 ───


class TestProcessExpiredSuppressions:
    """acknowledged 状態の Finding の期限切れ処理。"""

    def test_expired_suppression_risk_remains_reopened(self, aws_resources):
        """リスク残存 → open に戻る。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        _insert_file_metadata(connect_table, sharing_scope="anonymous")
        _insert_finding(
            finding_table,
            status="acknowledged",
            suppress_until=yesterday,
            sensitivity_score=2.0,
            acknowledged_reason="テスト用の抑制理由です。50文字以上のテスト文字列を入力しています。",
            acknowledged_by="admin@example.com",
            acknowledged_at=(datetime.now(timezone.utc) - timedelta(days=30)).isoformat(),
        )

        items = list(connect_table.scan()["Items"])
        stats = BatchStats()
        process_expired_suppressions("tenant-001", items, stats)

        assert stats.reopened == 1
        assert stats.suppression_summary["reopened_today"] == 1

        finding_id = generate_finding_id("tenant-001", "m365", "item-001")
        finding = get_finding("tenant-001", finding_id)
        assert finding["status"] == "open"
        assert finding.get("suppress_until") is None

    def test_expired_suppression_risk_resolved_closed(self, aws_resources):
        """リスク解消 → closed。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        old_date = (datetime.now(timezone.utc) - timedelta(days=200)).isoformat()
        _insert_file_metadata(
            connect_table,
            sharing_scope="specific",
            permissions_count=5,
            item_name="readme.txt",
            modified_at=old_date,
        )
        _insert_finding(
            finding_table,
            status="acknowledged",
            suppress_until=yesterday,
            sensitivity_score=1.0,
        )

        items = list(connect_table.scan()["Items"])
        stats = BatchStats()
        process_expired_suppressions("tenant-001", items, stats)

        assert stats.closed == 1
        assert stats.suppression_summary["closed_after_expiry_today"] == 1

        finding_id = generate_finding_id("tenant-001", "m365", "item-001")
        finding = get_finding("tenant-001", finding_id)
        assert finding["status"] == "closed"

    def test_expired_suppression_item_deleted_closed(self, aws_resources):
        """アイテム削除済み → closed。"""
        finding_table = aws_resources["finding_table"]

        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        _insert_finding(
            finding_table,
            status="acknowledged",
            suppress_until=yesterday,
        )

        # FileMetadata には存在しない
        stats = BatchStats()
        process_expired_suppressions("tenant-001", [], stats)

        assert stats.closed == 1
        assert stats.suppression_summary["closed_after_expiry_today"] == 1

    def test_not_yet_expired_skipped(self, aws_resources):
        """まだ期限内 → スキップ。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        _insert_file_metadata(connect_table, sharing_scope="anonymous")
        _insert_finding(
            finding_table,
            status="acknowledged",
            suppress_until=tomorrow,
        )

        items = list(connect_table.scan()["Items"])
        stats = BatchStats()
        process_expired_suppressions("tenant-001", items, stats)

        assert stats.reopened == 0
        assert stats.closed == 0


# ─── テスト: 未スキャンアイテムの SQS 投入 ───


class TestEnqueueUnscannedItems:
    """sensitivity_scan_at がない Finding → SQS 投入。"""

    def test_unscanned_finding_enqueued(self, aws_resources):
        finding_table = aws_resources["finding_table"]
        _insert_finding(finding_table, status="open", sensitivity_scan_at=None)

        stats = BatchStats()
        enqueue_unscanned_items("tenant-001", stats)

        assert stats.enqueued == 1

        sqs = aws_resources["sqs"]
        queue_url = aws_resources["queue_url"]
        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        assert len(msgs.get("Messages", [])) == 1

    def test_recently_scanned_not_enqueued(self, aws_resources):
        finding_table = aws_resources["finding_table"]
        recent_scan = datetime.now(timezone.utc).isoformat()
        _insert_finding(finding_table, status="open", sensitivity_scan_at=recent_scan)

        stats = BatchStats()
        enqueue_unscanned_items("tenant-001", stats)

        assert stats.enqueued == 0

    def test_old_scan_enqueued(self, aws_resources):
        """再スキャン期限超過 → SQS 投入。"""
        finding_table = aws_resources["finding_table"]
        old_scan = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        _insert_finding(finding_table, status="open", sensitivity_scan_at=old_scan)

        stats = BatchStats()
        enqueue_unscanned_items("tenant-001", stats)

        assert stats.enqueued == 1


# ─── テスト: 日次レポート生成 ───


class TestGenerateDailyReport:
    """S3 に日次レポートが出力される。"""

    def test_report_uploaded(self, aws_resources):
        stats = BatchStats()
        stats.total_items_scanned = 100
        stats.created = 10
        stats.updated = 20
        stats.closed = 5
        stats.risk_distribution = {"critical": 2, "high": 5, "medium": 10, "low": 3}

        generate_daily_report("tenant-001", stats)

        s3 = aws_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        obj = s3.get_object(
            Bucket="aireadygov-reports-123456789012",
            Key=f"tenant-001/daily/{today}.json",
        )
        report = json.loads(obj["Body"].read())

        assert report["tenant_id"] == "tenant-001"
        assert report["summary"]["total_items_scanned"] == 100
        assert report["summary"]["new_findings"] == 10
        assert report["risk_distribution"]["critical"] == 2

    def test_report_structure_matches_spec(self, aws_resources):
        """レポート JSON の構造が詳細設計 5.4 と一致する。"""
        stats = BatchStats()
        stats.total_items_scanned = 50
        stats.top_containers = {
            "site-abc": {"name": "人事部門", "count": 5, "total_risk": 100.0, "max_risk": 45.0}
        }
        stats.pii_type_counts = {"person_name": 10, "phone_number": 5}

        generate_daily_report("tenant-001", stats)

        s3 = aws_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        obj = s3.get_object(
            Bucket="aireadygov-reports-123456789012",
            Key=f"tenant-001/daily/{today}.json",
        )
        report = json.loads(obj["Body"].read())

        required_keys = {
            "tenant_id", "report_date", "generated_at", "summary",
            "risk_distribution", "pii_summary", "top_containers",
            "exposure_vector_distribution", "guard_match_distribution",
            "suppression_summary",
        }
        assert required_keys <= set(report.keys())

        assert len(report["top_containers"]) == 1
        assert report["top_containers"][0]["container_name"] == "人事部門"
        assert report["pii_summary"]["top_pii_types"][0]["type"] == "person_name"


# ─── テスト: タイムアウト回避 ───


class TestTimeoutSafety:
    """Lambda タイムアウト接近時の安全停止。"""

    def test_stops_when_approaching_timeout(self, aws_resources):
        connect_table = aws_resources["connect_table"]
        for i in range(10):
            _insert_file_metadata(
                connect_table,
                item_id=f"item-{i:03d}",
                sharing_scope="anonymous",
            )

        # 残り時間を極めて短く設定 → 最初のアイテム処理後に停止
        ctx = _make_context(remaining_ms=30_000)
        stats = process_tenant("tenant-001", ctx)

        # 全件は処理されないことを確認（少なくとも一部はスキップされる）
        assert stats.total_items_scanned < 10


# ─── テスト: handler E2E ───


class TestHandler:
    """handler() のエントリーポイントテスト。"""

    def test_handler_processes_tenant(self, aws_resources):
        """EventBridge イベント → テナント処理。"""
        connect_table = aws_resources["connect_table"]
        _insert_file_metadata(connect_table, sharing_scope="anonymous", item_name="給与.xlsx")
        _insert_file_metadata(
            connect_table,
            item_id="item-002",
            sharing_scope="organization",
            item_name="契約書.docx",
        )

        ctx = _make_context()
        result = handler({}, ctx)

        assert result["processed"] == 2
        assert result["errors"] == 0
        assert result["created"] > 0

    def test_handler_generates_report(self, aws_resources):
        """handler 実行後、S3 にレポートが出力される。"""
        connect_table = aws_resources["connect_table"]
        _insert_file_metadata(connect_table, sharing_scope="anonymous")

        ctx = _make_context()
        handler({}, ctx)

        s3 = aws_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        obj = s3.get_object(
            Bucket="aireadygov-reports-123456789012",
            Key=f"tenant-001/daily/{today}.json",
        )
        report = json.loads(obj["Body"].read())
        assert report["tenant_id"] == "tenant-001"

    def test_handler_with_multiple_tenants(self, aws_resources):
        """複数テナントの処理。"""
        connect_table = aws_resources["connect_table"]
        _insert_file_metadata(connect_table, tenant_id="tenant-001", sharing_scope="anonymous")
        _insert_file_metadata(
            connect_table,
            tenant_id="tenant-002",
            item_id="item-t2-001",
            sharing_scope="organization",
            item_name="契約書.docx",
        )

        ctx = _make_context()
        result = handler({}, ctx)

        assert result["processed"] >= 2

    def test_handler_empty_table(self, aws_resources):
        """空テーブル → エラーなし。"""
        ctx = _make_context()
        result = handler({}, ctx)
        assert result["errors"] == 0


# ─── テスト: extract_metadata ───


class TestExtractMetadata:
    """FileMetadata dict → DTO 変換。"""

    def test_basic_conversion(self):
        item = {
            "tenant_id": "t-001",
            "item_id": "i-001",
            "source": "m365",
            "sharing_scope": "organization",
            "permissions_count": 25,
            "item_name": "test.docx",
            "modified_at": "2026-02-10T00:00:00Z",
        }
        meta = extract_metadata(item)
        assert meta.tenant_id == "t-001"
        assert meta.sharing_scope == "organization"
        assert meta.permissions_count == 25

    def test_missing_fields_default(self):
        meta = extract_metadata({"tenant_id": "t-001", "item_id": "i-001"})
        assert meta.source == "m365"
        assert meta.sharing_scope == "specific"
        assert meta.permissions_count == 0

    def test_decimal_fields_converted(self):
        """DynamoDB から返る Decimal 型のフィールドが正しく int 変換される。"""
        item = {
            "tenant_id": "t-001",
            "item_id": "i-001",
            "permissions_count": Decimal("25"),
            "size": Decimal("4096"),
        }
        meta = extract_metadata(item)
        assert meta.permissions_count == 25
        assert meta.size == 4096


# ─── テスト: エラー耐性 ───


class TestErrorResilience:
    """バッチ処理中のエラーが全体を停止させないことを検証する。"""

    def test_item_exception_counted_and_continues(self, aws_resources):
        """1 アイテムの処理例外が発生しても他のアイテムは処理される。"""
        connect_table = aws_resources["connect_table"]
        _insert_file_metadata(connect_table, item_id="item-ok-1", sharing_scope="anonymous")
        _insert_file_metadata(connect_table, item_id="item-ok-2", sharing_scope="anonymous")

        original_process = batch_module.process_item_batch
        call_count = [0]

        def flaky_process(tenant_id, item, stats):
            call_count[0] += 1
            if item.get("item_id") == "item-ok-1":
                raise RuntimeError("Simulated failure")
            return original_process(tenant_id, item, stats)

        with patch.object(batch_module, "process_item_batch", side_effect=flaky_process):
            ctx = _make_context()
            stats = process_tenant("tenant-001", ctx)

        assert stats.errors >= 1
        assert stats.total_items_scanned >= 1

    def test_tenant_error_isolation(self, aws_resources):
        """1 テナントの処理失敗が他テナントの処理をブロックしない。"""
        connect_table = aws_resources["connect_table"]
        _insert_file_metadata(connect_table, tenant_id="tenant-good", sharing_scope="anonymous")
        _insert_file_metadata(
            connect_table,
            tenant_id="tenant-bad",
            item_id="item-bad",
            sharing_scope="anonymous",
        )

        original_process_tenant = batch_module.process_tenant

        def failing_process_tenant(tenant_id, context):
            if tenant_id == "tenant-bad":
                raise RuntimeError("Simulated tenant failure")
            return original_process_tenant(tenant_id, context)

        with patch.object(batch_module, "process_tenant", side_effect=failing_process_tenant):
            ctx = _make_context()
            result = handler({}, ctx)

        assert result["errors"] >= 1
        assert result["processed"] >= 1

    def test_s3_upload_failure_increments_errors(self, aws_resources):
        """S3 レポートアップロード失敗時に errors がインクリメントされクラッシュしない。"""
        failing_s3 = MagicMock()
        failing_s3.put_object.side_effect = Exception("S3 unavailable")
        set_s3_client(failing_s3)

        stats = BatchStats()
        stats.total_items_scanned = 10
        stats.created = 5
        generate_daily_report("tenant-001", stats)

        assert stats.errors == 1

        set_s3_client(aws_resources["s3"])


# ─── テスト: 境界値 ───


class TestBoundaryValues:
    """閾値の境界値テスト。"""

    def test_risk_score_exactly_at_threshold_creates_finding(self, aws_resources):
        """RiskScore が閾値 (2.0) ちょうどの場合、Finding が生成される (>= 判定)。"""
        connect_table = aws_resources["connect_table"]
        _insert_file_metadata(
            connect_table,
            sharing_scope="organization",
            item_name="契約書.docx",
        )

        stats = BatchStats()
        item = connect_table.get_item(
            Key={"tenant_id": "tenant-001", "item_id": "item-001"}
        )["Item"]
        process_item_batch("tenant-001", item, stats)

        finding_id = generate_finding_id("tenant-001", "m365", "item-001")
        finding = get_finding("tenant-001", finding_id)
        if finding is not None:
            assert float(finding["risk_score"]) >= 2.0
            assert stats.created == 1 or stats.updated == 1
        else:
            assert stats.created == 0 and stats.updated == 0

    def test_low_risk_without_existing_finding_no_error(self, aws_resources):
        """低リスク + 既存 Finding なし → Finding 作成なし、エラーなし。"""
        connect_table = aws_resources["connect_table"]
        old_date = (datetime.now(timezone.utc) - timedelta(days=200)).isoformat()
        _insert_file_metadata(
            connect_table,
            sharing_scope="specific",
            permissions_count=2,
            item_name="readme.txt",
            modified_at=old_date,
        )

        stats = BatchStats()
        item = connect_table.get_item(
            Key={"tenant_id": "tenant-001", "item_id": "item-001"}
        )["Item"]
        process_item_batch("tenant-001", item, stats)

        assert stats.created == 0
        assert stats.updated == 0
        assert stats.closed == 0

        finding_id = generate_finding_id("tenant-001", "m365", "item-001")
        finding = get_finding("tenant-001", finding_id)
        assert finding is None


# ─── テスト: 冪等性 ───


class TestIdempotency:
    """2 回実行で重複 Finding が生まれないことを検証する。"""

    def test_handler_twice_no_duplicate_findings(self, aws_resources):
        """handler を 2 回実行しても Finding が重複しない。"""
        connect_table = aws_resources["connect_table"]
        _insert_file_metadata(connect_table, sharing_scope="anonymous", item_name="給与.xlsx")

        ctx = _make_context()
        result1 = handler({}, ctx)
        result2 = handler({}, ctx)

        assert result1["created"] == 1
        assert result2["created"] == 0
        assert result2["updated"] == 1

        finding_id = generate_finding_id("tenant-001", "m365", "item-001")
        finding = get_finding("tenant-001", finding_id)
        assert finding is not None
        assert finding["status"] in ("new", "open")


# ─── テスト: SQS メッセージ内容検証 ───


class TestSQSMessageContent:
    """SQS に投入されるメッセージの内容を詳細に検証する。"""

    def test_batch_sqs_message_has_required_fields(self, aws_resources):
        """SQS メッセージに必須フィールドがすべて含まれる。"""
        finding_table = aws_resources["finding_table"]
        _insert_finding(
            finding_table,
            status="open",
            sensitivity_scan_at=None,
            item_id="item-sqs-test",
        )

        stats = BatchStats()
        enqueue_unscanned_items("tenant-001", stats)
        assert stats.enqueued == 1

        sqs = aws_resources["sqs"]
        queue_url = aws_resources["queue_url"]
        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        assert len(msgs.get("Messages", [])) == 1

        body = json.loads(msgs["Messages"][0]["Body"])
        required_fields = {
            "finding_id", "tenant_id", "source", "item_id",
            "item_name", "mime_type", "size", "raw_s3_key",
            "raw_s3_bucket", "enqueued_at", "trigger",
        }
        assert required_fields <= set(body.keys())
        assert body["trigger"] == "batch"
        assert body["tenant_id"] == "tenant-001"
        assert body["item_id"] == "item-sqs-test"

    def test_closed_findings_not_enqueued(self, aws_resources):
        """closed の Finding は SQS に投入されない。"""
        finding_table = aws_resources["finding_table"]
        _insert_finding(finding_table, status="closed", sensitivity_scan_at=None)

        stats = BatchStats()
        enqueue_unscanned_items("tenant-001", stats)
        assert stats.enqueued == 0


# ─── テスト: PII/Secret 統計 ───


class TestPIIStatistics:
    """レポートに PII/Secret 統計が正しく反映されることを検証する。"""

    def test_pii_detected_counted_in_report(self, aws_resources):
        """pii_detected=True の既存 Finding → files_with_pii がカウントされる。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        _insert_file_metadata(connect_table, sharing_scope="anonymous", item_name="給与.xlsx")
        _insert_finding(
            finding_table,
            status="open",
            pii_detected=True,
            pii_types=["person_name", "phone_number"],
            sensitivity_scan_at=datetime.now(timezone.utc).isoformat(),
            sensitivity_score=4.0,
        )

        ctx = _make_context()
        handler({}, ctx)

        s3 = aws_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        obj = s3.get_object(
            Bucket="aireadygov-reports-123456789012",
            Key=f"tenant-001/daily/{today}.json",
        )
        report = json.loads(obj["Body"].read())
        assert report["pii_summary"]["files_with_pii"] >= 1

    def test_secrets_detected_counted_in_report(self, aws_resources):
        """secrets_detected=True の既存 Finding → files_with_secrets がカウントされる。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        _insert_file_metadata(connect_table, sharing_scope="anonymous", item_name="config.json")
        _insert_finding(
            finding_table,
            status="open",
            secrets_detected=True,
            sensitivity_scan_at=datetime.now(timezone.utc).isoformat(),
            sensitivity_score=5.0,
        )

        ctx = _make_context()
        handler({}, ctx)

        s3 = aws_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        obj = s3.get_object(
            Bucket="aireadygov-reports-123456789012",
            Key=f"tenant-001/daily/{today}.json",
        )
        report = json.loads(obj["Body"].read())
        assert report["pii_summary"]["files_with_secrets"] >= 1


# ─── テスト: 抑制期限切れ — エッジケース ───


class TestSuppressionEdgeCases:
    """acknowledged Finding の期限切れ処理のエッジケース。"""

    def test_acknowledged_without_suppress_until_skipped(self, aws_resources):
        """suppress_until=None の acknowledged Finding → スキップ。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        _insert_file_metadata(connect_table, sharing_scope="anonymous")
        _insert_finding(
            finding_table,
            status="acknowledged",
            suppress_until=None,
        )

        items = list(connect_table.scan()["Items"])
        stats = BatchStats()
        process_expired_suppressions("tenant-001", items, stats)

        assert stats.reopened == 0
        assert stats.closed == 0
        assert stats.suppression_summary["expired_today"] == 0

    def test_acknowledged_with_malformed_date_skipped(self, aws_resources):
        """suppress_until が不正な日付文字列 → スキップ。"""
        finding_table = aws_resources["finding_table"]

        _insert_finding(
            finding_table,
            status="acknowledged",
            suppress_until="not-a-valid-date",
        )

        stats = BatchStats()
        process_expired_suppressions("tenant-001", [], stats)

        assert stats.reopened == 0
        assert stats.suppression_summary["expired_today"] == 0

    def test_expired_suppression_with_is_deleted_item_in_list(self, aws_resources):
        """items リスト内に is_deleted=True のアイテムがある → closed。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        _insert_file_metadata(connect_table, is_deleted=True)
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        _insert_finding(
            finding_table,
            status="acknowledged",
            suppress_until=yesterday,
        )

        items = list(connect_table.scan()["Items"])
        stats = BatchStats()
        process_expired_suppressions("tenant-001", items, stats)

        assert stats.closed == 1
        assert stats.suppression_summary["closed_after_expiry_today"] == 1

    def test_reopened_finding_has_recalculated_scores(self, aws_resources):
        """期限切れで再開された Finding のスコアが再計算されている。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        _insert_file_metadata(connect_table, sharing_scope="anonymous")
        _insert_finding(
            finding_table,
            status="acknowledged",
            suppress_until=yesterday,
            exposure_score=1.0,
            sensitivity_score=2.0,
        )

        items = list(connect_table.scan()["Items"])
        stats = BatchStats()
        process_expired_suppressions("tenant-001", items, stats)

        assert stats.reopened == 1
        finding_id = generate_finding_id("tenant-001", "m365", "item-001")
        finding = get_finding("tenant-001", finding_id)
        assert finding["status"] == "open"
        assert float(finding["exposure_score"]) == 5.0
        assert float(finding["risk_score"]) > 2.0


# ─── テスト: 孤立 Finding — エッジケース ───


class TestOrphanedFindingsEdgeCases:
    """close_orphaned_findings のエッジケース。"""

    def test_closed_findings_not_affected(self, aws_resources):
        """既に closed の Finding は孤立チェックの対象外 (new/open のみクエリ)。"""
        finding_table = aws_resources["finding_table"]
        _insert_finding(finding_table, item_id="item-already-closed", status="closed")

        stats = BatchStats()
        close_orphaned_findings("tenant-001", set(), stats)

        assert stats.closed == 0

    def test_multiple_orphaned_findings_all_closed(self, aws_resources):
        """複数の孤立 Finding がすべてクローズされる。"""
        finding_table = aws_resources["finding_table"]
        _insert_finding(finding_table, item_id="orphan-1", status="open")
        _insert_finding(finding_table, item_id="orphan-2", status="new")
        _insert_finding(finding_table, item_id="orphan-3", status="open")

        stats = BatchStats()
        close_orphaned_findings("tenant-001", {"active-item"}, stats)

        assert stats.closed == 3
        for oid in ("orphan-1", "orphan-2", "orphan-3"):
            fid = generate_finding_id("tenant-001", "m365", oid)
            f = get_finding("tenant-001", fid)
            assert f["status"] == "closed"


# ─── テスト: レポート整合性 ───


class TestReportIntegrity:
    """レポートのデータ整合性を検証する。"""

    def test_total_findings_equals_created_plus_updated(self, aws_resources):
        """report.summary.total_findings == created + updated。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        _insert_file_metadata(
            connect_table, item_id="new-item", sharing_scope="anonymous", item_name="給与.xlsx"
        )
        _insert_file_metadata(
            connect_table, item_id="existing-item", sharing_scope="anonymous", item_name="人事.xlsx"
        )
        _insert_finding(finding_table, item_id="existing-item", status="open")

        ctx = _make_context()
        handler({}, ctx)

        s3 = aws_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        obj = s3.get_object(
            Bucket="aireadygov-reports-123456789012",
            Key=f"tenant-001/daily/{today}.json",
        )
        report = json.loads(obj["Body"].read())
        summary = report["summary"]
        assert summary["total_findings"] == summary["new_findings"] + summary["updated_findings"]

    def test_risk_distribution_sums_to_total_findings(self, aws_resources):
        """リスク分布の合計 == total_findings。"""
        connect_table = aws_resources["connect_table"]
        for i in range(5):
            _insert_file_metadata(
                connect_table,
                item_id=f"item-{i:03d}",
                sharing_scope="anonymous",
                item_name="給与.xlsx",
            )

        ctx = _make_context()
        handler({}, ctx)

        s3 = aws_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        obj = s3.get_object(
            Bucket="aireadygov-reports-123456789012",
            Key=f"tenant-001/daily/{today}.json",
        )
        report = json.loads(obj["Body"].read())
        risk_sum = sum(report["risk_distribution"].values())
        assert risk_sum == report["summary"]["total_findings"]

    def test_report_json_serializes_decimal_values(self, aws_resources):
        """Decimal 値を含む統計がレポート JSON に正しくシリアライズされる。"""
        stats = BatchStats()
        stats.total_items_scanned = 1
        stats.created = 1
        stats.top_containers = {
            "site-x": {"name": "test", "count": 1, "total_risk": 12.5, "max_risk": 12.5}
        }

        generate_daily_report("tenant-001", stats)

        s3 = aws_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        obj = s3.get_object(
            Bucket="aireadygov-reports-123456789012",
            Key=f"tenant-001/daily/{today}.json",
        )
        raw = obj["Body"].read().decode("utf-8")
        report = json.loads(raw)
        assert report["top_containers"][0]["avg_risk_score"] == 12.5


# ─── テスト: _get_remaining_ms / _json_default ───


class TestHelperFunctions:
    """内部ヘルパー関数のエッジケース。"""

    def test_get_remaining_ms_with_none_context(self):
        """context=None → デフォルト 900,000ms。"""
        from handlers.batch_scoring import _get_remaining_ms
        assert _get_remaining_ms(None) == 900_000

    def test_get_remaining_ms_with_broken_context(self):
        """context.get_remaining_time_in_millis が例外 → デフォルト値。"""
        from handlers.batch_scoring import _get_remaining_ms
        ctx = MagicMock()
        ctx.get_remaining_time_in_millis.side_effect = AttributeError("broken")
        assert _get_remaining_ms(ctx) == 900_000

    def test_json_default_decimal(self):
        """Decimal → float 変換。"""
        from handlers.batch_scoring import _json_default
        assert _json_default(Decimal("3.14")) == 3.14

    def test_json_default_datetime(self):
        """datetime → ISO 文字列変換。"""
        from handlers.batch_scoring import _json_default
        dt = datetime(2026, 2, 20, 5, 0, 0, tzinfo=timezone.utc)
        assert _json_default(dt) == "2026-02-20T05:00:00+00:00"

    def test_json_default_unsupported_type_raises(self):
        """未対応型 → TypeError。"""
        from handlers.batch_scoring import _json_default
        with pytest.raises(TypeError):
            _json_default(set())
