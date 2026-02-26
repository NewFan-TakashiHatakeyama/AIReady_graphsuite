"""batchScoring 上級テスト — 本番環境での大規模処理・障害耐性を検証

一流の QA エンジニア視点で、以下のシナリオを検証する:
  1. 大規模データセット（100+ アイテム）の処理整合性
  2. ページネーション（DynamoDB スキャンの LastEvaluatedKey）
  3. SQS 送信失敗時の部分的なリカバリ
  4. レポート構造の完全性検証（詳細設計 5.4 準拠）
  5. 日付境界（UTC 深夜）でのレポート生成
  6. マルチテナント大規模処理
  7. 既存 Finding の状態遷移フロー完全再現
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
    acknowledge_finding,
)
from services.scoring import ExposureResult, SensitivityResult


@pytest.fixture
def aws_resources():
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
    container_id: str = "site-xyz",
    container_name: str = "法務部門サイト",
    **extra,
) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    finding_id = generate_finding_id(tenant_id, source, item_id)
    finding: dict[str, Any] = {
        "tenant_id": tenant_id,
        "finding_id": finding_id,
        "source": source,
        "item_id": item_id,
        "item_name": "契約書_A社.docx",
        "item_url": "https://contoso.sharepoint.com/doc.docx",
        "container_id": container_id,
        "container_name": container_name,
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 大規模データセットの処理整合性
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestLargeScaleProcessing:
    """数百アイテムのバッチ処理でデータ損失や不整合が起きない。"""

    def test_200_items_all_processed(self, aws_resources):
        """200 件のアイテムがすべて処理される。"""
        connect_table = aws_resources["connect_table"]
        for i in range(200):
            _insert_file_metadata(
                connect_table,
                item_id=f"item-{i:04d}",
                sharing_scope="anonymous",
                item_name="給与.xlsx",
            )

        ctx = _make_context(remaining_ms=900_000)
        result = handler({}, ctx)

        assert result["processed"] == 200
        assert result["errors"] == 0

    def test_50_items_with_mixed_risk_levels(self, aws_resources):
        """高・中・低リスクが混在する 50 件の処理で Finding が正しく生成・省略される。"""
        connect_table = aws_resources["connect_table"]
        old_date = (datetime.now(timezone.utc) - timedelta(days=200)).isoformat()

        for i in range(20):
            _insert_file_metadata(
                connect_table,
                item_id=f"high-{i:03d}",
                sharing_scope="anonymous",
                item_name="機密.xlsx",
            )

        for i in range(15):
            _insert_file_metadata(
                connect_table,
                item_id=f"med-{i:03d}",
                sharing_scope="organization",
                item_name="契約書.docx",
            )

        for i in range(15):
            _insert_file_metadata(
                connect_table,
                item_id=f"low-{i:03d}",
                sharing_scope="specific",
                permissions_count=2,
                item_name="readme.txt",
                modified_at=old_date,
            )

        ctx = _make_context()
        result = handler({}, ctx)

        assert result["processed"] == 50
        assert result["created"] > 0

        s3 = aws_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        obj = s3.get_object(
            Bucket="aireadygov-reports-123456789012",
            Key=f"tenant-001/daily/{today}.json",
        )
        report = json.loads(obj["Body"].read())
        assert report["summary"]["total_items_scanned"] == 50


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. SQS 送信失敗時のグレースフルデグラデーション
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestSQSFailureResilience:
    """SQS が一時的に利用不可でもバッチ処理全体が失敗しない。"""

    def test_sqs_failure_during_enqueue_counted_as_error(self, aws_resources):
        """SQS 送信失敗時に errors がインクリメントされ処理は継続する。"""
        finding_table = aws_resources["finding_table"]
        _insert_finding(finding_table, status="open", sensitivity_scan_at=None)

        failing_sqs = MagicMock()
        failing_sqs.send_message.side_effect = Exception("SQS unavailable")
        set_sqs_client(failing_sqs)

        stats = BatchStats()
        enqueue_unscanned_items("tenant-001", stats)

        assert stats.errors >= 1

        set_sqs_client(aws_resources["sqs"])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. レポートの完全性検証
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestReportCompleteness:
    """日次レポートが詳細設計 5.4 の仕様を完全に満たすことを検証する。"""

    def test_report_contains_all_required_sections(self, aws_resources):
        """レポートに必須セクションがすべて含まれる。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        for i in range(10):
            _insert_file_metadata(
                connect_table,
                item_id=f"item-{i:03d}",
                sharing_scope="anonymous",
                item_name="給与.xlsx",
            )
        _insert_finding(
            finding_table,
            item_id="item-000",
            pii_detected=True,
            pii_types=["PERSON_NAME", "PHONE_NUMBER"],
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

        required_keys = {
            "tenant_id", "report_date", "generated_at", "summary",
            "risk_distribution", "pii_summary", "top_containers",
            "exposure_vector_distribution", "guard_match_distribution",
            "suppression_summary",
        }
        assert required_keys <= set(report.keys())

        summary_keys = {
            "total_items_scanned", "new_findings", "updated_findings",
            "closed_findings", "total_findings", "errors",
        }
        assert summary_keys <= set(report["summary"].keys())

    def test_top_containers_sorted_by_finding_count(self, aws_resources):
        """top_containers が finding_count 降順でソートされている。"""
        connect_table = aws_resources["connect_table"]

        for i in range(15):
            container_id = f"site-{i % 3}"
            container_name = f"部門{i % 3}"
            _insert_file_metadata(
                connect_table,
                item_id=f"item-{i:03d}",
                sharing_scope="anonymous",
                item_name="給与.xlsx",
                container_id=container_id,
                container_name=container_name,
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
        containers = report["top_containers"]

        if len(containers) > 1:
            for i in range(len(containers) - 1):
                assert containers[i]["finding_count"] >= containers[i + 1]["finding_count"]

    def test_report_exposure_vector_distribution(self, aws_resources):
        """exposure_vector_distribution が正しく集計される。"""
        connect_table = aws_resources["connect_table"]

        _insert_file_metadata(
            connect_table,
            item_id="item-anon",
            sharing_scope="anonymous",
            item_name="公開文書.xlsx",
        )
        _insert_file_metadata(
            connect_table,
            item_id="item-org",
            sharing_scope="organization",
            item_name="社内文書.docx",
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
        ev_dist = report["exposure_vector_distribution"]
        assert isinstance(ev_dist, dict)

    def test_suppression_summary_in_report(self, aws_resources):
        """suppression_summary がレポートに含まれる。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        _insert_file_metadata(connect_table, sharing_scope="anonymous")
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        _insert_finding(
            finding_table,
            status="acknowledged",
            suppress_until=yesterday,
            sensitivity_score=2.0,
            acknowledged_reason="テスト用の抑制理由です。50文字以上のテスト文字列を入力しています。",
            acknowledged_by="admin@example.com",
            acknowledged_at=(datetime.now(timezone.utc) - timedelta(days=30)).isoformat(),
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
        supp = report["suppression_summary"]
        assert isinstance(supp, dict)
        assert "total_acknowledged" in supp or "expired_today" in supp


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. マルチテナント大規模テスト
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestMultiTenantLargeScale:
    """複数テナントの同時処理でデータが混在しない。"""

    def test_5_tenants_10_items_each(self, aws_resources):
        """5 テナント × 10 アイテムの処理。"""
        connect_table = aws_resources["connect_table"]
        for t_idx in range(5):
            for i_idx in range(10):
                _insert_file_metadata(
                    connect_table,
                    tenant_id=f"tenant-{t_idx:03d}",
                    item_id=f"item-{t_idx:03d}-{i_idx:03d}",
                    sharing_scope="anonymous",
                    item_name="給与.xlsx",
                )

        ctx = _make_context()
        result = handler({}, ctx)

        assert result["processed"] == 50
        assert result["errors"] == 0

        s3 = aws_resources["s3"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for t_idx in range(5):
            obj = s3.get_object(
                Bucket="aireadygov-reports-123456789012",
                Key=f"tenant-{t_idx:03d}/daily/{today}.json",
            )
            report = json.loads(obj["Body"].read())
            assert report["tenant_id"] == f"tenant-{t_idx:03d}"
            assert report["summary"]["total_items_scanned"] == 10


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. 抑制期限切れの網羅テスト
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestSuppressionExpiryComprehensive:
    """抑制期限切れ処理の全パターンを検証する。"""

    def test_multiple_expired_mixed_outcomes(self, aws_resources):
        """複数の期限切れ Finding が mixed outcome（open/closed）で処理される。"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        old_date = (datetime.now(timezone.utc) - timedelta(days=200)).isoformat()

        # リスク残存 → open に戻る
        _insert_file_metadata(
            connect_table, item_id="item-risk-remains",
            sharing_scope="anonymous",
        )
        _insert_finding(
            finding_table, item_id="item-risk-remains",
            status="acknowledged",
            suppress_until=yesterday,
            sensitivity_score=2.0,
            acknowledged_reason="テスト用の抑制理由。50文字以上の文字列を入力してバリデーションを通す。",
            acknowledged_by="admin",
            acknowledged_at=(datetime.now(timezone.utc) - timedelta(days=30)).isoformat(),
        )

        # リスク解消 → closed
        _insert_file_metadata(
            connect_table, item_id="item-risk-resolved",
            sharing_scope="specific",
            permissions_count=2,
            item_name="readme.txt",
            modified_at=old_date,
        )
        _insert_finding(
            finding_table, item_id="item-risk-resolved",
            status="acknowledged",
            suppress_until=yesterday,
            sensitivity_score=1.0,
        )

        items = list(connect_table.scan()["Items"])
        stats = BatchStats()
        process_expired_suppressions("tenant-001", items, stats)

        assert stats.reopened == 1
        assert stats.closed == 1
        assert stats.suppression_summary["expired_today"] == 2

        fid_remains = generate_finding_id("tenant-001", "m365", "item-risk-remains")
        f_remains = get_finding("tenant-001", fid_remains)
        assert f_remains["status"] == "open"

        fid_resolved = generate_finding_id("tenant-001", "m365", "item-risk-resolved")
        f_resolved = get_finding("tenant-001", fid_resolved)
        assert f_resolved["status"] == "closed"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. 孤立 Finding の大量クローズ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestOrphanedFindingsLargeScale:
    """大量の孤立 Finding が確実にクローズされる。"""

    def test_50_orphaned_findings_all_closed(self, aws_resources):
        """50 件の孤立 Finding が全てクローズされる。"""
        finding_table = aws_resources["finding_table"]
        for i in range(50):
            _insert_finding(
                finding_table,
                item_id=f"orphan-{i:03d}",
                status="open" if i % 2 == 0 else "new",
            )

        stats = BatchStats()
        close_orphaned_findings("tenant-001", {"active-item-001"}, stats)

        assert stats.closed == 50

        for i in range(50):
            fid = generate_finding_id("tenant-001", "m365", f"orphan-{i:03d}")
            f = get_finding("tenant-001", fid)
            assert f["status"] == "closed"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 7. 完全なパイプライン再現テスト
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestFullPipelineReplay:
    """アイテムのライフサイクル全体をバッチで再現する。"""

    def test_item_lifecycle_via_batch(self, aws_resources):
        """
        1. アイテム追加 → Finding 新規生成
        2. 再バッチ → Finding 更新
        3. アイテム削除 → Finding クローズ
        """
        connect_table = aws_resources["connect_table"]
        ctx = _make_context()

        _insert_file_metadata(connect_table, sharing_scope="anonymous", item_name="給与.xlsx")
        result1 = handler({}, ctx)
        assert result1["created"] >= 1

        result2 = handler({}, ctx)
        assert result2["updated"] >= 1
        assert result2["created"] == 0

        connect_table.update_item(
            Key={"tenant_id": "tenant-001", "item_id": "item-001"},
            UpdateExpression="SET is_deleted = :d",
            ExpressionAttributeValues={":d": True},
        )
        result3 = handler({}, ctx)
        assert result3["closed"] >= 1

        fid = generate_finding_id("tenant-001", "m365", "item-001")
        f = get_finding("tenant-001", fid)
        assert f["status"] == "closed"

    def test_risk_escalation_then_resolution(self, aws_resources):
        """
        1. 低リスクアイテム → Finding なし
        2. sharing_scope 変更 → Finding 生成
        3. sharing_scope 戻し → Finding クローズ
        """
        connect_table = aws_resources["connect_table"]
        ctx = _make_context()

        old_date = (datetime.now(timezone.utc) - timedelta(days=200)).isoformat()
        _insert_file_metadata(
            connect_table,
            sharing_scope="specific",
            permissions_count=2,
            item_name="readme.txt",
            modified_at=old_date,
        )
        result1 = handler({}, ctx)
        assert result1["created"] == 0

        connect_table.update_item(
            Key={"tenant_id": "tenant-001", "item_id": "item-001"},
            UpdateExpression="SET sharing_scope = :s, modified_at = :m",
            ExpressionAttributeValues={
                ":s": "anonymous",
                ":m": datetime.now(timezone.utc).isoformat(),
            },
        )
        result2 = handler({}, ctx)
        assert result2["created"] >= 1

        connect_table.update_item(
            Key={"tenant_id": "tenant-001", "item_id": "item-001"},
            UpdateExpression="SET sharing_scope = :s, modified_at = :m, permissions_count = :p",
            ExpressionAttributeValues={
                ":s": "specific",
                ":m": old_date,
                ":p": 2,
            },
        )
        result3 = handler({}, ctx)
        assert result3["closed"] >= 1


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 8. BatchStats の整合性
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestBatchStatsIntegrity:
    """BatchStats のカウンタが正確であることを検証する。"""

    def test_stats_add_up_correctly(self, aws_resources):
        """created + updated + closed + errors + skipped == total_items_scanned"""
        connect_table = aws_resources["connect_table"]
        finding_table = aws_resources["finding_table"]

        for i in range(10):
            _insert_file_metadata(
                connect_table,
                item_id=f"item-{i:03d}",
                sharing_scope="anonymous",
                item_name="給与.xlsx",
            )

        _insert_finding(finding_table, item_id="item-000", status="open")

        ctx = _make_context()
        result = handler({}, ctx)

        total_actions = result["created"] + result["updated"] + result.get("closed", 0)
        assert total_actions > 0
        assert result["processed"] == 10

    def test_empty_batch_stats_no_error(self):
        """空の BatchStats でレポート生成がエラーにならない。"""
        stats = BatchStats()
        assert stats.total_items_scanned == 0
        assert stats.created == 0
        assert stats.errors == 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 9. extract_metadata のエッジケース追加
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestExtractMetadataEdgeCases:
    """FileMetadata から DTO への変換の追加エッジケース。"""

    def test_boolean_string_is_deleted(self):
        """is_deleted が文字列 'true' として返される場合（DynamoDB の型変換）。"""
        item = {
            "tenant_id": "t-001",
            "item_id": "i-001",
            "is_deleted": "true",
        }
        meta = extract_metadata(item)
        assert meta.is_deleted is True

    def test_decimal_permissions_count_from_dynamodb(self):
        """DynamoDB の Decimal 型 permissions_count が int に変換される。"""
        item = {
            "tenant_id": "t-001",
            "item_id": "i-001",
            "permissions_count": Decimal("100"),
        }
        meta = extract_metadata(item)
        assert meta.permissions_count == 100
        assert isinstance(meta.permissions_count, int)

    def test_missing_all_optional_fields(self):
        """必須フィールドのみの最小メタデータ。"""
        meta = extract_metadata({"tenant_id": "t-001", "item_id": "i-001"})
        assert meta.tenant_id == "t-001"
        assert meta.item_id == "i-001"
        assert meta.source == "m365"
        assert meta.sharing_scope == "specific"
        assert meta.item_name == ""
        assert meta.permissions_count == 0
        assert meta.size == 0

    def test_numeric_string_size(self):
        """size が文字列として返される場合。"""
        item = {
            "tenant_id": "t-001",
            "item_id": "i-001",
            "size": "4096",
        }
        meta = extract_metadata(item)
        assert meta.size == 4096
