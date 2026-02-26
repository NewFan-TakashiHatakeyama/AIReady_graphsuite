"""analyzeExposure Lambda ハンドラの単体テスト

moto で DynamoDB / SQS をモックし、DynamoDB Streams イベントを手動生成して
ハンドラのルーティング・スコアリング・Finding upsert・SQS 送信を検証する。

詳細設計 3.3–3.8 / 11.1 節準拠。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from handlers.analyze_exposure import (
    enqueue_sensitivity_scan,
    extract_metadata,
    handler,
    is_scoring_relevant_change,
    process_record,
    should_enqueue_sensitivity_scan,
)
from services.exposure_vectors import FileMetadata
from services.finding_manager import get_finding, set_finding_table
import handlers.analyze_exposure as handler_module


# ─── Fixtures ───


@pytest.fixture
def aws_resources(monkeypatch):
    """moto で DynamoDB + SQS を構築し、環境変数を設定する。"""
    with mock_aws():
        region = "ap-northeast-1"

        dynamodb = boto3.resource("dynamodb", region_name=region)
        table = dynamodb.create_table(
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
        table.meta.client.get_waiter("table_exists").wait(
            TableName="AIReadyGov-ExposureFinding"
        )
        set_finding_table(table)

        sqs = boto3.client("sqs", region_name=region)
        queue = sqs.create_queue(QueueName="AIReadyGov-SensitivityDetectionQueue")
        queue_url = queue["QueueUrl"]

        monkeypatch.setenv("FINDING_TABLE_NAME", "AIReadyGov-ExposureFinding")
        monkeypatch.setenv("SENSITIVITY_QUEUE_URL", queue_url)
        monkeypatch.setenv("RAW_PAYLOAD_BUCKET", "aireadyconnect-raw-payload-123456789012")

        handler_module._sqs_client = sqs

        yield {
            "table": table,
            "sqs": sqs,
            "queue_url": queue_url,
        }

        set_finding_table(None)
        handler_module._sqs_client = None


# ─── Helper: DynamoDB Streams イベント生成 ───


def _ddb_s(val: str) -> dict:
    return {"S": val}


def _ddb_n(val) -> dict:
    return {"N": str(val)}


def _ddb_bool(val: bool) -> dict:
    return {"BOOL": val}


def _make_new_image(
    tenant_id: str = "t-001",
    item_id: str = "item-001",
    source: str = "m365",
    sharing_scope: str = "organization",
    permissions: str = "{}",
    permissions_count: int = 10,
    sensitivity_label: str | None = None,
    item_name: str = "契約書_A社.docx",
    modified_at: str | None = None,
    is_deleted: bool = False,
    raw_s3_key: str = "raw/t-001/item-001/2026-02-10.json",
    **extra,
) -> dict:
    """DynamoDB Streams 形式の NewImage を生成する。"""
    if modified_at is None:
        modified_at = datetime.now(timezone.utc).isoformat()

    image = {
        "tenant_id": _ddb_s(tenant_id),
        "item_id": _ddb_s(item_id),
        "source": _ddb_s(source),
        "container_id": _ddb_s("site-xyz"),
        "container_name": _ddb_s("法務部門サイト"),
        "container_type": _ddb_s("site"),
        "item_name": _ddb_s(item_name),
        "web_url": _ddb_s("https://contoso.sharepoint.com/contract.docx"),
        "sharing_scope": _ddb_s(sharing_scope),
        "permissions": _ddb_s(permissions),
        "permissions_count": _ddb_n(permissions_count),
        "mime_type": _ddb_s("application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        "size": _ddb_n(2048000),
        "modified_at": _ddb_s(modified_at),
        "is_deleted": _ddb_bool(is_deleted),
        "raw_s3_key": _ddb_s(raw_s3_key),
    }

    if sensitivity_label is not None:
        image["sensitivity_label"] = _ddb_s(sensitivity_label)

    for k, v in extra.items():
        if isinstance(v, str):
            image[k] = _ddb_s(v)
        elif isinstance(v, bool):
            image[k] = _ddb_bool(v)
        elif isinstance(v, (int, float)):
            image[k] = _ddb_n(v)

    return image


def _make_record(
    event_name: str = "INSERT",
    new_image: dict | None = None,
    old_image: dict | None = None,
) -> dict:
    """DynamoDB Streams のレコードを生成する。"""
    ddb: dict[str, Any] = {}
    if new_image is not None:
        ddb["NewImage"] = new_image
    if old_image is not None:
        ddb["OldImage"] = old_image

    return {
        "eventID": "test-event-001",
        "eventName": event_name,
        "dynamodb": ddb,
    }


def _make_event(*records: dict) -> dict:
    return {"Records": list(records)}


def _get_sqs_messages(sqs_client, queue_url: str) -> list[dict]:
    """SQS キューからメッセージを取得する。"""
    resp = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=0,
    )
    messages = resp.get("Messages", [])
    return [json.loads(m["Body"]) for m in messages]


# ─── INSERT イベント → Finding 作成 + SQS 送信 ───


class TestInsertEvent:
    def test_insert_creates_finding_and_enqueues(self, aws_resources):
        """INSERT イベントで Finding が new で作成され、SQS にメッセージが送信される"""
        new_img = _make_new_image(sharing_scope="anonymous")
        record = _make_record("INSERT", new_image=new_img)

        handler(_make_event(record), None)

        response = aws_resources["table"].scan()
        findings = response["Items"]
        assert len(findings) == 1

        finding = findings[0]
        assert finding["tenant_id"] == "t-001"
        assert finding["status"] == "new"
        assert float(finding["exposure_score"]) == 5.0
        assert "public_link" in finding["exposure_vectors"]

        messages = _get_sqs_messages(aws_resources["sqs"], aws_resources["queue_url"])
        assert len(messages) == 1
        assert messages[0]["tenant_id"] == "t-001"
        assert messages[0]["item_id"] == "item-001"
        assert messages[0]["trigger"] == "realtime"

    def test_insert_with_confidential_label(self, aws_resources):
        """Confidential ラベル付き INSERT → SensitivityScore に反映"""
        label = json.dumps({"name": "Confidential"})
        new_img = _make_new_image(
            sharing_scope="organization",
            sensitivity_label=label,
        )
        record = _make_record("INSERT", new_image=new_img)

        handler(_make_event(record), None)

        response = aws_resources["table"].scan()
        finding = response["Items"][0]
        assert float(finding["sensitivity_score"]) == 3.0


# ─── MODIFY イベント ───


class TestModifyEvent:
    def test_modify_sharing_scope_updates_finding(self, aws_resources):
        """sharing_scope 変更 MODIFY → Finding が更新される"""
        old_img = _make_new_image(sharing_scope="specific")
        new_img = _make_new_image(sharing_scope="anonymous")

        insert_rec = _make_record("INSERT", new_image=new_img)
        handler(_make_event(insert_rec), None)

        modify_rec = _make_record("MODIFY", new_image=new_img, old_image=old_img)
        handler(_make_event(modify_rec), None)

        response = aws_resources["table"].scan()
        findings = response["Items"]
        assert len(findings) == 1
        assert findings[0]["status"] == "open"

    def test_modify_web_url_only_skips(self, aws_resources):
        """web_url のみの変更 → スコアリングに影響しないためスキップ"""
        base = _make_new_image(sharing_scope="anonymous")
        old_img = dict(base)
        new_img = dict(base)
        new_img["web_url"] = _ddb_s("https://contoso.sharepoint.com/new-url")

        insert_rec = _make_record("INSERT", new_image=base)
        handler(_make_event(insert_rec), None)

        initial_scan = aws_resources["table"].scan()
        initial_count = len(initial_scan["Items"])

        modify_rec = _make_record("MODIFY", new_image=new_img, old_image=old_img)
        handler(_make_event(modify_rec), None)

        after_scan = aws_resources["table"].scan()
        assert len(after_scan["Items"]) == initial_count


# ─── REMOVE イベント → Finding Closed ───


class TestRemoveEvent:
    def test_remove_closes_finding(self, aws_resources):
        """REMOVE イベントで既存 Finding が Closed になる"""
        new_img = _make_new_image(sharing_scope="anonymous")
        insert_rec = _make_record("INSERT", new_image=new_img)
        handler(_make_event(insert_rec), None)

        old_img = new_img
        remove_rec = _make_record("REMOVE", old_image=old_img)
        handler(_make_event(remove_rec), None)

        response = aws_resources["table"].scan()
        findings = response["Items"]
        assert len(findings) == 1
        assert findings[0]["status"] == "closed"


# ─── is_deleted=true → Finding Closed ───


class TestIsDeleted:
    def test_is_deleted_closes_finding(self, aws_resources):
        """is_deleted=true の NewImage で Finding が Closed になる"""
        new_img = _make_new_image(sharing_scope="anonymous")
        insert_rec = _make_record("INSERT", new_image=new_img)
        handler(_make_event(insert_rec), None)

        deleted_img = _make_new_image(sharing_scope="anonymous", is_deleted=True)
        modify_rec = _make_record("MODIFY", new_image=deleted_img, old_image=new_img)
        handler(_make_event(modify_rec), None)

        response = aws_resources["table"].scan()
        findings = response["Items"]
        assert len(findings) == 1
        assert findings[0]["status"] == "closed"


# ─── 閾値未満 → Finding 作成されない ───


class TestBelowThreshold:
    def test_below_threshold_no_finding(self, aws_resources):
        """RiskScore < 閾値 → Finding が作成されない
        specific + 通常ファイル + 100日前更新 → 1.0*1.0*0.5*1.0=0.5 < 2.0
        """
        old_date = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
        new_img = _make_new_image(
            sharing_scope="specific",
            permissions_count=5,
            item_name="meeting_notes.txt",
            modified_at=old_date,
        )
        record = _make_record("INSERT", new_image=new_img)

        handler(_make_event(record), None)

        response = aws_resources["table"].scan()
        assert len(response["Items"]) == 0

    def test_below_threshold_closes_existing(self, aws_resources):
        """既存 Finding がある場合、閾値未満になったら Closed にする"""
        new_img = _make_new_image(sharing_scope="anonymous")
        insert_rec = _make_record("INSERT", new_image=new_img)
        handler(_make_event(insert_rec), None)

        old_date = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
        safe_img = _make_new_image(
            sharing_scope="specific", permissions_count=1, modified_at=old_date,
        )
        modify_rec = _make_record("MODIFY", new_image=safe_img, old_image=new_img)
        handler(_make_event(modify_rec), None)

        response = aws_resources["table"].scan()
        findings = response["Items"]
        assert len(findings) == 1
        assert findings[0]["status"] == "closed"


# ─── is_scoring_relevant_change ───


class TestIsScoringRelevantChange:
    def test_sharing_scope_change(self):
        assert is_scoring_relevant_change(
            {"sharing_scope": "anonymous"},
            {"sharing_scope": "specific"},
        ) is True

    def test_permissions_change(self):
        assert is_scoring_relevant_change(
            {"permissions": '{"entries": []}'},
            {"permissions": "{}"},
        ) is True

    def test_permissions_count_change(self):
        assert is_scoring_relevant_change(
            {"permissions_count": 100},
            {"permissions_count": 10},
        ) is True

    def test_sensitivity_label_change(self):
        assert is_scoring_relevant_change(
            {"sensitivity_label": "Confidential"},
            {"sensitivity_label": None},
        ) is True

    def test_item_name_change(self):
        assert is_scoring_relevant_change(
            {"item_name": "給与一覧.xlsx"},
            {"item_name": "document.xlsx"},
        ) is True

    def test_modified_at_change(self):
        assert is_scoring_relevant_change(
            {"modified_at": "2026-02-10T08:00:00Z"},
            {"modified_at": "2026-01-01T00:00:00Z"},
        ) is True

    def test_web_url_only_no_change(self):
        """web_url はスコアリングに影響しない"""
        assert is_scoring_relevant_change(
            {"web_url": "https://new-url", "sharing_scope": "specific"},
            {"web_url": "https://old-url", "sharing_scope": "specific"},
        ) is False

    def test_no_change(self):
        same = {"sharing_scope": "specific", "permissions": "{}"}
        assert is_scoring_relevant_change(same, same) is False


# ─── extract_metadata ───


class TestExtractMetadata:
    def test_full_extraction(self):
        image = {
            "tenant_id": "t-001",
            "item_id": "item-abc",
            "source": "m365",
            "container_id": "site-xyz",
            "container_name": "法務部門",
            "container_type": "site",
            "item_name": "契約書.docx",
            "web_url": "https://example.com/file",
            "sharing_scope": "organization",
            "permissions": '{"entries": []}',
            "permissions_count": 25,
            "sensitivity_label": '{"name": "Confidential"}',
            "mime_type": "application/docx",
            "size": 1024,
            "modified_at": "2026-02-10T00:00:00Z",
            "is_deleted": False,
            "raw_s3_key": "raw/t-001/item-abc/latest.json",
        }
        meta = extract_metadata(image)

        assert isinstance(meta, FileMetadata)
        assert meta.tenant_id == "t-001"
        assert meta.item_id == "item-abc"
        assert meta.sharing_scope == "organization"
        assert meta.permissions_count == 25
        assert meta.size == 1024

    def test_missing_fields_use_defaults(self):
        meta = extract_metadata({"tenant_id": "t-001", "item_id": "item-001"})
        assert meta.source == "m365"
        assert meta.sharing_scope == "specific"
        assert meta.permissions_count == 0
        assert meta.is_deleted is False


# ─── should_enqueue_sensitivity_scan ───


class TestShouldEnqueueSensitivityScan:
    def test_new_finding(self):
        assert should_enqueue_sensitivity_scan({"is_new": True}, None) is True

    def test_modified_at_changed(self):
        finding = {"is_new": False, "modified_at": "2026-02-10", "sensitivity_scan_at": "2026-02-09"}
        old = {"modified_at": "2026-01-01"}
        assert should_enqueue_sensitivity_scan(finding, old) is True

    def test_no_sensitivity_scan_at(self):
        finding = {"is_new": False, "sensitivity_scan_at": None}
        assert should_enqueue_sensitivity_scan(finding, None) is True

    def test_rescan_interval_exceeded(self):
        old_scan = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        finding = {"is_new": False, "sensitivity_scan_at": old_scan}
        assert should_enqueue_sensitivity_scan(finding, None) is True

    def test_recently_scanned_no_change(self):
        recent = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        finding = {"is_new": False, "sensitivity_scan_at": recent, "modified_at": "2026-02-10"}
        old = {"modified_at": "2026-02-10"}
        assert should_enqueue_sensitivity_scan(finding, old) is False

    def test_invalid_scan_at_triggers_rescan(self):
        finding = {"is_new": False, "sensitivity_scan_at": "invalid-date"}
        assert should_enqueue_sensitivity_scan(finding, None) is True


# ─── handler エラーハンドリング ───


class TestHandlerError:
    def test_error_in_record_raises(self, aws_resources):
        """レコード処理でエラーが発生した場合、例外が伝播する"""
        bad_record = {"eventID": "bad", "eventName": "INSERT", "dynamodb": {}}

        with patch(
            "handlers.analyze_exposure.deserialize_image",
            side_effect=Exception("deserialization error"),
        ):
            with pytest.raises(Exception, match="deserialization error"):
                handler(_make_event(bad_record), None)


# ─── 複数レコードバッチ ───


class TestBatchProcessing:
    def test_multiple_records(self, aws_resources):
        """複数レコードのバッチ処理"""
        rec1 = _make_record("INSERT", new_image=_make_new_image(
            item_id="item-001", sharing_scope="anonymous"))
        rec2 = _make_record("INSERT", new_image=_make_new_image(
            item_id="item-002", sharing_scope="organization",
            permissions=json.dumps({"entries": [
                {"identity": {"displayName": "Everyone except external users"}}
            ]}),
        ))

        result = handler(_make_event(rec1, rec2), None)
        assert result["processed"] == 2
        assert result["errors"] == 0

        response = aws_resources["table"].scan()
        assert len(response["Items"]) == 2


# ─── SQS client 自動解決 ───


class TestSQSClientAutoResolve:
    def test_auto_resolve_sqs_client(self, aws_resources):
        """_get_sqs_client が None の場合に自動解決する（Line 64）"""
        from handlers.analyze_exposure import _get_sqs_client
        handler_module._sqs_client = None
        client = _get_sqs_client()
        assert client is not None
        handler_module._sqs_client = aws_resources["sqs"]


# ─── FIFO キューの MessageGroupId ───


class TestFIFOQueue:
    def test_fifo_queue_sets_message_group_id(self, aws_resources, monkeypatch):
        """FIFO キューの場合 MessageGroupId が設定される（Line 258）"""
        sqs = aws_resources["sqs"]
        fifo_queue = sqs.create_queue(
            QueueName="Test.fifo",
            Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
        )
        fifo_url = fifo_queue["QueueUrl"]
        monkeypatch.setenv("SENSITIVITY_QUEUE_URL", fifo_url)

        meta = FileMetadata(tenant_id="t-001", item_id="item-001")
        finding = {"finding_id": "abc123", "tenant_id": "t-001"}
        enqueue_sensitivity_scan(finding, meta)

        resp = sqs.receive_message(QueueUrl=fifo_url, MaxNumberOfMessages=1)
        messages = resp.get("Messages", [])
        assert len(messages) == 1


# ─── SSM フォールバック ───


class TestSSMFallback:
    def test_risk_threshold_fallback(self, aws_resources):
        """SSM 例外時にデフォルト閾値 2.0 を返す（Lines 270-271）"""
        from handlers.analyze_exposure import _get_risk_threshold
        with patch("handlers.analyze_exposure.get_ssm_float", side_effect=Exception("SSM down")):
            assert _get_risk_threshold() == 2.0

    def test_rescan_interval_fallback(self):
        """SSM 例外時にデフォルト再スキャン間隔 7 を返す"""
        from handlers.analyze_exposure import _get_rescan_interval
        with patch("handlers.analyze_exposure.get_ssm_int", side_effect=Exception("SSM down")):
            assert _get_rescan_interval() == 7
