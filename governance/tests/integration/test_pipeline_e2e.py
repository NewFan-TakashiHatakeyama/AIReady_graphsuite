"""パイプライン E2E テスト（T-025）

3 つの Lambda を moto 上で直列に実行し、
analyzeExposure → SQS → detectSensitivity → Finding 更新 → batchScoring → レポート
の全フローが正常に動作することを検証する。

詳細設計 11.2 節・付録 C 準拠。
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

from services.finding_manager import (
    generate_finding_id,
    get_finding,
    set_finding_table,
)
from services.pii_detector import PIIDetectionResult, PIIEntity
from services.secret_detector import SecretDetectionResult, SecretEntity


# ─── Helpers ───


def _ddb_s(val: str) -> dict:
    return {"S": val}


def _ddb_n(val) -> dict:
    return {"N": str(val)}


def _ddb_bool(val: bool) -> dict:
    return {"BOOL": val}


def _make_new_image(
    tenant_id: str = "tenant-001",
    item_id: str = "item-e2e-001",
    source: str = "m365",
    sharing_scope: str = "anonymous",
    permissions: str = "{}",
    permissions_count: int = 10,
    sensitivity_label: str | None = None,
    item_name: str = "顧客リスト.xlsx",
    modified_at: str | None = None,
    is_deleted: bool = False,
    container_id: str = "site-sales",
    container_name: str = "営業部門サイト",
    raw_s3_key: str = "raw/tenant-001/item-e2e-001/data.xlsx",
    source_metadata: str | None = None,
    **extra,
) -> dict:
    if modified_at is None:
        modified_at = datetime.now(timezone.utc).isoformat()

    image: dict[str, Any] = {
        "tenant_id": _ddb_s(tenant_id),
        "item_id": _ddb_s(item_id),
        "source": _ddb_s(source),
        "container_id": _ddb_s(container_id),
        "container_name": _ddb_s(container_name),
        "container_type": _ddb_s("site"),
        "item_name": _ddb_s(item_name),
        "web_url": _ddb_s(f"https://contoso.sharepoint.com/sites/sales/{item_name}"),
        "sharing_scope": _ddb_s(sharing_scope),
        "permissions": _ddb_s(permissions),
        "permissions_count": _ddb_n(permissions_count),
        "mime_type": _ddb_s("text/plain"),
        "size": _ddb_n(500),
        "modified_at": _ddb_s(modified_at),
        "is_deleted": _ddb_bool(is_deleted),
        "raw_s3_key": _ddb_s(raw_s3_key),
    }

    if sensitivity_label is not None:
        image["sensitivity_label"] = _ddb_s(sensitivity_label)
    if source_metadata is not None:
        image["source_metadata"] = _ddb_s(source_metadata)

    for k, v in extra.items():
        if isinstance(v, str):
            image[k] = _ddb_s(v)
        elif isinstance(v, bool):
            image[k] = _ddb_bool(v)
        elif isinstance(v, (int, float)):
            image[k] = _ddb_n(v)

    return image


def _make_stream_record(
    event_name: str = "INSERT",
    new_image: dict | None = None,
    old_image: dict | None = None,
) -> dict:
    ddb: dict[str, Any] = {}
    if new_image is not None:
        ddb["NewImage"] = new_image
    if old_image is not None:
        ddb["OldImage"] = old_image
    return {"eventID": f"evt-{id(ddb)}", "eventName": event_name, "dynamodb": ddb}


def _make_stream_event(*records: dict) -> dict:
    return {"Records": list(records)}


def _get_sqs_messages(sqs_client, queue_url: str) -> list[dict]:
    resp = sqs_client.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=0,
    )
    return [json.loads(m["Body"]) for m in resp.get("Messages", [])]


def _drain_sqs(sqs_client, queue_url: str) -> list[dict]:
    """SQS から全メッセージを取得して削除する。"""
    messages = []
    while True:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=0,
        )
        batch = resp.get("Messages", [])
        if not batch:
            break
        for m in batch:
            messages.append(json.loads(m["Body"]))
            sqs_client.delete_message(
                QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"],
            )
    return messages


def _make_sqs_event(messages: list[dict]) -> dict:
    """detectSensitivity 用の SQS イベントを生成する。"""
    return {
        "Records": [
            {
                "messageId": f"msg-{i}",
                "body": json.dumps(msg),
            }
            for i, msg in enumerate(messages)
        ]
    }


def _make_batch_context(remaining_ms: int = 890_000):
    class FakeContext:
        def get_remaining_time_in_millis(self):
            return remaining_ms
    return FakeContext()


def _insert_connect_item(connect_table, tenant_id: str, item_id: str, **overrides):
    """Connect FileMetadata テーブルにテストデータを投入する。"""
    now = datetime.now(timezone.utc).isoformat()
    item = {
        "tenant_id": tenant_id,
        "item_id": item_id,
        "source": "m365",
        "container_id": "site-sales",
        "container_name": "営業部門サイト",
        "container_type": "site",
        "item_name": f"doc_{item_id}.xlsx",
        "web_url": f"https://contoso.sharepoint.com/sites/sales/doc_{item_id}.xlsx",
        "sharing_scope": "anonymous",
        "permissions": "{}",
        "permissions_count": 10,
        "mime_type": "text/plain",
        "size": 500,
        "modified_at": now,
        "is_deleted": False,
        "raw_s3_key": f"raw/{tenant_id}/{item_id}/data.xlsx",
    }
    item.update(overrides)
    connect_table.put_item(Item=item)
    return item


# ─── Fixture: 全 AWS リソースを moto で構築 ───


@pytest.fixture
def pipeline_env(monkeypatch):
    """パイプライン E2E テスト用の AWS 環境一式を moto で構築する。

    3 つの Lambda が共有する DynamoDB / SQS / S3 / SSM を一括セットアップし、
    各ハンドラモジュールのグローバルクライアントを差し替える。
    """
    with mock_aws():
        region = "ap-northeast-1"

        # --- DynamoDB: ExposureFinding テーブル ---
        ddb = boto3.resource("dynamodb", region_name=region)
        finding_table = ddb.create_table(
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
        finding_table.meta.client.get_waiter("table_exists").wait(
            TableName="AIReadyGov-ExposureFinding"
        )

        # --- DynamoDB: Connect FileMetadata テーブル ---
        connect_table = ddb.create_table(
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
        connect_table.meta.client.get_waiter("table_exists").wait(
            TableName="AIReadyConnect-FileMetadata"
        )

        # --- SQS ---
        sqs_client = boto3.client("sqs", region_name=region)
        queue = sqs_client.create_queue(
            QueueName="AIReadyGov-SensitivityDetectionQueue"
        )
        queue_url = queue["QueueUrl"]

        # --- S3 ---
        s3_client = boto3.client("s3", region_name=region)
        raw_bucket = "aireadyconnect-raw-payload-123456789012"
        report_bucket = "aireadygov-reports-123456789012"
        for bucket_name in (raw_bucket, report_bucket):
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )

        # --- SSM ---
        ssm_client = boto3.client("ssm", region_name=region)
        ssm_params = {
            "/aiready/governance/risk_score_threshold": "2.0",
            "/aiready/governance/max_exposure_score": "10.0",
            "/aiready/governance/permissions_count_threshold": "50",
            "/aiready/governance/rescan_interval_days": "7",
            "/aiready/governance/max_file_size_bytes": "52428800",
            "/aiready/governance/max_text_length": "500000",
        }
        for name, value in ssm_params.items():
            ssm_client.put_parameter(Name=name, Value=value, Type="String")

        # --- 環境変数 ---
        monkeypatch.setenv("FINDING_TABLE_NAME", "AIReadyGov-ExposureFinding")
        monkeypatch.setenv("CONNECT_TABLE_NAME", "AIReadyConnect-FileMetadata")
        monkeypatch.setenv("SENSITIVITY_QUEUE_URL", queue_url)
        monkeypatch.setenv("RAW_PAYLOAD_BUCKET", raw_bucket)
        monkeypatch.setenv("REPORT_BUCKET", report_bucket)

        # --- ハンドラモジュールのクライアント差し替え ---
        import handlers.analyze_exposure as ae_module
        import handlers.detect_sensitivity as ds_module
        import handlers.batch_scoring as bs_module
        import shared.config as config_module

        # analyzeExposure
        ae_module._sqs_client = sqs_client
        set_finding_table(finding_table)

        # detectSensitivity
        ds_module.set_finding_table(finding_table)
        ds_module.set_s3_client(s3_client)

        # batchScoring
        bs_module.set_finding_table(finding_table)
        bs_module.set_connect_table(connect_table)
        bs_module.set_sqs_client(sqs_client)
        bs_module.set_s3_client(s3_client)

        # SSM
        config_module._ssm_client = ssm_client
        config_module.clear_ssm_cache()

        yield {
            "finding_table": finding_table,
            "connect_table": connect_table,
            "sqs_client": sqs_client,
            "s3_client": s3_client,
            "queue_url": queue_url,
            "raw_bucket": raw_bucket,
            "report_bucket": report_bucket,
            "ae_module": ae_module,
            "ds_module": ds_module,
            "bs_module": bs_module,
        }

        # --- クリーンアップ ---
        set_finding_table(None)
        ae_module._sqs_client = None
        ds_module.set_finding_table(None)
        ds_module.set_s3_client(None)
        bs_module.set_finding_table(None)
        bs_module.set_connect_table(None)
        bs_module.set_sqs_client(None)
        bs_module.set_s3_client(None)
        config_module._ssm_client = None
        config_module.clear_ssm_cache()


# ============================================================
# パイプライン E2E テストシナリオ
# ============================================================


class TestPipelineE2E_RealtimeFlow:
    """シナリオ 1: リアルタイムフロー全通し

    FileMetadata INSERT → analyzeExposure → Finding 生成 + SQS 送信
    → detectSensitivity → Finding の sensitivity 情報更新
    """

    @patch("handlers.detect_sensitivity.detect_pii")
    @patch("handlers.detect_sensitivity.detect_secrets")
    def test_full_realtime_pipeline(
        self, mock_secrets, mock_pii, pipeline_env
    ):
        from handlers.analyze_exposure import handler as ae_handler
        from handlers.detect_sensitivity import handler as ds_handler

        env = pipeline_env
        s3 = env["s3_client"]

        # Step 1: S3 にファイルを配置
        s3.put_object(
            Bucket=env["raw_bucket"],
            Key="raw/tenant-001/item-e2e-001/data.xlsx",
            Body=b"Name,Phone\nTanaka Taro,090-1234-5678\n",
        )

        # Step 2: analyzeExposure 実行（DynamoDB Streams INSERT）
        new_img = _make_new_image(sharing_scope="anonymous")
        record = _make_stream_record("INSERT", new_image=new_img)
        ae_result = ae_handler(_make_stream_event(record), None)

        assert ae_result["processed"] == 1
        assert ae_result["errors"] == 0

        # Finding が生成されたことを確認
        findings = env["finding_table"].scan()["Items"]
        assert len(findings) == 1
        finding = findings[0]
        assert finding["status"] == "new"
        assert float(finding["exposure_score"]) == 5.0
        assert "public_link" in finding["exposure_vectors"]
        assert "G3" in finding["matched_guards"]
        assert finding["pii_detected"] is False
        assert finding["sensitivity_scan_at"] is None

        # SQS にメッセージが送信されたことを確認
        sqs_messages = _drain_sqs(env["sqs_client"], env["queue_url"])
        assert len(sqs_messages) == 1
        sqs_msg = sqs_messages[0]
        assert sqs_msg["tenant_id"] == "tenant-001"
        assert sqs_msg["trigger"] == "realtime"

        # Step 3: detectSensitivity 実行（SQS メッセージを手動投入）
        mock_pii.return_value = PIIDetectionResult(
            detected=True,
            types=["PERSON", "PHONE_NUMBER"],
            count=2,
            density="low",
            high_risk_detected=False,
            details=[
                PIIEntity(type="PERSON", start=0, end=11, score=0.85),
                PIIEntity(type="PHONE_NUMBER", start=12, end=25, score=0.9),
            ],
        )
        mock_secrets.return_value = SecretDetectionResult()

        ds_event = _make_sqs_event([sqs_msg])
        ds_result = ds_handler(ds_event, None)

        assert ds_result["processed"] == 1
        assert ds_result["errors"] == 0

        # Finding が更新されたことを確認
        updated_finding = env["finding_table"].get_item(
            Key={"tenant_id": finding["tenant_id"], "finding_id": finding["finding_id"]}
        )["Item"]
        assert updated_finding["pii_detected"] is True
        assert updated_finding["pii_count"] == 2
        assert updated_finding["pii_density"] == "low"
        assert set(updated_finding["pii_types"]) == {"PERSON", "PHONE_NUMBER"}
        assert updated_finding["sensitivity_scan_at"] is not None
        assert float(updated_finding["sensitivity_score"]) == 2.5
        assert float(updated_finding["risk_score"]) > 0

    @patch("handlers.detect_sensitivity.detect_pii")
    @patch("handlers.detect_sensitivity.detect_secrets")
    def test_secret_detection_pipeline(
        self, mock_secrets, mock_pii, pipeline_env
    ):
        """Secret 入りファイルの場合、sensitivity_score が 5.0 になる。"""
        from handlers.analyze_exposure import handler as ae_handler
        from handlers.detect_sensitivity import handler as ds_handler

        env = pipeline_env

        env["s3_client"].put_object(
            Bucket=env["raw_bucket"],
            Key="raw/tenant-001/item-secret-001/data.txt",
            Body=b"AKIAIOSFODNN7EXAMPLE\npassword=secret123!",
        )

        new_img = _make_new_image(
            item_id="item-secret-001",
            item_name="credentials.txt",
            raw_s3_key="raw/tenant-001/item-secret-001/data.txt",
        )
        ae_handler(_make_stream_event(_make_stream_record("INSERT", new_image=new_img)), None)

        sqs_messages = _drain_sqs(env["sqs_client"], env["queue_url"])
        assert len(sqs_messages) >= 1

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

        ds_handler(_make_sqs_event(sqs_messages), None)

        finding_id = generate_finding_id("tenant-001", "m365", "item-secret-001")
        updated = get_finding("tenant-001", finding_id)
        assert updated is not None
        assert updated["secrets_detected"] is True
        assert float(updated["sensitivity_score"]) == 5.0


class TestPipelineE2E_BatchScoringFlow:
    """シナリオ 2: batchScoring フロー

    FileMetadata にアイテムを投入 → batchScoring 実行 → Finding 生成 + レポート
    """

    def test_batch_creates_findings_and_report(self, pipeline_env):
        from handlers.batch_scoring import handler as bs_handler

        env = pipeline_env

        # Connect FileMetadata にテストデータを投入
        for i in range(5):
            _insert_connect_item(
                env["connect_table"],
                tenant_id="tenant-001",
                item_id=f"batch-item-{i:03d}",
                sharing_scope="anonymous",
            )

        ctx = _make_batch_context()
        result = bs_handler({}, ctx)

        assert result["processed"] >= 5
        assert result["errors"] == 0

        # Finding が生成されていることを確認
        findings = env["finding_table"].scan()["Items"]
        assert len(findings) >= 5

        for f in findings:
            assert f["status"] in ("new", "open")
            assert float(f["risk_score"]) >= 2.0
            assert "public_link" in f["exposure_vectors"]

        # S3 にレポートが出力されていることを確認
        report_objects = env["s3_client"].list_objects_v2(
            Bucket=env["report_bucket"], Prefix="tenant-001/daily/"
        )
        assert report_objects.get("KeyCount", 0) >= 1

        report_key = report_objects["Contents"][0]["Key"]
        report_body = env["s3_client"].get_object(
            Bucket=env["report_bucket"], Key=report_key
        )["Body"].read()
        report = json.loads(report_body)

        assert report["tenant_id"] == "tenant-001"
        assert report["summary"]["total_items_scanned"] >= 5
        assert "risk_distribution" in report
        assert "pii_summary" in report
        assert "exposure_vector_distribution" in report
        assert "guard_match_distribution" in report

    def test_batch_closes_orphaned_findings(self, pipeline_env):
        """FileMetadata にないアイテムの Finding が closed される。"""
        from handlers.batch_scoring import handler as bs_handler

        env = pipeline_env

        # Finding テーブルに孤立 Finding を直接追加
        env["finding_table"].put_item(Item={
            "tenant_id": "tenant-001",
            "finding_id": "orphan-finding-001",
            "item_id": "orphan-item-999",
            "source": "m365",
            "status": "new",
            "exposure_score": Decimal("5.0"),
            "sensitivity_score": Decimal("2.0"),
            "activity_score": Decimal("2.0"),
            "ai_amplification": Decimal("1.0"),
            "risk_score": Decimal("20.0"),
            "risk_level": "high",
            "exposure_vectors": ["public_link"],
            "matched_guards": ["G3"],
            "container_id": "site-old",
            "container_name": "旧サイト",
            "item_name": "old.docx",
            "detected_at": "2026-02-01T00:00:00Z",
            "last_evaluated_at": "2026-02-01T00:00:00Z",
            "pii_detected": False,
            "pii_count": 0,
            "pii_density": "none",
            "secrets_detected": False,
        })

        # FileMetadata にはアクティブアイテムを 1 件入れる
        _insert_connect_item(
            env["connect_table"],
            tenant_id="tenant-001",
            item_id="active-item-001",
            sharing_scope="anonymous",
        )

        ctx = _make_batch_context()
        bs_handler({}, ctx)

        # 孤立 Finding が closed されたことを確認
        orphan = env["finding_table"].get_item(
            Key={"tenant_id": "tenant-001", "finding_id": "orphan-finding-001"}
        )["Item"]
        assert orphan["status"] == "closed"

    def test_batch_enqueues_unscanned_items(self, pipeline_env):
        """sensitivity_scan_at 未設定の Finding が SQS に投入される。"""
        from handlers.batch_scoring import handler as bs_handler

        env = pipeline_env

        for i in range(3):
            _insert_connect_item(
                env["connect_table"],
                tenant_id="tenant-001",
                item_id=f"unscan-item-{i:03d}",
                sharing_scope="anonymous",
            )

        ctx = _make_batch_context()
        bs_handler({}, ctx)

        sqs_messages = _drain_sqs(env["sqs_client"], env["queue_url"])
        assert len(sqs_messages) >= 3

        for msg in sqs_messages:
            assert msg["trigger"] == "batch"
            assert "tenant_id" in msg
            assert "finding_id" in msg


class TestPipelineE2E_DeletionFlow:
    """シナリオ 3: 削除フロー

    analyzeExposure で Finding 作成 → is_deleted=true → Finding closed
    → batchScoring でも確認
    """

    def test_deletion_closes_finding_then_batch_confirms(self, pipeline_env):
        from handlers.analyze_exposure import handler as ae_handler
        from handlers.batch_scoring import handler as bs_handler

        env = pipeline_env

        # Step 1: Finding を作成
        new_img = _make_new_image(
            item_id="item-del-001",
            raw_s3_key="raw/tenant-001/item-del-001/data.xlsx",
        )
        ae_handler(_make_stream_event(_make_stream_record("INSERT", new_image=new_img)), None)
        _drain_sqs(env["sqs_client"], env["queue_url"])

        findings = env["finding_table"].scan()["Items"]
        assert len(findings) == 1
        assert findings[0]["status"] == "new"

        # Step 2: is_deleted で削除
        del_img = _make_new_image(
            item_id="item-del-001",
            is_deleted=True,
            raw_s3_key="raw/tenant-001/item-del-001/data.xlsx",
        )
        ae_handler(
            _make_stream_event(_make_stream_record("MODIFY", new_image=del_img, old_image=new_img)),
            None,
        )

        findings = env["finding_table"].scan()["Items"]
        assert len(findings) == 1
        assert findings[0]["status"] == "closed"

        # Step 3: batchScoring を実行しても closed のまま
        _insert_connect_item(
            env["connect_table"],
            tenant_id="tenant-001",
            item_id="item-del-001",
            is_deleted=True,
        )
        bs_handler({}, _make_batch_context())

        findings = env["finding_table"].scan()["Items"]
        closed_findings = [f for f in findings if f["item_id"] == "item-del-001"]
        assert all(f["status"] == "closed" for f in closed_findings)


class TestPipelineE2E_SuppressionFlow:
    """シナリオ 4: 抑制 → 期限切れ → 再評価

    analyzeExposure で Finding 作成 → acknowledged → batchScoring で期限切れ処理
    """

    def test_suppression_expiry_reopens_finding(self, pipeline_env):
        from handlers.analyze_exposure import handler as ae_handler
        from handlers.batch_scoring import handler as bs_handler
        from services.finding_manager import acknowledge_finding

        env = pipeline_env

        # Step 1: Finding を作成
        new_img = _make_new_image(
            item_id="item-sup-001",
            raw_s3_key="raw/tenant-001/item-sup-001/data.xlsx",
        )
        ae_handler(_make_stream_event(_make_stream_record("INSERT", new_image=new_img)), None)
        _drain_sqs(env["sqs_client"], env["queue_url"])

        findings = env["finding_table"].scan()["Items"]
        assert len(findings) == 1
        finding_id = findings[0]["finding_id"]

        # Step 2: acknowledged に変更（期限を過去に設定）
        past_time = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        acknowledge_finding(
            tenant_id="tenant-001",
            finding_id=finding_id,
            suppress_until=past_time,
            reason="テスト目的の抑制。業務上必要な共有であり一時的にリスクを受容する。30日後に再評価予定。",
            acknowledged_by="admin@contoso.com",
        )

        ack_finding = get_finding("tenant-001", finding_id)
        assert ack_finding["status"] == "acknowledged"

        # Step 3: Connect テーブルにアイテムを入れて batchScoring 実行
        _insert_connect_item(
            env["connect_table"],
            tenant_id="tenant-001",
            item_id="item-sup-001",
            sharing_scope="anonymous",
        )

        bs_handler({}, _make_batch_context())

        # 期限切れ → リスク残存 → open に戻る
        after = get_finding("tenant-001", finding_id)
        assert after["status"] == "open"
        assert after.get("suppress_until") is None


class TestPipelineE2E_MultiTenant:
    """シナリオ 5: マルチテナント処理

    複数テナントの Finding が独立して生成・レポートされる
    """

    def test_multi_tenant_batch_processing(self, pipeline_env):
        from handlers.batch_scoring import handler as bs_handler

        env = pipeline_env

        for i in range(3):
            _insert_connect_item(
                env["connect_table"],
                tenant_id="tenant-A",
                item_id=f"item-a-{i:03d}",
                sharing_scope="anonymous",
                container_name="テナントA営業",
            )
        for i in range(2):
            _insert_connect_item(
                env["connect_table"],
                tenant_id="tenant-B",
                item_id=f"item-b-{i:03d}",
                sharing_scope="organization",
                container_name="テナントBサイト",
            )

        result = bs_handler({}, _make_batch_context())
        assert result["processed"] >= 5
        assert result["errors"] == 0

        findings = env["finding_table"].scan()["Items"]
        tenant_ids = {f["tenant_id"] for f in findings}
        assert "tenant-A" in tenant_ids
        assert "tenant-B" in tenant_ids

        # テナント別レポートが出力されている
        for tid in ("tenant-A", "tenant-B"):
            objs = env["s3_client"].list_objects_v2(
                Bucket=env["report_bucket"], Prefix=f"{tid}/daily/"
            )
            assert objs.get("KeyCount", 0) >= 1


class TestPipelineE2E_DLQEmpty:
    """シナリオ 6: DLQ にメッセージが滞留しないことを確認

    正常処理フローの実行後、メインキューが空であることを確認する。
    """

    @patch("handlers.detect_sensitivity.detect_pii")
    @patch("handlers.detect_sensitivity.detect_secrets")
    def test_no_messages_left_after_pipeline(
        self, mock_secrets, mock_pii, pipeline_env
    ):
        from handlers.analyze_exposure import handler as ae_handler
        from handlers.detect_sensitivity import handler as ds_handler

        env = pipeline_env

        env["s3_client"].put_object(
            Bucket=env["raw_bucket"],
            Key="raw/tenant-001/item-dlq-001/data.txt",
            Body=b"test content",
        )

        new_img = _make_new_image(
            item_id="item-dlq-001",
            raw_s3_key="raw/tenant-001/item-dlq-001/data.txt",
        )
        ae_handler(_make_stream_event(_make_stream_record("INSERT", new_image=new_img)), None)

        mock_pii.return_value = PIIDetectionResult()
        mock_secrets.return_value = SecretDetectionResult()

        sqs_messages = _drain_sqs(env["sqs_client"], env["queue_url"])
        ds_handler(_make_sqs_event(sqs_messages), None)

        # 全メッセージ処理後、キューが空
        remaining = _get_sqs_messages(env["sqs_client"], env["queue_url"])
        assert len(remaining) == 0


class TestPipelineE2E_HighRiskPII:
    """シナリオ 7: 高リスク PII（マイナンバー）検出 → sensitivity_score = 4.0

    analyzeExposure → detectSensitivity でマイナンバー検出時のスコア確認
    """

    @patch("handlers.detect_sensitivity.detect_pii")
    @patch("handlers.detect_sensitivity.detect_secrets")
    def test_high_risk_pii_updates_score(
        self, mock_secrets, mock_pii, pipeline_env
    ):
        from handlers.analyze_exposure import handler as ae_handler
        from handlers.detect_sensitivity import handler as ds_handler

        env = pipeline_env

        env["s3_client"].put_object(
            Bucket=env["raw_bucket"],
            Key="raw/tenant-001/item-mynumber-001/data.txt",
            Body="マイナンバー: 1234-5678-9012".encode("utf-8"),
        )

        new_img = _make_new_image(
            item_id="item-mynumber-001",
            item_name="マイナンバー台帳.xlsx",
            raw_s3_key="raw/tenant-001/item-mynumber-001/data.txt",
        )
        ae_handler(_make_stream_event(_make_stream_record("INSERT", new_image=new_img)), None)

        sqs_messages = _drain_sqs(env["sqs_client"], env["queue_url"])
        assert len(sqs_messages) >= 1

        mock_pii.return_value = PIIDetectionResult(
            detected=True,
            types=["my_number"],
            count=1,
            density="low",
            high_risk_detected=True,
            details=[PIIEntity(type="my_number", start=0, end=16, score=0.95)],
        )
        mock_secrets.return_value = SecretDetectionResult()

        ds_handler(_make_sqs_event(sqs_messages), None)

        finding_id = generate_finding_id("tenant-001", "m365", "item-mynumber-001")
        updated = get_finding("tenant-001", finding_id)
        assert updated is not None
        assert updated["pii_detected"] is True
        assert float(updated["sensitivity_score"]) == 4.0


class TestPipelineE2E_ScoreRecalculation:
    """シナリオ 8: batchScoring が detectSensitivity の正式スコアを維持する

    detectSensitivity 後の sensitivity_score が batchScoring で上書きされないことを確認
    """

    @patch("handlers.detect_sensitivity.detect_pii")
    @patch("handlers.detect_sensitivity.detect_secrets")
    def test_batch_preserves_formal_sensitivity_score(
        self, mock_secrets, mock_pii, pipeline_env
    ):
        from handlers.analyze_exposure import handler as ae_handler
        from handlers.detect_sensitivity import handler as ds_handler
        from handlers.batch_scoring import handler as bs_handler

        env = pipeline_env

        env["s3_client"].put_object(
            Bucket=env["raw_bucket"],
            Key="raw/tenant-001/item-preserve-001/data.txt",
            Body=b"Name: Tanaka",
        )

        new_img = _make_new_image(
            item_id="item-preserve-001",
            item_name="general_doc.txt",
            raw_s3_key="raw/tenant-001/item-preserve-001/data.txt",
        )
        ae_handler(_make_stream_event(_make_stream_record("INSERT", new_image=new_img)), None)

        sqs_messages = _drain_sqs(env["sqs_client"], env["queue_url"])

        mock_pii.return_value = PIIDetectionResult(
            detected=True,
            types=["PERSON", "PHONE_NUMBER"],
            count=15,
            density="medium",
            high_risk_detected=False,
            details=[PIIEntity(type="PERSON", start=0, end=6, score=0.85)] * 15,
        )
        mock_secrets.return_value = SecretDetectionResult()

        ds_handler(_make_sqs_event(sqs_messages), None)

        finding_id = generate_finding_id("tenant-001", "m365", "item-preserve-001")
        after_ds = get_finding("tenant-001", finding_id)
        formal_sensitivity = float(after_ds["sensitivity_score"])
        assert formal_sensitivity == 3.5

        # batchScoring 実行
        _insert_connect_item(
            env["connect_table"],
            tenant_id="tenant-001",
            item_id="item-preserve-001",
            sharing_scope="anonymous",
            item_name="general_doc.txt",
        )

        bs_handler({}, _make_batch_context())
        _drain_sqs(env["sqs_client"], env["queue_url"])

        after_batch = get_finding("tenant-001", finding_id)
        assert float(after_batch["sensitivity_score"]) == formal_sensitivity


class TestPipelineE2E_CloudWatchLogs:
    """シナリオ 9: 各 Lambda の処理結果にログ相当の情報が含まれる

    ハンドラの返却値で処理数・エラー数を確認する。
    """

    def test_all_handlers_return_stats(self, pipeline_env):
        from handlers.analyze_exposure import handler as ae_handler
        from handlers.batch_scoring import handler as bs_handler

        env = pipeline_env

        # analyzeExposure
        new_img = _make_new_image(item_id="item-log-001")
        ae_result = ae_handler(
            _make_stream_event(_make_stream_record("INSERT", new_image=new_img)), None,
        )
        assert "processed" in ae_result
        assert "errors" in ae_result
        _drain_sqs(env["sqs_client"], env["queue_url"])

        # batchScoring
        _insert_connect_item(
            env["connect_table"],
            tenant_id="tenant-001",
            item_id="item-log-001",
            sharing_scope="anonymous",
        )
        bs_result = bs_handler({}, _make_batch_context())
        assert "processed" in bs_result
        assert "errors" in bs_result
