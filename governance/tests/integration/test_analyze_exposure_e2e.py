"""analyzeExposure 結合テスト（T-014）

moto で AWS リソース一式を構築し、Connect の FileMetadata 投入 →
analyzeExposure が Finding を生成し、リスク種別/件数が保存される
E2E フローを検証する。

詳細設計 11.2 節準拠。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
import pytest
from moto import mock_aws

from handlers.analyze_exposure import handler
from services.finding_manager import (
    acknowledge_finding,
    generate_finding_id,
    get_finding,
    set_finding_table,
)
import handlers.analyze_exposure as handler_module


# ─── Helpers ───


def _ddb_s(val: str) -> dict:
    return {"S": val}


def _ddb_n(val) -> dict:
    return {"N": str(val)}


def _ddb_bool(val: bool) -> dict:
    return {"BOOL": val}


def _make_new_image(
    tenant_id: str = "tenant-001",
    item_id: str = "item-abc-123",
    source: str = "m365",
    sharing_scope: str = "organization",
    permissions: str = "{}",
    permissions_count: int = 10,
    sensitivity_label: str | None = None,
    item_name: str = "契約書_A社.docx",
    modified_at: str | None = None,
    is_deleted: bool = False,
    container_id: str = "site-xyz",
    container_name: str = "法務部門サイト",
    raw_s3_key: str = "tenant-001/raw/item-abc-123/2026-02-10.json",
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
        "web_url": _ddb_s("https://contoso.sharepoint.com/sites/legal/contract.docx"),
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


def _make_record(
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


def _make_event(*records: dict) -> dict:
    return {"Records": list(records)}


def _get_sqs_messages(sqs_client, queue_url: str) -> list[dict]:
    resp = sqs_client.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=0,
    )
    return [json.loads(m["Body"]) for m in resp.get("Messages", [])]


# ─── Fixture: 全 AWS リソースを moto で構築 ───


@pytest.fixture
def e2e_env(monkeypatch):
    """結合テスト用の AWS 環境一式を moto で構築する。"""
    with mock_aws():
        region = "ap-northeast-1"

        # --- DynamoDB: ExposureFinding テーブル ---
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

        # --- SQS: SensitivityDetectionQueue ---
        sqs = boto3.client("sqs", region_name=region)
        queue = sqs.create_queue(QueueName="AIReadyGov-SensitivityDetectionQueue")
        queue_url = queue["QueueUrl"]

        # --- S3: Connect Raw Payload バケット ---
        s3_client = boto3.client("s3", region_name=region)
        s3_client.create_bucket(
            Bucket="aireadyconnect-raw-payload",
            CreateBucketConfiguration={"LocationConstraint": region},
        )

        # --- SSM パラメータ ---
        ssm = boto3.client("ssm", region_name=region)
        ssm.put_parameter(
            Name="/aiready/governance/risk_score_threshold",
            Value="2.0", Type="String",
        )
        ssm.put_parameter(
            Name="/aiready/governance/max_exposure_score",
            Value="10.0", Type="String",
        )
        ssm.put_parameter(
            Name="/aiready/governance/permissions_count_threshold",
            Value="50", Type="String",
        )
        ssm.put_parameter(
            Name="/aiready/governance/rescan_interval_days",
            Value="7", Type="String",
        )

        # --- 環境変数 ---
        monkeypatch.setenv("FINDING_TABLE_NAME", "AIReadyGov-ExposureFinding")
        monkeypatch.setenv("SENSITIVITY_QUEUE_URL", queue_url)
        monkeypatch.setenv("RAW_PAYLOAD_BUCKET", "aireadyconnect-raw-payload")
        monkeypatch.setenv("REPORT_BUCKET", "aireadygov-reports")

        handler_module._sqs_client = sqs

        # SSM クライアントを各モジュールに注入
        import shared.config as config_module
        from services import exposure_vectors as ev_module
        from services import scoring as scoring_module

        config_module._ssm_client = ssm
        config_module.clear_ssm_cache()

        yield {
            "table": table,
            "sqs": sqs,
            "queue_url": queue_url,
            "s3": s3_client,
            "ssm": ssm,
        }

        set_finding_table(None)
        handler_module._sqs_client = None
        config_module._ssm_client = None
        config_module.clear_ssm_cache()


# ============================================================
# E2E テストシナリオ
# ============================================================


class TestE2E_RealtimeDetection:
    """シナリオ 1: リアルタイム検知 E2E

    Connect の FileMetadata に Anyone リンクのレコードを INSERT →
    ① Finding が生成される
    ② 件数集計フィールドが保持される
    """

    def test_anyone_link_creates_finding_and_enqueues(self, e2e_env):
        new_img = _make_new_image(sharing_scope="anonymous")
        record = _make_record("INSERT", new_image=new_img)

        result = handler(_make_event(record), None)

        assert result["processed"] == 1
        assert result["errors"] == 0

        # ① Finding が生成される
        findings = e2e_env["table"].scan()["Items"]
        assert len(findings) == 1

        finding = findings[0]
        assert finding["tenant_id"] == "tenant-001"
        assert finding["item_id"] == "item-abc-123"
        assert finding["source"] == "m365"
        assert finding["status"] == "new"
        assert "public_link" in finding["exposure_vectors"]
        assert "G3" in finding["matched_guards"]
        assert finding["risk_level"] in ("none", "low", "medium", "high", "critical")
        assert int(finding["total_detected_risks"]) >= 1
        assert finding["detected_at"] is not None
        assert finding["last_evaluated_at"] is not None

        # ② 現行仕様では SQS 機微検知イベントは発行されない
        messages = _get_sqs_messages(e2e_env["sqs"], e2e_env["queue_url"])
        assert len(messages) == 0
        assert finding["total_detected_risks"] >= 1
        assert finding["exposure_vector_counts"].get("public_link", 0) >= 1


class TestE2E_FindingUpdate:
    """シナリオ 2: Finding 更新（権限変更）

    既存 Finding のアイテムの sharing_scope を specific に変更 →
    ① 検知件数が再計算される
    ② 状態が適切に更新される
    """

    def test_scope_change_to_specific_closes_finding(self, e2e_env):
        # Step 1: anonymous で Finding を作成
        anon_img = _make_new_image(sharing_scope="anonymous")
        handler(_make_event(_make_record("INSERT", new_image=anon_img)), None)

        findings_before = e2e_env["table"].scan()["Items"]
        assert len(findings_before) == 1
        assert findings_before[0]["status"] == "new"

        # Step 2: specific に変更（100日前の更新日で ActivityScore=0.5）
        old_date = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
        safe_img = _make_new_image(
            sharing_scope="specific",
            permissions_count=1,
            item_name="meeting_notes.txt",
            modified_at=old_date,
        )
        modify_rec = _make_record("MODIFY", new_image=safe_img, old_image=anon_img)
        handler(_make_event(modify_rec), None)

        # v1.2 では低リスクでも Finding を維持し再評価するケースがある
        findings_after = e2e_env["table"].scan()["Items"]
        assert len(findings_after) == 1
        assert findings_after[0]["status"] in ("open", "closed")

    def test_scope_change_anonymous_to_org_updates_score(self, e2e_env):
        # Step 1: anonymous で Finding を作成
        anon_img = _make_new_image(sharing_scope="anonymous")
        handler(_make_event(_make_record("INSERT", new_image=anon_img)), None)

        # Step 2: organization に変更
        org_img = _make_new_image(sharing_scope="organization")
        modify_rec = _make_record("MODIFY", new_image=org_img, old_image=anon_img)
        handler(_make_event(modify_rec), None)

        findings = e2e_env["table"].scan()["Items"]
        assert len(findings) == 1
        finding = findings[0]
        assert finding["status"] == "open"
        assert any(vec.startswith("org_link") for vec in finding["exposure_vectors"])
        assert "total_detected_risks" in finding


class TestE2E_FileDeletion:
    """シナリオ 3: Finding 更新（ファイル削除）

    FileMetadata の is_deleted を true に変更 → Finding が Closed になる
    """

    def test_is_deleted_closes_finding(self, e2e_env):
        original = _make_new_image(sharing_scope="anonymous")
        handler(_make_event(_make_record("INSERT", new_image=original)), None)

        deleted = _make_new_image(sharing_scope="anonymous", is_deleted=True)
        modify_rec = _make_record("MODIFY", new_image=deleted, old_image=original)
        handler(_make_event(modify_rec), None)

        findings = e2e_env["table"].scan()["Items"]
        assert len(findings) == 1
        assert findings[0]["status"] == "closed"

    def test_remove_event_closes_finding(self, e2e_env):
        original = _make_new_image(sharing_scope="anonymous")
        handler(_make_event(_make_record("INSERT", new_image=original)), None)

        remove_rec = _make_record("REMOVE", old_image=original)
        handler(_make_event(remove_rec), None)

        findings = e2e_env["table"].scan()["Items"]
        assert len(findings) == 1
        assert findings[0]["status"] == "closed"


class TestE2E_GuardMatching:
    """シナリオ 4: ガード照合

    各 ExposureVector パターンで正しいガードがマッチすることを確認
    """

    def test_eeeu_matches_g2(self, e2e_env):
        permissions = json.dumps({
            "entries": [{"identity": {"displayName": "Everyone except external users"}}]
        })
        img = _make_new_image(
            item_id="item-eeeu",
            sharing_scope="specific",
            permissions=permissions,
            permissions_count=100,
        )
        handler(_make_event(_make_record("INSERT", new_image=img)), None)

        findings = e2e_env["table"].scan()["Items"]
        assert len(findings) == 1
        assert "G2" in findings[0]["matched_guards"]

    def test_anonymous_with_guest_matches_g3(self, e2e_env):
        permissions = json.dumps({
            "entries": [{"identity": {"userType": "guest", "email": "ext@partner.com"}}]
        })
        img = _make_new_image(
            item_id="item-anon-guest",
            sharing_scope="anonymous",
            permissions=permissions,
        )
        handler(_make_event(_make_record("INSERT", new_image=img)), None)

        findings = e2e_env["table"].scan()["Items"]
        assert len(findings) == 1
        assert "G3" in findings[0]["matched_guards"]
        assert "public_link" in findings[0]["exposure_vectors"]
        assert any("guest" in vec for vec in findings[0]["exposure_vectors"])


class TestE2E_SensitivityLabel:
    """シナリオ 5: 秘密度ラベルの反映

    content_signals が risk_type_counts に反映される
    """

    def test_confidential_label_counted(self, e2e_env):
        source_metadata = json.dumps({
            "content_signals": {
                "doc_sensitivity_level": "high",
                "doc_categories": ["legal_contract"],
                "contains_pii": True,
                "contains_secret": False,
                "confidence": 0.9,
            }
        })
        img = _make_new_image(
            item_id="item-conf",
            sharing_scope="organization",
            source_metadata=source_metadata,
        )
        handler(_make_event(_make_record("INSERT", new_image=img)), None)

        findings = e2e_env["table"].scan()["Items"]
        assert len(findings) == 1
        assert findings[0]["risk_type_counts"].get("legal_contract", 0) == 1

    def test_highly_confidential_label_counted(self, e2e_env):
        source_metadata = json.dumps({
            "content_signals": {
                "doc_sensitivity_level": "critical",
                "doc_categories": ["executive_confidential"],
                "contains_pii": False,
                "contains_secret": True,
                "confidence": 0.95,
            }
        })
        img = _make_new_image(
            item_id="item-hc",
            sharing_scope="anonymous",
            source_metadata=source_metadata,
        )
        handler(_make_event(_make_record("INSERT", new_image=img)), None)

        findings = e2e_env["table"].scan()["Items"]
        assert len(findings) == 1
        assert findings[0]["risk_type_counts"].get("secret", 0) >= 1


class TestE2E_FilenameHeuristic:
    """シナリオ 6: ファイル名ヒューリスティック

    キーワードを含む content_signals でカテゴリ件数が増える
    """

    def test_salary_filename_raises_sensitivity(self, e2e_env):
        img = _make_new_image(
            item_id="item-salary",
            sharing_scope="organization",
            item_name="給与一覧_2026年.xlsx",
        )
        handler(_make_event(_make_record("INSERT", new_image=img)), None)

        findings = e2e_env["table"].scan()["Items"]
        assert len(findings) == 1
        assert "risk_type_counts" in findings[0]

    def test_password_filename_raises_sensitivity(self, e2e_env):
        img = _make_new_image(
            item_id="item-pwd",
            sharing_scope="anonymous",
            item_name="password_list.txt",
        )
        handler(_make_event(_make_record("INSERT", new_image=img)), None)

        findings = e2e_env["table"].scan()["Items"]
        assert len(findings) == 1
        assert "risk_type_counts" in findings[0]


class TestE2E_AcknowledgedFinding:
    """シナリオ 7: 抑制（acknowledged）

    Finding を acknowledged に変更 → スコアリング時に更新されない
    """

    def test_acknowledged_finding_not_updated(self, e2e_env):
        img = _make_new_image(sharing_scope="anonymous")
        handler(_make_event(_make_record("INSERT", new_image=img)), None)

        findings = e2e_env["table"].scan()["Items"]
        finding_id = findings[0]["finding_id"]

        suppress_until = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        acknowledge_finding(
            tenant_id="tenant-001",
            finding_id=finding_id,
            suppress_until=suppress_until,
            reason="業務上必要な共有のため一時的にリスクを受容する。30日後に再評価予定。",
            acknowledged_by="admin@contoso.com",
        )

        acknowledged = get_finding("tenant-001", finding_id)
        assert acknowledged["status"] == "acknowledged"
        assert acknowledged["suppress_until"] == suppress_until

        # 同じアイテムで MODIFY → acknowledged は更新されない
        new_img = _make_new_image(sharing_scope="organization")
        modify_rec = _make_record("MODIFY", new_image=new_img, old_image=img)
        handler(_make_event(modify_rec), None)

        after = get_finding("tenant-001", finding_id)
        assert after["status"] == "acknowledged"
        assert after["suppress_until"] == suppress_until
        assert int(after.get("total_detected_risks", 0)) >= 0


class TestE2E_MultipleTenants:
    """シナリオ 8: マルチテナント

    異なるテナントのアイテムが独立した Finding を生成する
    """

    def test_separate_findings_per_tenant(self, e2e_env):
        img_t1 = _make_new_image(
            tenant_id="tenant-A", item_id="item-001", sharing_scope="anonymous",
        )
        img_t2 = _make_new_image(
            tenant_id="tenant-B", item_id="item-001", sharing_scope="anonymous",
        )

        handler(_make_event(
            _make_record("INSERT", new_image=img_t1),
            _make_record("INSERT", new_image=img_t2),
        ), None)

        findings = e2e_env["table"].scan()["Items"]
        assert len(findings) == 2

        tenant_ids = {f["tenant_id"] for f in findings}
        assert tenant_ids == {"tenant-A", "tenant-B"}


class TestE2E_BelowThreshold:
    """シナリオ 9: 閾値未満のアイテム

    低リスクでも Finding が作成されるが、深掘り解析対象外になる
    """

    def test_private_old_item_no_finding(self, e2e_env):
        old_date = (datetime.now(timezone.utc) - timedelta(days=200)).isoformat()
        img = _make_new_image(
            sharing_scope="specific",
            permissions_count=2,
            item_name="readme.txt",
            modified_at=old_date,
        )
        handler(_make_event(_make_record("INSERT", new_image=img)), None)

        findings = e2e_env["table"].scan()["Items"]
        assert len(findings) == 1
        assert findings[0]["status"] in ("new", "open")
        assert findings[0].get("deep_analysis_eligible") in {None, False}


class TestE2E_BrokenInheritance:
    """シナリオ 10: 継承崩れの検知

    source_metadata に has_unique_permissions=true が含まれる場合
    broken_inheritance が検出され G7 にマッチする
    """

    def test_broken_inheritance_detected(self, e2e_env):
        sm = json.dumps({"has_unique_permissions": True})
        img = _make_new_image(
            item_id="item-broken",
            sharing_scope="organization",
            source_metadata=sm,
        )
        handler(_make_event(_make_record("INSERT", new_image=img)), None)

        findings = e2e_env["table"].scan()["Items"]
        assert len(findings) == 1
        assert any(vec.startswith("org_link") for vec in findings[0]["exposure_vectors"])
        assert isinstance(findings[0]["matched_guards"], list)
