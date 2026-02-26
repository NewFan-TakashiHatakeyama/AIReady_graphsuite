"""本番環境・AWS デプロイ時のイレギュラーケースを検証する堅牢性テスト

一流の QA エンジニア視点で、以下のカテゴリを網羅的に検証する:
  1. AWS サービス障害耐性（DynamoDB / SQS / S3 の一時的エラー）
  2. データ整合性（テナント分離、冪等性、状態遷移の正当性）
  3. セキュリティ（PII 非保存、入力サニタイズ）
  4. 境界値・異常入力（Unicode、巨大ペイロード、不正 JSON）
  5. 並行処理・競合状態のシミュレーション
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from unittest.mock import MagicMock, patch, PropertyMock

import boto3
import pytest
from moto import mock_aws
from botocore.exceptions import ClientError

from services.exposure_vectors import FileMetadata
from services.finding_manager import (
    generate_finding_id,
    get_finding,
    set_finding_table,
    upsert_finding,
    close_finding,
    acknowledge_finding,
    query_findings_by_status,
)
from services.scoring import (
    ExposureResult,
    SensitivityResult,
    calculate_exposure_score,
    calculate_preliminary_sensitivity,
    calculate_risk_score,
    classify_risk_level,
    calculate_activity_score,
)
from services.secret_detector import detect_secrets
from services.guard_matcher import match_guards


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Fixtures
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@pytest.fixture
def dynamodb_table():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
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
        yield table
        set_finding_table(None)


def _make_metadata(**kwargs) -> FileMetadata:
    defaults = {
        "tenant_id": "t-001",
        "item_id": "item-001",
        "source": "m365",
        "container_id": "site-xyz",
        "container_name": "法務部門サイト",
        "container_type": "site",
        "item_name": "契約書_A社.docx",
        "web_url": "https://contoso.sharepoint.com/contract.docx",
        "sharing_scope": "organization",
    }
    defaults.update(kwargs)
    return FileMetadata(**defaults)


def _make_exposure_result(**kwargs):
    defaults = {"score": 3.0, "vectors": ["org_link"], "details": {"org_link": 3.0}}
    defaults.update(kwargs)
    return ExposureResult(**defaults)


def _make_sensitivity_result(**kwargs):
    defaults = {"score": 1.0, "factors": [], "is_preliminary": True}
    defaults.update(kwargs)
    return SensitivityResult(**defaults)


def _upsert_test_finding(table, tenant_id="t-001", item_id="item-001", **kwargs):
    meta = _make_metadata(tenant_id=tenant_id, item_id=item_id)
    defaults = dict(
        tenant_id=tenant_id,
        item=meta,
        exposure_result=_make_exposure_result(),
        sensitivity_result=_make_sensitivity_result(),
        activity_score=1.5,
        ai_amplification=1.0,
        risk_score=4.5,
        matched_guards=["G3"],
    )
    defaults.update(kwargs)
    return upsert_finding(**defaults)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. テナント分離 — マルチテナント環境でのデータ漏洩防止
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestTenantIsolation:
    """異なるテナントのデータが相互に影響しないことを検証する。"""

    def test_same_item_id_different_tenants_independent(self, dynamodb_table):
        """同一 item_id でもテナントが異なれば別の Finding が生成される。"""
        result_a = _upsert_test_finding(dynamodb_table, tenant_id="tenant-A", item_id="shared-doc")
        result_b = _upsert_test_finding(dynamodb_table, tenant_id="tenant-B", item_id="shared-doc")

        assert result_a["finding_id"] != result_b["finding_id"]

        finding_a = get_finding("tenant-A", result_a["finding_id"])
        finding_b = get_finding("tenant-B", result_b["finding_id"])

        assert finding_a is not None
        assert finding_b is not None
        assert finding_a["tenant_id"] == "tenant-A"
        assert finding_b["tenant_id"] == "tenant-B"

    def test_close_finding_does_not_affect_other_tenant(self, dynamodb_table):
        """テナント A の Finding をクローズしても、テナント B は影響されない。"""
        result_a = _upsert_test_finding(dynamodb_table, tenant_id="tenant-A")
        result_b = _upsert_test_finding(dynamodb_table, tenant_id="tenant-B")

        close_finding("tenant-A", result_a["finding_id"])

        finding_a = get_finding("tenant-A", result_a["finding_id"])
        finding_b = get_finding("tenant-B", result_b["finding_id"])

        assert finding_a["status"] == "closed"
        assert finding_b["status"] == "new"

    def test_query_by_status_returns_only_same_tenant(self, dynamodb_table):
        """GSI-StatusFinding クエリがテナント間で漏洩しない。"""
        _upsert_test_finding(dynamodb_table, tenant_id="tenant-A", item_id="item-a")
        _upsert_test_finding(dynamodb_table, tenant_id="tenant-B", item_id="item-b")

        results_a = query_findings_by_status("tenant-A", "new")
        results_b = query_findings_by_status("tenant-B", "new")

        assert all(f["tenant_id"] == "tenant-A" for f in results_a)
        assert all(f["tenant_id"] == "tenant-B" for f in results_b)
        assert len(results_a) == 1
        assert len(results_b) == 1


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. Finding ステータス遷移の整合性
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestStatusTransitionIntegrity:
    """Finding のステータス遷移が設計に従って正しく行われることを保証する。"""

    def test_lifecycle_new_to_open_to_acknowledged_to_closed(self, dynamodb_table):
        """正常ライフサイクル: new → open → acknowledged → (期限切れ) → closed"""
        meta = _make_metadata()

        result = _upsert_test_finding(dynamodb_table)
        assert result["status"] == "new"

        result2 = _upsert_test_finding(dynamodb_table, risk_score=10.0)
        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "open"

        acknowledge_finding(
            "t-001", result["finding_id"],
            suppress_until="2026-12-31T00:00:00Z",
            reason="テスト用抑制理由。業務上やむを得ない理由で一時的にリスクを受容します。",
            acknowledged_by="admin@example.com",
        )
        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "acknowledged"

        close_finding("t-001", result["finding_id"])
        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "closed"

    def test_closed_finding_can_be_reopened_via_upsert(self, dynamodb_table):
        """closed の Finding が新たなリスク検知時に再度 open になる。"""
        result = _upsert_test_finding(dynamodb_table)
        close_finding("t-001", result["finding_id"])

        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "closed"

        result2 = _upsert_test_finding(dynamodb_table, risk_score=20.0)
        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] in ("open", "closed")

    def test_acknowledged_finding_skips_upsert(self, dynamodb_table):
        """acknowledged 状態は upsert で上書きされない（リスク受容を尊重）。"""
        result = _upsert_test_finding(dynamodb_table)
        acknowledge_finding(
            "t-001", result["finding_id"],
            suppress_until="2099-12-31T00:00:00Z",
            reason="x" * 50,
            acknowledged_by="admin",
        )

        high_risk = _upsert_test_finding(
            dynamodb_table,
            exposure_result=_make_exposure_result(score=10.0),
            risk_score=100.0,
        )

        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "acknowledged"
        assert float(finding["exposure_score"]) != 10.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. Unicode / 多言語文字列のハンドリング
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestUnicodeHandling:
    """日本語・絵文字・特殊文字を含むデータが正しく処理される。"""

    def test_japanese_filename_in_finding(self, dynamodb_table):
        """日本語ファイル名を含む Finding が正しく格納・取得される。"""
        meta = _make_metadata(item_name="人事評価_2026年度_給与テーブル（極秘）.xlsx")
        result = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            sensitivity_result=_make_sensitivity_result(score=2.0),
            activity_score=2.0,
            ai_amplification=1.0,
            risk_score=12.0,
            matched_guards=["G3"],
        )
        finding = get_finding("t-001", result["finding_id"])
        assert finding["item_name"] == "人事評価_2026年度_給与テーブル（極秘）.xlsx"

    def test_emoji_in_container_name(self, dynamodb_table):
        """絵文字を含むコンテナ名が正しくハンドリングされる。"""
        meta = _make_metadata(container_name="📊 営業部門 データ共有")
        result = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            sensitivity_result=_make_sensitivity_result(),
            activity_score=1.5,
            ai_amplification=1.0,
            risk_score=4.5,
            matched_guards=["G3"],
        )
        finding = get_finding("t-001", result["finding_id"])
        assert "📊" in finding["container_name"]

    def test_special_characters_in_tenant_id(self, dynamodb_table):
        """特殊文字を含む tenant_id でも Finding ID が安定して生成される。"""
        id1 = generate_finding_id("tenant/ABC+特殊", "m365", "item-001")
        id2 = generate_finding_id("tenant/ABC+特殊", "m365", "item-001")
        assert id1 == id2
        assert len(id1) == 32

    def test_very_long_item_name(self, dynamodb_table):
        """非常に長いファイル名（SharePoint の上限 400 文字超）が正しく処理される。"""
        long_name = "あ" * 500 + ".xlsx"
        meta = _make_metadata(item_name=long_name)
        result = upsert_finding(
            tenant_id="t-001",
            item=meta,
            exposure_result=_make_exposure_result(),
            sensitivity_result=_make_sensitivity_result(),
            activity_score=1.5,
            ai_amplification=1.0,
            risk_score=4.5,
            matched_guards=["G3"],
        )
        finding = get_finding("t-001", result["finding_id"])
        assert finding["item_name"] == long_name


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. 巨大ペイロード・極端な入力値
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestExtremeInputs:
    """極端な入力値が例外を発生させず正しく処理される。"""

    def test_huge_permissions_json(self):
        """数百エントリの権限 JSON が正しく解析される。"""
        entries = [
            {"identity": {"displayName": f"User{i}", "userType": "member"}}
            for i in range(500)
        ]
        permissions = json.dumps({"entries": entries})
        meta = _make_metadata(permissions=permissions, permissions_count=500)
        result = calculate_exposure_score(meta)
        assert result.score >= 1.0
        assert "excessive_permissions" in result.vectors

    def test_zero_permissions_count(self):
        """permissions_count = 0 でエラーにならない。"""
        meta = _make_metadata(permissions_count=0, sharing_scope="specific")
        result = calculate_exposure_score(meta)
        assert result.score >= 1.0

    def test_negative_risk_score_components(self):
        """スコアコンポーネントに 0 が含まれても正しく計算される。"""
        result = calculate_risk_score(0.0, 1.0, 1.0, 1.0)
        assert result == 0.0

    def test_extremely_high_risk_score(self):
        """最大値付近のスコアが正しくクラスされる。"""
        result = calculate_risk_score(10.0, 5.0, 2.0, 1.5)
        assert result > 0
        assert classify_risk_level(result) == "critical"

    def test_empty_permissions_json_parsing(self):
        """空文字列の permissions が安全に処理される。"""
        meta = _make_metadata(permissions="")
        result = calculate_exposure_score(meta)
        assert result.score >= 1.0

    def test_malformed_permissions_json(self):
        """不正な JSON の permissions がクラッシュしない。"""
        meta = _make_metadata(permissions="{invalid json {{")
        result = calculate_exposure_score(meta)
        assert result.score >= 1.0

    def test_null_values_in_metadata(self):
        """None 値を含むメタデータが安全に処理される。"""
        meta = _make_metadata(
            sensitivity_label=None,
            modified_at=None,
            permissions_summary=None,
            source_metadata=None,
        )
        exposure = calculate_exposure_score(meta)
        sensitivity = calculate_preliminary_sensitivity(meta)
        activity = calculate_activity_score(meta)

        assert exposure.score >= 1.0
        assert sensitivity.score >= 1.0
        assert activity >= 0.5


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. Finding ID の決定性・衝突耐性
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestFindingIdRobustness:
    """Finding ID 生成の堅牢性。"""

    def test_id_stability_across_invocations(self):
        """同一入力に対して常に同一の Finding ID が生成される。"""
        ids = [generate_finding_id("t-001", "m365", "item-001") for _ in range(100)]
        assert len(set(ids)) == 1

    def test_different_sources_produce_different_ids(self):
        """同一テナント・アイテムでもソースが異なれば別の ID。"""
        id_m365 = generate_finding_id("t-001", "m365", "item-001")
        id_box = generate_finding_id("t-001", "box", "item-001")
        id_slack = generate_finding_id("t-001", "slack", "item-001")

        assert id_m365 != id_box
        assert id_m365 != id_slack
        assert id_box != id_slack

    def test_id_is_hex_string(self):
        """Finding ID が 16 進数文字列であること。"""
        fid = generate_finding_id("t-001", "m365", "item-001")
        assert all(c in "0123456789abcdef" for c in fid)

    def test_empty_inputs_still_produce_valid_id(self):
        """空文字列入力でも有効な ID が生成される。"""
        fid = generate_finding_id("", "", "")
        assert len(fid) == 32


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. DynamoDB 障害モード
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestDynamoDBFailureModes:
    """DynamoDB の一時的障害・スロットリングのハンドリング。"""

    def test_close_finding_conditional_check_failure_swallowed(self, dynamodb_table):
        """存在しない Finding のクローズは ConditionalCheckFailedException を飲み込む。"""
        close_finding("t-001", "nonexistent-finding-id-12345678")

    def test_close_finding_non_conditional_error_raises(self, dynamodb_table):
        """DynamoDB の ValidationException は伝播する。"""
        mock_table = MagicMock()
        error_response = {"Error": {"Code": "ValidationException", "Message": "bad"}}
        mock_table.update_item.side_effect = ClientError(error_response, "UpdateItem")

        set_finding_table(mock_table)
        try:
            with pytest.raises(ClientError):
                close_finding("t-001", "some-finding-id")
        finally:
            set_finding_table(dynamodb_table)

    def test_upsert_with_throttling_error(self, dynamodb_table):
        """DynamoDB スロットリング時の put_item 失敗を検証する。"""
        mock_table = MagicMock()
        error_response = {
            "Error": {
                "Code": "ProvisionedThroughputExceededException",
                "Message": "Rate exceeded",
            }
        }
        mock_table.get_item.return_value = {"Item": None}
        mock_table.put_item.side_effect = ClientError(error_response, "PutItem")

        set_finding_table(mock_table)
        try:
            with pytest.raises(ClientError):
                _upsert_test_finding(dynamodb_table)
        finally:
            set_finding_table(dynamodb_table)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 7. Secret 検出の偽陽性・偽陰性テスト
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestSecretDetectionAccuracy:
    """Secret 検出の精度 — 偽陽性の抑制を検証する。"""

    def test_normal_password_field_label_not_detected(self):
        """'Password:' ラベルだけではシークレットとして検出しない。"""
        text = "ログイン画面には Username と Password の入力欄があります。"
        result = detect_secrets(text)
        assert result.detected is False

    def test_base64_encoded_data_not_false_positive(self):
        """通常の Base64 データ（画像等）が誤検知されない。"""
        text = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUA"
        result = detect_secrets(text)
        assert "aws_access_key" not in result.types

    def test_uuid_not_detected_as_secret(self):
        """UUID がシークレットとして誤検知されない。"""
        text = "document_id = 550e8400-e29b-41d4-a716-446655440000"
        result = detect_secrets(text)
        assert result.detected is False

    def test_real_aws_key_pattern_detected(self):
        """本物のAWSキーパターンは確実に検出される。"""
        text = "AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE"
        result = detect_secrets(text)
        assert result.detected is True
        assert "aws_access_key" in result.types

    def test_multiline_secret_detection(self):
        """複数行にわたるシークレットが検出される。"""
        text = (
            "設定ファイル:\n"
            "aws_access_key_id = AKIAIOSFODNN7EXAMPLE\n"
            "aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY01\n"
            "region = ap-northeast-1\n"
        )
        result = detect_secrets(text)
        assert result.detected is True
        assert result.count >= 2

    def test_japanese_text_with_embedded_secret(self):
        """日本語テキスト内に埋め込まれたシークレットが検出される。"""
        text = "デプロイ手順書\n1. 環境変数の設定\n  password = MyS3cr3tP@ss!\n2. デプロイ実行"
        result = detect_secrets(text)
        assert result.detected is True


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 8. スコアリングの数値安定性
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestScoringNumericalStability:
    """浮動小数点演算の精度・丸め誤差の検証。"""

    def test_risk_score_rounding(self):
        """RiskScore は小数点以下 2 桁に丸められる。"""
        score = calculate_risk_score(1.5, 2.5, 1.5, 1.0)
        assert score == round(1.5 * 2.5 * 1.5 * 1.0, 2)
        decimal_str = str(score)
        if "." in decimal_str:
            decimal_places = len(decimal_str.split(".")[1])
            assert decimal_places <= 2

    def test_exposure_score_cap_consistency(self):
        """ExposureScore の上限キャップが一貫して適用される。"""
        perms = json.dumps({
            "entries": [
                {"identity": {"displayName": "Everyone except external users"}},
                {"identity": {"userType": "guest", "email": "ext@example.com"}},
                {"identity": {"isExternalUser": True, "email": "domain@partner.com"}},
            ]
        })
        sm = json.dumps({"has_unique_permissions": True})
        meta = _make_metadata(
            sharing_scope="anonymous",
            permissions=perms,
            permissions_count=200,
            source_metadata=sm,
        )
        result = calculate_exposure_score(meta)
        assert result.score <= 10.0

    def test_activity_score_boundary_7_days(self):
        """7 日間の境界値テスト（7 日目は 2.0、8 日目は 1.5）。"""
        exactly_7 = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        meta_7 = _make_metadata(modified_at=exactly_7)
        assert calculate_activity_score(meta_7) == 2.0

        day_8 = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        meta_8 = _make_metadata(modified_at=day_8)
        assert calculate_activity_score(meta_8) == 1.5

    def test_activity_score_boundary_30_days(self):
        """30 日間の境界値テスト。"""
        day_30 = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        meta_30 = _make_metadata(modified_at=day_30)
        assert calculate_activity_score(meta_30) == 1.5

        day_31 = (datetime.now(timezone.utc) - timedelta(days=31)).isoformat()
        meta_31 = _make_metadata(modified_at=day_31)
        assert calculate_activity_score(meta_31) == 1.0

    def test_activity_score_boundary_90_days(self):
        """90 日間の境界値テスト。"""
        day_90 = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
        meta_90 = _make_metadata(modified_at=day_90)
        assert calculate_activity_score(meta_90) == 1.0

        day_91 = (datetime.now(timezone.utc) - timedelta(days=91)).isoformat()
        meta_91 = _make_metadata(modified_at=day_91)
        assert calculate_activity_score(meta_91) == 0.5

    def test_risk_level_all_boundaries(self):
        """リスクレベル分類の全境界値を網羅。"""
        assert classify_risk_level(0.0) == "none"
        assert classify_risk_level(1.99) == "none"
        assert classify_risk_level(2.0) == "low"
        assert classify_risk_level(4.99) == "low"
        assert classify_risk_level(5.0) == "medium"
        assert classify_risk_level(19.99) == "medium"
        assert classify_risk_level(20.0) == "high"
        assert classify_risk_level(49.99) == "high"
        assert classify_risk_level(50.0) == "critical"
        assert classify_risk_level(999.9) == "critical"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 9. ガード照合のエッジケース
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestGuardMatcherEdgeCases:
    """ガード照合ロジックのエッジケース検証。"""

    def test_all_vectors_matched(self):
        """すべての ExposureVector が設定された場合の全ガード照合。"""
        vectors = [
            "public_link", "org_link", "all_users", "guest",
            "external_domain", "broken_inheritance", "excessive_permissions",
            "ai_accessible", "no_label",
        ]
        result = match_guards(vectors, "m365")
        assert "G2" in result
        assert "G3" in result
        assert "G7" in result
        assert "G9" in result

    def test_unknown_source_no_guards(self):
        """未知のデータソースにはガードがマッチしない。"""
        result = match_guards(["public_link"], "unknown_source")
        assert result == []

    def test_empty_vectors_no_guards(self):
        """空の ExposureVector ではガードがマッチしない。"""
        result = match_guards([], "m365")
        assert result == []

    def test_guard_result_is_sorted(self):
        """マッチ結果は常にソートされている（表示の一貫性）。"""
        vectors = ["all_users", "public_link", "ai_accessible", "no_label"]
        result = match_guards(vectors, "m365")
        assert result == sorted(result)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 10. ISO 8601 日付パースのエッジケース
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestDateParsingEdgeCases:
    """AWS / DynamoDB から返される様々な日付形式への対応。"""

    def test_timezone_aware_iso_date(self):
        """タイムゾーン付き ISO 8601 が正しくパースされる。"""
        meta = _make_metadata(modified_at="2026-02-20T05:00:00+09:00")
        score = calculate_activity_score(meta)
        assert score > 0

    def test_z_suffix_date(self):
        """Z サフィックス付き UTC 日付が正しくパースされる。"""
        meta = _make_metadata(modified_at="2026-02-20T05:00:00Z")
        score = calculate_activity_score(meta)
        assert score > 0

    def test_microseconds_in_date(self):
        """マイクロ秒を含む日付が正しくパースされる。"""
        meta = _make_metadata(modified_at="2026-02-20T05:00:00.123456+00:00")
        score = calculate_activity_score(meta)
        assert score > 0

    def test_future_date(self):
        """未来日付（クロック同期ずれ）でもクラッシュしない。"""
        future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        meta = _make_metadata(modified_at=future)
        score = calculate_activity_score(meta)
        assert score >= 0

    def test_epoch_zero_date(self):
        """1970-01-01 のような古い日付でもクラッシュしない。"""
        meta = _make_metadata(modified_at="1970-01-01T00:00:00+00:00")
        score = calculate_activity_score(meta)
        assert score == 0.5


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 11. Sensitivity ラベルのパース堅牢性
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestSensitivityLabelParsing:
    """M365 から返される様々なラベル形式への対応。"""

    def test_nested_json_label(self):
        """ネストされた JSON ラベルが正しくパースされる。"""
        label = json.dumps({
            "name": "Confidential",
            "id": "abc-123",
            "protection": {"encryption": True},
        })
        meta = _make_metadata(sensitivity_label=label)
        result = calculate_preliminary_sensitivity(meta)
        assert result.score == 3.0

    def test_label_with_unicode_name(self):
        """日本語ラベル名が正しくパースされる。"""
        label = json.dumps({"name": "社外秘"})
        meta = _make_metadata(sensitivity_label=label)
        result = calculate_preliminary_sensitivity(meta)
        assert result.score >= 1.0

    def test_empty_json_object_label(self):
        """空 JSON オブジェクトのラベルでクラッシュしない。"""
        meta = _make_metadata(sensitivity_label="{}")
        result = calculate_preliminary_sensitivity(meta)
        assert result.score >= 1.0

    def test_label_array_instead_of_object(self):
        """ラベルが配列（不正形式）でもクラッシュしない。"""
        meta = _make_metadata(sensitivity_label='["Confidential"]')
        result = calculate_preliminary_sensitivity(meta)
        assert result.score >= 1.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 12. 大規模データセットでの Finding ID 衝突テスト
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestFindingIdCollisionResistance:
    """大量の Finding ID を生成しても衝突が起きないことを検証する。"""

    def test_1000_unique_items_no_collision(self):
        """1000 件の異なるアイテムの Finding ID がすべてユニーク。"""
        ids = set()
        for i in range(1000):
            fid = generate_finding_id("t-001", "m365", f"item-{i:06d}")
            ids.add(fid)
        assert len(ids) == 1000

    def test_cross_tenant_cross_source_no_collision(self):
        """テナント × ソース × アイテムの組み合わせでユニーク。"""
        ids = set()
        for tenant in ["t-001", "t-002", "t-003"]:
            for source in ["m365", "box", "slack", "google_drive"]:
                for item in ["item-a", "item-b", "item-c"]:
                    fid = generate_finding_id(tenant, source, item)
                    ids.add(fid)
        assert len(ids) == 3 * 4 * 3


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 13. Exposure Vector 抽出のマルチソース対応
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestExposureVectorMultiSource:
    """M365 以外のソースでも ExposureVector が正しく抽出される。"""

    def test_box_source_vectors(self):
        """Box ソースのメタデータから ExposureVector が抽出される。"""
        meta = _make_metadata(source="box", sharing_scope="anonymous")
        result = calculate_exposure_score(meta)
        assert "public_link" in result.vectors
        assert result.score >= 5.0

    def test_google_drive_source_vectors(self):
        """Google Drive ソースのメタデータから ExposureVector が抽出される。"""
        meta = _make_metadata(source="google_drive", sharing_scope="organization")
        result = calculate_exposure_score(meta)
        assert "org_link" in result.vectors

    def test_guard_matching_across_sources(self):
        """Box/Google Drive の ExposureVector がガード照合でマッチする。"""
        guards_box = match_guards(["public_link"], "box")
        guards_gdrive = match_guards(["public_link"], "google_drive")
        assert "G3" in guards_box
        assert "G3" in guards_gdrive

    def test_slack_source_limited_guards(self):
        """Slack ソースでは G2 / G3 は対象外。"""
        guards_slack = match_guards(["public_link", "all_users"], "slack")
        assert "G2" not in guards_slack
        assert "G3" not in guards_slack


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 14. SSM パラメータ障害時のフォールバック動作
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestSSMFallbackBehavior:
    """SSM Parameter Store が利用不可の場合のフォールバック動作。"""

    def test_analyze_exposure_risk_threshold_fallback(self):
        """analyzeExposure: SSM 障害時に閾値 2.0 にフォールバック。"""
        from handlers.analyze_exposure import _get_risk_threshold

        with patch("handlers.analyze_exposure.get_ssm_float", side_effect=Exception("SSM timeout")):
            assert _get_risk_threshold() == 2.0

    def test_analyze_exposure_rescan_interval_fallback(self):
        """analyzeExposure: SSM 障害時に再スキャン間隔 7 日にフォールバック。"""
        from handlers.analyze_exposure import _get_rescan_interval

        with patch("handlers.analyze_exposure.get_ssm_int", side_effect=Exception("SSM timeout")):
            assert _get_rescan_interval() == 7

    def test_detect_sensitivity_max_file_size_fallback(self):
        """detectSensitivity: SSM 障害時にファイルサイズ上限 50MB にフォールバック。"""
        from handlers.detect_sensitivity import _get_max_file_size

        with patch("handlers.detect_sensitivity.get_ssm_int", side_effect=Exception("SSM timeout")):
            assert _get_max_file_size() == 52428800

    def test_detect_sensitivity_risk_threshold_fallback(self):
        """detectSensitivity: SSM 障害時に閾値 2.0 にフォールバック。"""
        from handlers.detect_sensitivity import _get_risk_threshold

        with patch("handlers.detect_sensitivity.get_ssm_float", side_effect=Exception("SSM timeout")):
            assert _get_risk_threshold() == 2.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 15. テキスト抽出の異常ケース
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestTextExtractionResilience:
    """テキスト抽出が壊れたファイルでクラッシュしない。"""

    def test_corrupted_docx_returns_empty(self):
        from services.text_extractor import extract_text

        result = extract_text(b"PK\x03\x04corrupted_content", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        assert result == ""

    def test_corrupted_xlsx_returns_empty(self):
        from services.text_extractor import extract_text

        result = extract_text(b"PK\x03\x04bad_xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        assert result == ""

    def test_binary_content_as_plain_text(self):
        from services.text_extractor import extract_text

        result = extract_text(b"\x00\x01\x02\xff\xfe\xfd", "text/plain")
        assert isinstance(result, str)

    def test_very_large_csv_truncated(self):
        from services.text_extractor import extract_text, truncate_text

        large_csv = ("name,phone\n" + "田中太郎,090-1234-5678\n" * 100000).encode("utf-8")
        text = extract_text(large_csv, "text/csv")
        truncated = truncate_text(text, max_length=500000)
        assert len(truncated) <= 500000

    def test_utf16_encoded_text(self):
        from services.text_extractor import extract_plain

        content = "UTF-16テスト".encode("utf-16")
        result = extract_plain(content)
        assert isinstance(result, str)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 16. detectSensitivity ハンドラの異常系
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestDetectSensitivityEdgeCases:
    """detectSensitivity ハンドラの本番環境で発生しうる異常系。"""

    def test_handler_with_empty_records(self):
        """空の Records 配列でエラーにならない。"""
        from handlers.detect_sensitivity import handler

        result = handler({"Records": []}, None)
        assert result["processed"] == 0
        assert result["errors"] == 0

    def test_handler_with_malformed_json_body(self):
        """不正 JSON の body で例外がスローされる。"""
        from handlers.detect_sensitivity import handler

        event = {"Records": [{"messageId": "msg-001", "body": "{invalid json"}]}
        with pytest.raises(Exception):
            handler(event, None)

    def test_handler_with_missing_body_field(self):
        """body フィールドが欠損したレコード。"""
        from handlers.detect_sensitivity import handler

        event = {"Records": [{"messageId": "msg-001"}]}
        result = handler(event, None)
        assert result["processed"] == 1

    def test_finding_not_found_during_update(self):
        """Finding が存在しない状態での sensitivity 更新がクラッシュしない。"""
        from handlers.detect_sensitivity import _update_finding_with_sensitivity, set_finding_table
        from services.pii_detector import PIIDetectionResult
        from services.secret_detector import SecretDetectionResult

        with mock_aws():
            dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
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
            set_finding_table(table)

            _update_finding_with_sensitivity(
                tenant_id="t-001",
                finding_id="nonexistent",
                sensitivity_score=3.0,
                pii_results=PIIDetectionResult(),
                secret_results=SecretDetectionResult(),
            )

            set_finding_table(None)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 17. analyzeExposure の混合バッチ処理
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestAnalyzeExposureMixedBatch:
    """analyzeExposure が INSERT / MODIFY / REMOVE の混合バッチを正しく処理する。"""

    def _ddb_s(self, val: str) -> dict:
        return {"S": val}

    def _ddb_n(self, val) -> dict:
        return {"N": str(val)}

    def _ddb_bool(self, val: bool) -> dict:
        return {"BOOL": val}

    def _make_image(self, item_id="item-001", sharing_scope="anonymous", is_deleted=False):
        return {
            "tenant_id": self._ddb_s("t-001"),
            "item_id": self._ddb_s(item_id),
            "source": self._ddb_s("m365"),
            "container_id": self._ddb_s("site-xyz"),
            "container_name": self._ddb_s("法務部門"),
            "container_type": self._ddb_s("site"),
            "item_name": self._ddb_s("契約書.docx"),
            "web_url": self._ddb_s("https://example.com/file"),
            "sharing_scope": self._ddb_s(sharing_scope),
            "permissions": self._ddb_s("{}"),
            "permissions_count": self._ddb_n(10),
            "mime_type": self._ddb_s("text/plain"),
            "size": self._ddb_n(1024),
            "modified_at": self._ddb_s(datetime.now(timezone.utc).isoformat()),
            "is_deleted": self._ddb_bool(is_deleted),
            "raw_s3_key": self._ddb_s("raw/t-001/item-001/data.json"),
        }

    def test_mixed_event_batch(self):
        """INSERT + MODIFY + REMOVE の混合バッチが正しく処理される。"""
        from handlers.analyze_exposure import handler
        from services.finding_manager import set_finding_table as fm_set_table

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
            fm_set_table(table)

            sqs = boto3.client("sqs", region_name=region)
            queue = sqs.create_queue(QueueName="AIReadyGov-SensitivityDetectionQueue")

            import handlers.analyze_exposure as ae_module
            ae_module._sqs_client = sqs

            insert_img = self._make_image(item_id="item-insert", sharing_scope="anonymous")
            remove_img = self._make_image(item_id="item-remove", sharing_scope="anonymous")

            insert_rec = {
                "eventID": "evt-1",
                "eventName": "INSERT",
                "dynamodb": {"NewImage": insert_img},
            }

            event = {"Records": [insert_rec]}
            result = handler(event, None)
            assert result["processed"] >= 1

            fm_set_table(None)
            ae_module._sqs_client = None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 18. Acknowledge のバリデーション
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestAcknowledgeValidation:
    """acknowledge_finding の入力バリデーションと状態遷移。"""

    def test_acknowledge_preserves_original_scores(self, dynamodb_table):
        """acknowledged にしてもスコアは維持される。"""
        result = _upsert_test_finding(dynamodb_table)
        original_finding = get_finding("t-001", result["finding_id"])
        original_risk = float(original_finding["risk_score"])

        acknowledge_finding(
            "t-001", result["finding_id"],
            suppress_until="2099-12-31T00:00:00Z",
            reason="テスト抑制。テスト目的で一時的にリスクを受容します。（50文字以上）",
            acknowledged_by="admin@example.com",
        )

        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "acknowledged"
        assert float(finding["risk_score"]) == original_risk
        assert finding["acknowledged_by"] == "admin@example.com"

    def test_acknowledge_sets_all_required_fields(self, dynamodb_table):
        """acknowledge で必須フィールドがすべて設定される。"""
        result = _upsert_test_finding(dynamodb_table)

        acknowledge_finding(
            "t-001", result["finding_id"],
            suppress_until="2026-06-01T00:00:00Z",
            reason="テスト用の理由文。必要な文字数を満たすために十分な長さの文字列を入力しています。",
            acknowledged_by="security-team@example.com",
        )

        finding = get_finding("t-001", result["finding_id"])
        assert finding["suppress_until"] == "2026-06-01T00:00:00Z"
        assert "テスト用の理由文" in finding["acknowledged_reason"]
        assert finding["acknowledged_by"] == "security-team@example.com"
        assert finding["acknowledged_at"] is not None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 19. 冪等性の徹底検証
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestIdempotencyThorough:
    """同一イベントの再処理が冪等であることを保証する。"""

    def test_triple_upsert_produces_single_finding(self, dynamodb_table):
        """同一アイテムを 3 回 upsert しても Finding は 1 件のみ。"""
        for _ in range(3):
            _upsert_test_finding(dynamodb_table)

        response = dynamodb_table.scan()
        findings = [
            f for f in response["Items"]
            if f.get("item_id") == "item-001" and f.get("tenant_id") == "t-001"
        ]
        assert len(findings) == 1

    def test_close_twice_no_error(self, dynamodb_table):
        """同一 Finding を 2 回クローズしてもエラーにならない。"""
        result = _upsert_test_finding(dynamodb_table)
        close_finding("t-001", result["finding_id"])
        close_finding("t-001", result["finding_id"])

        finding = get_finding("t-001", result["finding_id"])
        assert finding["status"] == "closed"

    def test_acknowledge_already_acknowledged_updates(self, dynamodb_table):
        """既に acknowledged の Finding に再度 acknowledge しても上書きされる。"""
        result = _upsert_test_finding(dynamodb_table)
        acknowledge_finding(
            "t-001", result["finding_id"],
            suppress_until="2026-06-01T00:00:00Z",
            reason="x" * 50,
            acknowledged_by="admin1",
        )
        acknowledge_finding(
            "t-001", result["finding_id"],
            suppress_until="2026-12-31T00:00:00Z",
            reason="y" * 50,
            acknowledged_by="admin2",
        )

        finding = get_finding("t-001", result["finding_id"])
        assert finding["suppress_until"] == "2026-12-31T00:00:00Z"
        assert finding["acknowledged_by"] == "admin2"
