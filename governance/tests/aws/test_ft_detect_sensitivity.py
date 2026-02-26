"""FT-2: detectSensitivity Lambda — 機密情報検出テスト

S3 にファイルをアップロードし SQS メッセージを送信、detectSensitivity Lambda が
Finding の sensitivity 関連フィールドを正しく更新することを検証する。
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tests.aws.conftest import (
    FINDING_TABLE_NAME,
    RAW_PAYLOAD_BUCKET,
    TEST_TENANT_ID,
    make_file_metadata,
    wait_for_finding,
    wait_for_finding_by_item,
)


def _generate_finding_id(tenant_id: str, source: str, item_id: str) -> str:
    import hashlib

    return hashlib.sha256(f"{tenant_id}:{source}:{item_id}".encode()).hexdigest()[:32]


def _create_finding(finding_table, tenant_id: str, item_id: str, **overrides) -> dict:
    """テスト用 Finding を直接作成する。"""
    finding_id = _generate_finding_id(tenant_id, "m365", item_id)
    now = datetime.now(timezone.utc).isoformat()
    item = {
        "tenant_id": tenant_id,
        "finding_id": finding_id,
        "source": "m365",
        "item_id": item_id,
        "item_name": "test-file.txt",
        "container_id": "site-test-001",
        "container_name": "テスト部門サイト",
        "status": "new",
        "exposure_score": Decimal("6.0"),
        "risk_score": Decimal("6.0"),
        "created_at": now,
        "last_evaluated_at": now,
    }
    item.update(overrides)
    finding_table.put_item(Item=item)
    return item


def _upload_raw_payload(s3_client, s3_key: str, content: bytes, content_type: str = "text/plain"):
    """S3 にテストファイルをアップロードする。"""
    s3_client.put_object(
        Bucket=RAW_PAYLOAD_BUCKET,
        Key=s3_key,
        Body=content,
        ContentType=content_type,
    )


def _send_sensitivity_message(
    sqs_client,
    queue_url: str,
    finding_id: str,
    item_id: str,
    s3_key: str,
    *,
    mime_type: str = "text/plain",
    size: int = 1000,
):
    """SQS に detectSensitivity 用メッセージを送信する。"""
    now = datetime.now(timezone.utc).isoformat()
    body = {
        "finding_id": finding_id,
        "tenant_id": TEST_TENANT_ID,
        "source": "m365",
        "item_id": item_id,
        "item_name": "test.txt",
        "mime_type": mime_type,
        "size": size,
        "raw_s3_key": s3_key,
        "raw_s3_bucket": RAW_PAYLOAD_BUCKET,
        "enqueued_at": now,
        "trigger": "test",
    }
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))


class TestFT2DetectSensitivity:
    """detectSensitivity Lambda の機密情報検出テスト群。"""

    def test_ft_2_01_pii_english(
        self, finding_table, s3_client, sqs_client, sensitivity_queue_url,
    ):
        """英語 PII (名前・メール・電話) → pii_detected=true。"""
        item_id = f"item-{uuid.uuid4().hex[:12]}"
        finding = _create_finding(finding_table, TEST_TENANT_ID, item_id)
        s3_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        _upload_raw_payload(
            s3_client, s3_key,
            b"John Smith john@example.com 555-123-4567",
        )
        _send_sensitivity_message(
            sqs_client, sensitivity_queue_url,
            finding["finding_id"], item_id, s3_key,
        )

        updated = wait_for_finding(
            finding_table, TEST_TENANT_ID, finding["finding_id"],
            expected_field="pii_detected", expected_value=True, max_wait=120,
        )
        assert updated is not None, "Finding not updated with pii_detected=true"
        pii_types = updated.get("pii_types", [])
        assert any("email" in t.lower() for t in pii_types), f"email not in {pii_types}"
        assert any("person" in t.lower() for t in pii_types), f"person not in {pii_types}"

    def test_ft_2_02_pii_mynumber(
        self, finding_table, s3_client, sqs_client, sensitivity_queue_url,
    ):
        """マイナンバー検出 → pii_types に my_number、sensitivity_score >= 4.0。"""
        item_id = f"item-{uuid.uuid4().hex[:12]}"
        finding = _create_finding(finding_table, TEST_TENANT_ID, item_id)
        s3_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        _upload_raw_payload(
            s3_client, s3_key,
            "個人番号 1234 5678 9012".encode("utf-8"),
        )
        _send_sensitivity_message(
            sqs_client, sensitivity_queue_url,
            finding["finding_id"], item_id, s3_key,
        )

        updated = wait_for_finding(
            finding_table, TEST_TENANT_ID, finding["finding_id"],
            expected_field="pii_detected", expected_value=True, max_wait=120,
        )
        assert updated is not None, "Finding not updated for my_number PII"
        pii_types = updated.get("pii_types", [])
        assert any("my_number" in t.lower() for t in pii_types), f"my_number not in {pii_types}"
        assert Decimal(str(updated.get("sensitivity_score", 0))) >= Decimal("4.0")

    def test_ft_2_03_pii_bank_account(
        self, finding_table, s3_client, sqs_client, sensitivity_queue_url,
    ):
        """銀行口座情報検出 → pii_types に bank_account。"""
        item_id = f"item-{uuid.uuid4().hex[:12]}"
        finding = _create_finding(finding_table, TEST_TENANT_ID, item_id)
        s3_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        _upload_raw_payload(
            s3_client, s3_key,
            "口座 普通 1234567 銀行".encode("utf-8"),
        )
        _send_sensitivity_message(
            sqs_client, sensitivity_queue_url,
            finding["finding_id"], item_id, s3_key,
        )

        updated = wait_for_finding(
            finding_table, TEST_TENANT_ID, finding["finding_id"],
            expected_field="pii_detected", expected_value=True, max_wait=120,
        )
        assert updated is not None, "Finding not updated for bank_account PII"
        pii_types = updated.get("pii_types", [])
        assert any("bank_account" in t.lower() for t in pii_types), (
            f"bank_account not in {pii_types}"
        )

    def test_ft_2_04_secret_aws_key(
        self, finding_table, s3_client, sqs_client, sensitivity_queue_url,
    ):
        """AWS アクセスキー検出 → secrets_detected=true, sensitivity_score=5.0。"""
        item_id = f"item-{uuid.uuid4().hex[:12]}"
        finding = _create_finding(finding_table, TEST_TENANT_ID, item_id)
        s3_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        _upload_raw_payload(
            s3_client, s3_key,
            b"AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE",
        )
        _send_sensitivity_message(
            sqs_client, sensitivity_queue_url,
            finding["finding_id"], item_id, s3_key,
        )

        updated = wait_for_finding(
            finding_table, TEST_TENANT_ID, finding["finding_id"],
            expected_field="secrets_detected", expected_value=True, max_wait=120,
        )
        assert updated is not None, "Finding not updated for AWS secret"
        assert Decimal(str(updated.get("sensitivity_score", 0))) == Decimal("5.0")

    def test_ft_2_05_secret_github_token(
        self, finding_table, s3_client, sqs_client, sensitivity_queue_url,
    ):
        """GitHub トークン検出 → secret_types に github_token。"""
        item_id = f"item-{uuid.uuid4().hex[:12]}"
        finding = _create_finding(finding_table, TEST_TENANT_ID, item_id)
        s3_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        _upload_raw_payload(
            s3_client, s3_key,
            b"token: ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij",
        )
        _send_sensitivity_message(
            sqs_client, sensitivity_queue_url,
            finding["finding_id"], item_id, s3_key,
        )

        updated = wait_for_finding(
            finding_table, TEST_TENANT_ID, finding["finding_id"],
            expected_field="secrets_detected", expected_value=True, max_wait=120,
        )
        assert updated is not None, "Finding not updated for GitHub token"
        secret_types = updated.get("secret_types", [])
        assert any("github" in t.lower() for t in secret_types), (
            f"github_token not in {secret_types}"
        )

    def test_ft_2_06_docx_extraction(
        self, finding_table, s3_client, sqs_client, sensitivity_queue_url,
    ):
        """DOCX ファイルからの PII 抽出 → pii_detected=true。"""
        docx_mod = pytest.importorskip("docx", reason="python-docx not installed")
        import io

        doc = docx_mod.Document()
        doc.add_paragraph("田中太郎 taro.tanaka@example.com 090-1234-5678")
        buf = io.BytesIO()
        doc.save(buf)
        docx_bytes = buf.getvalue()

        item_id = f"item-{uuid.uuid4().hex[:12]}"
        finding = _create_finding(finding_table, TEST_TENANT_ID, item_id)
        s3_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.docx"
        _upload_raw_payload(
            s3_client, s3_key, docx_bytes,
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        _send_sensitivity_message(
            sqs_client, sensitivity_queue_url,
            finding["finding_id"], item_id, s3_key,
            mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            size=len(docx_bytes),
        )

        updated = wait_for_finding(
            finding_table, TEST_TENANT_ID, finding["finding_id"],
            expected_field="pii_detected", expected_value=True, max_wait=120,
        )
        assert updated is not None, "Finding not updated for DOCX PII extraction"

    def test_ft_2_07_xlsx_extraction(
        self, finding_table, s3_client, sqs_client, sensitivity_queue_url,
    ):
        """XLSX ファイルからの PII 抽出 → pii_detected=true。"""
        openpyxl = pytest.importorskip("openpyxl", reason="openpyxl not installed")
        import io

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Name", "Email", "Phone"])
        ws.append(["佐藤花子", "hanako@example.com", "080-9876-5432"])
        buf = io.BytesIO()
        wb.save(buf)
        xlsx_bytes = buf.getvalue()

        item_id = f"item-{uuid.uuid4().hex[:12]}"
        finding = _create_finding(finding_table, TEST_TENANT_ID, item_id)
        s3_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.xlsx"
        _upload_raw_payload(
            s3_client, s3_key, xlsx_bytes,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        _send_sensitivity_message(
            sqs_client, sensitivity_queue_url,
            finding["finding_id"], item_id, s3_key,
            mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            size=len(xlsx_bytes),
        )

        updated = wait_for_finding(
            finding_table, TEST_TENANT_ID, finding["finding_id"],
            expected_field="pii_detected", expected_value=True, max_wait=120,
        )
        assert updated is not None, "Finding not updated for XLSX PII extraction"

    def test_ft_2_08_file_too_large_skip(
        self, finding_table, sqs_client, sensitivity_queue_url,
    ):
        """50MB 超ファイル → スキップされ pii_detected 未更新。"""
        item_id = f"item-{uuid.uuid4().hex[:12]}"
        finding = _create_finding(finding_table, TEST_TENANT_ID, item_id)
        s3_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.bin"
        _send_sensitivity_message(
            sqs_client, sensitivity_queue_url,
            finding["finding_id"], item_id, s3_key,
            size=60_000_000,
        )

        time.sleep(60)
        resp = finding_table.get_item(
            Key={"tenant_id": TEST_TENANT_ID, "finding_id": finding["finding_id"]},
        )
        updated = resp.get("Item", {})
        assert updated.get("pii_detected") is None or updated.get("pii_detected") is False, (
            "Large file should be skipped — pii_detected should remain unset"
        )

    def test_ft_2_09_unsupported_format_skip(
        self, finding_table, sqs_client, sensitivity_queue_url,
    ):
        """非対応形式 (application/zip) → スキップされ pii_detected 未更新。"""
        item_id = f"item-{uuid.uuid4().hex[:12]}"
        finding = _create_finding(finding_table, TEST_TENANT_ID, item_id)
        s3_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.zip"
        _send_sensitivity_message(
            sqs_client, sensitivity_queue_url,
            finding["finding_id"], item_id, s3_key,
            mime_type="application/zip",
        )

        time.sleep(60)
        resp = finding_table.get_item(
            Key={"tenant_id": TEST_TENANT_ID, "finding_id": finding["finding_id"]},
        )
        updated = resp.get("Item", {})
        assert updated.get("pii_detected") is None or updated.get("pii_detected") is False, (
            "Unsupported format should be skipped"
        )

    def test_ft_2_10_auto_close_below_threshold(
        self, finding_table, s3_client, sqs_client, sensitivity_queue_url,
    ):
        """低 exposure_score + PII 未検出 → Finding が auto-close される。"""
        item_id = f"item-{uuid.uuid4().hex[:12]}"
        finding = _create_finding(
            finding_table, TEST_TENANT_ID, item_id,
            exposure_score=Decimal("1.0"),
            risk_score=Decimal("1.0"),
        )
        s3_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        _upload_raw_payload(s3_client, s3_key, b"This is a safe document with no PII.")
        _send_sensitivity_message(
            sqs_client, sensitivity_queue_url,
            finding["finding_id"], item_id, s3_key,
        )

        closed = wait_for_finding(
            finding_table, TEST_TENANT_ID, finding["finding_id"],
            expected_field="status", expected_value="closed", max_wait=120,
        )
        assert closed is not None, "Low-risk Finding without PII should be auto-closed"
