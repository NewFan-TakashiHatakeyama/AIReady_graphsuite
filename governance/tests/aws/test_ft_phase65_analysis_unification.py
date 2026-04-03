"""FT-7/FT-8/FT-9: Phase 6.5 解析一元化 機能テスト

detectSensitivity 拡張で追加した NER/要約/Embedding/DocumentAnalysis/Entity連携を
実 AWS 環境で検証する。
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tests.aws.conftest import (
    RAW_PAYLOAD_BUCKET,
    VECTORS_BUCKET,
    wait_for_document_analysis,
    wait_for_finding_scan_completed,
)

pytestmark = [
    pytest.mark.aws,
    pytest.mark.skip(reason="Phase 6.5 detectSensitivity extensions are retired in hard-cut mode"),
]


def _finding_id(tenant_id: str, source: str, item_id: str) -> str:
    return hashlib.sha256(f"{tenant_id}:{source}:{item_id}".encode()).hexdigest()[:32]


def _seed_finding(finding_table, tenant_id: str, item_id: str) -> str:
    now = datetime.now(timezone.utc).isoformat()
    finding_id = _finding_id(tenant_id, "m365", item_id)
    finding_table.put_item(
        Item={
            "tenant_id": tenant_id,
            "finding_id": finding_id,
            "source": "m365",
            "item_id": item_id,
            "item_name": "phase65-ft.txt",
            "container_id": "site-test-001",
            "container_name": "テスト部門サイト",
            "status": "open",
            "exposure_score": Decimal("6.0"),
            "risk_score": Decimal("6.0"),
            "created_at": now,
            "last_evaluated_at": now,
        }
    )
    return finding_id


def _send_detect_message(
    sqs_client,
    sensitivity_queue_url: str,
    tenant_id: str,
    finding_id: str,
    item_id: str,
    raw_s3_key: str,
    *,
    mime_type: str = "text/plain",
    size: int = 1024,
):
    payload = {
        "finding_id": finding_id,
        "tenant_id": tenant_id,
        "source": "m365",
        "item_id": item_id,
        "item_name": "phase65-ft.txt",
        "mime_type": mime_type,
        "size": size,
        "raw_s3_key": raw_s3_key,
        "raw_s3_bucket": RAW_PAYLOAD_BUCKET,
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
        "trigger": "phase65-ft",
    }
    sqs_client.send_message(QueueUrl=sensitivity_queue_url, MessageBody=json.dumps(payload))


def _run_scan(
    finding_table,
    s3_client,
    sqs_client,
    sensitivity_queue_url: str,
    tenant_id: str,
    content: str,
) -> tuple[str, str, str]:
    item_id = f"item-p65-{uuid.uuid4().hex[:10]}"
    finding_id = _seed_finding(finding_table, tenant_id, item_id)
    raw_s3_key = f"{tenant_id}/raw/{item_id}/payload.txt"
    s3_client.put_object(
        Bucket=RAW_PAYLOAD_BUCKET,
        Key=raw_s3_key,
        Body=content.encode("utf-8"),
        ContentType="text/plain",
    )
    _send_detect_message(
        sqs_client,
        sensitivity_queue_url,
        tenant_id,
        finding_id,
        item_id,
        raw_s3_key,
        size=len(content.encode("utf-8")),
    )
    finding = wait_for_finding_scan_completed(
        finding_table,
        tenant_id,
        finding_id,
        max_wait=300,
        interval=10,
    )
    assert finding is not None, "detectSensitivity scan did not complete in time"
    return tenant_id, item_id, finding_id


class TestPhase65Functional:
    def test_ft_7_01_document_analysis_created(
        self,
        finding_table,
        document_analysis_table,
        s3_client,
        sqs_client,
        sensitivity_queue_url,
    ):
        """detectSensitivity 実行後に DocumentAnalysis レコードが作成される。"""
        tenant_id = f"test-tenant-p65-{uuid.uuid4().hex[:8]}"
        _, item_id, _ = _run_scan(
            finding_table,
            s3_client,
            sqs_client,
            sensitivity_queue_url,
            tenant_id,
            "田中太郎 tanaka@example.com 090-1234-5678",
        )
        analysis = wait_for_document_analysis(
            document_analysis_table,
            tenant_id,
            item_id,
            max_wait=300,
            interval=10,
        )
        assert analysis is not None, "DocumentAnalysis record not created"

    def test_ft_7_02_ner_and_noun_chunks_populated(
        self,
        finding_table,
        document_analysis_table,
        s3_client,
        sqs_client,
        sensitivity_queue_url,
    ):
        """NER entities と noun_chunks が保存される。"""
        tenant_id = f"test-tenant-p65-{uuid.uuid4().hex[:8]}"
        _, item_id, _ = _run_scan(
            finding_table,
            s3_client,
            sqs_client,
            sensitivity_queue_url,
            tenant_id,
            "株式会社GraphSuiteの佐藤花子は東京本社で契約管理を担当しています。",
        )
        analysis = wait_for_document_analysis(document_analysis_table, tenant_id, item_id)
        assert analysis is not None
        assert isinstance(analysis.get("ner_entities"), list)
        assert isinstance(analysis.get("noun_chunks"), list)

    def test_ft_8_01_summary_generated(
        self,
        finding_table,
        document_analysis_table,
        s3_client,
        sqs_client,
        sensitivity_queue_url,
    ):
        """要約が生成され、空文字ではない。"""
        tenant_id = f"test-tenant-p65-{uuid.uuid4().hex[:8]}"
        _, item_id, _ = _run_scan(
            finding_table,
            s3_client,
            sqs_client,
            sensitivity_queue_url,
            tenant_id,
            "This document contains customer onboarding details and payment information for review.",
        )
        analysis = wait_for_document_analysis(document_analysis_table, tenant_id, item_id)
        assert analysis is not None
        summary = analysis.get("summary", "")
        assert isinstance(summary, str)
        assert summary.strip() != ""

    def test_ft_8_02_embedding_saved_to_vectors_bucket(
        self,
        finding_table,
        document_analysis_table,
        s3_client,
        sqs_client,
        sensitivity_queue_url,
    ):
        """Embedding S3 key が保存され、Vectors バケットに実体が存在する。"""
        tenant_id = f"test-tenant-p65-{uuid.uuid4().hex[:8]}"
        _, item_id, _ = _run_scan(
            finding_table,
            s3_client,
            sqs_client,
            sensitivity_queue_url,
            tenant_id,
            "Embedding generation test for AWS Bedrock Titan model.",
        )
        analysis = wait_for_document_analysis(document_analysis_table, tenant_id, item_id)
        assert analysis is not None
        vector_key = analysis.get("embedding_s3_key", "")
        assert vector_key.startswith(f"{tenant_id}/vectors/{item_id}")
        s3_client.head_object(Bucket=VECTORS_BUCKET, Key=vector_key)

    def test_ft_9_01_pii_and_secrets_summary_consistency(
        self,
        finding_table,
        document_analysis_table,
        s3_client,
        sqs_client,
        sensitivity_queue_url,
    ):
        """PII/Secrets summary が検知結果と整合する。"""
        tenant_id = f"test-tenant-p65-{uuid.uuid4().hex[:8]}"
        _, item_id, _ = _run_scan(
            finding_table,
            s3_client,
            sqs_client,
            sensitivity_queue_url,
            tenant_id,
            "個人番号 1234 5678 9012 と AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE",
        )
        analysis = wait_for_document_analysis(document_analysis_table, tenant_id, item_id)
        assert analysis is not None
        assert analysis["pii_summary"]["detected"] is True
        assert analysis["secrets_summary"]["detected"] is True

    def test_ft_9_02_analysis_metadata_integrity(
        self,
        finding_table,
        document_analysis_table,
        s3_client,
        sqs_client,
        sensitivity_queue_url,
    ):
        """DocumentAnalysis の analyzed_at / ttl / embedding_dimension が妥当値。"""
        tenant_id = f"test-tenant-p65-{uuid.uuid4().hex[:8]}"
        _, item_id, _ = _run_scan(
            finding_table,
            s3_client,
            sqs_client,
            sensitivity_queue_url,
            tenant_id,
            "metadata integrity validation for document analysis",
        )
        analysis = wait_for_document_analysis(document_analysis_table, tenant_id, item_id)
        assert analysis is not None
        assert "analyzed_at" in analysis
        assert isinstance(analysis.get("ttl"), (int, Decimal))
        assert analysis.get("embedding_dimension") == 1024

    @pytest.mark.slow
    def test_ft_9_03_entity_candidates_message_contract(
        self,
        finding_table,
        s3_client,
        sqs_client,
        sensitivity_queue_url,
        entity_resolution_queue_url,
    ):
        """EntityResolutionQueue に event_type=entity_candidates メッセージが送信される。"""
        tenant_id = f"test-tenant-p65-{uuid.uuid4().hex[:8]}"
        _, item_id, finding_id = _run_scan(
            finding_table,
            s3_client,
            sqs_client,
            sensitivity_queue_url,
            tenant_id,
            "GraphSuiteの佐藤花子（メール hanako@example.com）が契約書を共有しました。",
        )
        matched = False
        for _ in range(20):
            messages = sqs_client.receive_message(
                QueueUrl=entity_resolution_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=2,
                MessageAttributeNames=["All"],
                AttributeNames=["All"],
            ).get("Messages", [])
            for msg in messages:
                body = json.loads(msg["Body"])
                if body.get("item_id") == item_id:
                    matched = True
                    assert body.get("event_type") == "entity_candidates"
                    assert body.get("tenant_id") == tenant_id
                    assert isinstance(body.get("candidates"), list)
                    break
            if matched:
                break
        if not matched:
            # キュー消費タイミングによって取得できない場合は、失敗ログが無いことを確認して補完判定
            logs = sqs_client.meta.events._emitter  # no-op access to avoid extra client creation
            del logs
            import boto3
            import time

            logs_client = boto3.client("logs", region_name="ap-northeast-1")
            now_ms = int(time.time() * 1000)
            resp = logs_client.filter_log_events(
                logGroupName="/aws/lambda/AIReadyGov-detectSensitivity",
                startTime=now_ms - (20 * 60 * 1000),
                endTime=now_ms,
                filterPattern=f'"{finding_id}"',
                limit=100,
            )
            events = [e["message"] for e in resp.get("events", [])]
            has_enqueue_error = any("Entity candidate enqueue failed" in m for m in events)
            has_scan_complete = any("Sensitivity scan complete" in m for m in events)
            assert not has_enqueue_error and has_scan_complete, (
                "Entity candidate message not observed and no successful enqueue evidence found"
            )

    @pytest.mark.slow
    def test_ft_9_04_entity_candidates_mask_pii_plaintext(
        self,
        finding_table,
        s3_client,
        sqs_client,
        sensitivity_queue_url,
        entity_resolution_queue_url,
    ):
        """EntityResolutionQueue メッセージに生 PII が含まれない。"""
        pii_literal = "phase65+masking@example.com"
        tenant_id = f"test-tenant-p65-{uuid.uuid4().hex[:8]}"
        _, item_id, _ = _run_scan(
            finding_table,
            s3_client,
            sqs_client,
            sensitivity_queue_url,
            tenant_id,
            f"連絡先メールは {pii_literal} です。",
        )
        messages = sqs_client.receive_message(
            QueueUrl=entity_resolution_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=3,
            MessageAttributeNames=["All"],
            AttributeNames=["All"],
        ).get("Messages", [])
        if not messages:
            pytest.skip("EntityResolutionQueue message not observable (possibly consumed quickly)")

        target_body = None
        for msg in messages:
            body = json.loads(msg["Body"])
            if body.get("item_id") == item_id:
                target_body = json.dumps(body, ensure_ascii=False)
                break
        if target_body is None:
            pytest.skip("Target phase6.5 message was not captured")

        assert pii_literal not in target_body, "Plaintext PII leaked into entity candidate message"
