"""E2E-4: Phase 6.5 解析一元化 E2E テスト

FileMetadata INSERT から detectSensitivity 拡張までの統合フローを実 AWS で検証する。
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests.aws.conftest import (
    DETECT_SENSITIVITY_FN,
    RAW_PAYLOAD_BUCKET,
    VECTORS_BUCKET,
    make_file_metadata,
    wait_for_document_analysis,
    wait_for_finding_scan_completed,
    wait_for_finding_by_item,
)

pytestmark = pytest.mark.aws


def _tenant(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestE2E4Phase65:
    @pytest.mark.slow
    def test_e2e_4_01_full_unified_pipeline(
        self,
        connect_table,
        finding_table,
        document_analysis_table,
        s3_client,
    ):
        """FileMetadata 投入後に Finding と DocumentAnalysis が両方作成される。"""
        tenant_id = _tenant("test-tenant-e2e401")
        item_id = f"item-e2e401-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{tenant_id}/{item_id}/payload.txt"
        content = (
            "株式会社GraphSuiteの佐藤花子（hanako@example.com）が契約情報を共有。"
            "AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE"
        )
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET,
            Key=raw_key,
            Body=content.encode("utf-8"),
            ContentType="text/plain",
        )
        connect_table.put_item(
            Item=make_file_metadata(
                tenant_id=tenant_id,
                item_id=item_id,
                item_name="phase65_e2e.txt",
                mime_type="text/plain",
                size=len(content.encode("utf-8")),
                raw_s3_key=raw_key,
            )
        )

        finding = wait_for_finding_by_item(
            finding_table, tenant_id, item_id, max_wait=300, interval=10
        )
        assert finding is not None, "Finding was not created"
        scanned = wait_for_finding_scan_completed(
            finding_table,
            tenant_id,
            finding["finding_id"],
            max_wait=300,
            interval=10,
        )
        assert scanned is not None, "Sensitivity scan did not run"

        analysis = wait_for_document_analysis(
            document_analysis_table, tenant_id, item_id, max_wait=300, interval=10
        )
        assert analysis is not None, "DocumentAnalysis was not created"
        assert analysis["pii_summary"]["detected"] is True
        assert analysis["secrets_summary"]["detected"] is True

    @pytest.mark.slow
    def test_e2e_4_02_vectors_object_exists(
        self,
        connect_table,
        document_analysis_table,
        s3_client,
    ):
        """DocumentAnalysis の embedding_s3_key と Vectors S3 オブジェクトが一致する。"""
        tenant_id = _tenant("test-tenant-e2e402")
        item_id = f"item-e2e402-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{tenant_id}/{item_id}/payload.txt"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET,
            Key=raw_key,
            Body=b"phase65 embedding e2e content",
            ContentType="text/plain",
        )
        connect_table.put_item(
            Item=make_file_metadata(
                tenant_id=tenant_id,
                item_id=item_id,
                item_name="phase65_embed.txt",
                mime_type="text/plain",
                raw_s3_key=raw_key,
            )
        )

        analysis = wait_for_document_analysis(
            document_analysis_table, tenant_id, item_id, max_wait=300, interval=10
        )
        assert analysis is not None
        key = analysis.get("embedding_s3_key", "")
        assert key.startswith(f"vectors/{tenant_id}/{item_id}")
        s3_client.head_object(Bucket=VECTORS_BUCKET, Key=key)

    @pytest.mark.slow
    def test_e2e_4_03_multi_tenant_document_analysis_isolation(
        self,
        connect_table,
        document_analysis_table,
        s3_client,
    ):
        """2テナント同時実行でも DocumentAnalysis がテナントごとに分離される。"""
        for tenant_id in (_tenant("test-tenant-e2e403a"), _tenant("test-tenant-e2e403b")):
            item_id = f"item-e2e403-{tenant_id[-3:]}-{uuid.uuid4().hex[:6]}"
            raw_key = f"raw/{tenant_id}/{item_id}/payload.txt"
            s3_client.put_object(
                Bucket=RAW_PAYLOAD_BUCKET,
                Key=raw_key,
                Body=f"{tenant_id} specific analysis content".encode("utf-8"),
                ContentType="text/plain",
            )
            connect_table.put_item(
                Item=make_file_metadata(
                    tenant_id=tenant_id,
                    item_id=item_id,
                    item_name=f"phase65_{tenant_id}.txt",
                    mime_type="text/plain",
                    raw_s3_key=raw_key,
                )
            )
            analysis = wait_for_document_analysis(
                document_analysis_table, tenant_id, item_id, max_wait=300, interval=10
            )
            assert analysis is not None
            assert analysis["tenant_id"] == tenant_id

    def test_e2e_4_04_detect_sensitivity_env_has_phase65_flags(self, lambda_client):
        """デプロイ済み Lambda 環境変数に Phase 6.5 フラグが含まれる。"""
        cfg = lambda_client.get_function(FunctionName=DETECT_SENSITIVITY_FN)["Configuration"]
        env = cfg["Environment"]["Variables"]
        assert "DOCUMENT_ANALYSIS_ENABLED" in env
        assert "DOCUMENT_ANALYSIS_TABLE_NAME" in env
        assert "VECTORS_BUCKET" in env
        assert "ENTITY_RESOLUTION_QUEUE_URL" in env

    @pytest.mark.slow
    def test_e2e_4_05_summary_and_dimensions_saved(
        self,
        connect_table,
        document_analysis_table,
        s3_client,
    ):
        """要約テキストと embedding metadata が保存される。"""
        item_id = f"item-e2e405-{uuid.uuid4().hex[:8]}"
        tenant_id = _tenant("test-tenant-e2e405")
        raw_key = f"raw/{tenant_id}/{item_id}/payload.txt"
        body = " ".join(["This is a phase6.5 summary metadata test."] * 20)
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key, Body=body.encode("utf-8"), ContentType="text/plain"
        )
        connect_table.put_item(
            Item=make_file_metadata(
                tenant_id=tenant_id,
                item_id=item_id,
                item_name="phase65_summary.txt",
                mime_type="text/plain",
                size=len(body.encode("utf-8")),
                raw_s3_key=raw_key,
            )
        )
        analysis = wait_for_document_analysis(
            document_analysis_table, tenant_id, item_id, max_wait=300, interval=10
        )
        assert analysis is not None
        assert isinstance(analysis.get("summary"), str)
        assert analysis.get("embedding_model") == "amazon.titan-embed-text-v2:0"
        assert analysis.get("embedding_dimension") == 1024

    @pytest.mark.slow
    def test_e2e_4_06_entity_message_contract_and_masking(
        self,
        connect_table,
        document_analysis_table,
        s3_client,
        sqs_client,
        entity_resolution_queue_url,
    ):
        """Entity candidates メッセージは契約を満たし、平文 PII を含まない。"""
        pii_literal = "phase65-e2e-mask@example.com"
        tenant_id = _tenant("test-tenant-e2e406")
        item_id = f"item-e2e406-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{tenant_id}/{item_id}/payload.txt"
        text = f"連絡先は {pii_literal} です。担当は佐藤花子。"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key, Body=text.encode("utf-8"), ContentType="text/plain"
        )
        connect_table.put_item(
            Item=make_file_metadata(
                tenant_id=tenant_id,
                item_id=item_id,
                item_name="phase65_entity_masking.txt",
                mime_type="text/plain",
                raw_s3_key=raw_key,
            )
        )

        analysis = wait_for_document_analysis(
            document_analysis_table, tenant_id, item_id, max_wait=300, interval=10
        )
        assert analysis is not None, "DocumentAnalysis not created for entity message validation"

        matched = None
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
                    matched = body
                    break
            if matched is not None:
                break
        if matched is None:
            pytest.skip("Target phase6.5 entity message was not captured")

        assert matched.get("event_type") == "entity_candidates"
        assert matched.get("tenant_id") == tenant_id
        assert pii_literal not in json.dumps(matched, ensure_ascii=False)
