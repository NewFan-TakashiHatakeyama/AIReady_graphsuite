"""Phase 6.5: DocumentAnalysis 結合テスト。"""

from __future__ import annotations

import json
import os
from decimal import Decimal
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from services.pii_detector import PIIDetectionResult, PIIEntity
from services.secret_detector import SecretDetectionResult


@pytest.fixture
def document_analysis_env(monkeypatch):
    with mock_aws():
        region = "ap-northeast-1"
        ddb = boto3.resource("dynamodb", region_name=region)
        s3 = boto3.client("s3", region_name=region)
        sqs = boto3.client("sqs", region_name=region)

        finding_table = ddb.create_table(
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
        finding_table.meta.client.get_waiter("table_exists").wait(
            TableName="AIReadyGov-ExposureFinding"
        )

        analysis_table = ddb.create_table(
            TableName="AIReadyGov-DocumentAnalysis",
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
        analysis_table.meta.client.get_waiter("table_exists").wait(
            TableName="AIReadyGov-DocumentAnalysis"
        )

        raw_bucket = "aireadyconnect-raw-payload-123456789012"
        vectors_bucket = "aiready-123456789012-vectors"
        s3.create_bucket(
            Bucket=raw_bucket,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
        s3.create_bucket(
            Bucket=vectors_bucket,
            CreateBucketConfiguration={"LocationConstraint": region},
        )

        queue_url = sqs.create_queue(
            QueueName="AIReadyOntology-EntityResolutionQueue.fifo",
            Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "false"},
        )["QueueUrl"]

        monkeypatch.setenv("FINDING_TABLE_NAME", "AIReadyGov-ExposureFinding")
        monkeypatch.setenv("RAW_PAYLOAD_BUCKET", raw_bucket)
        monkeypatch.setenv("DOCUMENT_ANALYSIS_TABLE_NAME", "AIReadyGov-DocumentAnalysis")
        monkeypatch.setenv("VECTORS_BUCKET", vectors_bucket)
        monkeypatch.setenv("ENTITY_RESOLUTION_QUEUE_URL", queue_url)
        monkeypatch.setenv("DOCUMENT_ANALYSIS_ENABLED", "true")

        import handlers.detect_sensitivity as ds
        import shared.config as config_module
        import services.domain_dictionary as domain_dict

        ds.set_finding_table(finding_table)
        ds.set_s3_client(s3)
        config_module._ssm_client = boto3.client("ssm", region_name=region)
        config_module.clear_ssm_cache()
        domain_dict._cached_domain_dict = []

        yield {
            "finding_table": finding_table,
            "analysis_table": analysis_table,
            "s3": s3,
            "raw_bucket": raw_bucket,
            "vectors_bucket": vectors_bucket,
            "sqs": sqs,
            "queue_url": queue_url,
        }


def _seed_finding(table, finding_id="finding-001", item_id="item-001"):
    table.put_item(
        Item={
            "tenant_id": "tenant-001",
            "finding_id": finding_id,
            "source": "m365",
            "item_id": item_id,
            "item_name": "sample.txt",
            "exposure_score": Decimal("5.0"),
            "sensitivity_score": Decimal("1.0"),
            "activity_score": Decimal("2.0"),
            "ai_amplification": Decimal("1.0"),
            "risk_score": Decimal("10.0"),
            "risk_level": "medium",
            "status": "new",
            "pii_detected": False,
            "pii_count": 0,
            "pii_density": "none",
            "secrets_detected": False,
        }
    )


def _build_event(raw_bucket: str, key: str, finding_id="finding-001", item_id="item-001"):
    return {
        "Records": [
            {
                "messageId": "msg-001",
                "body": json.dumps(
                    {
                        "finding_id": finding_id,
                        "tenant_id": "tenant-001",
                        "source": "m365",
                        "item_id": item_id,
                        "item_name": "sample.txt",
                        "mime_type": "text/plain",
                        "size": 200,
                        "raw_s3_key": key,
                        "raw_s3_bucket": raw_bucket,
                    }
                ),
            }
        ]
    }


@patch("handlers.detect_sensitivity.detect_pii")
@patch("handlers.detect_sensitivity.detect_secrets")
@patch("handlers.detect_sensitivity.extract_ner_and_noun_chunks")
@patch("handlers.detect_sensitivity.generate_summary")
@patch("handlers.detect_sensitivity.generate_embedding")
@patch("handlers.detect_sensitivity.save_embedding_to_s3")
def test_document_analysis_saved_and_enqueued(
    mock_save_embedding,
    mock_generate_embedding,
    mock_summary,
    mock_ner,
    mock_secrets,
    mock_pii,
    document_analysis_env,
):
    from handlers.detect_sensitivity import handler
    from services.ner_pipeline import NERDetectionResult, NEREntity

    env = document_analysis_env
    _seed_finding(env["finding_table"])

    key = "raw/tenant-001/item-001/data.txt"
    env["s3"].put_object(Bucket=env["raw_bucket"], Key=key, Body="田中太郎の連絡先".encode("utf-8"))

    mock_pii.return_value = PIIDetectionResult(
        detected=True,
        types=["PERSON_NAME_JA"],
        count=1,
        density="low",
        high_risk_detected=False,
        details=[PIIEntity(type="PERSON_NAME_JA", start=0, end=4, score=0.9)],
    )
    mock_secrets.return_value = SecretDetectionResult()
    mock_ner.return_value = NERDetectionResult(
        entities=[NEREntity(text="田中太郎", label="Person", start=0, end=4, confidence=0.85)],
        noun_chunks=["田中太郎", "連絡先"],
        language="ja",
    )
    mock_summary.return_value = "要約テキスト"
    mock_generate_embedding.return_value = [{"chunk_index": 0, "vector": [0.1], "dimension": 1024, "model": "amazon.titan-embed-text-v2:0", "text": "x"}]
    mock_save_embedding.return_value = "vectors/tenant-001/item-001.jsonl"

    result = handler(_build_event(env["raw_bucket"], key), None)
    assert result["processed"] == 1

    analysis_item = env["analysis_table"].get_item(
        Key={"tenant_id": "tenant-001", "item_id": "item-001"}
    )["Item"]
    assert analysis_item["summary"] == "要約テキスト"
    assert analysis_item["embedding_s3_key"] == "vectors/tenant-001/item-001.jsonl"

    messages = env["sqs"].receive_message(QueueUrl=env["queue_url"], MaxNumberOfMessages=1).get(
        "Messages", []
    )
    assert len(messages) == 1


@patch("handlers.detect_sensitivity.detect_pii")
@patch("handlers.detect_sensitivity.detect_secrets")
@patch("handlers.detect_sensitivity._run_document_analysis_extensions")
def test_document_analysis_disabled_keeps_legacy_behavior(
    mock_extensions, mock_secrets, mock_pii, document_analysis_env, monkeypatch
):
    from handlers.detect_sensitivity import handler

    env = document_analysis_env
    monkeypatch.setenv("DOCUMENT_ANALYSIS_ENABLED", "false")
    _seed_finding(env["finding_table"], finding_id="finding-002", item_id="item-002")

    key = "raw/tenant-001/item-002/data.txt"
    env["s3"].put_object(Bucket=env["raw_bucket"], Key=key, Body=b"sample")

    mock_pii.return_value = PIIDetectionResult()
    mock_secrets.return_value = SecretDetectionResult()

    result = handler(_build_event(env["raw_bucket"], key, finding_id="finding-002", item_id="item-002"), None)
    assert result["processed"] == 1
    mock_extensions.assert_not_called()


@pytest.mark.aws
def test_live_bedrock_summary_and_embedding():
    """実環境で Bedrock 実呼び出し（要約 + Embedding）を検証する。"""
    if os.environ.get("LIVE_BEDROCK_TEST", "false").lower() != "true":
        pytest.skip("Set LIVE_BEDROCK_TEST=true to run live Bedrock integration test")

    from services.embedding_generator import generate_embedding
    from services.summarizer import generate_summary

    sample = (
        "本契約書は、AI Ready 株式会社と田中太郎氏との間で締結される"
        "業務委託契約の条件を定めるものである。"
    )
    summary = generate_summary(sample)
    embeddings = generate_embedding(sample, chunk_size=100)

    assert summary
    assert len(summary) <= 200
    assert len(embeddings) >= 1
    assert embeddings[0]["dimension"] == 1024
