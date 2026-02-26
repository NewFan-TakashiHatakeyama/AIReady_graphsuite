from __future__ import annotations

import boto3
from moto import mock_aws

from services.document_analysis import save_document_analysis
from services.ner_pipeline import NERDetectionResult, NEREntity
from services.pii_detector import PIIDetectionResult, PIIEntity
from services.secret_detector import SecretDetectionResult


def test_save_document_analysis():
    with mock_aws():
        region = "ap-northeast-1"
        ddb = boto3.resource("dynamodb", region_name=region)
        table = ddb.create_table(
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
        table.meta.client.get_waiter("table_exists").wait(
            TableName="AIReadyGov-DocumentAnalysis"
        )

        import os

        os.environ["DOCUMENT_ANALYSIS_TABLE_NAME"] = "AIReadyGov-DocumentAnalysis"

        pii = PIIDetectionResult(
            detected=True,
            types=["PERSON_NAME_JA"],
            count=1,
            density="low",
            high_risk_detected=False,
            details=[PIIEntity(type="PERSON_NAME_JA", start=0, end=4, score=0.9)],
        )
        ner = NERDetectionResult(
            entities=[NEREntity(text="田中", label="Person", start=0, end=4, confidence=0.85)],
            noun_chunks=["契約書"],
            language="ja",
        )
        secrets = SecretDetectionResult()

        save_document_analysis(
            tenant_id="tenant-001",
            item_id="item-001",
            pii_results=pii,
            ner_results=ner,
            secret_results=secrets,
            summary="要約",
            embedding_s3_key="vectors/tenant-001/item-001.jsonl",
            source_text_length=123,
        )

        item = table.get_item(Key={"tenant_id": "tenant-001", "item_id": "item-001"})["Item"]
        assert item["summary"] == "要約"
        assert item["embedding_dimension"] == 1024
        assert item["ner_entities"][0]["pii_flag"] is True
        assert item["ttl"] > 0
