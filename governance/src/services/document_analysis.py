"""DocumentAnalysis テーブル保存。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from services.ner_pipeline import NERDetectionResult
from services.pii_detector import PIIDetectionResult
from services.secret_detector import SecretDetectionResult
from shared.config import ENV_DOCUMENT_ANALYSIS_TABLE_NAME, get_env
from shared.dynamodb import float_to_decimal, get_table


def _is_pii_span(start: int, end: int, pii_results: PIIDetectionResult) -> bool:
    for pii in pii_results.details:
        if pii.start == start and pii.end == end:
            return True
    return False


def save_document_analysis(
    tenant_id: str,
    item_id: str,
    pii_results: PIIDetectionResult,
    ner_results: NERDetectionResult,
    secret_results: SecretDetectionResult,
    summary: str,
    embedding_s3_key: str,
    source_text_length: int,
) -> None:
    """解析結果を DocumentAnalysis に保存する。"""
    table = get_table(get_env(ENV_DOCUMENT_ANALYSIS_TABLE_NAME))
    now = datetime.now(timezone.utc)
    ttl = int((now + timedelta(days=365)).timestamp())

    ner_entities = [
        {
            "text": e.text,
            "label": e.label,
            "start": e.start,
            "end": e.end,
            "confidence": float_to_decimal(e.confidence),
            "pii_flag": _is_pii_span(e.start, e.end, pii_results),
        }
        for e in ner_results.entities
    ]

    item = {
        "tenant_id": tenant_id,
        "item_id": item_id,
        "analyzed_at": now.isoformat(),
        "ner_entities": ner_entities,
        "noun_chunks": ner_results.noun_chunks,
        "pii_summary": {
            "detected": pii_results.detected,
            "types": pii_results.types,
            "count": pii_results.count,
            "density": pii_results.density,
            "high_risk_detected": pii_results.high_risk_detected,
        },
        "secrets_summary": {
            "detected": secret_results.detected,
            "types": secret_results.types,
            "count": secret_results.count,
        },
        "summary": summary,
        "embedding_s3_key": embedding_s3_key,
        "embedding_model": "amazon.titan-embed-text-v2:0",
        "embedding_dimension": 1024,
        "source_text_length": source_text_length,
        "ttl": ttl,
    }
    table.put_item(Item=item)
