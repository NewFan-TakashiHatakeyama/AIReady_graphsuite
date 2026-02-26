"""PII + NER の統合と EntityResolutionQueue 送信。"""

from __future__ import annotations

import hashlib
import json

import boto3

from services.ner_pipeline import NERDetectionResult
from services.pii_detector import PIIDetectionResult
from shared.config import ENV_ENTITY_RESOLUTION_QUEUE_URL, get_env

_sqs_client = None


def _get_sqs_client():
    global _sqs_client
    if _sqs_client is None:
        _sqs_client = boto3.client("sqs")
    return _sqs_client


def hash_pii(value: str) -> str:
    """PII を SHA-256 ハッシュ化する。"""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def merge_pii_and_ner(
    pii_results: PIIDetectionResult,
    ner_results: NERDetectionResult,
) -> list[dict]:
    """PII と NER を統合し、同一スパンは pii_flag=True として統合する。"""
    merged: dict[tuple[int, int], dict] = {}

    for entity in ner_results.entities:
        key = (entity.start, entity.end)
        merged[key] = {
            "text": entity.text,
            "label": entity.label,
            "start": entity.start,
            "end": entity.end,
            "pii_flag": False,
            "pii_type": None,
            "confidence": entity.confidence,
        }

    for pii in pii_results.details:
        key = (pii.start, pii.end)
        existing = merged.get(key)
        pii_hash = hash_pii(getattr(pii, "text", "") or f"{pii.start}:{pii.end}")
        if existing:
            existing["pii_flag"] = True
            existing["pii_type"] = pii.type
            existing["pii_hash"] = pii_hash
            existing["text"] = pii_hash
        else:
            merged[key] = {
                "text": pii_hash,
                "label": "PII",
                "start": pii.start,
                "end": pii.end,
                "pii_flag": True,
                "pii_type": pii.type,
                "pii_hash": pii_hash,
                "confidence": getattr(pii, "score", 0.85),
            }

    return sorted(merged.values(), key=lambda x: (x["start"], x["end"]))


def enqueue_entity_candidates(
    tenant_id: str,
    item_id: str,
    candidates: list[dict],
    source_document: dict | None = None,
) -> None:
    """統合エンティティ候補を FIFO SQS へ送信する。"""
    queue_url = get_env(ENV_ENTITY_RESOLUTION_QUEUE_URL)
    body = {
        "event_type": "entity_candidates",
        "tenant_id": tenant_id,
        "item_id": item_id,
        "candidates": candidates,
        "source_document": source_document or {},
    }
    dedup_source = f"{tenant_id}:{item_id}:{json.dumps(candidates, sort_keys=True)}"
    dedup_id = hashlib.sha256(dedup_source.encode("utf-8")).hexdigest()

    _get_sqs_client().send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(body, ensure_ascii=False),
        MessageGroupId=tenant_id,
        MessageDeduplicationId=dedup_id,
    )
