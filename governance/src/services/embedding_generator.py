"""Bedrock Titan Embeddings 生成 + S3 保存。"""

from __future__ import annotations

import json
from typing import Any

import boto3

from shared.config import ENV_VECTORS_BUCKET, get_env
from shared.logger import get_logger

logger = get_logger(__name__)

MODEL_ID = "amazon.titan-embed-text-v2:0"
DEFAULT_CHUNK_SIZE = 2000

_bedrock_client = None
_s3_client = None


def _get_bedrock_client():
    global _bedrock_client
    if _bedrock_client is None:
        _bedrock_client = boto3.client("bedrock-runtime")
    return _bedrock_client


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def split_text_into_chunks(text: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> list[str]:
    """テキストを固定長チャンクへ分割する。"""
    if not text:
        return []
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]


def generate_embedding(text: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> list[dict[str, Any]]:
    """Bedrock Titan Embeddings V2 でベクトルを生成する。"""
    client = _get_bedrock_client()
    chunks = split_text_into_chunks(text, chunk_size)
    results: list[dict[str, Any]] = []

    for idx, chunk in enumerate(chunks):
        response = client.invoke_model(
            modelId=MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(
                {
                    "inputText": chunk,
                    "dimensions": 1024,
                    "normalize": True,
                }
            ),
        )
        payload = json.loads(response["body"].read())
        results.append(
            {
                "chunk_index": idx,
                "text": chunk,
                "vector": payload["embedding"],
                "model": MODEL_ID,
                "dimension": 1024,
            }
        )
    return results


def save_embedding_to_s3(tenant_id: str, item_id: str, embedding: list[dict[str, Any]]) -> str:
    """Embedding を S3 Vectors へ JSON Lines 形式で保存する。"""
    bucket = get_env(ENV_VECTORS_BUCKET)
    key = f"vectors/{tenant_id}/{item_id}.jsonl"
    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in embedding)

    _get_s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/jsonl",
    )
    return key
