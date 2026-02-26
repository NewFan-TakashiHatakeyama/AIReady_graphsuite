"""Bedrock Claude Haiku による要約生成。"""

from __future__ import annotations

import json

import boto3

from shared.logger import get_logger

logger = get_logger(__name__)

MAX_INPUT_CHARS = 16000
FALLBACK_SUMMARY_CHARS = 200
MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"

_bedrock_client = None


def _get_bedrock_client():
    global _bedrock_client
    if _bedrock_client is None:
        _bedrock_client = boto3.client("bedrock-runtime")
    return _bedrock_client


def generate_summary(text: str, max_tokens: int = 512) -> str:
    """ドキュメント要約を生成する。"""
    if not text:
        return ""

    truncated = text[:MAX_INPUT_CHARS]
    client = _get_bedrock_client()

    try:
        response = client.invoke_model(
            modelId=MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(
                {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": max_tokens,
                    "messages": [
                        {
                            "role": "user",
                            "content": f"以下の文書を200文字以内で要約してください。\n\n{truncated}",
                        }
                    ],
                }
            ),
        )
        payload = json.loads(response["body"].read())
        return payload["content"][0]["text"][:FALLBACK_SUMMARY_CHARS]
    except Exception as exc:
        logger.warning(f"Summary generation failed; fallback used: {exc}")
        return truncated[:FALLBACK_SUMMARY_CHARS]
