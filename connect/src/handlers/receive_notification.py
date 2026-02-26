"""T-023: receive_notification Lambda

ALB Target Group から呼び出される Webhook 受信ハンドラー。
4つのリクエストパターンを処理する:

1. GET / (ヘルスチェック)              → 200 OK
2. GET ?validationToken=xxx (検証)    → 200 + トークン返却
3. POST (通知) + clientState 検証 OK  → SNS Publish + 202 Accepted
4. POST (通知) + clientState 不一致   → 403 Forbidden
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3

from src.shared.config import get_config
from src.shared.logger import get_logger, log_with_context
from src.shared.ssm import get_param
from src.connectors.m365.webhook import (
    extract_resource_info,
    get_validation_token,
    is_health_check,
    is_validation_request,
    parse_notification_body,
    verify_client_state,
)

# Lambda コールドスタート時に初期化
sns_client = boto3.client("sns", region_name=get_config().region)


def _response(status_code: int, body: str, content_type: str = "text/plain") -> dict:
    """ALB Lambda レスポンスを生成する"""
    return {
        "statusCode": status_code,
        "statusDescription": f"{status_code} OK",
        "headers": {"Content-Type": content_type},
        "body": body,
        "isBase64Encoded": False,
    }


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """ALB から呼び出される Webhook 受信ハンドラー"""
    cfg = get_config()
    request_id = getattr(context, "aws_request_id", "local")
    logger = get_logger(__name__, tenant_id=cfg.tenant_id, request_id=request_id)

    method = event.get("httpMethod", "").upper()
    path = event.get("path", "/")
    query_params = event.get("queryStringParameters") or {}
    multi_query = event.get("multiValueQueryStringParameters") or {}

    log_with_context(
        logger, logging.INFO,
        f"Webhook request: {method} {path}",
        extra_data={
            "method": method,
            "path": path,
            "queryStringParameters": query_params,
            "multiValueQueryStringParameters": multi_query,
            "headers": event.get("headers", {}),
        },
    )

    # ── 1. ヘルスチェック ──
    if is_health_check(event):
        log_with_context(logger, logging.INFO, "Health check — 200 OK")
        return _response(200, "OK")

    # ── 2. バリデーションリクエスト ──
    if is_validation_request(event):
        token = get_validation_token(event)
        log_with_context(
            logger, logging.INFO,
            f"Validation request — returning token ({len(token)} chars)",
        )
        return _response(200, token)

    # ── 3/4. POST 通知 ──
    if method != "POST":
        log_with_context(logger, logging.WARNING, f"Unexpected method: {method}")
        return _response(405, "Method Not Allowed")

    # 通知ペイロードをパース
    notifications = parse_notification_body(event)
    if not notifications:
        log_with_context(logger, logging.WARNING, "Empty notification body")
        return _response(400, "Bad Request")

    # clientState を SSM から取得
    expected_client_state = get_param(cfg.ssm_client_state)

    published_count = 0
    rejected_count = 0

    for notification in notifications:
        resource_info = extract_resource_info(notification)

        # clientState 検証
        if not verify_client_state(notification, expected_client_state):
            log_with_context(
                logger, logging.WARNING,
                "clientState mismatch — rejecting notification",
                event_id=resource_info.get("subscription_id", ""),
                extra_data=resource_info,
            )
            rejected_count += 1
            continue

        # SNS に Publish
        message = json.dumps({
            "subscription_id": resource_info["subscription_id"],
            "change_type": resource_info["change_type"],
            "resource": resource_info["resource"],
            "drive_id": resource_info["drive_id"],
            "item_id": resource_info["item_id"],
            "tenant_id": cfg.tenant_id,
        })

        sns_client.publish(
            TopicArn=cfg.notification_topic_arn,
            Message=message,
            MessageAttributes={
                "changeType": {
                    "DataType": "String",
                    "StringValue": resource_info["change_type"],
                },
                "driveId": {
                    "DataType": "String",
                    "StringValue": resource_info["drive_id"],
                },
            },
        )

        published_count += 1
        log_with_context(
            logger, logging.INFO,
            f"Published to SNS: {resource_info['change_type']} on {resource_info['resource']}",
            event_id=resource_info["subscription_id"],
            extra_data=resource_info,
        )

    if rejected_count > 0 and published_count == 0:
        return _response(403, "Forbidden — invalid clientState")

    log_with_context(
        logger, logging.INFO,
        f"Processed {published_count} notifications, rejected {rejected_count}",
        extra_data={"published": published_count, "rejected": rejected_count},
    )

    # 即座に 202 を返す（Graph は 2xx を期待、3秒以内に応答が必要）
    return _response(202, "Accepted")
