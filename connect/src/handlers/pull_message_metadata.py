"""pull_message_metadata Lambda handler."""

from __future__ import annotations

import json
import logging
from typing import Any

from src.connectors.m365.graph_client import GraphApiError, GraphClient
from src.connectors.m365.messages import (
    fetch_message_detail,
    normalize_message,
    parse_message_resource,
)
from src.shared.config import get_config
from src.shared.dynamodb import is_already_processed, mark_as_processed, put_message_metadata
from src.shared.logger import get_logger, log_with_context


def _process_single_record(record: dict[str, Any], logger: logging.Logger, cfg: Any) -> dict[str, int]:
    stats = {"processed": 0, "skipped": 0, "errors": 0}
    body = json.loads(record.get("body", "{}"))
    resource_type = str(body.get("resource_type") or "").strip().lower()
    if resource_type != "message":
        stats["skipped"] += 1
        return stats

    tenant_id = str(body.get("tenant_id") or cfg.tenant_id).strip() or cfg.tenant_id
    connection_id = str(body.get("connection_id") or "").strip()
    resource = str(body.get("resource") or "").strip()
    change_type = str(body.get("change_type") or "updated").strip()
    resource_info = parse_message_resource(resource)
    message_id = str(body.get("message_id") or resource_info.get("message_id") or "").strip()
    event_id = str(record.get("messageId") or f"msg:{message_id}:{change_type}").strip()

    if is_already_processed(event_id):
        stats["skipped"] += 1
        return stats

    client = GraphClient.from_ssm(tenant_id=tenant_id, connection_id=connection_id)
    if not client._access_token or client._access_token == "PLACEHOLDER_WILL_BE_UPDATED":
        client.refresh_and_store_token(tenant_id=tenant_id, connection_id=connection_id)

    message_payload: dict[str, Any] = {}
    try:
        if change_type.lower() != "deleted":
            message_payload = fetch_message_detail(client, resource)
        else:
            message_payload = {
                "id": message_id,
                "deletedDateTime": body.get("event_time") or "",
            }
        normalized = normalize_message(
            message_payload,
            tenant_id=tenant_id,
            connection_id=connection_id,
            resource=resource,
            change_type=change_type,
        )
        put_message_metadata(normalized)
        mark_as_processed(event_id, tenant_id)
        stats["processed"] += 1
        log_with_context(
            logger,
            logging.INFO,
            "Saved message metadata.",
            event_id=event_id,
            extra_data={
                "tenant_id": tenant_id,
                "connection_id": connection_id,
                "conversation_key": normalized.get("conversation_key", ""),
                "message_id": normalized.get("message_id", ""),
                "change_type": change_type,
            },
        )
    except (GraphApiError, ValueError, KeyError) as exc:
        stats["errors"] += 1
        log_with_context(
            logger,
            logging.ERROR,
            f"Failed to process message notification: {exc}",
            event_id=event_id,
            exc_info=True,
            extra_data={"body": body},
        )
    return stats


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    cfg = get_config()
    request_id = getattr(context, "aws_request_id", "local")
    logger = get_logger(__name__, tenant_id=cfg.tenant_id, request_id=request_id)
    records = event.get("Records", [])
    total_stats = {"processed": 0, "skipped": 0, "errors": 0}
    failed_record_ids: list[str] = []
    for record in records:
        try:
            stats = _process_single_record(record, logger, cfg)
            for key in total_stats:
                total_stats[key] += stats[key]
        except Exception as exc:
            failed_record_ids.append(str(record.get("messageId") or ""))
            log_with_context(
                logger,
                logging.ERROR,
                f"Unexpected failure while handling message metadata record: {exc}",
                exc_info=True,
            )
    log_with_context(logger, logging.INFO, f"Message metadata processing complete: {total_stats}")
    return {"batchItemFailures": [{"itemIdentifier": msg_id} for msg_id in failed_record_ids if msg_id]}
