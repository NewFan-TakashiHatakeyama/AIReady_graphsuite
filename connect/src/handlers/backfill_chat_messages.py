"""Backfill historical Teams chat messages into message_metadata (Graph list API).

Invoked asynchronously after chat-scoped onboarding (target_type=chat).
"""

from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import quote

from src.connectors.m365.graph_client import GraphApiError, GraphClient
from src.connectors.m365.messages import normalize_message
from src.shared.config import get_config
from src.shared.dynamodb import put_message_metadata
from src.shared.logger import get_logger, log_with_context


def _max_messages_cap() -> int:
    raw = os.getenv("CONNECT_CHAT_BACKFILL_MAX_MESSAGES")
    if raw is not None and raw.strip() != "":
        try:
            return max(0, int(raw.strip()))
        except ValueError:
            pass
    return get_config().chat_backfill_max_messages


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    cfg = get_config()
    request_id = getattr(context, "aws_request_id", "local")
    tenant_id = str(event.get("tenant_id") or cfg.tenant_id).strip() or cfg.tenant_id
    connection_id = str(event.get("connection_id") or "").strip()
    chat_id = str(event.get("chat_id") or "").strip()
    correlation_id = str(event.get("correlation_id") or "").strip()
    logger = get_logger(__name__, tenant_id=tenant_id, request_id=request_id)

    if not connection_id or not chat_id:
        log_with_context(
            logger,
            logging.ERROR,
            "backfill_chat_messages missing connection_id or chat_id",
            extra_data={"event": event},
        )
        return {"statusCode": 400, "body": {"error": "connection_id and chat_id are required"}}

    max_cap = _max_messages_cap()
    client = GraphClient.from_ssm(tenant_id=tenant_id, connection_id=connection_id)
    if not client._access_token or client._access_token == "PLACEHOLDER_WILL_BE_UPDATED":
        client.refresh_and_store_token(tenant_id=tenant_id, connection_id=connection_id)

    encoded_chat = quote(chat_id, safe="")
    path = f"/chats/{encoded_chat}/messages"
    params: dict[str, str] = {"$top": "50"}

    saved = 0
    pages = 0
    next_url: str | None = None
    first = True

    try:
        while True:
            if first:
                data = client.graph_get(path, params=params)
                first = False
            else:
                if not next_url:
                    break
                data = client.graph_get_absolute(next_url)

            pages += 1
            messages = data.get("value")
            if not isinstance(messages, list):
                break

            for msg in messages:
                if max_cap > 0 and saved >= max_cap:
                    next_url = None
                    break
                if not isinstance(msg, dict):
                    continue
                mid = str(msg.get("id") or "").strip()
                if not mid:
                    continue
                resource = f"chats/{chat_id}/messages/{mid}"
                try:
                    normalized = normalize_message(
                        msg,
                        tenant_id=tenant_id,
                        connection_id=connection_id,
                        resource=resource,
                        change_type="updated",
                    )
                    put_message_metadata(normalized)
                    saved += 1
                except (ValueError, KeyError, TypeError) as exc:
                    log_with_context(
                        logger,
                        logging.WARNING,
                        f"Skip message during backfill: {exc}",
                        extra_data={"message_id": mid, "resource": resource},
                    )

            if max_cap > 0 and saved >= max_cap:
                break

            next_url = str(data.get("@odata.nextLink") or "").strip() or None
            if not next_url:
                break

        log_with_context(
            logger,
            logging.INFO,
            "Chat message backfill complete",
            extra_data={
                "chat_id": chat_id,
                "connection_id": connection_id,
                "saved": saved,
                "pages": pages,
                "max_cap": max_cap,
                "correlation_id": correlation_id,
            },
        )
        return {
            "statusCode": 200,
            "body": {
                "saved": saved,
                "pages": pages,
                "chat_id": chat_id,
                "connection_id": connection_id,
            },
        }
    except GraphApiError as exc:
        log_with_context(
            logger,
            logging.ERROR,
            f"Graph error during chat backfill: {exc}",
            exc_info=True,
            extra_data={"chat_id": chat_id, "connection_id": connection_id},
        )
        return {"statusCode": 502, "body": {"error": str(exc)}}
