"""Teams/Chat message retrieval and normalization helpers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from src.connectors.m365.graph_client import GraphClient


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_message_resource(resource: str) -> dict[str, str]:
    normalized_resource = str(resource or "").strip().lstrip("/")
    parts = normalized_resource.split("/") if normalized_resource else []
    parsed = {
        "resource": normalized_resource,
        "team_id": "",
        "channel_id": "",
        "chat_id": "",
        "message_id": "",
        "conversation_key": "",
        "message_scope": "unknown",
    }
    if len(parts) >= 6 and parts[0] == "teams" and parts[2] == "channels" and parts[4] == "messages":
        parsed["team_id"] = parts[1]
        parsed["channel_id"] = parts[3]
        parsed["message_id"] = parts[5]
        parsed["conversation_key"] = f"team:{parts[1]}:channel:{parts[3]}"
        parsed["message_scope"] = "channel"
        return parsed
    if len(parts) >= 4 and parts[0] == "chats" and parts[2] == "messages":
        parsed["chat_id"] = parts[1]
        parsed["message_id"] = parts[3]
        parsed["conversation_key"] = f"chat:{parts[1]}"
        parsed["message_scope"] = "chat"
    return parsed


def fetch_message_detail(client: GraphClient, resource: str) -> dict[str, Any]:
    normalized_resource = str(resource or "").strip().lstrip("/")
    if not normalized_resource:
        raise ValueError("resource is required.")
    return client.graph_get(f"/{normalized_resource}")


def normalize_message(
    message: dict[str, Any],
    *,
    tenant_id: str,
    connection_id: str,
    resource: str,
    change_type: str,
) -> dict[str, Any]:
    resource_info = parse_message_resource(resource)
    body_obj = message.get("body", {}) if isinstance(message.get("body"), dict) else {}
    from_obj = message.get("from", {}) if isinstance(message.get("from"), dict) else {}
    from_user = from_obj.get("user", {}) if isinstance(from_obj.get("user"), dict) else {}
    message_id = str(message.get("id") or resource_info["message_id"]).strip()
    if not message_id:
        raise ValueError("message_id is required.")
    conversation_key = str(resource_info["conversation_key"] or "").strip()
    if not conversation_key:
        raise ValueError("conversation_key is required.")
    is_deleted = bool(message.get("deletedDateTime")) or str(change_type).lower() == "deleted"
    return {
        "conversation_key": conversation_key,
        "message_id": message_id,
        "tenant_id": str(tenant_id or "").strip(),
        "connection_id": str(connection_id or "").strip(),
        "resource_type": "message",
        "message_scope": resource_info["message_scope"],
        "resource": str(resource or "").strip(),
        "change_type": str(change_type or "").strip(),
        "team_id": resource_info["team_id"],
        "channel_id": resource_info["channel_id"],
        "chat_id": resource_info["chat_id"],
        "reply_to_id": str(message.get("replyToId") or "").strip(),
        "etag": str(message.get("etag") or "").strip(),
        "sender_id": str(from_user.get("id") or "").strip(),
        "sender_display_name": str(from_user.get("displayName") or "").strip(),
        "sender_user_identity_type": str(from_user.get("userIdentityType") or "").strip(),
        "subject": str(message.get("subject") or "").strip(),
        "summary": str(message.get("summary") or "").strip(),
        "body_content_type": str(body_obj.get("contentType") or "").strip(),
        "body_content": str(body_obj.get("content") or "").strip(),
        "created_at": str(message.get("createdDateTime") or "").strip(),
        "modified_at": str(message.get("lastModifiedDateTime") or "").strip(),
        "deleted_at": str(message.get("deletedDateTime") or "").strip(),
        "is_deleted": is_deleted,
        "synced_at": _now_iso(),
        "raw_message": json.dumps(message, ensure_ascii=False, default=str),
    }
