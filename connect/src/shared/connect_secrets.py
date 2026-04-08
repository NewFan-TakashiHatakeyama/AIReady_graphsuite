"""Secrets Manager helpers for Connect (per-connection client_secret)."""

from __future__ import annotations

import json
import logging
from typing import Any

import boto3
from botocore.exceptions import ClientError

from src.shared.config import get_config

logger = logging.getLogger(__name__)


def connection_client_secret_id(*, tenant_id: str, connection_id: str) -> str:
    t = str(tenant_id or "").strip()
    c = str(connection_id or "").strip()
    return f"/aiready/connect/{t}/{c}/client_secret"


def get_connection_client_secret(*, tenant_id: str, connection_id: str) -> str:
    """Return client_secret from the JSON secret created at onboarding; empty if missing."""
    t = str(tenant_id or "").strip()
    c = str(connection_id or "").strip()
    if not t or not c:
        return ""
    cfg = get_config()
    client = boto3.client("secretsmanager", region_name=cfg.region)
    secret_id = connection_client_secret_id(tenant_id=t, connection_id=c)
    try:
        resp: dict[str, Any] = client.get_secret_value(SecretId=secret_id)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", "") or "")
        if code in {"ResourceNotFoundException", "AccessDeniedException"}:
            return ""
        logger.warning("get_secret_value failed secret_id=%s code=%s", secret_id, code)
        return ""
    raw = str(resp.get("SecretString") or "").strip()
    if not raw:
        return ""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return ""
    return str(payload.get("client_secret") or "").strip()
