"""Hybrid SaaS: resolve Graph app credentials from a customer AWS account (AssumeRole + SM)."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

from src.shared.config import get_config

logger = logging.getLogger(__name__)


def _tenant_param_name(tenant_id: str, key: str) -> str:
    return f"/aiready/connect/{str(tenant_id or '').strip()}/{key}"

_CACHE: dict[str, tuple[float, tuple[str, str, str]]] = {}
_TTL_SEC = 300.0


def _ssm_plain(name: str) -> str:
    if not name:
        return ""
    cfg = get_config()
    client = boto3.client("ssm", region_name=cfg.region)
    try:
        resp = client.get_parameter(Name=name, WithDecryption=False)
        return str(resp.get("Parameter", {}).get("Value") or "").strip()
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", "") or "")
        if code in {"ParameterNotFound", "AccessDeniedException"}:
            return ""
        logger.warning("customer creds SSM read failed name=%s code=%s", name, code)
        return ""


def try_resolve_customer_graph_credentials(*, tenant_id: str) -> tuple[str, str, str] | None:
    """If tenant SSM references a cross-account role + secret, return (azure_tenant_id, client_id, client_secret)."""
    tid = str(tenant_id or "").strip()
    if not tid:
        return None

    cache_key = tid
    now = time.monotonic()
    cached = _CACHE.get(cache_key)
    if cached and cached[0] > now:
        return cached[1]

    role_arn = _ssm_plain(_tenant_param_name(tid, "customer_credentials_role_arn"))
    secret_arn = _ssm_plain(_tenant_param_name(tid, "customer_credentials_secret_arn"))
    external_id = _ssm_plain(_tenant_param_name(tid, "customer_credentials_external_id"))
    if not role_arn or not secret_arn:
        return None

    cfg = get_config()
    sts = boto3.client("sts", region_name=cfg.region)
    try:
        assume_kw: dict[str, Any] = {
            "RoleArn": role_arn,
            "RoleSessionName": "aiready-connect-graph",
        }
        if external_id:
            assume_kw["ExternalId"] = external_id
        assumed = sts.assume_role(**assume_kw)
        creds = assumed["Credentials"]
    except ClientError as exc:
        logger.warning(
            "AssumeRole failed for tenant_id=%s code=%s",
            tid,
            exc.response.get("Error", {}).get("Code", ""),
        )
        return None

    sm = boto3.client(
        "secretsmanager",
        region_name=cfg.region,
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )
    try:
        sec = sm.get_secret_value(SecretId=secret_arn)
    except ClientError as exc:
        logger.warning(
            "Customer secret read failed tenant_id=%s code=%s",
            tid,
            exc.response.get("Error", {}).get("Code", ""),
        )
        return None

    raw = str(sec.get("SecretString") or "").strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None

    azure = str(
        payload.get("azure_tenant_id")
        or payload.get("tenant_id")
        or payload.get("graph_tenant_id")
        or ""
    ).strip()
    cid = str(payload.get("client_id") or payload.get("graph_client_id") or "").strip()
    csec = str(payload.get("client_secret") or "").strip()
    if not azure or not cid or not csec:
        return None

    triple = (azure, cid, csec)
    _CACHE[cache_key] = (now + _TTL_SEC, triple)
    return triple
