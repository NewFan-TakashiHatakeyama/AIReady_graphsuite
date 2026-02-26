"""Configuration helpers with SSM cache support."""

from __future__ import annotations

import json
import os
import time
from typing import Any

import boto3
from botocore.client import BaseClient


_ssm_cache: dict[str, tuple[float, str]] = {}
DEFAULT_SSM_CACHE_TTL_SECONDS = 300


def get_env(name: str, default: str | None = None, required: bool = False) -> str:
    """Read an environment variable with optional required validation."""
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return value or ""


def get_tenant_parameter_path(tenant_id: str, key: str) -> str:
    """Build the standard SSM parameter path for a tenant."""
    normalized_key = key.lstrip("/")
    return f"/ai-ready/ontology/{tenant_id}/{normalized_key}"


def get_ssm_parameter(
    name: str,
    *,
    with_decryption: bool = False,
    ttl_seconds: int = DEFAULT_SSM_CACHE_TTL_SECONDS,
    ssm_client: BaseClient | None = None,
) -> str:
    """Read SSM parameter value with in-memory TTL cache."""
    now = time.time()
    cache_entry = _ssm_cache.get(name)
    if cache_entry and now < cache_entry[0]:
        return cache_entry[1]

    client = ssm_client or boto3.client("ssm")
    response = client.get_parameter(Name=name, WithDecryption=with_decryption)
    value = response["Parameter"]["Value"]
    _ssm_cache[name] = (now + ttl_seconds, value)
    return value


def get_ssm_json_parameter(
    name: str,
    *,
    with_decryption: bool = False,
    ttl_seconds: int = DEFAULT_SSM_CACHE_TTL_SECONDS,
    ssm_client: BaseClient | None = None,
) -> dict[str, Any]:
    """Read an SSM parameter and parse JSON object."""
    return json.loads(
        get_ssm_parameter(
            name,
            with_decryption=with_decryption,
            ttl_seconds=ttl_seconds,
            ssm_client=ssm_client,
        )
    )


def clear_ssm_cache() -> None:
    """Clear local SSM cache (mainly for tests)."""
    _ssm_cache.clear()
