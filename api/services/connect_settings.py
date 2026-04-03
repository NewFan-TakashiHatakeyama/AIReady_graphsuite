"""Connect runtime settings aggregation and startup validation."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.exceptions import ClientError

from services.runtime_config import load_tenant_registry


@dataclass(frozen=True)
class ConnectSettings:
    aws_region: str
    file_metadata_table_name: str
    message_metadata_table_name: str
    delta_tokens_table_name: str
    connections_table_name: str
    pull_file_metadata_lambda_name: str
    init_subscription_lambda_name: str
    # Empty = skip async chat history backfill (local dev). Set CONNECT_BACKFILL_CHAT_MESSAGES_LAMBDA_NAME in deploy.
    backfill_chat_messages_lambda_name: str
    log_groups: tuple[str, ...]
    startup_validate: bool
    startup_validate_resources: bool
    require_tenant_registry: bool
    require_connections_table: bool


def _read_bool_env(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_log_groups(raw: str | None) -> tuple[str, ...]:
    configured = tuple(entry.strip() for entry in (raw or "").split(",") if entry.strip())
    if configured:
        return configured
    return (
        "/aws/lambda/AIReadyConnect-pullFileMetadata",
        "/aws/lambda/AIReadyConnect-receiveNotification",
        "/aws/lambda/AIReadyConnect-renewSubscription",
        "/aws/lambda/AIReadyConnect-renewAccessToken",
    )


def load_connect_settings() -> ConnectSettings:
    region = (
        os.getenv("CONNECT_AWS_REGION")
        or os.getenv("AWS_REGION")
        or os.getenv("GOVERNANCE_AWS_REGION")
        or ""
    ).strip()
    return ConnectSettings(
        aws_region=region,
        file_metadata_table_name=(
            os.getenv("CONNECT_FILE_METADATA_TABLE_NAME") or "AIReadyConnect-FileMetadata"
        ).strip(),
        message_metadata_table_name=(
            os.getenv("CONNECT_MESSAGE_METADATA_TABLE_NAME") or "AIReadyConnect-MessageMetadata"
        ).strip(),
        delta_tokens_table_name=(
            os.getenv("CONNECT_DELTA_TOKENS_TABLE_NAME") or "AIReadyConnect-DeltaTokens"
        ).strip(),
        connections_table_name=(
            os.getenv("CONNECT_CONNECTIONS_TABLE_NAME") or "AIReadyConnect-Connections"
        ).strip(),
        pull_file_metadata_lambda_name=(
            os.getenv("CONNECT_PULL_FILE_METADATA_LAMBDA_NAME") or "AIReadyConnect-pullFileMetadata"
        ).strip(),
        init_subscription_lambda_name=(
            os.getenv("CONNECT_INIT_SUBSCRIPTION_LAMBDA_NAME") or "AIReadyConnect-initSubscription"
        ).strip(),
        backfill_chat_messages_lambda_name=(
            os.getenv("CONNECT_BACKFILL_CHAT_MESSAGES_LAMBDA_NAME") or ""
        ).strip(),
        log_groups=_parse_log_groups(os.getenv("CONNECT_LOG_GROUPS")),
        startup_validate=_read_bool_env("CONNECT_STARTUP_VALIDATE", True),
        startup_validate_resources=_read_bool_env("CONNECT_STARTUP_VALIDATE_RESOURCES", True),
        require_tenant_registry=_read_bool_env("CONNECT_REQUIRE_TENANT_REGISTRY", False),
        require_connections_table=_read_bool_env("CONNECT_REQUIRE_CONNECTIONS_TABLE", False),
    )


def validate_connect_settings(settings: ConnectSettings) -> None:
    missing: list[str] = []
    if not settings.aws_region:
        missing.append("CONNECT_AWS_REGION (or AWS_REGION)")
    if not settings.file_metadata_table_name:
        missing.append("CONNECT_FILE_METADATA_TABLE_NAME")
    if not settings.message_metadata_table_name:
        missing.append("CONNECT_MESSAGE_METADATA_TABLE_NAME")
    if not settings.delta_tokens_table_name:
        missing.append("CONNECT_DELTA_TOKENS_TABLE_NAME")
    if settings.require_connections_table and not settings.connections_table_name:
        missing.append("CONNECT_CONNECTIONS_TABLE_NAME")
    if not settings.pull_file_metadata_lambda_name:
        missing.append("CONNECT_PULL_FILE_METADATA_LAMBDA_NAME")
    if not settings.log_groups:
        missing.append("CONNECT_LOG_GROUPS")
    if missing:
        raise ValueError(
            "Connect startup configuration is invalid. Missing required settings: "
            + ", ".join(missing)
        )


def validate_connect_tenant_access(settings: ConnectSettings, tenant_id: str) -> dict[str, Any]:
    """Validate tenant boundary for Connect runtime access."""
    normalized_tenant_id = str(tenant_id or "").strip()
    if not normalized_tenant_id:
        raise ValueError("tenant_id is required for Connect access.")
    if not settings.require_tenant_registry:
        return {"tenant_id": normalized_tenant_id}

    registry = load_tenant_registry()
    tenant_override = registry.get(normalized_tenant_id)
    if tenant_override is None:
        raise ValueError(
            "Tenant is not registered for Connect access. "
            f"tenant_id={normalized_tenant_id}"
        )
    if tenant_override and not isinstance(tenant_override, dict):
        raise ValueError(
            f"Invalid tenant override for tenant '{normalized_tenant_id}'."
        )
    return {"tenant_id": normalized_tenant_id, "override": tenant_override or {}}


def check_connect_resources(settings: ConnectSettings) -> dict[str, str]:
    """Best-effort existence checks for critical Connect resources."""
    session = boto3.session.Session(region_name=settings.aws_region)
    ddb = session.client("dynamodb")
    logs = session.client("logs")
    lambda_client = session.client("lambda")

    ddb.describe_table(TableName=settings.file_metadata_table_name)
    ddb.describe_table(TableName=settings.message_metadata_table_name)
    ddb.describe_table(TableName=settings.delta_tokens_table_name)
    if settings.require_connections_table:
        ddb.describe_table(TableName=settings.connections_table_name)
    lambda_client.get_function(FunctionName=settings.pull_file_metadata_lambda_name)

    existing_log_groups = set()
    paginator = logs.get_paginator("describe_log_groups")
    for group_name in settings.log_groups:
        found = False
        for page in paginator.paginate(logGroupNamePrefix=group_name):
            for group in page.get("logGroups", []):
                name = str(group.get("logGroupName", ""))
                existing_log_groups.add(name)
                if name == group_name:
                    found = True
                    break
            if found:
                break
        if not found:
            raise ClientError(
                error_response={
                    "Error": {
                        "Code": "ResourceNotFoundException",
                        "Message": f"Log group not found: {group_name}",
                    }
                },
                operation_name="DescribeLogGroups",
            )

    return {
        "dynamodb": "ok",
        "lambda": "ok",
        "logs": f"ok({len(existing_log_groups)} groups)",
    }
