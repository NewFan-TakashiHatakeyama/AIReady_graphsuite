"""Runtime configuration and startup fail-fast validation for MS-2."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AwsRuntimeConfig:
    aws_region: str
    governance_finding_table_name: str
    governance_document_analysis_table_name: str
    governance_api_strict_mode: bool
    governance_allow_cloudwatch_fallback: bool
    governance_allow_missing_policy_scope_fallback: bool
    ontology_db_root: str
    startup_fail_fast: bool
    aws_healthcheck_on_startup: bool


def _read_bool_env(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    return normalized in {"1", "true", "yes", "on"}


def load_aws_runtime_config() -> AwsRuntimeConfig:
    region = (os.getenv("GOVERNANCE_AWS_REGION") or os.getenv("AWS_REGION") or "").strip()
    finding_table = (
        os.getenv("GOVERNANCE_FINDING_TABLE_NAME") or "AIReadyGov-ExposureFinding"
    ).strip()
    document_analysis_table = (
        os.getenv("GOVERNANCE_DOCUMENT_ANALYSIS_TABLE_NAME")
        or os.getenv("DOCUMENT_ANALYSIS_TABLE")
        or "AIReadyGov-DocumentAnalysis"
    ).strip()
    # Per-tenant SQLite under this root (ontology graph projection). For multiple API instances,
    # use a shared filesystem (EFS) or set ONTOLOGY_GRAPH_DB_PATH in ontology_graph_repository.
    ontology_db_root = (os.getenv("ONTOLOGY_GRAPH_DB_ROOT") or "./tenant_storage").strip()
    governance_api_strict_mode = _read_bool_env("GOVERNANCE_API_STRICT_MODE", False)
    governance_allow_cloudwatch_fallback = _read_bool_env(
        "GOVERNANCE_ALLOW_CLOUDWATCH_FALLBACK",
        not governance_api_strict_mode,
    )
    governance_allow_missing_policy_scope_fallback = _read_bool_env(
        "GOVERNANCE_ALLOW_MISSING_POLICY_SCOPE_FALLBACK",
        not governance_api_strict_mode,
    )

    return AwsRuntimeConfig(
        aws_region=region,
        governance_finding_table_name=finding_table,
        governance_document_analysis_table_name=document_analysis_table,
        governance_api_strict_mode=governance_api_strict_mode,
        governance_allow_cloudwatch_fallback=governance_allow_cloudwatch_fallback,
        governance_allow_missing_policy_scope_fallback=(
            governance_allow_missing_policy_scope_fallback
        ),
        ontology_db_root=ontology_db_root,
        startup_fail_fast=_read_bool_env("STARTUP_FAIL_FAST", True),
        aws_healthcheck_on_startup=_read_bool_env("AWS_HEALTHCHECK_ON_STARTUP", True),
    )


def validate_runtime_config(config: AwsRuntimeConfig) -> None:
    """Raise ValueError when required startup settings are missing."""
    missing_keys: list[str] = []
    if not config.aws_region:
        missing_keys.append("AWS_REGION (or GOVERNANCE_AWS_REGION)")
    if not config.governance_finding_table_name:
        missing_keys.append("GOVERNANCE_FINDING_TABLE_NAME")
    if not config.governance_document_analysis_table_name:
        missing_keys.append("GOVERNANCE_DOCUMENT_ANALYSIS_TABLE_NAME")
    if not config.ontology_db_root:
        missing_keys.append("ONTOLOGY_GRAPH_DB_ROOT")

    if missing_keys:
        details = ", ".join(missing_keys)
        raise ValueError(
            f"Startup configuration is invalid. Missing required settings: {details}"
        )


def _read_tenant_registry_json() -> dict[str, Any]:
    """Best-effort JSON parser for tenant routing overrides."""
    raw_registry = os.getenv("TENANT_DB_REGISTRY_JSON", "").strip()
    if not raw_registry:
        return {}

    import json

    parsed = json.loads(raw_registry)
    if not isinstance(parsed, dict):
        raise ValueError("TENANT_DB_REGISTRY_JSON must be a JSON object.")
    return parsed


def load_tenant_registry() -> dict[str, Any]:
    try:
        return _read_tenant_registry_json()
    except Exception as exc:
        raise ValueError(f"Failed to parse TENANT_DB_REGISTRY_JSON: {exc}") from exc
