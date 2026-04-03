"""Operational validators for Ontology M4 gap remediation."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from typing import Any

from services.aws_clients import get_rds_client, get_sns_client, get_stepfunctions_client
from services.runtime_config import load_aws_runtime_config

_runtime_config = load_aws_runtime_config()
_sns_client = None
_stepfunctions_client = None
_rds_client = None


def _sns():
    global _sns_client
    if _sns_client is None:
        _sns_client = get_sns_client(_runtime_config)
    return _sns_client


def _stepfunctions():
    global _stepfunctions_client
    if _stepfunctions_client is None:
        _stepfunctions_client = get_stepfunctions_client(_runtime_config)
    return _stepfunctions_client


def _rds():
    global _rds_client
    if _rds_client is None:
        _rds_client = get_rds_client(_runtime_config)
    return _rds_client


@dataclass(frozen=True)
class OntologyOpsCheckResult:
    name: str
    status: str
    detail: str


def check_aurora_proxy_connectivity() -> OntologyOpsCheckResult:
    host = (os.getenv("ONTOLOGY_AURORA_PROXY_ENDPOINT") or os.getenv("AURORA_PROXY_ENDPOINT") or "").strip()
    if not host:
        return OntologyOpsCheckResult(
            name="aurora_proxy",
            status="not_configured",
            detail="AURORA proxy endpoint is not configured.",
        )
    port = int(os.getenv("ONTOLOGY_AURORA_PORT", "5432"))
    try:
        with socket.create_connection((host, port), timeout=2):
            return OntologyOpsCheckResult(
                name="aurora_proxy",
                status="ok",
                detail=f"Connection to {host}:{port} succeeded.",
            )
    except Exception as exc:
        return OntologyOpsCheckResult(
            name="aurora_proxy",
            status="failed",
            detail=f"Connection to {host}:{port} failed: {exc}",
        )


def check_aurora_proxy_registration() -> OntologyOpsCheckResult:
    configured_host = (
        os.getenv("ONTOLOGY_AURORA_PROXY_ENDPOINT") or os.getenv("AURORA_PROXY_ENDPOINT") or ""
    ).strip()
    try:
        response = _rds().describe_db_proxies()
        proxies = response.get("DBProxies", [])
        endpoints = {
            str(proxy.get("Endpoint", "")).strip().lower()
            for proxy in proxies
            if proxy.get("Endpoint")
        }
        if not configured_host:
            if endpoints:
                return OntologyOpsCheckResult(
                    name="aurora_proxy_registration",
                    status="failed",
                    detail="Aurora proxy endpoint exists in AWS but is not configured in API env.",
                )
            return OntologyOpsCheckResult(
                name="aurora_proxy_registration",
                status="not_configured",
                detail="Aurora proxy endpoint is not configured and no proxy is found.",
            )
        if configured_host.lower() in endpoints:
            return OntologyOpsCheckResult(
                name="aurora_proxy_registration",
                status="ok",
                detail="Configured Aurora proxy endpoint is present in AWS.",
            )
        return OntologyOpsCheckResult(
            name="aurora_proxy_registration",
            status="failed",
            detail="Configured Aurora proxy endpoint was not found in describe-db-proxies output.",
        )
    except Exception as exc:
        return OntologyOpsCheckResult(
            name="aurora_proxy_registration",
            status="failed",
            detail=f"Failed to inspect Aurora proxies: {exc}",
        )


def check_stepfunctions_logging() -> OntologyOpsCheckResult:
    arn = (
        os.getenv("ONTOLOGY_BATCH_RECONCILER_STATE_MACHINE_ARN")
        or os.getenv("ONTOLOGY_STATE_MACHINE_ARN")
        or ""
    ).strip()
    if not arn:
        return OntologyOpsCheckResult(
            name="stepfunctions_logging",
            status="not_configured",
            detail=(
                "No Step Functions state machine ARN configured "
                "(ontology daily batch reconciler was removed)."
            ),
        )
    try:
        response = _stepfunctions().describe_state_machine(stateMachineArn=arn)
        logging_cfg = response.get("loggingConfiguration", {})
        level = str(logging_cfg.get("level", "OFF")).upper()
        if level == "OFF":
            return OntologyOpsCheckResult(
                name="stepfunctions_logging",
                status="failed",
                detail=f"State machine logging level is OFF for {arn}.",
            )
        return OntologyOpsCheckResult(
            name="stepfunctions_logging",
            status="ok",
            detail=f"State machine logging level is {level}.",
        )
    except Exception as exc:
        return OntologyOpsCheckResult(
            name="stepfunctions_logging",
            status="failed",
            detail=f"State machine describe failed: {exc}",
        )


def check_sns_subscription() -> OntologyOpsCheckResult:
    topic_arn = (
        os.getenv("ONTOLOGY_ALERT_TOPIC_ARN")
        or "arn:aws:sns:ap-northeast-1:565699611973:AIReadyOntology-Alerts"
    ).strip()
    if not topic_arn:
        return OntologyOpsCheckResult(
            name="sns_subscription",
            status="not_configured",
            detail="Ontology alerts topic ARN is not configured.",
        )
    try:
        response = _sns().get_topic_attributes(TopicArn=topic_arn)
        attributes = response.get("Attributes", {})
        confirmed = int(str(attributes.get("SubscriptionsConfirmed", "0")))
        if confirmed <= 0:
            return OntologyOpsCheckResult(
                name="sns_subscription",
                status="failed",
                detail=f"No confirmed SNS subscriptions for {topic_arn}.",
            )
        return OntologyOpsCheckResult(
            name="sns_subscription",
            status="ok",
            detail=f"{confirmed} confirmed SNS subscription(s).",
        )
    except Exception as exc:
        return OntologyOpsCheckResult(
            name="sns_subscription",
            status="failed",
            detail=f"SNS topic check failed: {exc}",
        )


def run_ontology_ops_checks() -> dict[str, Any]:
    checks = [
        check_aurora_proxy_registration(),
        check_aurora_proxy_connectivity(),
        check_stepfunctions_logging(),
        check_sns_subscription(),
    ]
    has_failure = any(check.status == "failed" for check in checks)
    has_ok = any(check.status == "ok" for check in checks)
    overall = "failed" if has_failure else "ok" if has_ok else "not_configured"
    return {
        "overall": overall,
        "checks": [
            {"name": check.name, "status": check.status, "detail": check.detail}
            for check in checks
        ],
    }


def run_production_gate_checks(
    ontology_ops_validation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ops = ontology_ops_validation or run_ontology_ops_checks()
    strict_mode = str(os.getenv("PRODUCTION_GATE_STRICT", "false")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    ontology_dummy_fallback_enabled = str(
        os.getenv("ENABLE_ONTOLOGY_DUMMY_FALLBACK", "false")
    ).strip().lower() in {"1", "true", "yes", "on"}
    webui_dummy_fallback_enabled = str(
        os.getenv("VITE_ENABLE_ONTOLOGY_DUMMY_FALLBACK", "false")
    ).strip().lower() in {"1", "true", "yes", "on"}
    checks: list[dict[str, str]] = [
        {
            "name": "ontology_dummy_fallback_disabled",
            "status": "ok"
            if not ontology_dummy_fallback_enabled and not webui_dummy_fallback_enabled
            else "failed",
            "detail": "Ontology dummy fallback is disabled.",
        },
        {
            "name": "governance_cloudwatch_fallback_disabled",
            "status": "ok"
            if not _runtime_config.governance_allow_cloudwatch_fallback
            else "failed",
            "detail": "Governance CloudWatch fallback is disabled.",
        },
        {
            "name": "governance_policy_scope_fallback_disabled",
            "status": "ok"
            if not _runtime_config.governance_allow_missing_policy_scope_fallback
            else "failed",
            "detail": "Governance missing policy scope fallback is disabled.",
        },
    ]
    ops_status = str(ops.get("overall", "not_configured")).lower()
    checks.append(
        {
            "name": "ontology_ops_monitoring",
            "status": "ok"
            if ops_status == "ok"
            else "warning"
            if not strict_mode
            else "failed",
            "detail": "Ontology operational checks are healthy."
            if ops_status == "ok"
            else "Ontology operational checks are not fully healthy.",
        }
    )
    has_failed = any(check["status"] == "failed" for check in checks)
    overall = "failed" if has_failed else "ok"
    return {"overall": overall, "strict_mode": strict_mode, "checks": checks}
