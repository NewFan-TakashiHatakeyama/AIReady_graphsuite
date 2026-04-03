"""Application services for dashboard and cross-domain audit."""

from __future__ import annotations

import csv
import io
import os
import secrets
import hashlib
import hmac
import uuid
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime, timedelta, timezone
from typing import Any

from services.aws_clients import get_s3_client
from services.connect_service import list_connect_audit, tenant_has_active_scoring_connection
from services.governance_api_service import (
    get_governance_overview,
    list_governance_audit_logs,
)
from services.ontology_api_service import (
    get_ontology_overview,
    list_ontology_audit_logs,
)
from services.repositories.dashboard_readiness_snapshot_repository import (
    DashboardReadinessSnapshotRepository,
    build_trend_labels,
    default_estimated_series,
)
from services.repositories.audit_writer_repository import CommonAuditRepository
from services.runtime_config import load_aws_runtime_config

_runtime_config = load_aws_runtime_config()
_s3 = get_s3_client(_runtime_config)
_common_audit_repository = CommonAuditRepository()
_dashboard_readiness_snapshot_repository = DashboardReadinessSnapshotRepository()
_audit_export_jobs: dict[str, dict[str, Any]] = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _audit_download_secret() -> str:
    configured = (os.getenv("AUDIT_EXPORT_DOWNLOAD_TOKEN_SECRET") or "").strip()
    if configured:
        return configured
    return "aiready-audit-export-download-secret"


def _encode_token_payload(payload: str) -> str:
    return urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")


def _decode_token_payload(payload_b64: str) -> str:
    padded = payload_b64 + "=" * (-len(payload_b64) % 4)
    return urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")


def _issue_download_token(*, tenant_id: str, job_id: str, expires_at: datetime) -> tuple[str, str]:
    nonce = secrets.token_urlsafe(12)
    payload = f"{tenant_id}|{job_id}|{int(expires_at.timestamp())}|{nonce}"
    digest = hmac.new(
        _audit_download_secret().encode("utf-8"),
        payload.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()
    token = f"{_encode_token_payload(payload)}.{digest}"
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    return token, token_hash


def _validate_download_token(*, token: str, tenant_id: str, job_id: str) -> tuple[bool, str]:
    parts = str(token or "").split(".")
    if len(parts) != 2:
        return False, "Invalid token."
    payload_b64, digest = parts
    try:
        payload = _decode_token_payload(payload_b64)
    except Exception:
        return False, "Invalid token payload."
    expected_digest = hmac.new(
        _audit_download_secret().encode("utf-8"),
        payload.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected_digest, digest):
        return False, "Invalid token signature."
    payload_parts = payload.split("|")
    if len(payload_parts) != 4:
        return False, "Invalid token structure."
    token_tenant_id, token_job_id, token_exp_ts, _nonce = payload_parts
    if token_tenant_id != tenant_id or token_job_id != job_id:
        return False, "Token tenant/job mismatch."
    try:
        exp_ts = int(token_exp_ts)
    except ValueError:
        return False, "Invalid token expiration."
    now_ts = int(datetime.now(timezone.utc).timestamp())
    if now_ts >= exp_ts:
        return False, "Token expired."
    return True, ""


def _to_score_percent(value: Any) -> float:
    raw = float(value)
    if raw <= 1.0:
        return max(0.0, min(100.0, raw * 100.0))
    return max(0.0, min(100.0, raw))


def _extract_signal_scores(signals: list[dict[str, Any]]) -> tuple[float, float, float, float, float]:
    governance_oversharing = _signal_score(signals, "governance_oversharing")
    governance_sensitive = _signal_score(signals, "governance_sensitive")
    governance_assurance = _signal_score(signals, "governance_assurance")
    ontology_foundation = _signal_score(signals, "ontology_foundation")
    ontology_usecase = _signal_score(signals, "ontology_usecase")
    return (
        governance_oversharing,
        governance_sensitive,
        governance_assurance,
        ontology_foundation,
        ontology_usecase,
    )


def _record_readiness_snapshot(
    tenant_id: str,
    *,
    target_score: int,
    readiness_score: float,
    signals: list[dict[str, Any]],
) -> None:
    (
        governance_oversharing,
        governance_sensitive,
        governance_assurance,
        ontology_foundation,
        ontology_usecase,
    ) = _extract_signal_scores(signals)
    _dashboard_readiness_snapshot_repository.upsert_snapshot(
        tenant_id,
        target_score=target_score,
        readiness_score=readiness_score,
        governance_oversharing=governance_oversharing,
        governance_sensitive=governance_sensitive,
        governance_assurance=governance_assurance,
        ontology_foundation=ontology_foundation,
        ontology_usecase=ontology_usecase,
    )


def get_dashboard_readiness(tenant_id: str) -> dict[str, Any]:
    governance = get_governance_overview(tenant_id=tenant_id)
    ontology = get_ontology_overview(tenant_id=tenant_id)
    governance_subscores = governance.get("subscores", {})
    governance_oversharing = _to_score_percent(governance_subscores.get("oversharing_control", 0.0))
    governance_sensitive = _to_score_percent(governance_subscores.get("sensitive_protection", 0.0))
    governance_assurance = _to_score_percent(governance_subscores.get("assurance", 0.0))
    ontology_foundation = _to_score_percent(ontology.get("base_ontology_score", 0.0))
    ontology_usecase = _to_score_percent(ontology.get("use_case_readiness", 0.0))
    ontology_score = _to_score_percent(ontology.get("ontology_score", 0.0))
    acknowledged = int(governance.get("counts", {}).get("acknowledged", 0))
    total_findings = int(governance.get("counts", {}).get("total_findings", 0))
    high_risk_count = int(governance.get("high_risk_count", 0))
    expiring_suppressions = int(governance.get("expiring_suppressions_24h", 0))
    action_required_count = int(governance.get("action_required_count", 0))
    stale_or_aging = int(ontology.get("stale_or_aging_documents", 0))
    unresolved_candidates = int(ontology.get("unresolved_candidates", 0))
    high_spread_entities = int(ontology.get("high_spread_entities", 0))
    unified_n = int(ontology.get("unified_document_count", 0))
    candidates_n = int(ontology.get("entity_candidate_count", 0))

    has_connect = tenant_has_active_scoring_connection(tenant_id)
    # No inventory in governance or ontology tables → do not show scores, even if a stale
    # Connect row remains after DynamoDB was cleared (avoids fake 100/35/67-style outputs).
    pipeline_empty = total_findings == 0 and unified_n == 0 and candidates_n == 0
    if pipeline_empty:
        governance_scores_available = False
        ontology_scores_available = False
    else:
        governance_scores_available = bool(has_connect or total_findings > 0)
        ontology_scores_available = bool(has_connect or unified_n > 0 or candidates_n > 0)

    gov_score_sensitive = round(governance_sensitive, 2) if governance_scores_available else None
    gov_score_oversharing = round(governance_oversharing, 2) if governance_scores_available else None
    gov_score_assurance = round(governance_assurance, 2) if governance_scores_available else None
    onto_score_foundation = round(ontology_foundation, 2) if ontology_scores_available else None
    onto_score_usecase = round(ontology_usecase, 2) if ontology_scores_available else None

    signals = [
        {
            "key": "governance_sensitive",
            "label": "機微情報の保護",
            "score": gov_score_sensitive,
            "issues": action_required_count,
            "target": 90,
            "sub_metrics": (
                [
                    {
                        "key": "sensitive_protection",
                        "label": "機微情報保護スコア",
                        "value": round(governance_sensitive, 2),
                        "unit": "score",
                    },
                    {
                        "key": "action_required_count",
                        "label": "対応要件数",
                        "value": float(action_required_count),
                        "unit": "count",
                    },
                    {
                        "key": "total_findings",
                        "label": "検知総数",
                        "value": float(total_findings),
                        "unit": "count",
                    },
                ]
                if governance_scores_available
                else []
            ),
        },
        {
            "key": "governance_oversharing",
            "label": "過剰共有の抑制",
            "score": gov_score_oversharing,
            "issues": high_risk_count,
            "target": 90,
            "sub_metrics": (
                [
                    {
                        "key": "oversharing_control",
                        "label": "過剰共有保護スコア",
                        "value": round(governance_oversharing, 2),
                        "unit": "score",
                    },
                    {
                        "key": "high_risk_findings",
                        "label": "高リスク検知件数",
                        "value": float(high_risk_count),
                        "unit": "count",
                    },
                    {
                        "key": "expiring_suppressions_24h",
                        "label": "24h内期限切れ抑止",
                        "value": float(expiring_suppressions),
                        "unit": "count",
                    },
                ]
                if governance_scores_available
                else []
            ),
        },
        {
            "key": "governance_assurance",
            "label": "運用・保証",
            "score": gov_score_assurance,
            "issues": acknowledged,
            "target": 90,
            "sub_metrics": (
                [
                    {
                        "key": "assurance_score",
                        "label": "運用・保証スコア",
                        "value": round(governance_assurance, 2),
                        "unit": "score",
                    },
                    {
                        "key": "acknowledged_findings",
                        "label": "対応中（acknowledged）",
                        "value": float(acknowledged),
                        "unit": "count",
                    },
                    {
                        "key": "total_findings",
                        "label": "検知総数",
                        "value": float(total_findings),
                        "unit": "count",
                    },
                ]
                if governance_scores_available
                else []
            ),
        },
        {
            "key": "ontology_foundation",
            "label": "情報整備スコア",
            "score": onto_score_foundation,
            "issues": stale_or_aging,
            "target": 90,
            "sub_metrics": (
                [
                    {
                        "key": "base_ontology_score",
                        "label": "情報整備スコア",
                        "value": round(ontology_foundation, 2),
                        "unit": "score",
                    },
                    {
                        "key": "stale_or_aging_documents",
                        "label": "鮮度課題ドキュメント",
                        "value": float(stale_or_aging),
                        "unit": "count",
                    },
                    {
                        "key": "ontology_score",
                        "label": "Ontology総合スコア",
                        "value": round(ontology_score, 2),
                        "unit": "score",
                    },
                ]
                if ontology_scores_available
                else []
            ),
        },
        {
            "key": "ontology_usecase",
            "label": "ユースケース解決力",
            "score": onto_score_usecase,
            "issues": unresolved_candidates,
            "target": 90,
            "sub_metrics": (
                [
                    {
                        "key": "use_case_readiness",
                        "label": "ユースケース解決力",
                        "value": round(ontology_usecase, 2),
                        "unit": "score",
                    },
                    {
                        "key": "unresolved_candidates",
                        "label": "未解決候補",
                        "value": float(unresolved_candidates),
                        "unit": "count",
                    },
                    {
                        "key": "high_spread_entities",
                        "label": "高分散エンティティ",
                        "value": float(high_spread_entities),
                        "unit": "count",
                    },
                ]
                if ontology_scores_available
                else []
            ),
        },
    ]
    all_scores_ready = all(signal.get("score") is not None for signal in signals)
    readiness = (
        round(sum(float(s["score"]) for s in signals) / max(1, len(signals)), 2)
        if all_scores_ready
        else None
    )
    payload = {
        "readiness_score": readiness,
        "target_score": 90,
        "signals": signals,
    }
    if readiness is not None:
        try:
            _record_readiness_snapshot(
                tenant_id,
                target_score=int(payload["target_score"]),
                readiness_score=float(payload["readiness_score"]),
                signals=signals,
            )
        except Exception:
            # Snapshot persistence failure must not break dashboard rendering.
            pass
    return payload


def _clamp_score(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


def _signal_score(signals: list[dict[str, Any]], key: str) -> float:
    row = next((signal for signal in signals if str(signal.get("key", "")) == key), None)
    if row is None:
        return 0.0
    raw = row.get("score")
    if raw is None:
        return 0.0
    return _clamp_score(float(raw))


def get_dashboard_readiness_trend(tenant_id: str) -> dict[str, Any]:
    try:
        snapshots = _dashboard_readiness_snapshot_repository.list_recent(tenant_id, limit=12)
    except Exception:
        snapshots = []
    if len(snapshots) >= 2:
        selected = sorted(snapshots, key=lambda row: row.captured_at)[-5:]
        labels = build_trend_labels(len(selected))
        rows: list[dict[str, Any]] = []
        for index, snapshot in enumerate(selected):
            rows.append(
                {
                    "label": labels[index],
                    "governance_oversharing": _clamp_score(snapshot.governance_oversharing),
                    "governance_sensitive": _clamp_score(snapshot.governance_sensitive),
                    "governance_assurance": _clamp_score(snapshot.governance_assurance),
                    "ontology_foundation": _clamp_score(snapshot.ontology_foundation),
                    "ontology_usecase": _clamp_score(snapshot.ontology_usecase),
                }
            )
        latest_snapshot = selected[-1]
        return {
            "rows": rows,
            "target_score": int(latest_snapshot.target_score),
            "source": "snapshot",
        }
    readiness = get_dashboard_readiness(tenant_id=tenant_id)
    if readiness.get("readiness_score") is None:
        return {
            "rows": [],
            "target_score": int(readiness.get("target_score", 90)),
            "source": "insufficient_data",
        }
    signals = readiness.get("signals", [])
    (
        governance_oversharing,
        governance_sensitive,
        governance_assurance,
        ontology_foundation,
        ontology_usecase,
    ) = _extract_signal_scores(signals)
    return {
        "rows": default_estimated_series(
            governance_oversharing=governance_oversharing,
            governance_sensitive=governance_sensitive,
            governance_assurance=governance_assurance,
            ontology_foundation=ontology_foundation,
            ontology_usecase=ontology_usecase,
        ),
        "target_score": int(readiness.get("target_score", 90)),
        "source": "estimated",
    }


def list_dashboard_recommended_actions(tenant_id: str) -> dict[str, Any]:
    governance = get_governance_overview(tenant_id=tenant_id)
    ontology = get_ontology_overview(tenant_id=tenant_id)
    return {
        "rows": [
            {
                "priority": "P1",
                "domain": "Governance",
                "summary": f"高リスク検知が {governance.get('high_risk_count', 0)} 件あります。",
            },
            {
                "priority": "P2",
                "domain": "Ontology",
                "summary": f"未解決候補が {ontology.get('unresolved_candidates', 0)} 件あります。",
            },
        ]
    }


def list_audit_records(
    tenant_id: str,
    *,
    domain: str = "all",
    q: str = "",
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    normalized_domain = domain.lower().strip()
    include_connect = normalized_domain in {"all", "connect"}
    include_governance = normalized_domain in {"all", "governance"}
    include_ontology = normalized_domain in {"all", "ontology"}

    fetch_limit = min(500, max(int(limit) + int(offset) + 100, 200))
    common_rows = _common_audit_repository.list_recent(
        tenant_id=tenant_id,
        domain=normalized_domain if normalized_domain in {"connect", "governance", "ontology"} else "all",
        limit=fetch_limit,
        offset=0,
    )
    if common_rows:
        rows.extend(
            {
                "domain": str(item.domain),
                "audit_id": str(item.audit_id),
                "occurred_at": str(item.occurred_at),
                "operator": str(item.actor),
                "action": str(item.action),
                "target": str(item.target),
                "correlation_id": str(item.correlation_id),
            }
            for item in common_rows
            if str(item.domain) in {"connect", "governance", "ontology"}
        )

    # Keep legacy data sources as fallback/complement for historical rows.
    if include_connect:
        connect = list_connect_audit(tenant_id=tenant_id, query=None, limit=fetch_limit, offset=0).get(
            "rows", []
        )
        rows.extend(
            {
                "domain": "connect",
                "audit_id": str(item.get("id", "")),
                "occurred_at": str(item.get("operated_at", "")),
                "operator": str(item.get("operator", "system")),
                "action": str(item.get("action", "")),
                "target": f"{item.get('target_type', '')}:{item.get('target_id', '')}".strip(":"),
                "correlation_id": str(item.get("correlation_id", "")),
            }
            for item in connect
        )

    if include_governance:
        governance = list_governance_audit_logs(tenant_id=tenant_id, limit=fetch_limit, offset=0).get("rows", [])
        rows.extend(
            {
                "domain": "governance",
                "audit_id": str(item.get("id", "")),
                "occurred_at": str(item.get("timestamp", "")),
                "operator": str(item.get("operator", "system")),
                "action": str(item.get("event", "")),
                "target": str(item.get("target", item.get("message", ""))),
                "correlation_id": str(item.get("correlation_id", "")),
            }
            for item in governance
        )

    if include_ontology:
        ontology = list_ontology_audit_logs(tenant_id=tenant_id, limit=fetch_limit, offset=0).get("rows", [])
        rows.extend(
            {
                "domain": "ontology",
                "audit_id": str(item.get("audit_id", item.get("lineage_id", ""))),
                "occurred_at": str(item.get("timestamp", "")),
                "operator": str(item.get("operator", "system")),
                "action": str(item.get("event", item.get("status", ""))),
                "target": str(item.get("job_name", item.get("source", ""))),
                "correlation_id": str(item.get("correlation_id", "")),
            }
            for item in ontology
        )

    keyword = q.strip().lower()
    if keyword:
        rows = [
            row
            for row in rows
            if keyword in " ".join(
                [
                    row.get("audit_id", ""),
                    row.get("operator", ""),
                    row.get("action", ""),
                    row.get("target", ""),
                    row.get("correlation_id", ""),
                    row.get("occurred_at", ""),
                ]
            ).lower()
        ]

    if not rows:
        rows = _build_snapshot_audit_rows(
            tenant_id=tenant_id,
            include_connect=include_connect,
            include_governance=include_governance,
            include_ontology=include_ontology,
        )
        if keyword:
            rows = [
                row
                for row in rows
                if keyword in " ".join(
                    [
                        row.get("audit_id", ""),
                        row.get("operator", ""),
                        row.get("action", ""),
                        row.get("target", ""),
                        row.get("correlation_id", ""),
                        row.get("occurred_at", ""),
                    ]
                ).lower()
            ]

    rows.sort(key=lambda item: str(item.get("occurred_at", "")), reverse=True)
    bounded_limit = max(1, min(int(limit), 500))
    bounded_offset = max(0, int(offset))
    return {
        "rows": rows[bounded_offset : bounded_offset + bounded_limit],
        "pagination": {
            "limit": bounded_limit,
            "offset": bounded_offset,
            "total_count": len(rows),
        },
    }


def _build_snapshot_audit_rows(
    *,
    tenant_id: str,
    include_connect: bool,
    include_governance: bool,
    include_ontology: bool,
) -> list[dict[str, Any]]:
    snapshot_at = _now_iso()
    correlation_id = f"snapshot-{tenant_id}-{snapshot_at[:13].replace(':', '')}"
    rows: list[dict[str, Any]] = []
    if include_connect:
        rows.append(
            {
                "domain": "connect",
                "audit_id": f"snapshot-connect-{tenant_id}",
                "occurred_at": snapshot_at,
                "operator": "system",
                "action": "overview.snapshot",
                "target": "subscriptions:snapshot",
                "correlation_id": correlation_id,
            }
        )
    if include_governance:
        overview = get_governance_overview(tenant_id=tenant_id)
        counts = overview.get("counts", {})
        rows.append(
            {
                "domain": "governance",
                "audit_id": f"snapshot-governance-{tenant_id}",
                "occurred_at": snapshot_at,
                "operator": "system",
                "action": "overview.snapshot",
                "target": f"findings:{int(counts.get('total_findings', 0))}",
                "correlation_id": correlation_id,
            }
        )
    if include_ontology:
        overview = get_ontology_overview(tenant_id=tenant_id)
        rows.append(
            {
                "domain": "ontology",
                "audit_id": f"snapshot-ontology-{tenant_id}",
                "occurred_at": snapshot_at,
                "operator": "system",
                "action": "overview.snapshot",
                "target": f"high_spread:{int(overview.get('high_spread_entities', 0))}",
                "correlation_id": correlation_id,
            }
        )
    return rows


def create_audit_export_job(
    tenant_id: str,
    *,
    domain: str,
    q: str,
    export_format: str,
) -> dict[str, Any]:
    records = list_audit_records(
        tenant_id=tenant_id,
        domain=domain,
        q=q,
        limit=1000,
        offset=0,
    ).get("rows", [])
    job_id = f"audit-export-{uuid.uuid4().hex[:12]}"
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat()
    expires_at_dt = datetime.fromisoformat(expires_at)

    if export_format == "pdf":
        lines = [
            "Audit Export",
            f"tenant_id={tenant_id}",
            f"domain={domain}",
            f"query={q}",
            "",
        ]
        for row in records:
            lines.append(
                " | ".join(
                    [
                        str(row.get("occurred_at", "")),
                        str(row.get("domain", "")),
                        str(row.get("operator", "")),
                        str(row.get("action", "")),
                        str(row.get("target", "")),
                        str(row.get("correlation_id", "")),
                    ]
                )
            )
        payload_bytes = "\n".join(lines).encode("utf-8")
        content_type = "text/plain; charset=utf-8"
        extension = "txt"
    else:
        stream = io.StringIO()
        writer = csv.writer(stream)
        writer.writerow(["occurred_at", "domain", "operator", "action", "target", "correlation_id", "audit_id"])
        for row in records:
            writer.writerow(
                [
                    row.get("occurred_at", ""),
                    row.get("domain", ""),
                    row.get("operator", ""),
                    row.get("action", ""),
                    row.get("target", ""),
                    row.get("correlation_id", ""),
                    row.get("audit_id", ""),
                ]
            )
        payload_bytes = stream.getvalue().encode("utf-8")
        content_type = "text/csv; charset=utf-8"
        extension = "csv"

    download_url: str | None = None
    key: str | None = None
    bucket = (os.getenv("AUDIT_EXPORT_BUCKET") or "").strip()
    if bucket:
        key = f"exports/{tenant_id}/{job_id}.{extension}"
        _s3.put_object(Bucket=bucket, Key=key, Body=payload_bytes, ContentType=content_type)
    token, token_hash = _issue_download_token(
        tenant_id=tenant_id,
        job_id=job_id,
        expires_at=expires_at_dt,
    )
    download_url = f"/audit/exports/{job_id}/download?token={token}"
    _audit_export_jobs[job_id] = {
        "job_id": job_id,
        "tenant_id": tenant_id,
        "status": "completed" if bucket else "failed",
        "format": "pdf" if export_format == "pdf" else "csv",
        "download_url": download_url,
        "bucket": bucket,
        "key": key,
        "content_type": content_type,
        "file_name": f"{job_id}.{extension}",
        "token_hash": token_hash,
        "token_used": False,
        "payload_bytes": payload_bytes if not bucket else None,
        "created_at": _now_iso(),
        "expires_at": expires_at,
        "error_message": None if bucket else "AUDIT_EXPORT_BUCKET is not configured.",
    }
    return {"job_id": job_id, "status": "accepted"}


def get_audit_export_job_status(*, tenant_id: str, job_id: str) -> dict[str, Any]:
    job = _audit_export_jobs.get(job_id)
    if not job or str(job.get("tenant_id", "")) != tenant_id:
        return {"job_id": job_id, "status": "failed", "format": "csv", "download_url": None, "error_message": "Job not found."}
    return dict(job)


def get_audit_export_download(
    *,
    tenant_id: str,
    job_id: str,
    token: str,
) -> dict[str, Any]:
    job = _audit_export_jobs.get(job_id)
    if not job or str(job.get("tenant_id", "")) != tenant_id:
        raise ValueError("Job not found.")
    ok, reason = _validate_download_token(token=token, tenant_id=tenant_id, job_id=job_id)
    if not ok:
        raise ValueError(reason)
    incoming_hash = hashlib.sha256(str(token).encode("utf-8")).hexdigest()
    if incoming_hash != str(job.get("token_hash", "")):
        raise ValueError("Token mismatch.")
    if bool(job.get("token_used", False)):
        raise ValueError("Token already used.")
    expires_at = str(job.get("expires_at", ""))
    if expires_at:
        try:
            expiry = datetime.fromisoformat(expires_at)
            if datetime.now(timezone.utc) >= expiry:
                raise ValueError("Token expired.")
        except ValueError:
            raise
        except Exception as exc:
            raise ValueError("Invalid token expiry.") from exc
    bucket = str(job.get("bucket", "")).strip()
    key = str(job.get("key", "")).strip()
    payload_bytes: bytes
    if bucket and key:
        response = _s3.get_object(Bucket=bucket, Key=key)
        payload_bytes = bytes(response["Body"].read())
    else:
        inline_payload = job.get("payload_bytes")
        if inline_payload is None:
            raise ValueError("Export payload is unavailable.")
        payload_bytes = bytes(inline_payload)
    job["token_used"] = True
    return {
        "payload_bytes": payload_bytes,
        "content_type": str(job.get("content_type", "application/octet-stream")),
        "file_name": str(job.get("file_name", f"{job_id}.txt")),
    }
