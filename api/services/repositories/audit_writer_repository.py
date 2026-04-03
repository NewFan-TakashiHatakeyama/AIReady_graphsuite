"""Common append-only audit repository across domains."""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from services.runtime_config import load_aws_runtime_config
from services.tenant_db_resolver import TenantDbResolver

_runtime_config = load_aws_runtime_config()
_tenant_db_resolver = TenantDbResolver(_runtime_config)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect_for_tenant(tenant_id: str) -> sqlite3.Connection:
    binding = _tenant_db_resolver.resolve(tenant_id)
    conn = sqlite3.connect(binding.ontology_sqlite_path)
    conn.row_factory = sqlite3.Row
    return conn


@dataclass(frozen=True)
class CommonAuditRecord:
    audit_id: str
    tenant_id: str
    domain: str
    actor: str
    action: str
    target: str
    correlation_id: str
    occurred_at: str
    metadata_json: str

    def metadata(self) -> dict[str, Any]:
        try:
            parsed = json.loads(self.metadata_json or "{}")
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
        return {}


class CommonAuditRepository:
    """Stores common audit records for connect/governance/ontology."""

    def _ensure_table(self, tenant_id: str) -> None:
        with _connect_for_tenant(tenant_id) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS common_audit_records (
                    audit_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target TEXT NOT NULL,
                    correlation_id TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_common_audit_tenant_time
                ON common_audit_records(tenant_id, occurred_at DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_common_audit_tenant_domain_time
                ON common_audit_records(tenant_id, domain, occurred_at DESC)
                """
            )
            conn.commit()

    def append(
        self,
        tenant_id: str,
        *,
        domain: str,
        actor: str,
        action: str,
        target: str,
        correlation_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        normalized_tenant_id = str(tenant_id or "").strip()
        if not normalized_tenant_id:
            return
        self._ensure_table(normalized_tenant_id)
        with _connect_for_tenant(normalized_tenant_id) as conn:
            conn.execute(
                """
                INSERT INTO common_audit_records (
                    audit_id, tenant_id, domain, actor, action, target,
                    correlation_id, occurred_at, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"audit-{uuid.uuid4().hex[:16]}",
                    normalized_tenant_id,
                    str(domain or "unknown"),
                    str(actor or "system"),
                    str(action or ""),
                    str(target or ""),
                    str(correlation_id or ""),
                    _now_iso(),
                    json.dumps(metadata or {}, ensure_ascii=True),
                ),
            )
            conn.commit()

    def list_recent(
        self,
        tenant_id: str,
        *,
        domain: str = "all",
        limit: int = 500,
        offset: int = 0,
    ) -> list[CommonAuditRecord]:
        normalized_tenant_id = str(tenant_id or "").strip()
        if not normalized_tenant_id:
            return []
        self._ensure_table(normalized_tenant_id)
        bounded_limit = max(1, min(int(limit), 1000))
        bounded_offset = max(0, int(offset))
        normalized_domain = str(domain or "all").strip().lower()
        query = """
            SELECT audit_id, tenant_id, domain, actor, action, target, correlation_id, occurred_at, metadata_json
            FROM common_audit_records
            WHERE tenant_id = ?
        """
        params: list[Any] = [normalized_tenant_id]
        if normalized_domain and normalized_domain != "all":
            query += " AND domain = ?"
            params.append(normalized_domain)
        query += " ORDER BY occurred_at DESC LIMIT ? OFFSET ?"
        params.extend([bounded_limit, bounded_offset])
        with _connect_for_tenant(normalized_tenant_id) as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [
            CommonAuditRecord(
                audit_id=str(row["audit_id"]),
                tenant_id=str(row["tenant_id"]),
                domain=str(row["domain"]),
                actor=str(row["actor"]),
                action=str(row["action"]),
                target=str(row["target"]),
                correlation_id=str(row["correlation_id"]),
                occurred_at=str(row["occurred_at"]),
                metadata_json=str(row["metadata_json"] or "{}"),
            )
            for row in rows
        ]
