"""Repository for persisted dashboard readiness snapshots."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from services.runtime_config import load_aws_runtime_config
from services.tenant_db_resolver import TenantDbResolver

_runtime_config = load_aws_runtime_config()
_tenant_db_resolver = TenantDbResolver(_runtime_config)


def _connect_for_tenant(tenant_id: str) -> sqlite3.Connection:
    binding = _tenant_db_resolver.resolve(tenant_id)
    conn = sqlite3.connect(binding.ontology_sqlite_path)
    conn.row_factory = sqlite3.Row
    return conn


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _slot_5m(now: datetime) -> datetime:
    minute_floor = (now.minute // 5) * 5
    return now.replace(minute=minute_floor, second=0, microsecond=0)


@dataclass(frozen=True)
class DashboardReadinessSnapshot:
    captured_at: str
    target_score: int
    readiness_score: float
    governance_oversharing: float
    governance_sensitive: float
    governance_assurance: float
    ontology_foundation: float
    ontology_usecase: float


class DashboardReadinessSnapshotRepository:
    """Stores tenant-scoped readiness snapshots for trend visualization."""

    def _ensure_table(self, tenant_id: str) -> None:
        with _connect_for_tenant(tenant_id) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_readiness_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slot_5m TEXT NOT NULL UNIQUE,
                    captured_at TEXT NOT NULL,
                    target_score INTEGER NOT NULL,
                    readiness_score REAL NOT NULL,
                    oversharing REAL NOT NULL,
                    sensitive REAL NOT NULL,
                    freshness REAL NOT NULL,
                    duplication REAL NOT NULL,
                    location REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_dashboard_readiness_snapshots_captured_at
                ON dashboard_readiness_snapshots(captured_at DESC)
                """
            )
            conn.commit()

    def upsert_snapshot(
        self,
        tenant_id: str,
        *,
        target_score: int,
        readiness_score: float,
        governance_oversharing: float,
        governance_sensitive: float,
        governance_assurance: float,
        ontology_foundation: float,
        ontology_usecase: float,
    ) -> None:
        self._ensure_table(tenant_id)
        now = _now_utc()
        slot = _slot_5m(now).isoformat()
        captured_at = now.isoformat()
        with _connect_for_tenant(tenant_id) as conn:
            conn.execute(
                """
                INSERT INTO dashboard_readiness_snapshots (
                    slot_5m,
                    captured_at,
                    target_score,
                    readiness_score,
                    oversharing,
                    sensitive,
                    freshness,
                    duplication,
                    location
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(slot_5m) DO UPDATE SET
                    captured_at = excluded.captured_at,
                    target_score = excluded.target_score,
                    readiness_score = excluded.readiness_score,
                    oversharing = excluded.oversharing,
                    sensitive = excluded.sensitive,
                    freshness = excluded.freshness,
                    duplication = excluded.duplication,
                    location = excluded.location
                """,
                (
                    slot,
                    captured_at,
                    int(target_score),
                    float(readiness_score),
                    float(governance_oversharing),
                    float(governance_sensitive),
                    float(governance_assurance),
                    float(ontology_foundation),
                    float(ontology_usecase),
                ),
            )
            conn.commit()

    def list_recent(
        self,
        tenant_id: str,
        *,
        limit: int = 12,
    ) -> list[DashboardReadinessSnapshot]:
        self._ensure_table(tenant_id)
        bounded_limit = max(1, min(int(limit), 1440))
        with _connect_for_tenant(tenant_id) as conn:
            rows = conn.execute(
                """
                SELECT captured_at, target_score, readiness_score, oversharing, sensitive, freshness, duplication, location
                FROM dashboard_readiness_snapshots
                ORDER BY captured_at DESC
                LIMIT ?
                """,
                (bounded_limit,),
            ).fetchall()
        return [
            DashboardReadinessSnapshot(
                captured_at=str(row["captured_at"]),
                target_score=int(row["target_score"]),
                readiness_score=float(row["readiness_score"]),
                governance_oversharing=float(row["oversharing"]),
                governance_sensitive=float(row["sensitive"]),
                governance_assurance=float(row["freshness"]),
                ontology_foundation=float(row["duplication"]),
                ontology_usecase=float(row["location"]),
            )
            for row in rows
        ]


def build_trend_labels(count: int) -> list[str]:
    if count <= 0:
        return []
    if count == 1:
        return ["Now"]
    labels = [f"T-{idx}" for idx in range(count - 1, 0, -1)]
    labels.append("Now")
    return labels


def default_estimated_series(
    *,
    governance_oversharing: float,
    governance_sensitive: float,
    governance_assurance: float,
    ontology_foundation: float,
    ontology_usecase: float,
) -> list[dict[str, float | str]]:
    offsets = [8, 6, 4, 2, 0]
    labels = ["T-4", "T-3", "T-2", "T-1", "Now"]
    rows: list[dict[str, float | str]] = []
    for index, offset in enumerate(offsets):
        rows.append(
            {
                "label": labels[index],
                "governance_oversharing": max(0.0, governance_oversharing - offset),
                "governance_sensitive": max(0.0, governance_sensitive - offset),
                "governance_assurance": max(0.0, governance_assurance - offset),
                "ontology_foundation": max(0.0, ontology_foundation - offset),
                "ontology_usecase": max(0.0, ontology_usecase - offset),
            }
        )
    return rows

