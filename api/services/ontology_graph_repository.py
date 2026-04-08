"""
Ontology graph repository backed by a dedicated SQLite database.

This storage is intentionally separated from LightRAG storages to provide
an ontology-focused graph optimized for visual exploration.
"""

from __future__ import annotations

import os
import re
import json
import hashlib
import math
import sqlite3
from datetime import datetime, timezone
from contextlib import contextmanager
from contextvars import ContextVar
from collections import deque
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from .governance_repository import list_governance_findings
from services.aws_clients import get_dynamodb_resource, get_lambda_client
from services.runtime_config import load_aws_runtime_config, load_tenant_registry
from services.tenant_db_resolver import TenantDbResolver


DEFAULT_DB_FILE = "ontology_graph.db"

import logging as _logging

_graph_logger = _logging.getLogger(__name__)


def _enrich_document_nodes_from_dynamodb(
    nodes_by_id: Dict[str, "OntologyNode"],
    tenant_id: str,
) -> None:
    """Overwrite document-node profile properties with live DynamoDB data."""
    try:
        resolver = TenantDbResolver(load_aws_runtime_config())
        binding = resolver.resolve(tenant_id)
        table_name = binding.ontology_unified_metadata_table_name
        if not table_name:
            return
        cfg = load_aws_runtime_config()
        dynamodb = get_dynamodb_resource(cfg)
        table = dynamodb.Table(table_name)
        from boto3.dynamodb.conditions import Key as DDBKey
        response = table.query(KeyConditionExpression=DDBKey("tenant_id").eq(tenant_id))
        rows = response.get("Items", [])
        while response.get("LastEvaluatedKey"):
            response = table.query(
                KeyConditionExpression=DDBKey("tenant_id").eq(tenant_id),
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            rows.extend(response.get("Items", []))

        profile_by_item_id: Dict[str, dict] = {}
        for row in rows:
            item_id = str(row.get("item_id", "")).strip()
            if not item_id or row.get("is_deleted"):
                continue
            ext = row.get("extensions") or {}
            if isinstance(ext, str):
                try:
                    ext = json.loads(ext)
                except Exception:
                    ext = {}
            dp = ext.get("document_profile") or {}
            if isinstance(dp, str):
                try:
                    dp = json.loads(dp)
                except Exception:
                    dp = {}
            owner = str(row.get("owner") or dp.get("owner") or "").strip()
            project = str(row.get("project") or dp.get("project") or "").strip()
            canonical = str(row.get("canonical_doc_id") or dp.get("canonical_doc_id") or "").strip()
            topics_raw = row.get("topic_categories") or dp.get("topic_categories") or []
            if not isinstance(topics_raw, list):
                topics_raw = []
            topics = [str(v).strip() for v in topics_raw if str(v).strip()]
            cqs = row.get("content_quality_score")
            profile_by_item_id[item_id] = {
                "owner": owner,
                "project": project,
                "canonical_doc_id": canonical,
                "topic_categories": topics,
                "content_quality_score": cqs,
            }

        for node in nodes_by_id.values():
            if node.properties.get("entity_type") != "document":
                continue
            node_item_id = str(node.properties.get("item_id", "")).strip()
            profile = profile_by_item_id.get(node_item_id)
            if not profile:
                continue
            if profile["owner"]:
                node.properties["owner"] = profile["owner"]
                node.properties["creator_name"] = profile["owner"]
            if profile["project"]:
                node.properties["project"] = profile["project"]
                node.properties["project_name"] = profile["project"]
            if profile["canonical_doc_id"]:
                node.properties["canonical_doc_id"] = profile["canonical_doc_id"]
            if profile["topic_categories"]:
                node.properties["topic_categories"] = profile["topic_categories"]
            if profile["content_quality_score"] is not None:
                node.properties["contentQualityScore"] = profile["content_quality_score"]
    except Exception as exc:
        _graph_logger.warning("DynamoDB profile enrichment skipped: %s", exc)
_runtime_config = load_aws_runtime_config()
_tenant_db_resolver = TenantDbResolver(_runtime_config)
_current_tenant_id: ContextVar[str | None] = ContextVar(
    "ontology_repository_tenant_id", default=None
)
_dynamodb_resource = None


def _get_env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _get_env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, str(default))).strip()
    try:
        return float(raw)
    except ValueError:
        return default


def _get_env_int(name: str, default: int, *, minimum: int = 1, maximum: int = 10000) -> int:
    raw = str(os.getenv(name, str(default))).strip()
    try:
        parsed = int(raw)
    except ValueError:
        parsed = default
    return max(minimum, min(maximum, parsed))


@dataclass
class OntologyNode:
    id: str
    labels: List[str]
    properties: Dict[str, object]


@dataclass
class OntologyEdge:
    id: str
    source: str
    target: str
    type: str
    properties: Dict[str, object]


def _require_tenant_id(tenant_id: str | None = None) -> str:
    resolved_tenant_id = tenant_id or _current_tenant_id.get()
    normalized_tenant_id = str(resolved_tenant_id or "").strip()
    if not normalized_tenant_id:
        raise ValueError("tenant_id is required for ontology repository access.")
    return normalized_tenant_id


def _enforce_ontology_tenant_registry(tenant_id: str) -> None:
    if str(os.getenv("ONTOLOGY_REQUIRE_TENANT_REGISTRY", "false")).strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return
    registry = load_tenant_registry()
    if tenant_id not in registry:
        raise ValueError(
            "Tenant is not registered for Ontology access. "
            f"tenant_id={tenant_id}"
        )


@contextmanager
def _tenant_scope(tenant_id: str):
    normalized_tenant_id = _require_tenant_id(tenant_id)
    _current_tenant_id.set(normalized_tenant_id)
    yield normalized_tenant_id


def _db_path() -> str:
    tenant_id = _require_tenant_id()
    _enforce_ontology_tenant_registry(tenant_id)
    binding = _tenant_db_resolver.resolve(tenant_id)
    override_path = os.getenv("ONTOLOGY_GRAPH_DB_PATH", "").strip()
    if override_path:
        configured = Path(override_path).resolve()
        return str(configured)
    return str(Path(binding.ontology_sqlite_path).resolve())


def get_resolved_ontology_graph_db_path(tenant_id: str) -> str:
    """Resolve absolute path to ontology graph SQLite for this tenant (ops / logging).

    Graph projection for GET /graphs is read from this file; it is separate from
    DynamoDB UnifiedMetadata. Multi-process deployments need a shared path or
    externalized projection store.
    """
    with _tenant_scope(tenant_id):
        return _db_path()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _get_dynamodb_resource():
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = get_dynamodb_resource(_runtime_config)
    return _dynamodb_resource


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {}
    return {}


def _parse_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except json.JSONDecodeError:
            return [value.strip()]
    return []


def _infer_shadow_owner(title: str, summary: str) -> tuple[str, float]:
    lowered = f"{title} {summary}".lower()
    if any(token in lowered for token in ("owner", "担当", "責任者", "author")):
        return "document-owner", 0.72
    return "unknown", 0.38


def _infer_shadow_project(title: str, summary: str) -> tuple[str, float]:
    tokens = [token.strip() for token in re.split(r"[\s_\-/]+", title) if token.strip()]
    for token in tokens:
        lowered = token.lower()
        if any(marker in lowered for marker in ("project", "proj", "案件", "プロジェクト")):
            return token, 0.76
    if "project" in summary.lower() or "プロジェクト" in summary:
        return "project-derived", 0.64
    return "general", 0.35


def _infer_shadow_topics(title: str, summary: str) -> tuple[list[str], float]:
    combined = f"{title} {summary}".lower()
    candidates: list[str] = []
    if any(token in combined for token in ("security", "incident", "脆弱", "セキュリティ")):
        candidates.append("security")
    if any(token in combined for token in ("budget", "finance", "会計", "予算")):
        candidates.append("finance")
    if any(token in combined for token in ("contract", "legal", "法務", "契約")):
        candidates.append("legal")
    if any(token in combined for token in ("architecture", "api", "design", "設計", "開発")):
        candidates.append("engineering")
    if not candidates:
        return ["general"], 0.33
    return candidates, min(0.82, 0.56 + 0.08 * len(candidates))


def _build_shadow_prediction_payload(
    *,
    item_id: str,
    title: str,
    summary: str,
    canonical_doc_id: str,
) -> dict[str, Any]:
    owner_predicted, owner_confidence = _infer_shadow_owner(title, summary)
    project_predicted, project_confidence = _infer_shadow_project(title, summary)
    topic_predicted, topic_confidence = _infer_shadow_topics(title, summary)
    canonical_predicted = canonical_doc_id or item_id
    return {
        "owner_predicted": owner_predicted,
        "owner_confidence": round(owner_confidence, 2),
        "project_predicted": project_predicted,
        "project_confidence": round(project_confidence, 2),
        "topic_predicted": topic_predicted,
        "topic_confidence": round(topic_confidence, 2),
        "canonical_doc_predicted": canonical_predicted,
        "canonical_doc_confidence": 0.8 if canonical_doc_id else 0.48,
        "prediction_source": "llm_shadow_v1",
        "review_status": "pending_review",
    }


def _infer_project_from_unified(item: dict[str, Any]) -> str:
    source_identifiers = _parse_json_object(item.get("source_identifiers"))
    for key in ("project_id", "project", "site_id", "site_name", "drive_id"):
        value = str(source_identifiers.get(key) or "").strip()
        if value:
            return value
    path = str(item.get("hierarchy_path") or "").replace("\\", "/").strip("/")
    segments = [segment.strip() for segment in path.split("/") if segment.strip()]
    if not segments:
        return "general"
    for segment in segments:
        lowered = segment.lower()
        if any(token in lowered for token in ("project", "proj", "案件", "プロジェクト")):
            return segment
    return segments[0]


def _map_topic_categories_from_keywords(topic_keywords: list[str]) -> list[str]:
    lowered_keywords = [keyword.lower() for keyword in topic_keywords]
    category_map = {
        "security": {"security", "auth", "iam", "脆弱", "セキュリティ", "認証"},
        "finance": {"finance", "budget", "cost", "会計", "予算", "経費"},
        "legal": {"legal", "contract", "compliance", "法務", "契約", "規約"},
        "hr": {"hr", "hiring", "payroll", "人事", "採用", "給与"},
        "engineering": {"engineering", "architecture", "api", "技術", "設計", "開発"},
        "operations": {"operations", "runbook", "monitoring", "運用", "手順書", "監視"},
    }
    categories: list[str] = []
    for category, words in category_map.items():
        if any(any(word in keyword for word in words) for keyword in lowered_keywords):
            categories.append(category)
    if not categories and lowered_keywords:
        categories.append("general")
    return categories


def _sync_unified_after_plan_state_change(
    *,
    tenant_id: str,
    item_id: str,
    remediation_state: str,
    approved_profile: Optional[Dict[str, Any]] = None,
) -> None:
    """Delegate UnifiedMetadata sync to ontology profileUpdate Lambda."""
    normalized_item_id = str(item_id or "").strip()
    if not normalized_item_id:
        return
    function_name = (
        os.getenv("ONTOLOGY_PROFILE_UPDATE_LAMBDA_NAME")
        or "AIReadyOntology-profileUpdate"
    ).strip()
    payload = {
        "action": "plan_state_sync",
        "tenant_id": tenant_id,
        "item_id": normalized_item_id,
        "remediation_state": remediation_state,
        "approved_profile": approved_profile or {},
    }
    lambda_client = get_lambda_client(load_aws_runtime_config())
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload, ensure_ascii=True).encode("utf-8"),
    )
    status_code = int(response.get("StatusCode", 500))
    if status_code < 200 or status_code >= 300:
        raise RuntimeError(f"ontology profileUpdate lambda invoke failed: status={status_code}")


def initialize_ontology_graph_db() -> None:
    Path(_db_path()).parent.mkdir(parents=True, exist_ok=True)
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_documents (
                doc_id TEXT PRIMARY KEY,
                item_id TEXT,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                source_system TEXT NOT NULL,
                content_quality_score REAL NOT NULL,
                freshness_status TEXT NOT NULL,
                creator_person_id TEXT,
                primary_project_id TEXT,
                primary_org_id TEXT,
                canonical_doc_id TEXT,
                document_kind TEXT NOT NULL DEFAULT 'derived',
                lineage_id TEXT NOT NULL,
                correlation_id TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_entities (
                entity_id TEXT PRIMARY KEY,
                canonical_name TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                resolution_status TEXT NOT NULL,
                confidence REAL NOT NULL,
                pii_flag INTEGER NOT NULL,
                extraction_source TEXT NOT NULL,
                spread_factor INTEGER NOT NULL,
                lineage_id TEXT NOT NULL,
                correlation_id TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_edges (
                edge_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                weight REAL NOT NULL,
                evidence TEXT NOT NULL,
                FOREIGN KEY(source_id) REFERENCES ontology_entities(entity_id),
                FOREIGN KEY(target_id) REFERENCES ontology_entities(entity_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_document_entity_links (
                link_id TEXT PRIMARY KEY,
                doc_id TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                weight REAL NOT NULL,
                evidence TEXT NOT NULL,
                signal_source TEXT NOT NULL DEFAULT 'projection',
                FOREIGN KEY(doc_id) REFERENCES ontology_documents(doc_id),
                FOREIGN KEY(entity_id) REFERENCES ontology_entities(entity_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_document_lineage_links (
                link_id TEXT PRIMARY KEY,
                source_doc_id TEXT NOT NULL,
                target_doc_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                weight REAL NOT NULL,
                evidence TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_folder_nodes (
                folder_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                folder_name TEXT NOT NULL,
                folder_path TEXT NOT NULL UNIQUE,
                depth INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_document_folder_links (
                link_id TEXT PRIMARY KEY,
                doc_id TEXT NOT NULL,
                folder_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                weight REAL NOT NULL,
                evidence TEXT NOT NULL,
                FOREIGN KEY(doc_id) REFERENCES ontology_documents(doc_id),
                FOREIGN KEY(folder_id) REFERENCES ontology_folder_nodes(folder_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_folder_hierarchy_links (
                link_id TEXT PRIMARY KEY,
                parent_folder_id TEXT NOT NULL,
                child_folder_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                weight REAL NOT NULL,
                evidence TEXT NOT NULL,
                FOREIGN KEY(parent_folder_id) REFERENCES ontology_folder_nodes(folder_id),
                FOREIGN KEY(child_folder_id) REFERENCES ontology_folder_nodes(folder_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_document_similarity_links (
                link_id TEXT PRIMARY KEY,
                source_doc_id TEXT NOT NULL,
                target_doc_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                similarity_score REAL NOT NULL,
                evidence TEXT NOT NULL,
                algorithm TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_entity_candidates (
                candidate_id TEXT PRIMARY KEY,
                surface_form TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                extraction_source TEXT NOT NULL,
                confidence REAL NOT NULL,
                pii_flag INTEGER NOT NULL,
                item_id TEXT NOT NULL,
                received_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                resolved_entity_id TEXT,
                resolution_type TEXT,
                resolved_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_entity_surface_forms (
                surface_id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL,
                surface_form TEXT NOT NULL,
                source_item_id TEXT NOT NULL,
                source_candidate_id TEXT NOT NULL,
                resolution_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(entity_id, surface_form, source_item_id, source_candidate_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_plan_quality_state (
                plan_id TEXT PRIMARY KEY,
                item_id TEXT NOT NULL UNIQUE,
                remediation_state TEXT NOT NULL DEFAULT 'ai_proposed',
                dry_run_status TEXT NOT NULL DEFAULT 'not_run',
                owner_predicted TEXT NOT NULL DEFAULT 'unknown',
                owner_confidence REAL NOT NULL DEFAULT 0.0,
                project_predicted TEXT NOT NULL DEFAULT 'general',
                project_confidence REAL NOT NULL DEFAULT 0.0,
                topic_predicted TEXT NOT NULL DEFAULT '[]',
                topic_confidence REAL NOT NULL DEFAULT 0.0,
                canonical_doc_predicted TEXT NOT NULL DEFAULT '',
                canonical_doc_confidence REAL NOT NULL DEFAULT 0.0,
                prediction_source TEXT NOT NULL DEFAULT 'llm_shadow_v1',
                review_status TEXT NOT NULL DEFAULT 'pending_review',
                approved_by TEXT NOT NULL DEFAULT '',
                approved_at TEXT NOT NULL DEFAULT '',
                rejected_reason TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS governance_plan_state (
                plan_id TEXT PRIMARY KEY,
                finding_id TEXT,
                remediation_state TEXT NOT NULL DEFAULT 'ai_proposed',
                dry_run_status TEXT NOT NULL DEFAULT 'not_run',
                updated_at TEXT NOT NULL
            )
            """
        )
        # Backward-compatible migration for existing local DBs
        existing_cols = {
            row["name"]
            for row in cur.execute("PRAGMA table_info(ontology_documents)").fetchall()
        }
        alter_statements = [
            ("item_id", "ALTER TABLE ontology_documents ADD COLUMN item_id TEXT"),
            ("creator_person_id", "ALTER TABLE ontology_documents ADD COLUMN creator_person_id TEXT"),
            ("primary_project_id", "ALTER TABLE ontology_documents ADD COLUMN primary_project_id TEXT"),
            ("primary_org_id", "ALTER TABLE ontology_documents ADD COLUMN primary_org_id TEXT"),
            ("canonical_doc_id", "ALTER TABLE ontology_documents ADD COLUMN canonical_doc_id TEXT"),
            ("document_kind", "ALTER TABLE ontology_documents ADD COLUMN document_kind TEXT NOT NULL DEFAULT 'derived'"),
        ]
        for col, stmt in alter_statements:
            if col not in existing_cols:
                cur.execute(stmt)
        document_entity_link_cols = {
            row["name"] for row in cur.execute("PRAGMA table_info(ontology_document_entity_links)").fetchall()
        }
        if "signal_source" not in document_entity_link_cols:
            cur.execute(
                "ALTER TABLE ontology_document_entity_links ADD COLUMN signal_source TEXT NOT NULL DEFAULT 'projection'"
            )
        ontology_plan_cols = {
            row["name"] for row in cur.execute("PRAGMA table_info(ontology_plan_quality_state)").fetchall()
        }
        if "dry_run_status" not in ontology_plan_cols:
            cur.execute(
                "ALTER TABLE ontology_plan_quality_state ADD COLUMN dry_run_status TEXT NOT NULL DEFAULT 'not_run'"
            )
        prediction_col_alters = [
            ("owner_predicted", "ALTER TABLE ontology_plan_quality_state ADD COLUMN owner_predicted TEXT NOT NULL DEFAULT 'unknown'"),
            ("owner_confidence", "ALTER TABLE ontology_plan_quality_state ADD COLUMN owner_confidence REAL NOT NULL DEFAULT 0.0"),
            ("project_predicted", "ALTER TABLE ontology_plan_quality_state ADD COLUMN project_predicted TEXT NOT NULL DEFAULT 'general'"),
            ("project_confidence", "ALTER TABLE ontology_plan_quality_state ADD COLUMN project_confidence REAL NOT NULL DEFAULT 0.0"),
            ("topic_predicted", "ALTER TABLE ontology_plan_quality_state ADD COLUMN topic_predicted TEXT NOT NULL DEFAULT '[]'"),
            ("topic_confidence", "ALTER TABLE ontology_plan_quality_state ADD COLUMN topic_confidence REAL NOT NULL DEFAULT 0.0"),
            ("canonical_doc_predicted", "ALTER TABLE ontology_plan_quality_state ADD COLUMN canonical_doc_predicted TEXT NOT NULL DEFAULT ''"),
            ("canonical_doc_confidence", "ALTER TABLE ontology_plan_quality_state ADD COLUMN canonical_doc_confidence REAL NOT NULL DEFAULT 0.0"),
            ("prediction_source", "ALTER TABLE ontology_plan_quality_state ADD COLUMN prediction_source TEXT NOT NULL DEFAULT 'llm_shadow_v1'"),
            ("review_status", "ALTER TABLE ontology_plan_quality_state ADD COLUMN review_status TEXT NOT NULL DEFAULT 'pending_review'"),
            ("approved_by", "ALTER TABLE ontology_plan_quality_state ADD COLUMN approved_by TEXT NOT NULL DEFAULT ''"),
            ("approved_at", "ALTER TABLE ontology_plan_quality_state ADD COLUMN approved_at TEXT NOT NULL DEFAULT ''"),
            ("rejected_reason", "ALTER TABLE ontology_plan_quality_state ADD COLUMN rejected_reason TEXT NOT NULL DEFAULT ''"),
        ]
        for col, stmt in prediction_col_alters:
            if col not in ontology_plan_cols:
                cur.execute(stmt)
        governance_plan_cols = {
            row["name"] for row in cur.execute("PRAGMA table_info(governance_plan_state)").fetchall()
        }
        if "dry_run_status" not in governance_plan_cols:
            cur.execute(
                "ALTER TABLE governance_plan_state ADD COLUMN dry_run_status TEXT NOT NULL DEFAULT 'not_run'"
            )
        conn.commit()


def _build_candidate_rows(total: int = 100) -> List[Tuple[Any, ...]]:
    rows: List[Tuple[Any, ...]] = []
    for i in range(1, total + 1):
        entity_type = ["organization", "person", "project", "system", "policy"][(i - 1) % 5]
        if entity_type == "organization":
            surface_form = f"株式会社ACME-{i:02d}"
        elif entity_type == "person":
            surface_form = f"利用者{i:03d}"
        elif entity_type == "project":
            surface_form = f"プロジェクト暁-{i:02d}"
        elif entity_type == "system":
            surface_form = f"システム統制基盤-{i:02d}"
        else:
            surface_form = f"データ保持方針-{i:02d}"

        rows.append(
            (
                f"CAND-{i:04d}",
                surface_form,
                entity_type,
                "governance+ner" if i % 3 else "noun_chunk",
                round(0.45 + ((i * 13) % 50) / 100, 2),
                1 if entity_type == "person" or i % 12 == 0 else 0,
                f"item-{i:03d}",
                "2026-02-28 00:00:00 UTC",
                "pending",
                None,
                None,
                None,
            )
        )
    return rows


def _maybe_seed_ontology_graph_sample_data(tenant_id: str) -> None:
    """Optional SQLite sample seed on read paths (off by default; Dynamo is canonical for plans)."""
    if str(os.getenv("ONTOLOGY_SEED_SAMPLE_ON_ONTOLOGY_QUALITY_READ", "false")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        seed_ontology_graph_sample_data(tenant_id=tenant_id, force=False)


def seed_ontology_graph_sample_data(tenant_id: str, force: bool = False) -> Dict[str, int]:
    with _tenant_scope(tenant_id):
        initialize_ontology_graph_db()
        conn = _connect()
        cur = conn.cursor()
        auto_seed_enabled = str(os.getenv("ONTOLOGY_AUTO_SEED_SAMPLE", "false")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if not force and not auto_seed_enabled:
            existing = cur.execute("SELECT COUNT(1) AS c FROM ontology_documents").fetchone()["c"]
            candidate_count = cur.execute("SELECT COUNT(1) AS c FROM ontology_entity_candidates").fetchone()["c"]
            return {"seeded": 0, "documents": existing, "entity_candidates": candidate_count}
        if not force:
            existing = cur.execute("SELECT COUNT(1) AS c FROM ontology_documents").fetchone()["c"]
            if existing > 0:
                plan_state_count = cur.execute(
                    "SELECT COUNT(1) AS c FROM ontology_plan_quality_state"
                ).fetchone()["c"]
                if plan_state_count == 0:
                    docs = cur.execute("SELECT doc_id FROM ontology_documents ORDER BY doc_id ASC").fetchall()
                    plan_rows = []
                    for doc in docs:
                        suffix = str(doc["doc_id"]).split("-")[-1]
                        if not suffix.isdigit():
                            continue
                        idx = int(suffix)
                        item_id = f"item-{idx:03d}"
                        plan_rows.append(
                            (
                                f"plan-20260226-{500 + idx:03d}",
                                item_id,
                                _derive_remediation_state(doc["doc_id"]),
                                "unknown",
                                0.0,
                                "general",
                                0.0,
                                "[]",
                                0.0,
                                "",
                                0.0,
                                "llm_shadow_v1",
                                "pending_review",
                                "",
                                "",
                                "",
                                "2026-02-28 00:00:00 UTC",
                            )
                        )
                    if plan_rows:
                        cur.executemany(
                            """
                            INSERT OR IGNORE INTO ontology_plan_quality_state
                            (
                                plan_id, item_id, remediation_state,
                                owner_predicted, owner_confidence, project_predicted, project_confidence,
                                topic_predicted, topic_confidence,
                                canonical_doc_predicted, canonical_doc_confidence,
                                prediction_source, review_status, approved_by, approved_at, rejected_reason,
                                updated_at
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            plan_rows,
                        )
                        conn.commit()
                candidate_count = cur.execute("SELECT COUNT(1) AS c FROM ontology_entity_candidates").fetchone()["c"]
                if candidate_count == 0:
                    candidate_rows = _build_candidate_rows(100)
                    cur.executemany(
                        """
                        INSERT OR IGNORE INTO ontology_entity_candidates
                        (candidate_id, surface_form, entity_type, extraction_source, confidence, pii_flag, item_id, received_at, status, resolved_entity_id, resolution_type, resolved_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        candidate_rows,
                    )
                    conn.commit()
                    return {"seeded": 1, "documents": existing, "entity_candidates": len(candidate_rows)}
                return {"seeded": 0, "documents": existing, "entity_candidates": candidate_count}

        cur.execute("DELETE FROM ontology_document_entity_links")
        cur.execute("DELETE FROM ontology_document_lineage_links")
        cur.execute("DELETE FROM ontology_edges")
        cur.execute("DELETE FROM ontology_entity_surface_forms")
        cur.execute("DELETE FROM ontology_entity_candidates")
        cur.execute("DELETE FROM ontology_entities")
        cur.execute("DELETE FROM ontology_documents")
        cur.execute("DELETE FROM ontology_plan_quality_state")

        organizations = [
            "株式会社ACME",
            "青海ソリューションズ株式会社",
            "みらい解析合同会社",
            "朱雀コンサルティング株式会社",
            "山桜パートナーズ株式会社",
        ]
        projects = [
            "プロジェクト暁",
            "プロジェクト灯台",
            "プロジェクト旋風",
            "プロジェクト水面",
            "プロジェクト月影",
            "プロジェクト潮流",
            "プロジェクト白夜",
            "プロジェクト光輪",
            "プロジェクト電波",
            "プロジェクト若葉",
        ]
        people = [f"利用者{i:03d}" for i in range(1, 31)]

        source_systems = [
            "microsoft365",
            "box",
            "notion",
            "jira",
            "slack",
            "mail",
            "google-drive",
        ]
        freshness_cycle = ["active", "active", "aging", "aging", "stale"]
        document_title_bases = [
            "FY2026_Budget",
            "HR_Employee_Master",
            "Security_Incident_Report",
            "Vendor_Assessment",
            "Audit_Evidence",
            "Architecture_Decision_Record",
            "Contract_Review",
            "Roadmap_Update",
            "Project_Retrospective",
            "Data_Retention_Policy",
        ]
        document_extensions = ["docx", "xlsx", "pdf", "csv"]

        documents = []
        for i in range(1, 41):
            source = source_systems[(i - 1) % len(source_systems)]
            freshness = freshness_cycle[(i - 1) % len(freshness_cycle)]
            quality = round(0.45 + ((i * 7) % 46) / 100, 2)
            project_name = projects[(i - 1) % len(projects)]
            title_base = document_title_bases[(i - 1) % len(document_title_bases)]
            extension = document_extensions[(i - 1) % len(document_extensions)]
            creator_idx = ((i - 1) % len(people)) + 1
            project_idx = ((i - 1) % len(projects)) + 1
            org_idx = ((i - 1) % len(organizations)) + 1
            canonical_idx = ((i - 1) % len(projects)) + 1
            is_canonical = i <= len(projects)
            canonical_doc_id = f"DOC-{i:03d}" if is_canonical else f"DOC-{canonical_idx:03d}"
            documents.append(
                (
                    f"DOC-{i:03d}",
                    f"{title_base}-{i:03d}.{extension}",
                    f"{project_name}に関するダミー文書。作成者、関連プロジェクト、正本系譜の管理検証用データ。",
                    source,
                    quality,
                    freshness,
                    f"ENT-PER-{creator_idx:03d}",
                    f"ENT-PROJ-{project_idx:03d}",
                    f"ENT-ORG-{org_idx:03d}",
                    canonical_doc_id,
                    "canonical" if is_canonical else "derived",
                    f"lin-doc-{i:03d}",
                    f"corr-doc-{i:03d}",
                    "2026-02-28 00:00:00 UTC",
                )
            )
        cur.executemany(
            """
            INSERT INTO ontology_documents
            (doc_id, title, summary, source_system, content_quality_score, freshness_status,
             creator_person_id, primary_project_id, primary_org_id, canonical_doc_id, document_kind,
             lineage_id, correlation_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            documents,
        )
        plan_quality_rows = []
        for doc in documents:
            doc_id = doc[0]
            suffix = str(doc_id).split("-")[-1]
            if not suffix.isdigit():
                continue
            idx = int(suffix)
            plan_quality_rows.append(
                (
                    f"plan-20260226-{500 + idx:03d}",
                    f"item-{idx:03d}",
                    _derive_remediation_state(doc_id),
                    "unknown",
                    0.0,
                    "general",
                    0.0,
                    "[]",
                    0.0,
                    "",
                    0.0,
                    "llm_shadow_v1",
                    "pending_review",
                    "",
                    "",
                    "",
                    "2026-02-28 00:00:00 UTC",
                )
            )
        cur.executemany(
            """
            INSERT INTO ontology_plan_quality_state
            (
                plan_id, item_id, remediation_state,
                owner_predicted, owner_confidence, project_predicted, project_confidence,
                topic_predicted, topic_confidence,
                canonical_doc_predicted, canonical_doc_confidence,
                prediction_source, review_status, approved_by, approved_at, rejected_reason,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            plan_quality_rows,
        )

        entities = []
        for i, org_name in enumerate(organizations, start=1):
            status = "resolved" if i <= 3 else "review"
            entities.append(
                (
                    f"ENT-ORG-{i:03d}",
                    org_name,
                    "organization",
                    status,
                    round(0.78 + i * 0.03, 2),
                    0,
                    "governance+ner",
                    6 + i,
                    f"lin-ent-org-{i:03d}",
                    f"corr-ent-org-{i:03d}",
                    "2026-02-28 00:00:00 UTC",
                )
            )

        for i, project_name in enumerate(projects, start=1):
            status = "resolved" if i % 4 != 0 else "review"
            entities.append(
                (
                    f"ENT-PROJ-{i:03d}",
                    project_name,
                    "project",
                    status,
                    round(0.72 + ((i * 5) % 24) / 100, 2),
                    0,
                    "connect_metadata",
                    4 + (i % 7),
                    f"lin-ent-proj-{i:03d}",
                    f"corr-ent-proj-{i:03d}",
                    "2026-02-28 00:00:00 UTC",
                )
            )

        for i, person_name in enumerate(people, start=1):
            confidence = round(0.52 + ((i * 9) % 45) / 100, 2)
            if confidence < 0.7:
                status = "pending"
            elif confidence < 0.85:
                status = "review"
            else:
                status = "resolved"
            entities.append(
                (
                    f"ENT-PER-{i:03d}",
                    person_name,
                    "person",
                    status,
                    confidence,
                    1 if i % 3 == 0 else 0,
                    "noun_chunk" if i % 4 == 0 else "governance+ner",
                    1 + (i % 8),
                    f"lin-ent-per-{i:03d}",
                    f"corr-ent-per-{i:03d}",
                    "2026-02-28 00:00:00 UTC",
                )
            )
        cur.executemany(
            """
            INSERT INTO ontology_entities
            (entity_id, canonical_name, entity_type, resolution_status, confidence, pii_flag, extraction_source, spread_factor, lineage_id, correlation_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            entities,
        )

        edges = []
        edge_seq = 1
        for i in range(1, 11):
            org_idx = ((i - 1) % 5) + 1
            edges.append(
                (
                    f"EDGE-{edge_seq:03d}",
                    f"ENT-PROJ-{i:03d}",
                    f"ENT-ORG-{org_idx:03d}",
                    "related_to",
                    round(0.66 + (i % 5) * 0.06, 2),
                    "project to organization alignment",
                )
            )
            edge_seq += 1

        for i in range(1, 31):
            org_idx = ((i - 1) % 5) + 1
            project_idx = ((i - 1) % 10) + 1
            edges.append(
                (
                    f"EDGE-{edge_seq:03d}",
                    f"ENT-PER-{i:03d}",
                    f"ENT-ORG-{org_idx:03d}",
                    "member_of",
                    round(0.58 + (i % 6) * 0.06, 2),
                    "person organization membership candidate",
                )
            )
            edge_seq += 1
            edges.append(
                (
                    f"EDGE-{edge_seq:03d}",
                    f"ENT-PER-{i:03d}",
                    f"ENT-PROJ-{project_idx:03d}",
                    "referenced_by",
                    round(0.54 + (i % 7) * 0.05, 2),
                    "person appears in project records",
                )
            )
            edge_seq += 1

        for i in range(1, 6):
            nxt = (i % 5) + 1
            edges.append(
                (
                    f"EDGE-{edge_seq:03d}",
                    f"ENT-ORG-{i:03d}",
                    f"ENT-ORG-{nxt:03d}",
                    "related_to",
                    0.61,
                    "cross-organization collaboration pattern",
                )
            )
            edge_seq += 1
        cur.executemany(
            """
            INSERT INTO ontology_edges
            (edge_id, source_id, target_id, relation_type, weight, evidence)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            edges,
        )

        doc_links = []
        link_seq = 1
        for i in range(1, 41):
            project_idx = ((i - 1) % 10) + 1
            org_idx = ((i - 1) % 5) + 1
            person_idx = ((i * 3 - 1) % 30) + 1
            doc_links.append(
                (
                    f"LINK-{link_seq:03d}",
                    f"DOC-{i:03d}",
                    f"ENT-PROJ-{project_idx:03d}",
                    "belongs_to_project",
                    round(0.72 + (i % 5) * 0.05, 2),
                    "文書の主対象プロジェクト",
                )
            )
            link_seq += 1
            doc_links.append(
                (
                    f"LINK-{link_seq:03d}",
                    f"DOC-{i:03d}",
                    f"ENT-PER-{person_idx:03d}",
                    "created_by",
                    round(0.78 + (i % 4) * 0.04, 2),
                    "文書作成者",
                )
            )
            link_seq += 1
            doc_links.append(
                (
                    f"LINK-{link_seq:03d}",
                    f"DOC-{i:03d}",
                    f"ENT-ORG-{org_idx:03d}",
                    "owned_by",
                    round(0.7 + (i % 4) * 0.06, 2),
                    "文書の管理組織",
                )
            )
            link_seq += 1
        cur.executemany(
            """
            INSERT INTO ontology_document_entity_links
            (link_id, doc_id, entity_id, relation_type, weight, evidence)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            doc_links,
        )

        doc_lineage_links = []
        lineage_seq = 1
        for i in range(11, 41):
            canonical_idx = ((i - 1) % 10) + 1
            doc_lineage_links.append(
                (
                    f"DOC-LIN-{lineage_seq:03d}",
                    f"DOC-{i:03d}",
                    f"DOC-{canonical_idx:03d}",
                    "derived_from",
                    0.92,
                    "派生文書 -> 正本文書の系譜リンク",
                )
            )
            lineage_seq += 1
        cur.executemany(
            """
            INSERT INTO ontology_document_lineage_links
            (link_id, source_doc_id, target_doc_id, relation_type, weight, evidence)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            doc_lineage_links,
        )

        candidate_rows = _build_candidate_rows(100)
        cur.executemany(
            """
            INSERT INTO ontology_entity_candidates
            (candidate_id, surface_form, entity_type, extraction_source, confidence, pii_flag, item_id, received_at, status, resolved_entity_id, resolution_type, resolved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            candidate_rows,
        )

        conn.commit()
        return {
            "seeded": 1,
            "documents": len(documents),
            "entities": len(entities),
            "edges": len(edges) + len(doc_links) + len(doc_lineage_links),
            "entity_candidates": len(candidate_rows),
        }


def get_ontology_labels(tenant_id: str) -> List[str]:
    with _tenant_scope(tenant_id):
        initialize_ontology_graph_db()
        seed_ontology_graph_sample_data(tenant_id=tenant_id, force=False)
        with _connect() as conn:
            cur = conn.cursor()
            labels = []
            for row in cur.execute(
                "SELECT canonical_name FROM ontology_entities ORDER BY canonical_name"
            ):
                labels.append(row["canonical_name"])
            for row in cur.execute("SELECT title FROM ontology_documents ORDER BY title"):
                labels.append(row["title"])
        return ["*"] + labels


def _load_graph_rows() -> Tuple[Dict[str, OntologyNode], List[OntologyEdge]]:
    def _item_id_from_doc_row(row: sqlite3.Row) -> str:
        persisted_item_id = str(row["item_id"] or "").strip() if "item_id" in row.keys() else ""
        if persisted_item_id:
            return persisted_item_id
        doc_id_value = str(row["doc_id"] or "")
        suffix = doc_id_value.split("-")[-1]
        if suffix.isdigit():
            return f"item-{int(suffix):03d}"
        return doc_id_value.lower()

    nodes: Dict[str, OntologyNode] = {}
    edges: List[OntologyEdge] = []
    with _connect() as conn:
        cur = conn.cursor()
        entity_name_map: Dict[str, str] = {}
        for row in cur.execute("SELECT * FROM ontology_entities"):
            entity_name_map[row["entity_id"]] = row["canonical_name"]
            nodes[row["entity_id"]] = OntologyNode(
                id=row["entity_id"],
                labels=[row["canonical_name"]],
                properties={
                    "entity_type": row["entity_type"],
                    "resolution_status": row["resolution_status"],
                    "confidence": row["confidence"],
                    "pii_flag": bool(row["pii_flag"]),
                    "extraction_source": row["extraction_source"],
                    "spread_factor": row["spread_factor"],
                    "lineage_id": row["lineage_id"],
                    "correlation_id": row["correlation_id"],
                    "updated_at": row["updated_at"],
                },
            )

        for row in cur.execute("SELECT * FROM ontology_documents"):
            creator_id = row["creator_person_id"]
            project_id = row["primary_project_id"]
            org_id = row["primary_org_id"]
            nodes[row["doc_id"]] = OntologyNode(
                id=row["doc_id"],
                labels=[row["title"]],
                properties={
                    "entity_type": "document",
                    "item_id": _item_id_from_doc_row(row),
                    "file_name": row["title"],
                    "document_kind": row["document_kind"],
                    "canonical_doc_id": row["canonical_doc_id"],
                    "creator_person_id": creator_id,
                    "creator_name": entity_name_map.get(creator_id, "-"),
                    "owner": entity_name_map.get(creator_id, "-"),
                    "primary_project_id": project_id,
                    "project_name": entity_name_map.get(project_id, "-"),
                    "project": entity_name_map.get(project_id, "-"),
                    "primary_org_id": org_id,
                    "organization_name": entity_name_map.get(org_id, "-"),
                    "topic_categories": [],
                    "resolution_status": "resolved",
                    "confidence": 1.0,
                    "source": row["source_system"],
                    "contentQualityScore": row["content_quality_score"],
                    "freshnessStatus": row["freshness_status"],
                    "lineage_id": row["lineage_id"],
                    "correlation_id": row["correlation_id"],
                    "description": row["summary"],
                    "updated_at": row["updated_at"],
                },
            )
        for row in cur.execute("SELECT * FROM ontology_folder_nodes"):
            nodes[row["folder_id"]] = OntologyNode(
                id=row["folder_id"],
                labels=[row["folder_name"]],
                properties={
                    "entity_type": "folder",
                    "folder_path": row["folder_path"],
                    "depth": row["depth"],
                    "tenant_id": row["tenant_id"],
                    "updated_at": row["updated_at"],
                },
            )

        for row in cur.execute("SELECT * FROM ontology_edges"):
            edges.append(
                OntologyEdge(
                    id=row["edge_id"],
                    source=row["source_id"],
                    target=row["target_id"],
                    type=row["relation_type"],
                    properties={"weight": row["weight"], "keywords": row["evidence"]},
                )
            )
        for row in cur.execute("SELECT * FROM ontology_document_entity_links"):
            edges.append(
                OntologyEdge(
                    id=row["link_id"],
                    source=row["doc_id"],
                    target=row["entity_id"],
                    type=row["relation_type"],
                    properties={
                        "weight": row["weight"],
                        "keywords": row["evidence"],
                        "signal_source": row["signal_source"] if "signal_source" in row.keys() else "projection",
                    },
                )
            )
        for row in cur.execute("SELECT * FROM ontology_document_lineage_links"):
            edges.append(
                OntologyEdge(
                    id=row["link_id"],
                    source=row["source_doc_id"],
                    target=row["target_doc_id"],
                    type=row["relation_type"],
                    properties={"weight": row["weight"], "keywords": row["evidence"]},
                )
            )
        for row in cur.execute("SELECT * FROM ontology_document_folder_links"):
            edges.append(
                OntologyEdge(
                    id=row["link_id"],
                    source=row["doc_id"],
                    target=row["folder_id"],
                    type=row["relation_type"],
                    properties={"weight": row["weight"], "keywords": row["evidence"]},
                )
            )
        for row in cur.execute("SELECT * FROM ontology_folder_hierarchy_links"):
            edges.append(
                OntologyEdge(
                    id=row["link_id"],
                    source=row["parent_folder_id"],
                    target=row["child_folder_id"],
                    type=row["relation_type"],
                    properties={"weight": row["weight"], "keywords": row["evidence"]},
                )
            )
        for row in cur.execute("SELECT * FROM ontology_document_similarity_links"):
            edges.append(
                OntologyEdge(
                    id=row["link_id"],
                    source=row["source_doc_id"],
                    target=row["target_doc_id"],
                    type=row["relation_type"],
                    properties={
                        "weight": row["similarity_score"],
                        "similarity_score": row["similarity_score"],
                        "keywords": row["evidence"],
                        "algorithm": row["algorithm"],
                    },
                )
            )
    return nodes, edges


def get_ontology_graph(
    tenant_id: str, label: str, max_depth: int, max_nodes: int
) -> Dict[str, object]:
    with _tenant_scope(tenant_id):
        initialize_ontology_graph_db()
        seed_ontology_graph_sample_data(tenant_id=tenant_id, force=False)
        nodes_by_id, all_edges = _load_graph_rows()
    _enrich_document_nodes_from_dynamodb(nodes_by_id, tenant_id)
    if not nodes_by_id:
        return {"nodes": [], "edges": [], "is_truncated": False}

    label_lc = (label or "*").lower().strip()
    adjacency: Dict[str, List[Tuple[str, OntologyEdge]]] = {}
    degree_by_node: Dict[str, int] = {}
    for edge in all_edges:
        adjacency.setdefault(edge.source, []).append((edge.target, edge))
        adjacency.setdefault(edge.target, []).append((edge.source, edge))
        degree_by_node[edge.source] = degree_by_node.get(edge.source, 0) + 1
        degree_by_node[edge.target] = degree_by_node.get(edge.target, 0) + 1
    if label_lc == "*":
        # Pick multiple high-degree seeds for wildcard to avoid single-component bias.
        ranked_node_ids = sorted(
            nodes_by_id.keys(),
            key=lambda node_id: (-degree_by_node.get(node_id, 0), node_id),
        )
        max_start_nodes = max(1, min(10, int(max_nodes)))
        start_nodes = ranked_node_ids[:max_start_nodes]
        if not start_nodes:
            start_nodes = list(nodes_by_id.keys())[:1]
    else:
        start_nodes = [
            node_id
            for node_id, node in nodes_by_id.items()
            if (
                any(label_lc in lbl.lower() for lbl in node.labels)
                or label_lc in node.id.lower()
                or label_lc in str(node.properties.get("item_id", "")).lower()
                or label_lc in str(node.properties.get("file_name", "")).lower()
            )
        ][:10]
        if not start_nodes:
            return {"nodes": [], "edges": [], "is_truncated": False}

    visited = set(start_nodes)
    q = deque([(sid, 0) for sid in start_nodes])
    selected_edges: Dict[str, OntologyEdge] = {}

    while q and len(visited) < max_nodes:
        current, depth = q.popleft()
        if depth >= max_depth:
            continue
        for neighbor, edge in adjacency.get(current, []):
            selected_edges[edge.id] = edge
            if neighbor not in visited and len(visited) < max_nodes:
                visited.add(neighbor)
                q.append((neighbor, depth + 1))

    selected_nodes = [nodes_by_id[node_id] for node_id in visited if node_id in nodes_by_id]
    selected_node_ids = {node.id for node in selected_nodes}
    filtered_edges = [
        edge
        for edge in selected_edges.values()
        if edge.source in selected_node_ids and edge.target in selected_node_ids
    ]

    return {
        "nodes": [
            {"id": node.id, "labels": node.labels, "properties": node.properties}
            for node in selected_nodes
        ],
        "edges": [
            {
                "id": edge.id,
                "source": edge.source,
                "target": edge.target,
                "type": edge.type,
                "properties": edge.properties,
            }
            for edge in filtered_edges
        ],
        "is_truncated": len(selected_nodes) >= max_nodes,
    }


def get_ontology_graph_by_item(
    tenant_id: str,
    item_id: str,
    file_name: str = "",
    max_depth: int = 2,
    max_nodes: int = 80,
) -> Dict[str, object]:
    def _is_hidden_root_folder(node: OntologyNode) -> bool:
        entity_type = str(node.properties.get("entity_type", "")).strip().lower()
        if entity_type != "folder":
            return False
        label = str(node.labels[0] if node.labels else "").strip().lower()
        folder_path = str(node.properties.get("folder_path", "")).strip().lower()
        return label in {"root", "root:"} or folder_path in {"root", "root:"}

    with _tenant_scope(tenant_id):
        initialize_ontology_graph_db()
        seed_ontology_graph_sample_data(tenant_id=tenant_id, force=False)
        nodes_by_id, all_edges = _load_graph_rows()
    _enrich_document_nodes_from_dynamodb(nodes_by_id, tenant_id)
    hidden_node_ids = {
        node_id for node_id, node in nodes_by_id.items() if _is_hidden_root_folder(node)
    }
    if hidden_node_ids:
        nodes_by_id = {
            node_id: node
            for node_id, node in nodes_by_id.items()
            if node_id not in hidden_node_ids
        }
        all_edges = [
            edge
            for edge in all_edges
            if edge.source not in hidden_node_ids and edge.target not in hidden_node_ids
        ]
    if not nodes_by_id:
        return {
            "nodes": [],
            "edges": [],
            "is_truncated": False,
            "start_node_id": None,
            "matched_by": "fallback",
        }

    normalized_item_id = (item_id or "").strip()
    normalized_file_name = (file_name or "").strip().lower()
    start_nodes: List[str] = []
    matched_by = "fallback"

    if normalized_item_id and normalized_item_id in nodes_by_id:
        start_nodes = [normalized_item_id]
        matched_by = "item_id"
    if not start_nodes and normalized_item_id:
        matched_nodes = [
            node_id
            for node_id, node in nodes_by_id.items()
            if str(node.properties.get("item_id", "")).strip().lower() == normalized_item_id.lower()
        ][:1]
        if matched_nodes:
            start_nodes = matched_nodes
            matched_by = "item_id"
    elif normalized_item_id.lower().startswith("item-"):
        suffix = normalized_item_id.split("-", 1)[1].strip()
        if suffix.isdigit():
            converted_doc_id = f"DOC-{int(suffix):03d}"
            if converted_doc_id in nodes_by_id:
                start_nodes = [converted_doc_id]
                matched_by = "item_id"
    if not start_nodes and normalized_file_name:
        start_nodes = [
            node_id
            for node_id, node in nodes_by_id.items()
            if normalized_file_name
            in str(node.properties.get("file_name", node.labels[0] if node.labels else "")).lower()
        ][:1]
        if start_nodes:
            matched_by = "file_name"

    if not start_nodes:
        return {
            "nodes": [],
            "edges": [],
            "is_truncated": False,
            "start_node_id": None,
            "matched_by": matched_by,
        }

    include_similarity_edges = _get_env_bool(
        "ONTOLOGY_ITEM_GRAPH_INCLUDE_SIMILARITY",
        False,
    )
    adjacency: Dict[str, List[Tuple[str, OntologyEdge]]] = {}
    for edge in all_edges:
        if not include_similarity_edges and str(edge.type).strip().lower() == "similar_to":
            continue
        adjacency.setdefault(edge.source, []).append((edge.target, edge))
        adjacency.setdefault(edge.target, []).append((edge.source, edge))

    visited = set(start_nodes)
    q = deque([(sid, 0) for sid in start_nodes])
    selected_edges: Dict[str, OntologyEdge] = {}

    while q and len(visited) < max_nodes:
        current, depth = q.popleft()
        if depth >= max_depth:
            continue
        for neighbor, edge in adjacency.get(current, []):
            selected_edges[edge.id] = edge
            if neighbor not in visited and len(visited) < max_nodes:
                visited.add(neighbor)
                q.append((neighbor, depth + 1))

    selected_nodes = [nodes_by_id[node_id] for node_id in visited if node_id in nodes_by_id]
    selected_node_ids = {node.id for node in selected_nodes}
    filtered_edges = [
        edge
        for edge in selected_edges.values()
        if edge.source in selected_node_ids and edge.target in selected_node_ids
    ]

    return {
        "nodes": [
            {"id": node.id, "labels": node.labels, "properties": node.properties}
            for node in selected_nodes
        ],
        "edges": [
            {
                "id": edge.id,
                "source": edge.source,
                "target": edge.target,
                "type": edge.type,
                "properties": edge.properties,
            }
            for edge in filtered_edges
        ],
        "is_truncated": len(selected_nodes) >= max_nodes,
        "start_node_id": start_nodes[0],
        "matched_by": matched_by,
    }


def rebuild_ontology_graph_projection_from_unified(
    tenant_id: str,
    *,
    unified_rows: List[Dict[str, Any]],
    clear_existing: bool = True,
    projection_options: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    """Rebuild document/entity graph projection from UnifiedMetadata rows."""

    projection_options = projection_options or {}

    def _to_text(value: Any, default: str = "") -> str:
        if value is None:
            return default
        text = str(value).strip()
        return text if text else default

    def _to_doc_id(item_id: str) -> str:
        normalized_item_id = _to_text(item_id)
        if normalized_item_id.lower().startswith("item-"):
            suffix = normalized_item_id.split("-", 1)[1].strip()
            if suffix.isdigit():
                return f"DOC-{int(suffix):03d}"
        token = re.sub(r"[^A-Za-z0-9]+", "-", normalized_item_id).strip("-").upper()
        if not token:
            token = hashlib.sha1(normalized_item_id.encode("utf-8")).hexdigest()[:8].upper()
        return f"DOC-{token[:24]}"

    def _entity_key(entity_type: str, raw_value: str) -> str:
        normalized = _to_text(raw_value)
        token = re.sub(r"[^A-Za-z0-9]+", "-", normalized).strip("-").upper()
        if not token:
            token = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:8].upper()
        return f"ENT-{entity_type[:3].upper()}-{token[:24]}"

    def _stable_id(prefix: str, seed: str) -> str:
        digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:24]
        return f"{prefix}-{digest}"

    def _parse_string_list(value: Any) -> List[str]:
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return []
            if raw.startswith("[") and raw.endswith("]"):
                inner = raw[1:-1]
                return [segment.strip().strip("'\"") for segment in inner.split(",") if segment.strip()]
            return [segment.strip() for segment in raw.split(",") if segment.strip()]
        return []

    def _normalize_hierarchy_segments(path_value: str) -> List[str]:
        normalized = str(path_value or "").replace("\\", "/").strip("/")
        if not normalized:
            return []
        segments = [segment.strip() for segment in normalized.split("/") if segment.strip()]
        filtered: List[str] = []
        for segment in segments:
            normalized_segment = segment.lower()
            if normalized_segment in {"root", "root:"}:
                continue
            filtered.append(segment)
        return filtered

    def _parse_embedding_vector(raw_value: Any) -> List[float]:
        if isinstance(raw_value, list):
            try:
                return [float(v) for v in raw_value]
            except (TypeError, ValueError):
                return []
        if isinstance(raw_value, str):
            cleaned = raw_value.strip()
            if not cleaned:
                return []
            if cleaned.startswith("[") and cleaned.endswith("]"):
                cleaned = cleaned[1:-1]
            try:
                vector = [float(part.strip()) for part in cleaned.split(",") if part.strip()]
            except ValueError:
                return []
            return vector
        return []

    def _cosine_similarity(left: List[float], right: List[float]) -> float:
        if not left or not right:
            return 0.0
        if len(left) != len(right):
            return 0.0
        left_norm = math.sqrt(sum(value * value for value in left))
        right_norm = math.sqrt(sum(value * value for value in right))
        if left_norm <= 0.0 or right_norm <= 0.0:
            return 0.0
        dot = sum(left[idx] * right[idx] for idx in range(len(left)))
        return dot / (left_norm * right_norm)

    def _build_text_fallback_vector(
        *,
        title: str,
        summary: str,
        topic_keywords: List[str],
        hierarchy_path: str,
        dim: int = 64,
    ) -> List[float]:
        tokens: List[str] = []
        for raw_text in [title, summary, hierarchy_path]:
            normalized = str(raw_text or "").strip().lower()
            if not normalized:
                continue
            normalized = re.sub(r"[\\/_\\-]+", " ", normalized)
            tokens.extend([part for part in normalized.split() if len(part) >= 2])
        tokens.extend([str(keyword).strip().lower() for keyword in topic_keywords if str(keyword).strip()])
        if not tokens:
            return []
        vector = [0.0] * dim
        for token in tokens:
            digest = hashlib.sha1(token.encode("utf-8")).hexdigest()
            index = int(digest[:8], 16) % dim
            vector[index] += 1.0
        norm = math.sqrt(sum(value * value for value in vector))
        if norm <= 0.0:
            return []
        return [value / norm for value in vector]

    def _parse_stopwords(value: Any) -> set[str]:
        if isinstance(value, str):
            return {part.strip().lower() for part in value.split(",") if part.strip()}
        if isinstance(value, list):
            return {str(part).strip().lower() for part in value if str(part).strip()}
        return set()

    def _normalize_keyword(keyword: Any) -> str:
        normalized = str(keyword or "").strip().lower()
        if not normalized:
            return ""
        normalized = re.sub(r"[\s]+", " ", normalized)
        # Trim common JSON/path punctuation from both ends.
        normalized = normalized.strip("\"'`[]{}()<>.,:;|\\/")
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _is_noisy_keyword(keyword: str) -> bool:
        if not keyword:
            return True
        if len(keyword) > 64:
            return True
        if "://" in keyword or keyword.startswith("http"):
            return True
        if any(ch in keyword for ch in ['{', '}', '[', ']', '"']):
            return True
        if re.fullmatch(r"[0-9a-f]{16,}", keyword):
            return True
        if re.fullmatch(r"[a-z0-9_-]{28,}", keyword):
            return True
        return False

    def _is_valid_autopromote_keyword(
        keyword: str,
        *,
        stopwords: set[str],
        min_alnum_chars: int,
        min_cjk_chars: int,
    ) -> bool:
        normalized = str(keyword or "").strip().lower()
        if not normalized:
            return False
        if normalized in stopwords:
            return False
        if re.fullmatch(r"[\W_]+", normalized):
            return False
        alnum_count = len(re.findall(r"[a-z0-9]", normalized))
        cjk_count = len(re.findall(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff]", normalized))
        return alnum_count >= min_alnum_chars or cjk_count >= min_cjk_chars

    def _to_bool(value: Any, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        raw = str(value).strip().lower()
        if raw in {"1", "true", "yes", "on"}:
            return True
        if raw in {"0", "false", "no", "off"}:
            return False
        return default

    def _to_float(value: Any, default: float) -> float:
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _to_int(value: Any, default: int, *, minimum: int, maximum: int) -> int:
        if value is None:
            parsed = default
        else:
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                parsed = default
        return max(minimum, min(maximum, parsed))

    def _option_bool(name: str, default: bool) -> bool:
        if name in projection_options:
            return _to_bool(projection_options.get(name), default)
        return _get_env_bool(name, default)

    def _option_float(name: str, default: float) -> float:
        if name in projection_options:
            return _to_float(projection_options.get(name), default)
        return _get_env_float(name, default)

    def _option_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
        if name in projection_options:
            return _to_int(projection_options.get(name), default, minimum=minimum, maximum=maximum)
        return _get_env_int(name, default, minimum=minimum, maximum=maximum)

    enable_contained_in = _option_bool("ENABLE_CONTAINED_IN_EDGES", True)
    enable_mentions_entity = _option_bool("ENABLE_MENTIONS_ENTITY_EDGES", True)
    enable_similarity = _option_bool("ENABLE_SIMILARITY_EDGES", True)
    enable_similarity_text_fallback = _option_bool("ENABLE_SIMILARITY_TEXT_FALLBACK", True)
    enable_mentions_autopromote = _option_bool("ENABLE_MENTIONS_AUTOPROMOTE", True)
    mentions_autopromote_stopwords = {
        "the",
        "and",
        "or",
        "for",
        "with",
        "document",
        "documents",
        "file",
        "files",
        "data",
        "draft",
        "tmp",
        "test",
        "n/a",
        "na",
        "none",
        "null",
        "資料",
        "文書",
        "ファイル",
        "その他",
        "不明",
    }
    if "MENTIONS_AUTOPROMOTE_STOPWORDS" in projection_options:
        mentions_autopromote_stopwords.update(
            _parse_stopwords(projection_options.get("MENTIONS_AUTOPROMOTE_STOPWORDS"))
        )
    else:
        mentions_autopromote_stopwords.update(
            _parse_stopwords(os.getenv("MENTIONS_AUTOPROMOTE_STOPWORDS", ""))
        )
    mentions_autopromote_min_alnum_chars = _option_int(
        "MENTIONS_AUTOPROMOTE_MIN_ALNUM_CHARS",
        3,
        minimum=1,
        maximum=32,
    )
    mentions_autopromote_min_cjk_chars = _option_int(
        "MENTIONS_AUTOPROMOTE_MIN_CJK_CHARS",
        2,
        minimum=1,
        maximum=32,
    )
    mentions_max_links_per_doc = _option_int(
        "MENTIONS_MAX_LINKS_PER_DOC",
        24,
        minimum=1,
        maximum=500,
    )
    mentions_autopromote_max_per_doc = _option_int(
        "MENTIONS_AUTOPROMOTE_MAX_PER_DOC",
        8,
        minimum=0,
        maximum=128,
    )
    similarity_threshold = _option_float("SIMILARITY_THRESHOLD", 0.78)
    top_k_neighbors = _option_int("TOP_K_NEIGHBORS", 5, minimum=1, maximum=50)
    similarity_max_docs = _option_int("SIMILARITY_MAX_DOCS", 2000, minimum=1, maximum=20000)

    with _tenant_scope(tenant_id):
        initialize_ontology_graph_db()
        with _connect() as conn:
            cur = conn.cursor()
            if clear_existing:
                cur.execute("DELETE FROM ontology_document_similarity_links")
                cur.execute("DELETE FROM ontology_document_folder_links")
                cur.execute("DELETE FROM ontology_folder_hierarchy_links")
                cur.execute("DELETE FROM ontology_folder_nodes")
                cur.execute("DELETE FROM ontology_document_lineage_links")
                cur.execute("DELETE FROM ontology_document_entity_links")
                cur.execute("DELETE FROM ontology_documents")

            projected_documents = 0
            projected_entities = 0
            projected_links = 0
            projected_lineage_links = 0
            contained_in_links = 0
            folder_hierarchy_links = 0
            mentions_links = 0
            similarity_links = 0
            skipped_similarity_docs = 0
            text_fallback_vectors = 0
            auto_promoted_entities = 0
            seen_entity_ids: set[str] = set()
            doc_to_item_id: Dict[str, str] = {}
            entity_name_to_ids: Dict[str, set[str]] = {}
            mention_by_item_id: Dict[str, set[str]] = {}
            doc_vectors: Dict[str, List[float]] = {}

            for row in cur.execute("SELECT entity_id, canonical_name FROM ontology_entities"):
                canonical_name = str(row["canonical_name"] or "").strip().lower()
                if canonical_name:
                    entity_name_to_ids.setdefault(canonical_name, set()).add(str(row["entity_id"]))
            for row in cur.execute("SELECT entity_id, source_item_id FROM ontology_entity_surface_forms"):
                source_item_id = str(row["source_item_id"] or "").strip()
                entity_id = str(row["entity_id"] or "").strip()
                if source_item_id and entity_id:
                    mention_by_item_id.setdefault(source_item_id, set()).add(entity_id)

            folder_cache: set[str] = set()

            for row in unified_rows:
                item_id = _to_text(row.get("item_id"))
                if not item_id:
                    continue
                doc_id = _to_doc_id(item_id)
                doc_to_item_id[doc_id] = item_id
                extensions = row.get("extensions")
                if isinstance(extensions, str):
                    try:
                        extensions = json.loads(extensions)
                    except json.JSONDecodeError:
                        extensions = {}
                if not isinstance(extensions, dict):
                    extensions = {}
                raw_document_profile = extensions.get("document_profile")
                document_profile = raw_document_profile if isinstance(raw_document_profile, dict) else {}
                title = _to_text(row.get("title"), item_id)
                source_system = _to_text(row.get("source"), "aws")
                quality_score = float(row.get("content_quality_score", 0.0) or 0.0)
                freshness_status = _to_text(row.get("freshness_status"), "active")
                owner_name = _to_text(row.get("owner") or document_profile.get("owner"))
                project_name = _to_text(row.get("project") or document_profile.get("project"))
                creator_person_id = _to_text(row.get("creator_person_id"))
                if owner_name:
                    creator_person_id = _entity_key("person", owner_name)
                primary_project_id = _to_text(row.get("primary_project_id"))
                if project_name:
                    primary_project_id = _entity_key("project", project_name)
                primary_org_id = _to_text(row.get("primary_org_id"))
                canonical_doc_id = _to_text(
                    row.get("canonical_doc_id") or document_profile.get("canonical_doc_id")
                )
                topic_categories = row.get("topic_categories") or document_profile.get("topic_categories") or []
                if not isinstance(topic_categories, list):
                    topic_categories = []
                topic_categories = [str(v).strip() for v in topic_categories if str(v).strip()]
                document_kind = _to_text(row.get("document_kind"), "derived")
                lineage_id = _to_text(row.get("lineage_id"), f"lin-{item_id}")
                correlation_id = _to_text(row.get("correlation_id"), f"corr-{item_id}")
                updated_at = _to_text(
                    row.get("transformed_at")
                    or row.get("last_modified")
                    or row.get("updated_at"),
                    "1970-01-01T00:00:00Z",
                )
                summary = _to_text(
                    row.get("document_summary") or row.get("summary"),
                    "Unified metadata projection",
                )
                hierarchy_path = _to_text(row.get("hierarchy_path"))
                topic_keywords_raw = _parse_string_list(row.get("topic_keywords"))
                topic_keywords: List[str] = []
                seen_topic_keywords: set[str] = set()
                for keyword in topic_keywords_raw:
                    normalized_keyword = _normalize_keyword(keyword)
                    if (
                        not normalized_keyword
                        or normalized_keyword in seen_topic_keywords
                        or _is_noisy_keyword(normalized_keyword)
                    ):
                        continue
                    seen_topic_keywords.add(normalized_keyword)
                    topic_keywords.append(normalized_keyword)
                embedding_ref = row.get("embedding_ref") or row.get("embedding_vector")
                vector = _parse_embedding_vector(embedding_ref)
                if not vector and enable_similarity_text_fallback:
                    vector = _build_text_fallback_vector(
                        title=title,
                        summary=summary,
                        topic_keywords=topic_keywords,
                        hierarchy_path=hierarchy_path,
                    )
                    if vector:
                        text_fallback_vectors += 1
                if vector:
                    doc_vectors[doc_id] = vector

                cur.execute(
                    """
                    INSERT INTO ontology_documents
                    (doc_id, item_id, title, summary, source_system, content_quality_score, freshness_status,
                     creator_person_id, primary_project_id, primary_org_id, canonical_doc_id, document_kind,
                     lineage_id, correlation_id, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(doc_id) DO UPDATE SET
                        item_id=excluded.item_id,
                        title=excluded.title,
                        summary=excluded.summary,
                        source_system=excluded.source_system,
                        content_quality_score=excluded.content_quality_score,
                        freshness_status=excluded.freshness_status,
                        creator_person_id=excluded.creator_person_id,
                        primary_project_id=excluded.primary_project_id,
                        primary_org_id=excluded.primary_org_id,
                        canonical_doc_id=excluded.canonical_doc_id,
                        document_kind=excluded.document_kind,
                        lineage_id=excluded.lineage_id,
                        correlation_id=excluded.correlation_id,
                        updated_at=excluded.updated_at
                    """,
                    (
                        doc_id,
                        item_id,
                        title,
                        summary,
                        source_system,
                        quality_score,
                        freshness_status,
                        creator_person_id or None,
                        primary_project_id or None,
                        primary_org_id or None,
                        canonical_doc_id or None,
                        document_kind,
                        lineage_id,
                        correlation_id,
                        updated_at,
                    ),
                )
                projected_documents += 1

                entity_specs = [
                    (
                        "person",
                        creator_person_id,
                        _to_text(row.get("creator_name"), owner_name or creator_person_id),
                        "created_by",
                    ),
                    (
                        "project",
                        primary_project_id,
                        _to_text(row.get("project_name"), project_name or primary_project_id),
                        "belongs_to_project",
                    ),
                    ("organization", primary_org_id, _to_text(row.get("organization_name"), primary_org_id), "owned_by"),
                ]
                entity_specs.extend(
                    [
                        ("topic_category", category, category, "categorized_as")
                        for category in topic_categories
                    ]
                )
                for entity_type, entity_key_raw, entity_label, relation in entity_specs:
                    if not entity_key_raw:
                        continue
                    entity_id = _entity_key(entity_type, entity_key_raw)
                    if entity_id not in seen_entity_ids:
                        cur.execute(
                            """
                            INSERT INTO ontology_entities
                            (entity_id, canonical_name, entity_type, resolution_status, confidence, pii_flag,
                             extraction_source, spread_factor, lineage_id, correlation_id, updated_at)
                            VALUES (?, ?, ?, 'resolved', 1.0, 0, 'aws_projection', 1, ?, ?, ?)
                            ON CONFLICT(entity_id) DO UPDATE SET
                                canonical_name=excluded.canonical_name,
                                entity_type=excluded.entity_type,
                                resolution_status='resolved',
                                confidence=MAX(ontology_entities.confidence, excluded.confidence),
                                updated_at=excluded.updated_at
                            """,
                            (entity_id, entity_label, entity_type, lineage_id, correlation_id, updated_at),
                        )
                        seen_entity_ids.add(entity_id)
                        projected_entities += 1

                    link_id = f"LINK-{doc_id}-{entity_id}-{relation}"
                    cur.execute(
                        """
                        INSERT INTO ontology_document_entity_links
                        (link_id, doc_id, entity_id, relation_type, weight, evidence, signal_source)
                        VALUES (?, ?, ?, ?, 1.0, ?, 'projection')
                        ON CONFLICT(link_id) DO UPDATE SET
                            relation_type=excluded.relation_type,
                            weight=excluded.weight,
                            evidence=excluded.evidence,
                            signal_source=excluded.signal_source
                        """,
                        (link_id, doc_id, entity_id, relation, "AWS projected relation"),
                    )
                    projected_links += 1

                if enable_contained_in:
                    segments = _normalize_hierarchy_segments(hierarchy_path)
                    if segments:
                        cumulative_segments: List[str] = []
                        folder_ids_in_path: List[str] = []
                        for index, segment in enumerate(segments):
                            cumulative_segments.append(segment)
                            folder_path = "/".join(cumulative_segments)
                            folder_id = _stable_id("FOLDER", f"{tenant_id}:{folder_path}")
                            folder_ids_in_path.append(folder_id)
                            if folder_id not in folder_cache:
                                cur.execute(
                                    """
                                    INSERT INTO ontology_folder_nodes
                                    (folder_id, tenant_id, folder_name, folder_path, depth, updated_at)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                    ON CONFLICT(folder_id) DO UPDATE SET
                                        folder_name=excluded.folder_name,
                                        folder_path=excluded.folder_path,
                                        depth=excluded.depth,
                                        updated_at=excluded.updated_at
                                    """,
                                    (folder_id, tenant_id, segment, folder_path, index + 1, updated_at),
                                )
                                folder_cache.add(folder_id)
                        for parent_folder_id, child_folder_id in zip(
                            folder_ids_in_path,
                            folder_ids_in_path[1:],
                        ):
                            hierarchy_link_id = _stable_id(
                                "FHLINK",
                                f"{parent_folder_id}:{child_folder_id}:folder_parent_of",
                            )
                            cur.execute(
                                """
                                INSERT INTO ontology_folder_hierarchy_links
                                (link_id, parent_folder_id, child_folder_id, relation_type, weight, evidence)
                                VALUES (?, ?, ?, 'folder_parent_of', 1.0, ?)
                                ON CONFLICT(link_id) DO UPDATE SET
                                    relation_type=excluded.relation_type,
                                    weight=excluded.weight,
                                    evidence=excluded.evidence
                                """,
                                (
                                    hierarchy_link_id,
                                    parent_folder_id,
                                    child_folder_id,
                                    "Folder hierarchy projection",
                                ),
                            )
                            folder_hierarchy_links += 1
                        leaf_path = "/".join(cumulative_segments)
                        leaf_folder_id = _stable_id("FOLDER", f"{tenant_id}:{leaf_path}")
                        contained_link_id = _stable_id("DFLINK", f"{doc_id}:{leaf_folder_id}:contained_in")
                        cur.execute(
                            """
                            INSERT INTO ontology_document_folder_links
                            (link_id, doc_id, folder_id, relation_type, weight, evidence)
                            VALUES (?, ?, ?, 'contained_in', 1.0, ?)
                            ON CONFLICT(link_id) DO UPDATE SET
                                relation_type=excluded.relation_type,
                                weight=excluded.weight,
                                evidence=excluded.evidence
                            """,
                            (contained_link_id, doc_id, leaf_folder_id, "Hierarchy path projection"),
                        )
                        contained_in_links += 1

                if enable_mentions_entity:
                    mention_entities: List[str] = []
                    mention_entity_seen: set[str] = set()

                    def _append_mention_entity(entity_id: str) -> None:
                        normalized_entity_id = str(entity_id or "").strip()
                        if not normalized_entity_id or normalized_entity_id in mention_entity_seen:
                            return
                        mention_entity_seen.add(normalized_entity_id)
                        mention_entities.append(normalized_entity_id)

                    for existing_entity_id in sorted(mention_by_item_id.get(item_id, set())):
                        _append_mention_entity(existing_entity_id)
                    normalized_keywords = sorted({keyword.lower() for keyword in topic_keywords})
                    for keyword in topic_keywords:
                        normalized_keyword = keyword.lower()
                        for existing_entity_id in sorted(
                            entity_name_to_ids.get(normalized_keyword, set())
                        ):
                            _append_mention_entity(existing_entity_id)
                    if summary:
                        summary_lc = summary.lower()
                        for entity_name, entity_ids in entity_name_to_ids.items():
                            if not entity_name:
                                continue
                            normalized_entity_name = entity_name.strip().lower()
                            if _is_noisy_keyword(normalized_entity_name):
                                continue
                            if normalized_entity_name not in summary_lc:
                                continue
                            for existing_entity_id in sorted(entity_ids):
                                _append_mention_entity(existing_entity_id)
                    if enable_mentions_autopromote:
                        auto_promoted_for_document = 0
                        for keyword in normalized_keywords:
                            if auto_promoted_for_document >= mentions_autopromote_max_per_doc:
                                break
                            if not _is_valid_autopromote_keyword(
                                keyword,
                                stopwords=mentions_autopromote_stopwords,
                                min_alnum_chars=mentions_autopromote_min_alnum_chars,
                                min_cjk_chars=mentions_autopromote_min_cjk_chars,
                            ):
                                continue
                            if keyword in entity_name_to_ids:
                                continue
                            concept_id = _entity_key("concept", keyword)
                            if concept_id not in seen_entity_ids:
                                cur.execute(
                                    """
                                    INSERT INTO ontology_entities
                                    (entity_id, canonical_name, entity_type, resolution_status, confidence, pii_flag,
                                     extraction_source, spread_factor, lineage_id, correlation_id, updated_at)
                                    VALUES (?, ?, 'concept', 'resolved', 0.7, 0, 'mentions_autopromote', 1, ?, ?, ?)
                                    ON CONFLICT(entity_id) DO UPDATE SET
                                        canonical_name=excluded.canonical_name,
                                        entity_type=excluded.entity_type,
                                        updated_at=excluded.updated_at
                                    """,
                                    (concept_id, keyword, lineage_id, correlation_id, updated_at),
                                )
                                seen_entity_ids.add(concept_id)
                                projected_entities += 1
                                auto_promoted_entities += 1
                                auto_promoted_for_document += 1
                            entity_name_to_ids.setdefault(keyword, set()).add(concept_id)
                            _append_mention_entity(concept_id)
                    for entity_id in mention_entities[:mentions_max_links_per_doc]:
                        mentions_link_id = _stable_id(
                            "MELINK",
                            f"{doc_id}:{entity_id}:mentions_entity",
                        )
                        cur.execute(
                            """
                            INSERT INTO ontology_document_entity_links
                            (link_id, doc_id, entity_id, relation_type, weight, evidence, signal_source)
                            VALUES (?, ?, ?, 'mentions_entity', ?, ?, 'summary_ner')
                            ON CONFLICT(link_id) DO UPDATE SET
                                relation_type=excluded.relation_type,
                                weight=excluded.weight,
                                evidence=excluded.evidence,
                                signal_source=excluded.signal_source
                            """,
                            (mentions_link_id, doc_id, entity_id, 0.85, "summary/ner mention projection"),
                        )
                        mentions_links += 1

                if canonical_doc_id and canonical_doc_id != doc_id:
                    lineage_link_id = f"DOC-LIN-{doc_id}-{canonical_doc_id}"
                    cur.execute(
                        """
                        INSERT INTO ontology_document_lineage_links
                        (link_id, source_doc_id, target_doc_id, relation_type, weight, evidence)
                        VALUES (?, ?, ?, 'derived_from', 0.92, ?)
                        ON CONFLICT(link_id) DO UPDATE SET
                            source_doc_id=excluded.source_doc_id,
                            target_doc_id=excluded.target_doc_id,
                            relation_type=excluded.relation_type,
                            weight=excluded.weight,
                            evidence=excluded.evidence
                        """,
                        (lineage_link_id, doc_id, canonical_doc_id, "AWS projected lineage relation"),
                    )
                    projected_lineage_links += 1

            if enable_similarity:
                vector_items = list(doc_vectors.items())
                if len(vector_items) > similarity_max_docs:
                    vector_items = vector_items[:similarity_max_docs]
                else:
                    skipped_similarity_docs = max(0, len(doc_to_item_id) - len(vector_items))

                similarity_pairs: Dict[Tuple[str, str], float] = {}
                for source_doc_id, source_vector in vector_items:
                    scored: List[Tuple[str, float]] = []
                    for target_doc_id, target_vector in vector_items:
                        if source_doc_id == target_doc_id:
                            continue
                        score = _cosine_similarity(source_vector, target_vector)
                        if score >= similarity_threshold:
                            scored.append((target_doc_id, score))
                    scored.sort(key=lambda item: item[1], reverse=True)
                    for target_doc_id, score in scored[:top_k_neighbors]:
                        doc_pair = tuple(sorted((source_doc_id, target_doc_id)))
                        existing_score = similarity_pairs.get(doc_pair, 0.0)
                        if score > existing_score:
                            similarity_pairs[doc_pair] = score

                for (source_doc_id, target_doc_id), score in similarity_pairs.items():
                    similarity_link_id = _stable_id(
                        "SIMLINK",
                        f"{source_doc_id}:{target_doc_id}:similar_to",
                    )
                    cur.execute(
                        """
                        INSERT INTO ontology_document_similarity_links
                        (link_id, source_doc_id, target_doc_id, relation_type, similarity_score, evidence, algorithm, updated_at)
                        VALUES (?, ?, ?, 'similar_to', ?, ?, 'cosine', CURRENT_TIMESTAMP)
                        ON CONFLICT(link_id) DO UPDATE SET
                            relation_type=excluded.relation_type,
                            similarity_score=excluded.similarity_score,
                            evidence=excluded.evidence,
                            algorithm=excluded.algorithm,
                            updated_at=excluded.updated_at
                        """,
                        (similarity_link_id, source_doc_id, target_doc_id, score, "embedding cosine top-k"),
                    )
                    similarity_links += 1

            conn.commit()
            return {
                "projected_documents": projected_documents,
                "projected_entities": projected_entities,
                "projected_links": projected_links,
                "projected_lineage_links": projected_lineage_links,
                "contained_in_links": contained_in_links,
                "folder_hierarchy_links": folder_hierarchy_links,
                "mentions_links": mentions_links,
                "similarity_links": similarity_links,
                "skipped_similarity_docs": skipped_similarity_docs,
                "text_fallback_vectors": text_fallback_vectors,
                "auto_promoted_entities": auto_promoted_entities,
            }


def _normalize_for_similarity(value: str) -> str:
    normalized = (value or "").strip().lower()
    for token in ["株式会社", "合同会社", "有限会社", "corp", "inc", "co.,ltd.", "co., ltd."]:
        normalized = normalized.replace(token.lower(), "")
    return normalized.replace(" ", "").replace("-", "").replace("_", "")


def load_ontology_entity_candidate_rows_from_graph_db(
    tenant_id: str,
) -> tuple[list[dict[str, Any]], int, int]:
    """テナント別 SQLite（/ontology/entity-candidates と同一 DB）から候補行を読む。

    DynamoDB ``AIReadyOntology-EntityCandidate`` へ書き込むパイプラインが無い環境では
    テーブルが空のままとなる。オーバービューの件数・high_spread は UI と整合させるため、
    SQLite に 1 件以上あればそちらを正とする。
    """
    with _tenant_scope(tenant_id):
        initialize_ontology_graph_db()
        seed_ontology_graph_sample_data(tenant_id=tenant_id, force=False)
    with _connect() as conn:
        cur = conn.cursor()
        rows_raw = cur.execute(
            """
            SELECT candidate_id, surface_form, entity_type, item_id, status, resolved_entity_id
            FROM ontology_entity_candidates
            """
        ).fetchall()
        pending_row = cur.execute(
            """
            SELECT COUNT(1) AS c FROM ontology_entity_candidates
            WHERE LOWER(COALESCE(status, '')) = 'pending'
            """
        ).fetchone()
        pending = int(pending_row["c"] if pending_row else 0)
    rows: list[dict[str, Any]] = []
    for r in rows_raw:
        rows.append(
            {
                "tenant_id": tenant_id,
                "candidate_id": r["candidate_id"],
                "surface_form": r["surface_form"],
                "entity_type": r["entity_type"],
                "item_id": r["item_id"],
                "status": r["status"],
                "resolved_entity_id": r["resolved_entity_id"],
            }
        )
    return rows, len(rows), pending


def _candidate_match_score(surface_form: str, canonical_name: str) -> float:
    left = _normalize_for_similarity(surface_form)
    right = _normalize_for_similarity(canonical_name)
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    if left.startswith(right) or right.startswith(left):
        return 0.95
    if left in right or right in left:
        return 0.9
    return SequenceMatcher(None, left, right).ratio()


def get_ontology_entity_candidates(
    tenant_id: str,
    limit: int = 100,
    offset: int = 0,
    status: str = "pending",
) -> Dict[str, Any]:
    with _tenant_scope(tenant_id):
        initialize_ontology_graph_db()
        seed_ontology_graph_sample_data(tenant_id=tenant_id, force=False)
        effective_limit = max(1, min(limit, 500))
        effective_offset = max(0, offset)

    with _connect() as conn:
        cur = conn.cursor()
        acme_exists = cur.execute(
            "SELECT COUNT(1) AS c FROM ontology_entities WHERE canonical_name = ?",
            ("株式会社ACME",),
        ).fetchone()["c"]
        if acme_exists == 0:
            acme_entity_id = _next_entity_id(conn, "organization")
            cur.execute(
                """
                INSERT INTO ontology_entities
                (entity_id, canonical_name, entity_type, resolution_status, confidence, pii_flag, extraction_source, spread_factor, lineage_id, correlation_id, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    acme_entity_id,
                    "株式会社ACME",
                    "organization",
                    "resolved",
                    0.95,
                    0,
                    "seed_bootstrap",
                    1,
                    "lin-org-bootstrap-acme",
                    "corr-org-bootstrap-acme",
                    "2026-02-28 00:00:00 UTC",
                ),
            )
            conn.commit()
        candidates = cur.execute(
            """
            SELECT candidate_id, surface_form, entity_type, extraction_source, confidence, pii_flag, item_id, received_at, status
            FROM ontology_entity_candidates
            WHERE status = ?
            ORDER BY confidence ASC, received_at DESC
            """,
            (status,),
        ).fetchall()
        dictionary_entities = cur.execute(
            """
            SELECT entity_id, canonical_name, entity_type, confidence, pii_flag, updated_at
            FROM ontology_entities
            ORDER BY updated_at DESC
            """
        ).fetchall()

    all_rows: List[Dict[str, Any]] = []
    for candidate in candidates:
        suggestions = []
        for entity in dictionary_entities:
            if entity["entity_type"] != candidate["entity_type"]:
                continue
            score = _candidate_match_score(candidate["surface_form"], entity["canonical_name"])
            if score < 0.5:
                continue
            suggestions.append(
                {
                    "entity_id": entity["entity_id"],
                    "canonical_name": entity["canonical_name"],
                    "score": round(score, 3),
                    "confidence": float(entity["confidence"]),
                    "pii_flag": bool(entity["pii_flag"]),
                    "updated_at": entity["updated_at"],
                }
            )
        suggestions.sort(key=lambda item: (item["score"], item["confidence"]), reverse=True)
        # Exact dictionary match (100%) is treated as already resolved and should not remain in candidate queue.
        if suggestions and suggestions[0]["score"] >= 1.0:
            continue
        all_rows.append(
            {
                "candidate_id": candidate["candidate_id"],
                "surface_form": candidate["surface_form"],
                "entity_type": candidate["entity_type"],
                "extraction_source": candidate["extraction_source"],
                "confidence": float(candidate["confidence"]),
                "pii_flag": bool(candidate["pii_flag"]),
                "item_id": candidate["item_id"],
                "received_at": candidate["received_at"],
                "status": candidate["status"],
                "suggestions": suggestions[:3],
            }
        )

    total_count = len(all_rows)
    rows = all_rows[effective_offset : effective_offset + effective_limit]

    return {
        "rows": rows,
        "pagination": {
            "limit": effective_limit,
            "offset": effective_offset,
            "total_count": total_count,
        },
    }


def _next_entity_id(conn: sqlite3.Connection, entity_type: str) -> str:
    prefix_map = {
        "organization": "ENT-ORG",
        "person": "ENT-PER",
        "project": "ENT-PROJ",
        "system": "ENT-SYS",
        "policy": "ENT-POL",
    }
    prefix = prefix_map.get(entity_type, "ENT-ETC")
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT entity_id FROM ontology_entities WHERE entity_type = ?",
        (entity_type,),
    ).fetchall()
    max_index = 0
    for row in rows:
        entity_id = row["entity_id"]
        if not entity_id.startswith(prefix + "-"):
            continue
        suffix = entity_id.split("-")[-1]
        if suffix.isdigit():
            max_index = max(max_index, int(suffix))
    return f"{prefix}-{max_index + 1:03d}"


def resolve_ontology_entity_candidate_existing(
    tenant_id: str,
    candidate_id: str,
    target_entity_id: str,
    operator: str = "ui-user",
) -> Dict[str, Any]:
    with _tenant_scope(tenant_id):
        initialize_ontology_graph_db()
        seed_ontology_graph_sample_data(tenant_id=tenant_id, force=False)
        now = "2026-02-28 00:00:00 UTC"
        conn = _connect()
        cur = conn.cursor()
        candidate = cur.execute(
            """
            SELECT candidate_id, surface_form, entity_type, item_id, status
            FROM ontology_entity_candidates
            WHERE candidate_id = ?
            """,
            (candidate_id,),
        ).fetchone()
        if candidate is None:
            raise ValueError(f"Candidate not found: {candidate_id}")
        if candidate["status"] != "pending":
            raise ValueError(f"Candidate is already resolved: {candidate_id}")

        entity = cur.execute(
            """
            SELECT entity_id, canonical_name
            FROM ontology_entities
            WHERE entity_id = ?
            """,
            (target_entity_id,),
        ).fetchone()
        if entity is None:
            raise ValueError(f"Dictionary entity not found: {target_entity_id}")

        surface_id = f"SF-{candidate_id}"
        cur.execute(
            """
            INSERT OR IGNORE INTO ontology_entity_surface_forms
            (surface_id, entity_id, surface_form, source_item_id, source_candidate_id, resolution_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                surface_id,
                target_entity_id,
                candidate["surface_form"],
                candidate["item_id"],
                candidate["candidate_id"],
                "merge_existing",
                now,
            ),
        )
        cur.execute(
            """
            UPDATE ontology_entity_candidates
            SET status = 'resolved',
                resolved_entity_id = ?,
                resolution_type = 'merge_existing',
                resolved_at = ?
            WHERE candidate_id = ?
            """,
            (target_entity_id, now, candidate_id),
        )
        conn.commit()
        return {
            "status": "resolved",
            "candidate_id": candidate_id,
            "resolution_type": "merge_existing",
            "entity_id": target_entity_id,
            "canonical_name": entity["canonical_name"],
            "resolved_by": operator,
        }


def register_ontology_entity_candidate_new(
    tenant_id: str,
    candidate_id: str,
    operator: str = "ui-user",
) -> Dict[str, Any]:
    with _tenant_scope(tenant_id):
        initialize_ontology_graph_db()
        seed_ontology_graph_sample_data(tenant_id=tenant_id, force=False)
        now = "2026-02-28 00:00:00 UTC"
        conn = _connect()
        cur = conn.cursor()
        candidate = cur.execute(
            """
            SELECT candidate_id, surface_form, entity_type, extraction_source, confidence, pii_flag, item_id, status
            FROM ontology_entity_candidates
            WHERE candidate_id = ?
            """,
            (candidate_id,),
        ).fetchone()
        if candidate is None:
            raise ValueError(f"Candidate not found: {candidate_id}")
        if candidate["status"] != "pending":
            raise ValueError(f"Candidate is already resolved: {candidate_id}")

        new_entity_id = _next_entity_id(conn, candidate["entity_type"])
        cur.execute(
            """
            INSERT INTO ontology_entities
            (entity_id, canonical_name, entity_type, resolution_status, confidence, pii_flag, extraction_source, spread_factor, lineage_id, correlation_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_entity_id,
                candidate["surface_form"],
                candidate["entity_type"],
                "resolved",
                max(float(candidate["confidence"]), 0.7),
                int(candidate["pii_flag"]),
                "manual_register",
                1,
                f"lin-new-{candidate_id.lower()}",
                f"corr-new-{candidate_id.lower()}",
                now,
            ),
        )
        surface_id = f"SF-{candidate_id}"
        cur.execute(
            """
            INSERT OR IGNORE INTO ontology_entity_surface_forms
            (surface_id, entity_id, surface_form, source_item_id, source_candidate_id, resolution_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                surface_id,
                new_entity_id,
                candidate["surface_form"],
                candidate["item_id"],
                candidate["candidate_id"],
                "register_new",
                now,
            ),
        )
        cur.execute(
            """
            UPDATE ontology_entity_candidates
            SET status = 'resolved',
                resolved_entity_id = ?,
                resolution_type = 'register_new',
                resolved_at = ?
            WHERE candidate_id = ?
            """,
            (new_entity_id, now, candidate_id),
        )
        conn.commit()
        return {
            "status": "resolved",
            "candidate_id": candidate_id,
            "resolution_type": "register_new",
            "entity_id": new_entity_id,
            "canonical_name": candidate["surface_form"],
            "resolved_by": operator,
        }


def _derive_remediation_state(doc_id: str) -> str:
    suffix = str(doc_id).split("-")[-1]
    try:
        idx = int(suffix)
    except ValueError:
        return "ai_proposed"
    if idx % 8 == 0:
        return "executed"
    if idx % 5 == 0:
        return "approved"
    if idx % 2 == 0:
        return "pending_approval"
    return "ai_proposed"


def _quality_boost_by_state(remediation_state: str) -> Tuple[float, float, float]:
    # naming(relevance), dedup(uniqueness), freshness
    if remediation_state == "executed":
        return 0.07, 0.08, 0.07
    if remediation_state == "approved":
        return 0.04, 0.05, 0.04
    return 0.0, 0.0, 0.0


def _clamp_score(value: float) -> float:
    return max(0.05, min(0.99, value))


"""Ontology remediation workflow functions removed."""
