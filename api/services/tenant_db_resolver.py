"""Tenant-aware DB/resource resolver for repository layer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.runtime_config import AwsRuntimeConfig, load_tenant_registry


@dataclass(frozen=True)
class TenantDbBinding:
    tenant_id: str
    governance_finding_table_name: str
    governance_document_analysis_table_name: str
    ontology_sqlite_path: str
    ontology_unified_metadata_table_name: str
    ontology_entity_candidate_table_name: str
    ontology_lineage_event_table_name: str
    ontology_entity_master_table_name: str
    ontology_entity_audit_table_name: str


class TenantDbResolver:
    def __init__(self, config: AwsRuntimeConfig):
        self._config = config
        self._registry: dict[str, Any] = load_tenant_registry()

    @staticmethod
    def _validate_tenant_id(tenant_id: str) -> str:
        normalized = str(tenant_id or "").strip()
        if not normalized:
            raise ValueError("tenant_id is required for repository access.")
        return normalized

    def resolve(self, tenant_id: str) -> TenantDbBinding:
        normalized_tenant_id = self._validate_tenant_id(tenant_id)
        tenant_override = self._registry.get(normalized_tenant_id, {})
        if tenant_override and not isinstance(tenant_override, dict):
            raise ValueError(
                f"Invalid tenant override for tenant '{normalized_tenant_id}'."
            )

        finding_table_name = str(
            tenant_override.get(
                "governance_finding_table_name",
                self._config.governance_finding_table_name,
            )
        ).strip()
        document_analysis_table_name = str(
            tenant_override.get(
                "governance_document_analysis_table_name",
                self._config.governance_document_analysis_table_name,
            )
        ).strip()
        ontology_sqlite_path = str(
            Path(self._config.ontology_db_root)
            / normalized_tenant_id
            / tenant_override.get("ontology_sqlite_filename", "ontology_graph.db")
        )
        ontology_unified_metadata_table_name = str(
            tenant_override.get(
                "ontology_unified_metadata_table_name",
                "AIReadyOntology-UnifiedMetadata",
            )
        ).strip()
        ontology_entity_candidate_table_name = str(
            tenant_override.get(
                "ontology_entity_candidate_table_name",
                "AIReadyOntology-EntityCandidate",
            )
        ).strip()
        ontology_lineage_event_table_name = str(
            tenant_override.get(
                "ontology_lineage_event_table_name",
                "AIReadyOntology-LineageEvent",
            )
        ).strip()
        ontology_entity_master_table_name = str(
            tenant_override.get(
                "ontology_entity_master_table_name",
                "AIReadyOntology-EntityMaster",
            )
        ).strip()
        ontology_entity_audit_table_name = str(
            tenant_override.get(
                "ontology_entity_audit_table_name",
                "",
            )
        ).strip()

        return TenantDbBinding(
            tenant_id=normalized_tenant_id,
            governance_finding_table_name=finding_table_name,
            governance_document_analysis_table_name=document_analysis_table_name,
            ontology_sqlite_path=ontology_sqlite_path,
            ontology_unified_metadata_table_name=ontology_unified_metadata_table_name,
            ontology_entity_candidate_table_name=ontology_entity_candidate_table_name,
            ontology_lineage_event_table_name=ontology_lineage_event_table_name,
            ontology_entity_master_table_name=ontology_entity_master_table_name,
            ontology_entity_audit_table_name=ontology_entity_audit_table_name,
        )
