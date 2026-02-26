"""Entity candidate model."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class EntityCandidate:
    """Entity candidate extracted by governance analysis."""

    candidate_id: str
    tenant_id: str
    source_item_id: str
    surface_form: str
    normalized_form: str
    entity_type: str
    pii_flag: bool
    extraction_source: str
    confidence: float
    mention_count: int
    context_snippet: str
    ner_label: str
    language: str
    source_title: str
    extracted_at: str
    pii_category: str = ""
    analysis_id: str = ""
    lineage_id: str = ""
    source: str = "document_analysis"
