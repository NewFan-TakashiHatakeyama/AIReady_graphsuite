-- Phase 1 / T-006
-- Alias table

CREATE TABLE IF NOT EXISTS ontology.entity_aliases (
    alias_id VARCHAR(64) PRIMARY KEY DEFAULT uuid_generate_v4()::text,
    entity_id VARCHAR(64) NOT NULL REFERENCES ontology.entity_master(entity_id),
    alias_value BYTEA,
    alias_value_text VARCHAR(512),
    alias_hash VARCHAR(64) NOT NULL,
    alias_type VARCHAR(32),
    source_system VARCHAR(64),
    source_document_id VARCHAR(128),
    confidence REAL NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_alias_hash_entity UNIQUE (alias_hash, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_aliases_entity_id
    ON ontology.entity_aliases (entity_id);
CREATE INDEX IF NOT EXISTS idx_aliases_source_document
    ON ontology.entity_aliases (source_document_id)
    WHERE source_document_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_aliases_hash
    ON ontology.entity_aliases (alias_hash);
