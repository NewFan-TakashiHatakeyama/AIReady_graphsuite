-- Phase 1 / T-006
-- Gold master table

CREATE TABLE IF NOT EXISTS ontology.entity_master (
    entity_id VARCHAR(64) PRIMARY KEY,
    entity_type VARCHAR(32) NOT NULL,
    canonical_value BYTEA,
    canonical_value_text VARCHAR(512),
    canonical_hash VARCHAR(64) NOT NULL,
    pii_flag BOOLEAN NOT NULL DEFAULT false,
    pii_category VARCHAR(32),
    extraction_source VARCHAR(32) NOT NULL,
    confidence REAL NOT NULL DEFAULT 0.0,
    spread_factor INTEGER NOT NULL DEFAULT 0,
    mention_count INTEGER NOT NULL DEFAULT 1,
    related_items JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(16) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_entity_type CHECK (
        entity_type IN (
            'person', 'phone', 'email', 'credential', 'address', 'id_number',
            'document', 'organization', 'project', 'product', 'technology',
            'customer', 'location', 'contract', 'department', 'event',
            'concept', 'site', 'team', 'topic'
        )
    ),
    CONSTRAINT chk_extraction_source CHECK (
        extraction_source IN ('governance', 'ner', 'connect_metadata', 'noun_chunk', 'domain_dict')
    ),
    CONSTRAINT chk_status CHECK (status IN ('active', 'merged', 'orphan', 'deleted'))
);

CREATE INDEX IF NOT EXISTS idx_entity_master_type_hash
    ON ontology.entity_master (entity_type, canonical_hash);
CREATE INDEX IF NOT EXISTS idx_entity_master_type_status
    ON ontology.entity_master (entity_type, status)
    WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_entity_master_pii_flag
    ON ontology.entity_master (pii_flag)
    WHERE pii_flag = true;
CREATE INDEX IF NOT EXISTS idx_entity_master_extraction_source
    ON ontology.entity_master (extraction_source);
CREATE INDEX IF NOT EXISTS idx_entity_master_spread_factor
    ON ontology.entity_master (spread_factor DESC)
    WHERE status = 'active' AND pii_flag = true;
CREATE INDEX IF NOT EXISTS idx_entity_master_confidence
    ON ontology.entity_master (confidence ASC)
    WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_entity_master_canonical_text
    ON ontology.entity_master (canonical_value_text)
    WHERE pii_flag = false AND status = 'active';
CREATE INDEX IF NOT EXISTS idx_entity_master_hash_prefix
    ON ontology.entity_master (LEFT(canonical_hash, 8), entity_type)
    WHERE status = 'active';
