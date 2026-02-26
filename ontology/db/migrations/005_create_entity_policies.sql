-- Phase 1 / T-006
-- Entity policy table

CREATE TABLE IF NOT EXISTS ontology.entity_policies (
    policy_id VARCHAR(64) PRIMARY KEY DEFAULT uuid_generate_v4()::text,
    entity_id VARCHAR(64) NOT NULL REFERENCES ontology.entity_master(entity_id),
    action VARCHAR(32) NOT NULL,
    principal_type VARCHAR(32) NOT NULL,
    principal_value VARCHAR(128) NOT NULL,
    effect VARCHAR(8) NOT NULL DEFAULT 'allow',
    condition JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_effect CHECK (effect IN ('allow', 'deny'))
);

CREATE INDEX IF NOT EXISTS idx_policies_entity_id
    ON ontology.entity_policies (entity_id);
