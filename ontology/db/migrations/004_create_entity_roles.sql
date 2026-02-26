-- Phase 1 / T-006
-- Entity role table

CREATE TABLE IF NOT EXISTS ontology.entity_roles (
    role_id VARCHAR(64) PRIMARY KEY DEFAULT uuid_generate_v4()::text,
    entity_id VARCHAR(64) NOT NULL REFERENCES ontology.entity_master(entity_id),
    role_name VARCHAR(128) NOT NULL,
    scope VARCHAR(256),
    scope_type VARCHAR(32),
    valid_from TIMESTAMPTZ,
    valid_to TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_roles_entity_id
    ON ontology.entity_roles (entity_id);
CREATE INDEX IF NOT EXISTS idx_roles_active
    ON ontology.entity_roles (entity_id)
    WHERE valid_to IS NULL;
