-- Phase 1 / T-006
-- Audit log table with monthly partitions

CREATE TABLE IF NOT EXISTS ontology.entity_audit_log (
    log_id VARCHAR(64) NOT NULL,
    entity_id VARCHAR(64) NOT NULL,
    action VARCHAR(32) NOT NULL,
    actor VARCHAR(128) NOT NULL DEFAULT 'system',
    detail JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (log_id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE IF NOT EXISTS ontology.entity_audit_log_2026_01
    PARTITION OF ontology.entity_audit_log
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE IF NOT EXISTS ontology.entity_audit_log_2026_02
    PARTITION OF ontology.entity_audit_log
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE IF NOT EXISTS ontology.entity_audit_log_2026_03
    PARTITION OF ontology.entity_audit_log
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE INDEX IF NOT EXISTS idx_audit_entity
    ON ontology.entity_audit_log (entity_id, created_at DESC);
