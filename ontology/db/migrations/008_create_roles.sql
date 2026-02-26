-- Phase 1 / T-006
-- Application and reader roles

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ontology_app') THEN
        CREATE ROLE ontology_app LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ontology_ai_reader') THEN
        CREATE ROLE ontology_ai_reader LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'governance_reader') THEN
        CREATE ROLE governance_reader LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ontology_admin') THEN
        CREATE ROLE ontology_admin LOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA ontology TO ontology_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ontology TO ontology_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ontology TO ontology_app;

GRANT USAGE ON SCHEMA ontology TO ontology_ai_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ontology TO ontology_ai_reader;

GRANT USAGE ON SCHEMA ontology TO governance_reader;
GRANT SELECT ON ontology.entity_master TO governance_reader;
GRANT SELECT ON ontology.entity_aliases TO governance_reader;

GRANT ALL PRIVILEGES ON SCHEMA ontology TO ontology_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ontology TO ontology_admin;
