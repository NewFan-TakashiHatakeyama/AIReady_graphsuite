-- Phase 1 / T-006
-- SQL functions used by reconciliation jobs

CREATE OR REPLACE FUNCTION ontology.get_max_spread_factor(target_item_id TEXT)
RETURNS INTEGER
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(MAX(em.spread_factor), 0)
    FROM ontology.entity_master em
    JOIN ontology.entity_aliases ea ON ea.entity_id = em.entity_id
    WHERE ea.source_document_id = target_item_id
      AND em.status = 'active';
$$;
