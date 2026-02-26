-- Phase 1 / T-006
-- Base schema and required PostgreSQL extensions

CREATE SCHEMA IF NOT EXISTS ontology;

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
