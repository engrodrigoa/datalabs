-- Idempotent. Applied by docker-entrypoint (fresh volume) and by dag_setup_infrastructure (existing volume).
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS ctrl;          -- ingestion control (what was downloaded, when)
CREATE SCHEMA IF NOT EXISTS audit;         -- observability facts: step runs, data health checks
CREATE SCHEMA IF NOT EXISTS obs;           -- observability views consumed by Grafana
CREATE SCHEMA IF NOT EXISTS bronze;        -- raw landing tables (ANP)
CREATE SCHEMA IF NOT EXISTS landing_rfb;   -- raw landing tables (RFB)
CREATE SCHEMA IF NOT EXISTS stg;           -- dbt staging views
CREATE SCHEMA IF NOT EXISTS silver;        -- dbt conformed tables
CREATE SCHEMA IF NOT EXISTS gold;          -- dbt dimensional models
CREATE SCHEMA IF NOT EXISTS elementary;    -- Elementary (dbt run/test history, anomalies)
CREATE SCHEMA IF NOT EXISTS ai;            -- vector store + RAG evaluation
