-- RTC reference tables (ADR 0019): the official CST / cClassTrib / NCM tables of the Receita Federal tax calculator
-- (github.com/ReceitaFederal/calculadora-rtc-regime-regular), rebuilt by rtc01_fetch and loaded by rtc02_load.
--
-- ref_rtc.<table>   one table per calculator table the pipeline reads, lowercase, columns as in the source (raw:
--                   dates stay text 'YYYY-MM-DD', flags stay 0/1). Owned by rtc02_load, which replaces their content
--                   in one transaction per version; typing and business names live in dbt (stg_rtc__*).
-- ctrl.rtc_carga    one row per version loaded: the lineage of every reference row (which release, which git sha).

CREATE SCHEMA IF NOT EXISTS ref_rtc;

CREATE TABLE IF NOT EXISTS ctrl.rtc_carga (
    carga_id          bigserial PRIMARY KEY,
    sha               text NOT NULL,                -- git commit of the mirror that was built
    contract_version  int  NOT NULL DEFAULT 1,      -- flyway_replay.CONTRACT_VERSION: which tables were exported
    release           text,                         -- pom.xml <version> (e.g. 1.6.1)
    database_version  text,                         -- last migration recorded by the scripts (e.g. V0059)
    source_repo       text NOT NULL,
    built_at          timestamptz,                  -- rtc01 (replay of the Flyway scripts)
    loaded_at         timestamptz NOT NULL DEFAULT now(),
    row_counts        jsonb NOT NULL,               -- {"CLASSIFICACAO_TRIBUTARIA": 193, ...}
    schema_changes    jsonb NOT NULL DEFAULT '[]'::jsonb,  -- tables recreated because their columns changed
    step_run_id       uuid                          -- -> audit.pipeline_step_runs
);

-- contract v2 (2026-11): a version is (sha, contract_version); databases created with v1 are migrated in place
ALTER TABLE ctrl.rtc_carga ADD COLUMN IF NOT EXISTS contract_version int NOT NULL DEFAULT 1;
ALTER TABLE ctrl.rtc_carga DROP CONSTRAINT IF EXISTS rtc_carga_sha_key;
CREATE UNIQUE INDEX IF NOT EXISTS ux_rtc_carga_sha_contract ON ctrl.rtc_carga (sha, contract_version);
CREATE INDEX IF NOT EXISTS ix_rtc_carga_loaded_at ON ctrl.rtc_carga (loaded_at DESC);

COMMENT ON TABLE ctrl.rtc_carga IS
    'One row per RTC reference version loaded into ref_rtc (ADR 0019). The latest row is the version in use.';
