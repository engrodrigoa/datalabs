-- ============================================================================
-- Observability facts
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit.pipeline_step_runs (
    step_run_id    uuid PRIMARY KEY,
    pipeline       text        NOT NULL,
    step           text        NOT NULL,
    script         text,
    dag_id         text,
    dag_run_id     text,
    task_id        text,
    try_number     int,
    status         text        NOT NULL CHECK (status IN ('running','success','skipped','failed')),
    exit_code      int,
    started_at     timestamptz NOT NULL,
    finished_at    timestamptz,
    duration_s     double precision,
    rows_in        bigint,
    rows_out       bigint,
    rows_rejected  bigint,
    files_total    int,
    files_ok       int,
    files_failed   int,
    metrics        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    error_message  text,
    host           text
);
CREATE INDEX IF NOT EXISTS ix_step_runs_started   ON audit.pipeline_step_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS ix_step_runs_step      ON audit.pipeline_step_runs (pipeline, step, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_step_runs_dag_run   ON audit.pipeline_step_runs (dag_id, dag_run_id);

CREATE TABLE IF NOT EXISTS audit.data_quality_checks (
    id              bigserial PRIMARY KEY,
    check_run_id    uuid        NOT NULL,
    checked_at      timestamptz NOT NULL DEFAULT now(),
    pipeline        text,
    check_name      text        NOT NULL,
    check_type      text        NOT NULL,
    dataset         text,
    status          text        NOT NULL CHECK (status IN ('pass','warn','fail','error','skipped')),
    severity        text,
    observed_value  double precision,
    expected        text,
    message         text,
    dag_id          text,
    dag_run_id      text
);
CREATE INDEX IF NOT EXISTS ix_dq_checked ON audit.data_quality_checks (checked_at DESC);
CREATE INDEX IF NOT EXISTS ix_dq_name    ON audit.data_quality_checks (check_name, checked_at DESC);

-- ============================================================================
-- Views for Grafana (keep dashboard SQL short and stable)
-- ============================================================================
CREATE OR REPLACE VIEW obs.v_step_runs AS
SELECT r.*,
       CASE WHEN r.status = 'running' AND r.started_at < now() - interval '6 hours' THEN 'stale'
            ELSE r.status END                                        AS health,
       COALESCE(r.duration_s, EXTRACT(EPOCH FROM now() - r.started_at)) AS elapsed_s
FROM audit.pipeline_step_runs r;

CREATE OR REPLACE VIEW obs.v_step_last_run AS
SELECT DISTINCT ON (pipeline, step) *
FROM obs.v_step_runs
ORDER BY pipeline, step, started_at DESC;

CREATE OR REPLACE VIEW obs.v_check_last_result AS
SELECT DISTINCT ON (check_name) *
FROM audit.data_quality_checks
ORDER BY check_name, checked_at DESC;

-- Volume baseline: last run vs. median of the previous 10 successful runs of the same step
CREATE OR REPLACE VIEW obs.v_step_volume_baseline AS
WITH ranked AS (
    SELECT pipeline, step, started_at, rows_out,
           ROW_NUMBER() OVER (PARTITION BY pipeline, step ORDER BY started_at DESC) AS rn
    FROM audit.pipeline_step_runs
    WHERE status = 'success' AND rows_out IS NOT NULL
)
SELECT l.pipeline, l.step, l.started_at AS last_run_at, l.rows_out AS last_rows_out,
       b.median_rows_out,
       CASE WHEN b.median_rows_out > 0
            THEN round(((l.rows_out - b.median_rows_out) / b.median_rows_out * 100)::numeric, 1) END AS deviation_pct
FROM ranked l
LEFT JOIN LATERAL (
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY r.rows_out)::double precision AS median_rows_out
    FROM ranked r
    WHERE r.pipeline = l.pipeline AND r.step = l.step AND r.rn BETWEEN 2 AND 11
) b ON true
WHERE l.rn = 1;
