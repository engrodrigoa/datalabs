-- LLM usage per call (ADR 0018): one row per request a pipeline or app sends to a local LLM (Ollama).
-- Written by pipelines/commons/ai/llm_usage.py (best-effort: a failed write never fails the caller, ADR 0001).
--
-- Grain: one LLM request. The prompt and the reply are NOT stored: only a sha256 of the prompt and its size, so
-- usage can be grouped, repeated prompts found and tokens audited without turning this table into a copy of the
-- content (the pattern a real deployment needs when prompts carry personal or fiscal data).
--
-- Tokens: `prompt_tokens` is the best number available and `token_method` says which one it is:
--   reported   prompt_eval_count from the LLM's own response (the truth for this model and tokenizer)
--   estimated  words x tokens-per-word, when the response carries no usage
-- `prompt_tokens_estimate` is kept on every row, so the estimate the retriever budgets with can be checked against
-- the reported count (data quality check llm_token_estimate_drift).

CREATE TABLE IF NOT EXISTS ai.llm_call (
    call_id                 uuid PRIMARY KEY,
    called_at               timestamptz NOT NULL,             -- when the request was sent (UTC)
    app                     text NOT NULL,                    -- the consumer: lc214_generate
    pipeline                text,                             -- the runner's pipeline/step when run through it
    step                    text,
    step_run_id             uuid,                             -- -> audit.pipeline_step_runs (null for ad-hoc runs)
    dag_id                  text,
    dag_run_id              text,
    provider                text NOT NULL DEFAULT 'ollama',
    model                   text NOT NULL,
    model_digest            text,
    route                   text,                             -- the router's route (vector, combined, sql_*...)
    prompt_sha256           text NOT NULL,                    -- system + prompt, as sent
    prompt_chars            integer NOT NULL,
    prompt_words            integer NOT NULL,
    prompt_tokens           integer NOT NULL,                 -- reported when available, else the estimate
    prompt_tokens_reported  integer,
    prompt_tokens_estimate  integer NOT NULL,
    completion_tokens       integer,                          -- eval_count (reported only)
    total_tokens            integer NOT NULL,                 -- prompt_tokens + coalesce(completion_tokens, 0)
    token_method            text NOT NULL CHECK (token_method IN ('reported', 'estimated')),
    num_ctx                 integer,                          -- the context window requested (null = model default)
    done_reason             text,                             -- stop | length (cut by num_predict) | ...
    latency_ms              integer,
    load_ms                 integer,                          -- Ollama's split of the latency
    prompt_eval_ms          integer,
    eval_ms                 integer,
    tags                    jsonb NOT NULL DEFAULT '{}'::jsonb -- free labels (variant, case, phrasing...)
);

CREATE INDEX IF NOT EXISTS ix_llm_call_called_at ON ai.llm_call (called_at);
CREATE INDEX IF NOT EXISTS ix_llm_call_app_model ON ai.llm_call (app, model, called_at);
CREATE INDEX IF NOT EXISTS ix_llm_call_step_run ON ai.llm_call (step_run_id);

-- Daily roll-up for dashboards: one row per day x app x model.
CREATE OR REPLACE VIEW ai.vw_llm_usage_daily AS
SELECT date_trunc('day', called_at)                                   AS dia,
       app,
       model,
       count(*)                                                       AS calls,
       sum(prompt_tokens)                                             AS prompt_tokens,
       sum(coalesce(completion_tokens, 0))                            AS completion_tokens,
       sum(total_tokens)                                              AS total_tokens,
       round(avg(prompt_tokens))::integer                             AS avg_prompt_tokens,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)       AS p95_latency_ms,
       avg(CASE WHEN token_method = 'reported' THEN 1.0 ELSE 0.0 END) AS reported_share,
       count(*) FILTER (WHERE done_reason = 'length')                 AS truncated_calls
FROM ai.llm_call
GROUP BY 1, 2, 3;
