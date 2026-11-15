# ADR 0018 — LLM usage per call: one row per prompt, tokens as the model reported them

**Status:** accepted (`pipelines/commons/ai/llm_usage.py`, table `ai.llm_call` in
`infra/postgres/init/sql/07_llm_usage.sql`, row 6 of the board **DataLabs · RAG LC 214**, checks `llm_*`).

**Context.** The lab calls a local LLM in two places: the LC 214 answer generation (`lc06_generate`, qwen2.5:3b)
and the interactive ANP assistant (`rag03_generator`, llama3.2:1b). Ollama returns the token counts of every call
(`prompt_eval_count`, `eval_count`), but they were only used in aggregate: lc06 kept them inside its JSON report in
the lake and wrote one median (`tokens_per_word`) to `audit.pipeline_step_runs`; the ANP assistant dropped them.
There was no row per prompt to answer "how many tokens did this prompt use, and who is using the most?", the
question any team running an LLM in production asks first (cost per user, context budget, abuse). The same design is
the prototype for a real case outside the lab: prompts logged by an application, counted per user.

**Decisions.**

1. **Grain: one LLM request.** Routes that never call the LLM (`sql`, `clarify`, no context) make no row. Lab
   volume is tens of calls per week, so one short connection per insert is fine and keeps the caller's transaction
   untouched (a failed insert inside it would abort the caller's work).
2. **The model's own count is the truth; the estimate is kept next to it.** `prompt_tokens` = `prompt_eval_count`
   when the response carries it (`token_method = reported`), else words × tokens-per-word (`estimated`).
   `prompt_tokens_estimate` is stored on every row, so the retriever's budget (words × 1.7, ADR 0013) is measured
   call by call instead of by one median per run. No tokenizer in the lab: the count has to match the model that
   was actually called, and only the model knows it (another provider: its usage field; a tokenizer only as the
   fallback, and then labelled as such).
3. **Metadata, not content.** The prompt and the reply are not stored: only `prompt_sha256`, chars and words. The
   lake already keeps lc06's replies for audit; usage is a different question and does not need the text. A real
   deployment carries personal or fiscal data in prompts, and this table must be safe to give to a BI reader.
4. **Inside the `ai` schema, on the RAG board.** It is part of the AI layer, already in the `grafana_ro` grant loop;
   a new row on the existing board instead of a new dashboard.
5. **Best-effort (ADR 0001).** A failed write logs `llm_usage_write_failed` and returns; the answer is never lost
   because its metric was. The loss is caught by a check instead (`llm_usage_recorded`).
6. **Linked to the run.** `step_run_id`, `dag_id`, `dag_run_id` from the runner's step (null for ad-hoc use, such as
   the interactive assistant), so tokens join `audit.pipeline_step_runs`. Free labels in `tags` (lc06: variant,
   intent, golden case, phrasing).

7. **The value of the spend, not only the spend (lc06).** A token count says what was spent, not whether it
   became an answer. lc06 records each call after its decision, with `tags.outcome`: `answered` (always with a valid
   citation, barrier 3) or the refusal that decided it (`model`, `no_valid_citation`, `truncated`, `invalid_json`),
   `error` when the reply could not be read (the tokens were spent anyway: still recorded). `citations_from` says
   whether the model put the valid citation in the `citations` field or only in its text (read under `model_text`).
   Board: *Useful token share* (answered tokens / all lc06 tokens) and *Tokens by outcome*. It is the cost side of
   the refusal rate, not a quality gate: refusing an unanswerable question is correct, and correctness against the
   golden labels stays with the lc06 gate (ADR 0015). No column was added: the outcome is a tag, the table stays
   generic for other consumers.

   First call measured in the lab (2026-11-15, qwen2.5:3b, i7-7700HQ): 4,064 prompt tokens reported against an
   estimate of 3,887 (-4.4%, ≈1.78 tokens per word), 104 completion tokens, 336 s, of which 312 s (93%) reading the
   prompt at 13 tokens/s. In RAG the cost is the retrieved context, not the reply.

**Data quality and observability** (`pipelines/observability/checks.yml`, pipeline `lc214`, `requires: ai`):

| check | type | catches |
|---|---|---|
| `llm_usage_recorded` | reconciliation, 0% | calls lc06 made (step metric `llm_calls`) vs rows written for that step: a lost write |
| `llm_usage_estimated_calls` | threshold, warn | rows without the model's count in 30 days: the numbers stopped being the truth |
| `llm_prompt_fills_context` | threshold, warn | prompt reached `num_ctx`: the server truncated it before the model read it |
| `llm_token_estimate_drift` | threshold, warn | median relative error of the estimate > 25% (above the budget margin of 0.2) |

When they run: `health_checks_lc214` runs before `generate_answers` in `dag_rag_lc214`, so the reconciliation
of a generation is judged by the periodic `dag_observability_health` (every 6 h, all checks), not by the same DAG
run. A dedicated check task after generation is the follow-up if a 6 h delay ever matters.

Measured before merge (local Postgres 16, synthetic rows): an empty table passes all four (0 = 0); a step that
reported 3 calls with 2 rows fails the reconciliation; a prompt at `num_ctx` and a 40% median error both warn.

**Consequences.**

* Tokens per prompt, per app, per route and per golden case are queryable and plotted (board row 6), as the model
  counted them.
* The estimate the retriever plans with is now checked on every call: the open thread "calibrate
  `LC214_LLM_TOKENS_PER_WORD`" has its data.
* Cost is not computed: the LLM is local and free. A price per model with validity dates (a dimension joined on
  `called_at`) is the next step when there is a paid provider, never a constant in a query.
* There is no user in the lab. `app` and `route` play that role here; a real deployment needs the application to
  log a user id with each call, or cost per user cannot be attributed.
* Ollama reuses the KV cache of a shared prefix, so a call after the first may report fewer prompt tokens than it
  sent (ADR 0015). The drift check leaves out ratios below 1 token/word for that reason.
