# CLAUDE.md — DataLabs

Context for AI-assisted development. Read this before changing anything. Facts about the user-facing
project live in `README.md`; the *why* behind each design lives in `docs/adr/`. This file holds the
rules that keep changes consistent with both.

## What this is

A portfolio data platform (owner: Rodrigo Arruda, Data Engineer) that runs on one machine with Docker
Compose: public data (ANP fuel prices, Receita Federal CNPJ) and synthetic NF-e → medallion layers →
dbt gold → RAG on pgvector, with every step observable in Grafana.
Guiding question: *is the data right, complete and on time, and how would I know if it wasn't?*

Host: Windows + Docker Desktop (WSL2), PowerShell without `make` is the common case. Give commands that
work there (the runbooks show the `docker compose exec -T airflow-scheduler ...` form).

## Stack (pinned on purpose — do not bump versions as a side effect)

| Layer | Version / detail |
|---|---|
| Orchestration | Airflow 2.9.0 (Python 3.12, LocalExecutor) + astronomer-cosmos 1.15.1 |
| Transform | dbt-core 1.11.15, dbt-postgres 1.11.0, Elementary 0.25.1 (CLI = package version), isolated venv `/opt/airflow/dbt_venv` |
| Processing | Polars 1.x + PyArrow, single node (ADR 0002) |
| Storage | Postgres 15 + pgvector (`pg_data` volume, `shm_size: 1g`), Silo (community MinIO fork, pinned by digest, ADR 0004) |
| AI | sentence-transformers on CPU torch, `intfloat/multilingual-e5-small` (384 dims), Ollama `llama3.2:1b` (profile `ai`) |
| Observability | Loki 2.9.2 + Promtail 2.9.2 (EOL, ADR 0003), statsd-exporter → Prometheus v2.53.0, Grafana 12.0.0 |

Airflow deps are resolved against the official Airflow constraints; AI deps are installed with every
existing package frozen as a constraint. Keep it that way (see `Dockerfile`).

## Repository map

- `airflow/dags/` — `dag_dbt_anp` (Mon 06:00), `dag_rfb` (manual, params `ref_month`, `dev_mode`) → dataset
  `datalabs://landing/rfb` → `dag_rfb_dw_dbt`, `dag_nfe` (FileSensor, every 10 min), `dag_rag_anp`
  (dataset `datalabs://gold/anp`), `dag_rag_lc214` (Mon 06:40, LC 214 lc01→lc04a/lc04c + golden contract → lc05
  gate → lc06 generation; dataset `datalabs://silver/lc214`),
  `dag_observability_health` (every 6 h), `dag_setup_infrastructure`
- `pipelines/{anp,rfb,rag/anp,rag/lc214}/` — numbered scripts, one per task (`rag/lc214`: legislation RAG, ADR 0010)
- `pipelines/commons/` — `env_loader` (single config source), `logger` (structured events), `s3_client`, `pg_copy`, `ai/` (embeddings, router, context sync)
- `pipelines/observability/` — runner, step tracker, `checks.py` + `checks.yml`, alerting, Airflow callbacks, `dag_helpers`
- `pipelines/dbt_projects/` — staging (views, `stg`) → silver (tables) → gold (star schemas); tags `anp`, `rfb`, `nfe`
- `infra/postgres/init/` — bootstrap + idempotent DDL (the single source of truth for non-dbt tables)
- `grafana/dashboards-src/build.py` — dashboards as code → `grafana/dashboards/*.json`
- `docs/adr/` (0001–0015), `docs/runbook-rebuild.md`, `docs/nfe/README.md`, `tests/`

## Architecture decisions (summaries — read the ADR before contradicting one)

- **0001 Observability:** four signals — logs (Loki), platform metrics (Prometheus), step facts
  (`audit.pipeline_step_runs`), data health (`elementary.*`, `audit.data_quality_checks`). Observability never
  breaks the pipeline (audit writes are best-effort). The final `end` task fails the run if any task failed.
  Alert once, on the root cause. `run_id` stays in the payload, never a Loki label.
- **0002 Polars single node:** batches (`iter_batches` + Parquet), bounded by batch size env vars,
  `POLARS_MAX_THREADS` and `max_active_tasks=2` in `dag_rfb`. No Spark.
- **0003 Promtail → Grafana Alloy + Loki 3:** planned; Promtail polls the `./airflow/logs` bind mount (high CPU on Windows).
- **0004 Object storage:** `minio/minio` was deleted from Docker Hub (2026-09-11); the lab runs Silo (`pgsty/silo`, drop-in MinIO
  fork, tag + digest). All S3 access goes through `boto3` in `s3_client.py`, so a swap is a compose change. RustFS is the fallback.
- **0005 RAG router:** CNPJ or aggregation questions → parameterised SQL on gold; open questions → pgvector.
  Golden-set eval gate fails the DAG below `RAG_MIN_ROUTING_ACC` / `RAG_MIN_VALUE_ACC` (0.9).
- **0006 NF-e:** control per batch (`ctrl.nfe_lote`: `CARREGADO → PROCESSADO`), insert-only bronze, ingestion in
  PL/pgSQL (`pg_read_file`), parsing with `XMLTABLE` in dbt. Participant SCD2 keyed by the note's own attributes
  (no date-range join). Tests scoped to the batch in flight; business anomalies are `warn` + `store_failures` in `dq_nfe`.
- **0007 RAG incremental:** chunk id `id_fato:md5(text)[:16]`; each run embeds `gold − store`, deletes
  `store − gold`, exits 99 when nothing changed. Full rebuild only on empty store, model change or
  `RAG_FULL_REFRESH=1`. One transaction per sync (delete, `COPY`, HNSW); missing index self-heals.
- **0008 Volumes, not bind mounts:** every stateful service uses a named volume (`pg_data`, `minio_data`,
  `hf_cache`, `ollama_data`, `dbt_vol`, `grafana_data`, `prometheus_data`, `loki_data`). Bind mounts only for code
  and input files (`./datasource`). Input files are **streamed** (e.g. `zipfile.open`), temporaries go to the
  container disk or a volume. The Windows bind mount is ~260× slower for small-file I/O.
- **0009 RFB source contract:** the Receita share (Nextcloud WebDAV) cannot be listed (`PROPFIND` answers 200 + empty
  body for any path and any credentials), so `rfb01` downloads a fixed contract of 37 files per month, with retries
  and backoff (the source flips 200/404). The source check is a 1-byte ranged `GET` on `Cnaes.zip`: 206 present,
  401 link rejected, 404 missing (previous month present → exit 99, not published yet). The share token is static
  and public (`RFB_SHARE_URL`); the browser's session cookie / CSRF token belong to the web UI, not to WebDAV.

- **0010 Legislation RAG (LC 214):** structural source contract + sha256 versions (lc01); `pdftotext -layout`, the
  indentation is a parser signal; one row per provision with ids from the printed labels, notes as columns (lc02);
  source defects only via `pipelines/rag/lc214/known_anomalies.yml` (anything else fails the run); small-to-big
  chunks under the 512-token limit (lc03). Revoked/vetoed provisions are rows, never vectors. `dag_rag_lc214`: each
  step exits 99 when its own input and code version are unchanged, and the next steps run anyway (`NONE_FAILED`),
  so a `PARSER_VERSION`/`CHUNKER_VERSION` bump applies without a new PDF; the AI gate is a ShortCircuit. Storage:
  the lake only (`pipelines/rag/lc214/lake.py`): `landing/lc214` (PDF versions + manifest), `bronze/lc214/text`,
  `silver/lc214/<sha12>/`; reports/manifest written last (commit point); `LC214_LAKE=file:///dir` for tests.
  Golden set (`golden_set.yml`) = labelled data: its provision ids are re-checked against the current law on every run.
- **0011 LC 214 serving load:** `lc04_load` projects the current silver into `ai.lc214_dispositivo` /
  `ai.lc214_artigo` by upsert + orphan delete in one transaction (readers never blocked, unlike TRUNCATE).
  `row_hash` over content only (not version stamp, not `ordem`/`pagina`), so the step reports inserted/updated/
  deleted/restamped per amendment. Guards: `silver_inconsistent`, `mass_delete_blocked` (>5%). Checks:
  `lc214_serving_parity`, `lc214_serving_single_version`.
- **0012 LC 214 router:** `router.py` routes before retrieval: `sql` (citation only / its text, status, previous
  wording), `combined` (citation + question: the cited provision is an anchor, vectors complete), `vector` (no
  LC 214 citation; another law is `foreign`, never an anchor), `clarify` (ambiguous or missing: ask, never guess).
  Citations in chat and legal order, lists, caput convention only when it leaves one candidate. Deterministic, so
  the golden contract checks it exactly: R1 route, R2 anchors/options, R3 the law's 799 self-citations (≥ 99%).
- **0013 LC 214 vector search:** `lc04_embed` syncs `ai.lc214_chunk` by set difference on the chunk_id (same id =
  same text = same vector: a new version or parser only restamps), DELETE + INSERT in one transaction (no TRUNCATE),
  guards `chunks_estimated` and `store_shrink` (net loss > 5%; churn allowed). Exact cosine search, no HNSW (867
  vectors: 1-3 ms; an ANN index would add its misses to the lc05 baseline). `retriever.py`: anchors first, then hits
  by rank, one block per article, whole up to `LC214_ANCHOR_MAX_TOKENS` (2,000 ≈ 1,170 words, chosen by lc05)
  else in layers (caput, cited subtree, ranked chunks, labels left out); greedy budget in LLM tokens, ESTIMATED as
  words × 1.7 until the generation step calibrates it with `prompt_eval_count`. Checks: `lc214_vector_parity`,
  `lc214_vector_same_law`.
- **0014 LC 214 retrieval gate:** `lc05_eval` runs the golden set through `retrieve()`; success@k / hit@k / recall@k /
  MRR on the top-k chunks (route vector), context_recall on the blocks (vector + combined), per phrasing group
  (fiscal 31, formal 25) + the paired fiscal × formal gap. Thresholds = `eval_baseline.yml` (committed, written by
  `--write-baseline`) minus `LC214_EVAL_TOLERANCE_CASES` (1 case ≈ 3-4 pp); improvements are logged, never fail;
  no baseline = failure. Report per phrasing in the lake (`silver/lc214/<sha12>/eval/`). Every retrieval change is
  an experiment of one variable against this baseline.
- **0015 LC 214 answer generation:** `lc06_generate` (task `generate_answers`, after the lc05 gate): Ollama
  `llama3.2:1b` with a JSON Schema (`answerable`, `answer`, `citations[{artigo, dispositivo}]`), `temperature 0` +
  `LC214_GEN_SEED`, `num_ctx` explicit. "Não consta" by three barriers: no LLM call on `clarify` / no context block;
  the model's `answerable`; no citation left that the context showed (each citation resolved by the router's
  `parse_citations` + `Index.resolve`: valid | outside_context | not_found | foreign). Route `sql` answers by structure,
  never by the LLM; `foreign` still answers. Report `silver/lc214/<sha12>/answers/<fingerprint>.json` (law, chunks,
  golden, model digest, prompt version); exit 99 when it exists or Ollama is down. Metrics per group: refusal
  accuracy, citation validity, citation recall (all / in context). CI replays recorded Ollama bodies; the model is
  measured only in the lab. Step 6b (gate vs `answer_baseline.yml`) comes after the first lab run.
  Latency: `LC214_NUM_CTX` 6144 (same recall as 8192, swept by lc05); trade context for speed only within 2 fiscal /
  1 formal cases. CPU-bound: WSL needs every core (`.wslconfig processors`). Iterate prompts on `--golden --dev`.
  Prompt changes are named variants (`LC214_GEN_VARIANT`: baseline | verdict_last | answer_default, `+` composes),
  part of the fingerprint; dev runs land in `answers/dev/`. First dev run: refusal collapse (11/11 refused).

## Development rules

**Pipelines**
- Every script runs through the observability runner (`observed_script` / `observed_module` in `dag_helpers.py`).
  Exit codes: `0` success, `99` skipped (no new data), anything else failed. Never return 0 when nothing loaded.
- Report volumes with `current_step().add(rows_in=..., rows_out=..., files_total=...)`; log with `log_event()`
  and a stable `event` name (`file_loaded`, `schema_drift_*`, `step_finished`…). `add()` sums counters and numeric
  metrics (`bytes_downloaded`), `set()` overwrites, `label()` keeps every distinct value (a step over several
  entities). A step that loops over entities must not `set()` per-entity totals (it reported the last one only).
- External sources: check them with the request the step really makes (a ranged `GET` of a real file), never with
  a listing or metadata endpoint, and name each failure cause in its own event. Verify a source's behaviour with
  live requests before coding against it; record what was measured, and when, in the ADR.
- Configuration comes from `pipelines/commons/env_loader.py` (env > `.env` > defaults). New env vars go in
  `.env.example` with a comment.
- Loads are idempotent and resumable: a re-run skips what already landed. Keep it true for new steps.
- Polars, not pandas, for new code. Large inputs: batches or streaming, never load-all or extract-to-disk.

**dbt**
- New or renamed model/tag → `make dbt-parse` (Cosmos reads the manifest in `dbt_vol`, `LoadMode.DBT_MANIFEST`).
  DAGs never run `dbt deps` or `dbt ls`; `make dbt-deps` only when `packages.yml` changes.
- Put the domain tag on every model; multi-parent tests run in their own tasks (`should_detach_multiple_parents_tests`).
- Incremental facts use the ingestion watermark, not the business date (late-arriving data).

**Data quality**
- Cross-layer and time-based rules go in `pipelines/observability/checks.yml` (types: freshness, threshold,
  reconciliation); model contracts go in dbt tests. Guardrails per check: `timeout_s`, `cost: heavy`
  (full scans: run after their pipeline, not every 6 h), `suspended: "<reason>"` (temporary, shows in Grafana).
  Current state: 22 checks, 0 suspended. Suspending a check needs a reason and a follow-up.

**Database / Grafana**
- DDL lives in `infra/postgres/init/sql/*.sql`, idempotent (`IF NOT EXISTS`, `CREATE OR REPLACE`); applied on an empty
  volume and re-applied by `dag_setup_infrastructure` / `make db-bootstrap`.
- Dashboards: edit `grafana/dashboards-src/build.py`, then `make dashboards`. Never hand-edit the JSON (CI fails on drift).

**Reproducibility (a fresh clone must work)**
- A new external dependency (download host, container image, model) is added to `scripts/preflight.sh`, so a
  blocked or deleted dependency fails *before* `make up` with its cause, not inside a build or a DAG.
- Silent failures (empty bind mounts, missing model, `.env` passwords rejected by an older volume) belong in
  `scripts/doctor.sh`.
- `.env` and the volumes are a pair: the compose project name is fixed (`datalabs`), so a clone in ANY folder reuses
  the same `datalabs_*` volumes, and Postgres applies the `.env` passwords only when it creates `pg_data`. Keeping
  the volumes means keeping (copying) the old `.env`; a new `.env` needs new volumes. `make env` never overwrites an
  existing `.env`, adds only missing keys and never regenerates `AIRFLOW__CORE__FERNET_KEY`.
- On Windows/WSL, clone inside the WSL filesystem (`~/datalabs`), not under `/mnt/c`: drvfs breaks `chown`, is slow
  and can mount empty after `wsl --shutdown` (`make preflight` warns).
- Selective cleanup keeps the expensive downloads: remove `pg_data`/`minio_data`/`dbt_vol`/`grafana_data`/
  `prometheus_data`/`loki_data` by name, keep `hf_cache`, `ollama_data` and `./datasource` (RFB ZIPs ~21 min).
  Removing `pg_data` also drops the audit history Grafana trends on and the RAG store (~45 min rebuild).
- Optional layers degrade explicitly: no AI image -> RAG DAG exits 99 and its checks are `skipped` (`requires: ai`).
- Images stay pinned (by digest when the publisher is not well established); `image-availability.yml` checks weekly
  that every pinned image still exists.

**Tests & CI** (`.github/workflows/ci.yml`, must be green before merge; `image-availability.yml` runs weekly)
- `ruff check .` (line length 120) and `pytest`; dashboards regenerated with no diff; `docker compose config` for
  default and AI/GPU variants; data integration on real Postgres + pgvector (ANP landing → dbt + Elementary →
  NF-e batch → health checks → audit assertions → every Grafana query as `grafana_ro`).
- New behaviour gets a unit test in `tests/`. New Grafana SQL is covered by `tests/test_dashboards_sql.py`.

## Security

- **Secrets only in `.env`** (gitignored, together with every `.env.*` except `.env.example`). `make env` generates
  random passwords. Never commit, print, log or paste secret values; never read `.env` to answer a question —
  `.env.example` documents every key. Sensitive keys: `DB_PASSWORD`, `GRAFANA_DB_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`,
  `AIRFLOW__WEBSERVER__SECRET_KEY`, `AIRFLOW__CORE__FERNET_KEY` (never regenerate a set Fernet key),
  `MINIO_*`, `TELEGRAM_BOT_TOKEN`, `ALERT_WEBHOOK_URL`. `RFB_SHARE_URL` is a public link (configuration, not a secret).
- `profiles.yml` is committed because it only reads `env_var(...)`. Keep it free of literals.
- **Least privilege:** Grafana uses the read-only role `grafana_ro`. A new schema that Grafana reads must be added
  to the grant loop in `infra/postgres/init/00_bootstrap.sh`. Airflow metadata stays in its own `airflow` database.
  `AIRFLOW__WEBSERVER__EXPOSE_CONFIG` stays `false`.
- **SQL injection:** any value from a user, a question or an LLM is a bind parameter (`text(sql)` + params).
  f-strings in SQL only for trusted config (e.g. `RAG_UF`) or fixed identifiers.
- **NF-e data is 100% synthetic and fictitious** (`scripts/nfe/gen_nfe.py`). Never add real documents, CNPJs or CPFs.
  `pg_read_file` works because the lab user is superuser and `./datasource/nfe` is mounted read-only; a non-lab
  deployment needs a dedicated role (`pg_read_server_files`) instead.
- TLS verification stays on (pip, requests, boto3, Hugging Face, Ollama). Container images stay pinned. On a network
  with TLS inspection (corporate machine) the AI layer cannot download its models: that is a limit of the network,
  not a bug to work around. Never add `verify=False`, `CURL_CA_BUNDLE=""`, a custom CA bundle or
  `--trusted-host`; run the AI layer on another machine, or `make models-export` there and `make models-import` here.
- Lab-only caveats: all ports bind to every host interface and the object store is a young community fork. Review port bindings and
  the object store before exposing the stack on a network (e.g. the dedicated lambda server).
- **Destructive commands need explicit confirmation:** `make clean` / `docker compose ... down -v` (deletes every volume),
  dropping schemas, `--full-refresh` on large models, `RAG_FULL_REFRESH=1` (~45 min of CPU), force-push.

## Git workflow

- Branch from `master`: `feat/`, `fix/`, `perf/`, `docs/`, `chore/` + short topic. One concern per PR, merged via GitHub PR.
- Conventional Commits with scope, in English: `perf(rfb): stream CSVs from zip instead of extracting to the bind mount`.
  Body says *why*; measured numbers when it is a performance change (and update the ADR/README tables).
- Commit trailer: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
  **Never** include the Claude session link (`Claude-Session:` / `claude.ai/code/session...`) in commits or PR descriptions.
- When work is delivered as a patch (cloud session without push access), generate it with `git format-patch` and give
  the apply command with the full Windows path: `git am C:\Users\engro\Downloads\<patch-file-name>.patch`.
- Architecture changes get an ADR (`docs/adr/NNNN-kebab-title.md`: Status, Context, Decision, Consequences).

## Working with Claude on this repo

- Model: **Claude Opus 5.5** (`claude-opus-5-5`) for all development; pinned for Claude Code in `.claude/settings.json`.
- Language: code, docs and commits in English; NF-e docs and some code comments are in Portuguese (keep the file's language).
- Be direct, show results (row counts, durations, check status), and say when a request drifts from the ADRs.
- Verify before claiming: run the test, the check or the query; quote the numbers.

## Open threads (keep short; move to README roadmap when planned)

- Grafana `$uf` variable error on the ANP dashboard; `force_transform` param on `dag_dbt_anp`.
- Airflow logs on a Docker volume + Grafana Alloy (Promtail CPU on the bind mount).
- RFB bronze stores `dev_mode` / row count in S3 object metadata, so a full run reprocesses dev-mode files.
- RAG retrieval quality: hybrid search (FTS + vectors, RRF), similarity threshold. LC 214: fiscal phrasings miss the
  law's vocabulary (content-03: "matriz/filial" vs "estabelecimentos do mesmo contribuinte"), measured by lc05.
- LC 214 generation (ADR 0015): first lab run, digest stable across two `force` runs, record real replies as CI
  fixtures, then the 6b gate; calibrate `LC214_LLM_TOKENS_PER_WORD` from the report (its own commit, lc05 re-run).
- Dedicated lambda server: full RFB load with reference history (SCD2 by snapshot diff).
- Fresh-machine test (2026-10-12): `blocked` events for `dag_setup_infrastructure` in the Airflow audit log, cause unknown.
  Same day: RFB, ANP and NF-e ran end to end on a clean clone (RFB dev_mode: download 21 min, `dag_rfb_dw_dbt` 5m18s,
  landing × gold reconciliation 0.0%); RAG blocked by corporate TLS inspection, to be tested on a personal machine.
- `gold.dm_socios` has 4,999,952 rows for 5,000,000 in landing (dev_mode): confirm the 48 are an intended dedup.
- Default `ref_month` is 2026-08 while 2026-09 is published (`rfb_reference_freshness` warns, as designed).
- Pin the embedding model revision (commit hash) in `pipelines/commons/ai/embeddings.py`.
