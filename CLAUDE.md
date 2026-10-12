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
| Storage | Postgres 15 + pgvector (`pg_data` volume, `shm_size: 1g`), MinIO (frozen image, ADR 0004) |
| AI | sentence-transformers on CPU torch, `intfloat/multilingual-e5-small` (384 dims), Ollama `llama3.2:1b` (profile `ai`) |
| Observability | Loki 2.9.2 + Promtail 2.9.2 (EOL, ADR 0003), statsd-exporter → Prometheus v2.53.0, Grafana 12.0.0 |

Airflow deps are resolved against the official Airflow constraints; AI deps are installed with every
existing package frozen as a constraint. Keep it that way (see `Dockerfile`).

## Repository map

- `airflow/dags/` — `dag_dbt_anp` (Mon 06:00), `dag_rfb` (manual, params `ref_month`, `dev_mode`) → dataset
  `datalabs://landing/rfb` → `dag_rfb_dw_dbt`, `dag_nfe` (FileSensor, every 10 min), `dag_rag_anp`
  (dataset `datalabs://gold/anp`), `dag_observability_health` (every 6 h), `dag_setup_infrastructure`
- `pipelines/{anp,rfb,rag/anp}/` — numbered scripts, one per task
- `pipelines/commons/` — `env_loader` (single config source), `logger` (structured events), `s3_client`, `pg_copy`, `ai/` (embeddings, router, context sync)
- `pipelines/observability/` — runner, step tracker, `checks.py` + `checks.yml`, alerting, Airflow callbacks, `dag_helpers`
- `pipelines/dbt_projects/` — staging (views, `stg`) → silver (tables) → gold (star schemas); tags `anp`, `rfb`, `nfe`
- `infra/postgres/init/` — bootstrap + idempotent DDL (the single source of truth for non-dbt tables)
- `grafana/dashboards-src/build.py` — dashboards as code → `grafana/dashboards/*.json`
- `docs/adr/` (0001–0009), `docs/runbook-rebuild.md`, `docs/nfe/README.md`, `tests/`

## Architecture decisions (summaries — read the ADR before contradicting one)

- **0001 Observability:** four signals — logs (Loki), platform metrics (Prometheus), step facts
  (`audit.pipeline_step_runs`), data health (`elementary.*`, `audit.data_quality_checks`). Observability never
  breaks the pipeline (audit writes are best-effort). The final `end` task fails the run if any task failed.
  Alert once, on the root cause. `run_id` stays in the payload, never a Loki label.
- **0002 Polars single node:** batches (`iter_batches` + Parquet), bounded by batch size env vars,
  `POLARS_MAX_THREADS` and `max_active_tasks=2` in `dag_rfb`. No Spark.
- **0003 Promtail → Grafana Alloy + Loki 3:** planned; Promtail polls the `./airflow/logs` bind mount (high CPU on Windows).
- **0004 MinIO frozen:** acceptable for the lab only. All S3 access goes through `boto3` in `s3_client.py`, so a swap is a compose change.
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

## Development rules

**Pipelines**
- Every script runs through the observability runner (`observed_script` / `observed_module` in `dag_helpers.py`).
  Exit codes: `0` success, `99` skipped (no new data), anything else failed. Never return 0 when nothing loaded.
- Report volumes with `current_step().add(rows_in=..., rows_out=..., files_total=...)`; log with `log_event()`
  and a stable `event` name (`file_loaded`, `schema_drift_*`, `step_finished`…).
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
  Current state: 16 checks, 0 suspended. Suspending a check needs a reason and a follow-up.

**Database / Grafana**
- DDL lives in `infra/postgres/init/sql/*.sql`, idempotent (`IF NOT EXISTS`, `CREATE OR REPLACE`); applied on an empty
  volume and re-applied by `dag_setup_infrastructure` / `make db-bootstrap`.
- Dashboards: edit `grafana/dashboards-src/build.py`, then `make dashboards`. Never hand-edit the JSON (CI fails on drift).

**Tests & CI** (`.github/workflows/ci.yml`, must be green before merge)
- `ruff check .` (line length 120) and `pytest`; dashboards regenerated with no diff; `docker compose config` for
  default and AI/GPU variants; data integration on real Postgres + pgvector (ANP landing → dbt + Elementary →
  NF-e batch → health checks → audit assertions → every Grafana query as `grafana_ro`).
- New behaviour gets a unit test in `tests/`. New Grafana SQL is covered by `tests/test_dashboards_sql.py`.

## Security

- **Secrets only in `.env`** (gitignored, together with every `.env.*` except `.env.example`). `make env` generates
  random passwords. Never commit, print, log or paste secret values; never read `.env` to answer a question —
  `.env.example` documents every key. Sensitive keys: `DB_PASSWORD`, `GRAFANA_DB_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`,
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
- TLS verification stays on (pip, requests, boto3). Container images stay pinned.
- Lab-only caveats: all ports bind to every host interface and MinIO has no security updates. Review port bindings and
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
- RAG retrieval quality: hybrid search (FTS + vectors, RRF), similarity threshold, recall@k / MRR in the eval gate.
- Dedicated lambda server: full RFB load with reference history (SCD2 by snapshot diff).
