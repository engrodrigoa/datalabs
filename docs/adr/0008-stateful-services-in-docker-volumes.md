# ADR 0008 — Stateful services in Docker volumes, not Windows bind mounts

**Status:** accepted

**Context.** On Docker Desktop (Windows + WSL2), `./datalake/*` bind mounts cross the Windows/Linux
file-sharing layer. Measured in the lab: creating 2,000 small files took **104 s** on the bind mount
and **0.4 s** on the container disk (~260x). Postgres (`datalake/postgres_data`) paid this on every
page it read or wrote: a full scan of 790k rows took 40-50 s, a check over the same table timed out
at 120 s, and `COPY` of 32k vectors took 336 s. MinIO, the Hugging Face cache and Ollama models
lived on the same mounts.

**Decision.** Every stateful service uses a named Docker volume: `pg_data`, `minio_data`, `hf_cache`,
`ollama_data` (joining `dbt_vol`, `grafana_data`, `prometheus_data`, `loki_data`). Bind mounts are
kept only for code (DAGs, pipelines, dbt project) and input files (`./datasource`), which humans and
the NF-e generator write from the host.

**Measured after the switch (same data: 789,803 ANP rows rebuilt from zero).**

| Query | Bind mount | Docker volume |
|---|---|---|
| `anp_invalid_prices` (full scan of silver) | 46.5 s | 0.35 s |
| `anp_silver_to_gold_reconciliation` | 42.8 s | 0.31 s |
| `anp_fact_orphan_postos` | 51.9 s | 0.41 s |
| `anp_stg_to_silver_reconciliation` (staging views over all bronze) | > 5 min (timeout) | 35 s |
| whole `health_checks_anp` | ~7 min, 2 timeouts | 2.0 s |

The two checks suspended because of the slow disk were re-enabled; the staging reconciliation runs as
`cost: heavy` (after each ANP load, not in the 6-hourly health DAG).

**Consequences.**
- Database and object storage run at Linux disk speed on any host OS; the lab behaves the same on
  Windows, macOS and Linux.
- Data is no longer browsable as Windows folders: use `psql`/DBeaver, the MinIO console, or `pg_dump`.
- `make clean` (`down -v`) deletes the volumes. This is intended: the lab is **reproducible by design**:
  every table is rebuilt by the pipelines from public sources (ANP, RFB) or generators (NF-e), see
  `docs/runbook-rebuild.md`. Nothing in the lab is a system of record.
- Volumes live inside the WSL2 virtual disk (on `C:`), which grows and is not shrunk automatically.

**Follow-up: input files stay on the bind mount, so scripts must stream them.** The RFB step extracted
each CSV in full next to its ZIP in `./datasource` before reading it: extracting Estabelecimentos0 (2.2 GB zipped)
ran at ~3 MB/s before a single row was read, even in `dev_mode`, which uses only 500k rows. Reading the CSV as a stream from the ZIP
(`zipfile.open`) removed the write and re-read: 25 files / 10M rows (`dev_mode`) in 8 min, ~35 s per
Estabelecimentos file. Rule for new pipelines: read from the input mount, write temporaries to the
container disk or to a volume.
