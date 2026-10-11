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

**Consequences.**
- Database and object storage run at Linux disk speed on any host OS; the lab behaves the same on
  Windows, macOS and Linux.
- Data is no longer browsable as Windows folders: use `psql`/DBeaver, the MinIO console, or `pg_dump`.
- `make clean` (`down -v`) deletes the volumes. This is intended: the lab is **reproducible by design**:
  every table is rebuilt by the pipelines from public sources (ANP, RFB) or generators (NF-e), see
  `docs/runbook-rebuild.md`. Nothing in the lab is a system of record.
- Volumes live inside the WSL2 virtual disk (on `C:`), which grows and is not shrunk automatically.
