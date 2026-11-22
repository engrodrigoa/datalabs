# Runbook — rebuild the lab from zero

Everything in the lab is derived data: public sources (ANP, RFB), generators (NF-e) and the
pipelines that turn them into silver/gold/vectors. Rebuilding from zero is a supported operation
(and the best proof that the project is reproducible).

## 1. Wipe (Docker volumes + legacy folders)

```bash
make clean          # down -v: deletes pg_data, minio_data, hf_cache, ollama_data, dbt_vol, grafana...
```
PowerShell (no `make`):
```powershell
docker compose --profile ai --profile tools down -v
Remove-Item -Recurse -Force .\datalake -ErrorAction SilentlyContinue   # pre-ADR-0008 bind mounts
```

## 2. Start (with the AI image if you use the RAG)

```bash
make up-ai          # or: make up  (without RAG)
```
PowerShell:
```powershell
$env:INSTALL_AI = "true"; docker compose --profile ai up -d --build
```
On an empty `pg_data`, Postgres runs `infra/postgres/init` (databases, roles, schemas, DDL) and
`airflow-init` installs the dbt packages and generates the Cosmos manifest. Wait for it:
```powershell
docker compose ps airflow-init     # Exited (0)
```

## 3. Elementary tables

Automatic: on an empty database `airflow-init` runs `dbt run --select elementary` (its anomaly tests read
those tables and fail with `relation "none" does not exist` without them). Check the init log for
`elementary models already installed` or the dbt run summary. Manual refresh, if ever needed:
```powershell
docker compose exec -T airflow-scheduler bash -c "cd /opt/airflow/pipelines/dbt_projects && /opt/airflow/dbt_venv/bin/dbt run --select elementary --profiles-dir ."
```
(`make elementary` does the same.)

## 4. Repopulate

| Order | DAG | What it builds | Notes |
|---|---|---|---|
| 1 | `dag_dbt_anp` | ANP bronze → silver → gold + `ag_anp_preco_semanal` | real data from gov.br/anp (internet) |
| 2 | `dag_rfb` → `dag_rfb_dw_dbt` | RFB landing (MinIO) → DW | large download; trigger when needed |
| 3 | `dag_nfe` | NF-e lots → silver/gold | `gen_nfe.py 2000 --agora` (or `make nfe-quickstart`) |
| 4 | `dag_rag_lc214` | LC 214 lake → `ai.lc214_*` + vectors → lc05/lc06 gates | AI profile only (`make up-ai`): PDF from the Câmara → parse → 867 vectors → gates; the answer step (lc06) is the long one, ~40 min on CPU |

```powershell
docker compose exec -T airflow-scheduler airflow dags unpause dag_setup_infrastructure
docker compose exec -T airflow-scheduler airflow dags unpause dag_dbt_anp
docker compose exec -T airflow-scheduler airflow dags unpause dag_observability_health
docker compose exec -T airflow-scheduler airflow dags trigger dag_dbt_anp
```
(`make demo` does the same.) The LC 214 RAG, with the AI profile up:
```powershell
docker compose exec -T airflow-scheduler airflow dags unpause dag_rag_lc214
docker compose exec -T airflow-scheduler airflow dags trigger dag_rag_lc214
```

## 5. Verify

- Grafana `/d/datalabs-pipeline-health`, `/d/datalabs-anp`, `/d/datalabs-nfe` (and `/d/datalabs-rag-lc214`) show fresh data.
- `obs.v_check_last_result`: health checks pass (suspended ones show their reason).
- `lc214_serving_parity` / `lc214_vector_parity`: the served law and the vectors match the current parse.
