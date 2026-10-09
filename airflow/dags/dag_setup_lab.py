"""
dag_setup_infrastructure — idempotent provisioning of buckets, schemas and tables.

DDL lives in infra/postgres/init/sql/*.sql (single source of truth): the same files are
applied by the Postgres entrypoint on a fresh volume and by this DAG on existing ones.
"""
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipelines.commons.audit_logger import consolidate_dag_audit_logs
from pipelines.commons.env_loader import REQUIRED_BUCKETS
from pipelines.commons.s3_client import ensure_buckets_exist, get_s3_client
from pipelines.observability.airflow_callbacks import OBS_DAG_CALLBACKS, OBS_DEFAULT_ARGS

logger = logging.getLogger("airflow.task")

POSTGRES_CONN_ID = "postgres_default"
DDL_DIR = Path(os.getenv("DDL_DIR", "/opt/airflow/infra/postgres/init/sql"))
DIVIDER = "=" * 100


def _log_block(title: str, lines: list) -> None:
    logger.info(DIVIDER)
    logger.info(f" {title}")
    logger.info(DIVIDER)
    for line in lines:
        logger.info(f" {line}")
    logger.info(DIVIDER)


def _create_buckets():
    s3 = get_s3_client()
    try:
        existing_before = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    except Exception:
        existing_before = set()
    ensure_buckets_exist(REQUIRED_BUCKETS)
    _log_block("STORAGE (MinIO / S3) SETUP", [
        f"BUCKET s3://{b} -> {'already existed' if b in existing_before else 'created'}" for b in REQUIRED_BUCKETS
    ])


def _apply_ddl():
    files = sorted(DDL_DIR.glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"no DDL files found in {DDL_DIR} (is ./infra mounted?)")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    applied = []
    for f in files:
        hook.run(f.read_text(encoding="utf-8"))
        applied.append(f"applied {f.name}")
    schemas = hook.get_records(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name NOT LIKE 'pg_%' AND schema_name <> 'information_schema' ORDER BY 1"
    )
    applied.append("schemas: " + ", ".join(r[0] for r in schemas))
    _log_block("DATABASE DDL (idempotent)", applied)


def _confirm_environment_ready():
    _log_block("ENVIRONMENT SETUP COMPLETE", ["Buckets, schemas and tables are provisioned.", "Environment is ready."])


with DAG(
    dag_id="dag_setup_infrastructure",
    default_args={"owner": "datalab", "retries": 1, "retry_delay": timedelta(minutes=2), **OBS_DEFAULT_ARGS},
    description="Idempotent setup: MinIO buckets + DW schemas/tables (infra/postgres/init/sql)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["setup", "ddl", "infra"],
    **OBS_DAG_CALLBACKS,
) as dag:

    create_buckets = PythonOperator(task_id="create_buckets", python_callable=_create_buckets)
    apply_ddl = PythonOperator(task_id="apply_ddl", python_callable=_apply_ddl)
    confirm = PythonOperator(task_id="confirm_environment_ready", python_callable=_confirm_environment_ready)
    end = PythonOperator(task_id="end", python_callable=consolidate_dag_audit_logs, trigger_rule=TriggerRule.ALL_DONE)

    create_buckets >> apply_ddl >> confirm >> end
