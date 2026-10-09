"""
dag_observability_health — scheduled data health checks for every pipeline.

Runs independently of the pipelines, so a source that silently stops publishing
(or a DAG that stops being scheduled) is still detected by the freshness checks.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipelines.observability.airflow_callbacks import OBS_DAG_CALLBACKS, OBS_DEFAULT_ARGS
from pipelines.observability.dag_helpers import health_checks

with DAG(
    dag_id="dag_observability_health",
    doc_md=__doc__,
    default_args={"owner": "datalab", "retries": 0, **OBS_DEFAULT_ARGS},
    description="Freshness / volume / reconciliation checks for all pipelines",
    schedule="15 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["observability", "data-quality", "freshness"],
    **OBS_DAG_CALLBACKS,
) as dag:
    health_checks("all", fail_on="error", skip_heavy=True, task_id="health_checks_all")
