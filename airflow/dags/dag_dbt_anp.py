"""
dag_dbt_anp — ANP fuel prices: scraping -> landing -> dbt (stg/silver/gold) -> data health.

Observability built in:
  * every script runs through pipelines.observability.run  -> audit.pipeline_step_runs
  * dbt tests + Elementary anomaly tests run per model (Cosmos)  -> schema elementary
  * health checks (freshness, volume, reconciliation) ALWAYS run, even when there was no
    new file to process -> audit.data_quality_checks
  * task failures / SLA misses alert through pipelines.observability.alerting
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig  # type: ignore

from pipelines.commons.audit_logger import consolidate_dag_audit_logs
from pipelines.observability.airflow_callbacks import OBS_DAG_CALLBACKS, OBS_DEFAULT_ARGS, on_sla_miss
from pipelines.observability.dag_helpers import (
    DBT_BIN, DBT_PROJECT_DIR, dbt_command, elementary_report, health_checks, observed_module, observed_script,
)

GOLD_ANP = Dataset("datalabs://gold/anp")

profile_config = ProfileConfig(
    profile_name="datalab",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)

default_args = {
    "owner": "datalab",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    **OBS_DEFAULT_ARGS,
}

with DAG(
    dag_id="dag_dbt_anp",
    doc_md=__doc__,
    default_args=default_args,
    description="ANP: scraping -> landing -> dbt + Elementary -> data health checks",
    schedule="0 6 * * 1",  # Mondays 06:00 (America/Sao_Paulo); DAGs start paused
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=on_sla_miss,
    tags=["anp", "dbt", "cosmos", "elementary", "observability"],
    **OBS_DAG_CALLBACKS,
) as dag:

    setup_infra = TriggerDagRunOperator(
        task_id="trigger_setup_infra",
        trigger_dag_id="dag_setup_infrastructure",
        wait_for_completion=True,
        poke_interval=10,
    )

    # ---------------------------------------------------------------- landing
    scrap_semanal = observed_script("scrap_semanal", "anp", "anp/anp_01_anp_week_file_scrap.py",
                                    execution_timeout=timedelta(minutes=10))
    landing_semanal = observed_script("landing_semanal", "anp", "anp/anp_03_anp_landing_semanal.py",
                                      execution_timeout=timedelta(minutes=15))
    scrap_mensal = observed_script("scrap_mensal", "anp", "anp/anp_02_anp_month_file_scrap.py",
                                   execution_timeout=timedelta(minutes=30))
    landing_mensal = observed_script("landing_mensal", "anp", "anp/anp_04_anp_landing_mensal.py",
                                     execution_timeout=timedelta(minutes=30))

    join_landing = EmptyOperator(task_id="join_landing", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    # ---------------------------------------------------------------- transformation
    dbt_deps = dbt_command("dbt_deps", "deps", execution_timeout=timedelta(minutes=5))

    dbt_transformations = DbtTaskGroup(
        group_id="dbt_transformations",
        # packages are installed once by airflow-init (and refreshed by the dbt_deps task):
        # no `dbt deps` on every DAG parse / every Cosmos task
        project_config=ProjectConfig(DBT_PROJECT_DIR, install_dbt_deps=False),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_BIN),
        # emit_datasets lives in RenderConfig since Cosmos 1.9 (operator_args is overridden): we publish
        # our own coarse dataset (publish_gold) instead of one dataset per dbt model
        render_config=RenderConfig(select=["tag:anp"], exclude=["package:elementary"], emit_datasets=False),
    )

    publish_gold = EmptyOperator(task_id="publish_gold", outlets=[GOLD_ANP], sla=timedelta(hours=3))

    # ---------------------------------------------------------------- observability
    elementary_models = dbt_command("elementary_models", "run --select elementary",
                                    trigger_rule=TriggerRule.ALL_DONE)
    elementary_html = elementary_report("elementary_report", "anp", trigger_rule=TriggerRule.ALL_DONE)
    archive_artifacts = observed_module("archive_artifacts", "obs", "pipelines.observability.archive_dbt_artifacts",
                                        args="--pipeline anp", trigger_rule=TriggerRule.ALL_DONE)
    data_health = health_checks("anp", trigger_rule=TriggerRule.ALL_DONE)

    end = PythonOperator(task_id="end", python_callable=consolidate_dag_audit_logs, trigger_rule=TriggerRule.ALL_DONE)

    # ---------------------------------------------------------------- flow
    setup_infra >> [scrap_semanal, scrap_mensal]
    scrap_semanal >> landing_semanal
    scrap_mensal >> landing_mensal
    [landing_semanal, landing_mensal] >> join_landing >> dbt_deps >> dbt_transformations >> publish_gold
    dbt_transformations >> elementary_models >> elementary_html >> archive_artifacts
    [publish_gold, archive_artifacts] >> data_health >> end
