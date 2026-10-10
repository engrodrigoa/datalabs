"""dag_rfb_dw_dbt — RFB DW models (dbt + Cosmos), triggered by the `datalabs://landing/rfb` dataset."""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from cosmos import DbtTaskGroup, ExecutionConfig, LoadMode, ProfileConfig, ProjectConfig, RenderConfig  # type: ignore

from pipelines.commons.audit_logger import consolidate_dag_audit_logs
from pipelines.observability.airflow_callbacks import OBS_DAG_CALLBACKS, OBS_DEFAULT_ARGS
from pipelines.observability.dag_helpers import (
    DBT_BIN, DBT_PROJECT_DIR, dbt_command, elementary_report, health_checks, observed_module,
)

LANDING_RFB = Dataset("datalabs://landing/rfb")

profile_config = ProfileConfig(
    profile_name="datalab",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)

with DAG(
    dag_id="dag_rfb_dw_dbt",
    doc_md=__doc__,
    default_args={
        "owner": "datalab",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        **OBS_DEFAULT_ARGS,
    },
    start_date=datetime(2026, 9, 1),
    schedule=[LANDING_RFB],
    catchup=False,
    max_active_runs=1,
    tags=["rfb", "dbt", "gold", "elementary", "observability"],
    **OBS_DAG_CALLBACKS,
) as dag:

    dbt_transformations = DbtTaskGroup(
        group_id="dbt_transformations",
        # dbt packages are installed once by airflow-init (`make dbt-deps` to refresh): DAG runs never touch the network
        project_config=ProjectConfig(DBT_PROJECT_DIR, install_dbt_deps=False),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_BIN),
        render_config=RenderConfig(select=["tag:rfb"], exclude=["package:elementary"], load_method=LoadMode.DBT_LS,
                                   emit_datasets=False),
    )

    elementary_models = dbt_command("elementary_models", "run --select elementary", trigger_rule=TriggerRule.ALL_DONE)
    elementary_html = elementary_report("elementary_report", "rfb", trigger_rule=TriggerRule.ALL_DONE)
    archive_artifacts = observed_module("archive_artifacts", "obs", "pipelines.observability.archive_dbt_artifacts",
                                        args="--pipeline rfb", trigger_rule=TriggerRule.ALL_DONE)
    data_health = health_checks("rfb", trigger_rule=TriggerRule.ALL_DONE)
    end = PythonOperator(task_id="end", python_callable=consolidate_dag_audit_logs, trigger_rule=TriggerRule.ALL_DONE)

    dbt_transformations >> elementary_models >> elementary_html >> archive_artifacts >> data_health >> end
