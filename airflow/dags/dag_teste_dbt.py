import os, sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator  # type: ignore
from airflow.datasets import Dataset # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore
from cosmos import RenderConfig, LoadMode # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipelines.commons.audit_logger import consolidate_dag_audit_logs

landing_rfb_dataset = Dataset("postgres://localhost:5432/datalab/landing_rfb/estabelecimentos")

# Importações do Astronomer Cosmos
from cosmos import (  # type: ignore
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)

# Caminhos do ambiente
DBT_PROJECT_DIR = "/opt/airflow/pipelines/dbt_projects"
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt"
EDR_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/edr"

# dbt/cosmos
profile_config = ProfileConfig(
    profile_name="datalab",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
)

default_args = {
    "owner": "datalab",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}



with DAG(
    dag_id="DAG_TEST_DBT",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
   #schedule=NONE,
    catchup=False,
    tags=["rfb", "dbt", "gold"]
) as dag:

    start = EmptyOperator(task_id="start")

    run_dbt_deps = BashOperator(
                task_id="run_dbt_deps",
                bash_command=(
                    f"{DBT_EXECUTABLE_PATH} deps "
                    f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
                    f"--no-version-check"
                ),
                execution_timeout=timedelta(minutes=5)
            )

    run_dbt_debug = BashOperator(
        task_id="run_dbt_debug",
        bash_command=(
            f"{DBT_EXECUTABLE_PATH} debug "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            f"--no-version-check"
        ),
        execution_timeout=timedelta(minutes=5)
    )


    end = EmptyOperator(task_id="end")

    # Definição de Ordem/Linhagem
    start >> run_dbt_deps >> run_dbt_debug >> end