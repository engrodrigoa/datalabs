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
    dag_id="dag_rfb_dw_dbt",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule=[landing_rfb_dataset],
    catchup=False,
    tags=["rfb", "dbt", "gold"]
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Task isolada para instalar pacotes (ex: dbt_utils)
    run_dbt_deps = BashOperator(
                task_id="run_dbt_deps_rfb",
                bash_command=(
                    f"{DBT_EXECUTABLE_PATH} deps "
                    f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
                    f"--no-version-check"
                    
                ),
                execution_timeout=timedelta(minutes=5),
                trigger_rule=TriggerRule.ALL_DONE,
            )

    # 2. Grupo de Tasks gerenciado pelo Cosmos (Lê os modelos e transforma em tasks do Airflow)
    dbt_transformations = DbtTaskGroup(
            group_id="dbt_transformations",
            project_config=ProjectConfig(DBT_PROJECT_DIR),
            profile_config=profile_config,
            execution_config=ExecutionConfig(
                dbt_executable_path=DBT_EXECUTABLE_PATH
            ),
            render_config=RenderConfig(
                select=["tag:rfb"], 
                exclude=["package:elementary"],
                load_method=LoadMode.DBT_LS
                ),
            operator_args={"emit_datasets": False},
        )

   

    run_elementary_models = BashOperator(
        task_id="run_elementary_models",
        bash_command=(
            f"{DBT_EXECUTABLE_PATH} run --select elementary "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            f"--profile datalab --target dev"
        ),
        execution_timeout=timedelta(minutes=10)
    )

    generate_elementary_report = BashOperator(
        task_id="generate_elementary_report",
        bash_command=(
            f"DBT_PACKAGES_DIR=/tmp {EDR_EXECUTABLE_PATH} report "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            f"--file-path {DBT_PROJECT_DIR}/logs/elementary_logs/edr_report_rfb_{{{{ ts_nodash }}}}.html"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    export_dbt_logs = BashOperator(
        task_id="export_dbt_logs",
        # Aponta exatamente para o seu novo script da RFB
        bash_command="python -u /opt/airflow/pipelines/rfb/rfb12_export_dbt_logs.py", 
        execution_timeout=timedelta(minutes=5)
    )

    end = EmptyOperator(task_id="end")

    # NOVA LINHAGEM: Garante que a auditoria ocorra após a transformação
    start >> run_dbt_deps >> dbt_transformations >> run_elementary_models >> generate_elementary_report >> export_dbt_logs >> end