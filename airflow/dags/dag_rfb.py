import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore
from airflow.models.param import Param # type: ignore # airflow ui toggle
from airflow.datasets import Dataset  # type: ignore

landing_rfb_dataset = Dataset("postgres://localhost:5432/datalab/landing_rfb/estabelecimentos")

##################################
# COMMONS UTILS
##################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipelines.commons.audit_logger import consolidate_dag_audit_logs
##################################

default_args = {
    'owner': 'datalab',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_rfb',
    default_args=default_args,
    description='pipeline for rfb webdav download and bronze parquet conversion',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['rfb', 'bronze', 's3', 'polars', 'parquet', 'grafana'],
    params={
        'dev_mode': Param(True, type="boolean", description="Ativa limite de linhas para testes rápidos nas tabelas Fato")
    }
) as dag:

    start = EmptyOperator(task_id="start")

    trigger_infra_setup = TriggerDagRunOperator(
        task_id='trigger_infra_setup',
        trigger_dag_id='dag_setup_infrastructure',
        wait_for_completion=True,
        poke_interval=10,
    )

    task_download_files = BashOperator(
        task_id="task_download_files",
        bash_command="python3 -u /opt/airflow/pipelines/rfb/rfb01_download_files.py",
        execution_timeout=timedelta(minutes=260),
    )

    # Variável de ambiente DEV_MODE injetada via Jinja
    task_extract_to_bronze = BashOperator(
        task_id="task_extract_to_bronze",
        bash_command="DEV_MODE={{ params.dev_mode }} python3 -u /opt/airflow/pipelines/rfb/rfb02_s3_unzip_bronze.py",
        execution_timeout=timedelta(minutes=260),
    )

    task_load_s3_silver_dims = BashOperator(
        task_id="task_load_s3_silver_dims",
        bash_command="python3 -u /opt/airflow/pipelines/rfb/rfb03_s3_silver_dimensions.py",
        execution_timeout=timedelta(minutes=260),
    )

    task_load_s3_silver_estabelecimentos = BashOperator(
        task_id="task_load_s3_silver_estabelecimentos",
        bash_command="python3 -u /opt/airflow/pipelines/rfb/rfb04_s3_silver_estabelecimentos.py",
        execution_timeout=timedelta(minutes=260),
    )

    task_load_s3_silver_empresas = BashOperator(
        task_id="task_load_s3_silver_empresas",
        bash_command="python3 -u /opt/airflow/pipelines/rfb/rfb05_s3_silver_empresas.py",
        execution_timeout=timedelta(minutes=260),
    )

    task_load_s3_silver_socios = BashOperator(
        task_id="task_load_s3_silver_socios",
        bash_command="python3 -u /opt/airflow/pipelines/rfb/rfb06_s3_silver_socios.py",
        execution_timeout=timedelta(minutes=260),
    )

    # DW LANDING
    task_load_dw_landing_estabelecimentos = BashOperator(
        task_id="task_load_dw_landing_estabelecimentos",
        bash_command="python3 -u /opt/airflow/pipelines/rfb/rfb07_dw_landing_estabelecimentos.py",
        execution_timeout=timedelta(minutes=260),
    )

    task_load_dw_landing_empresas = BashOperator(
        task_id="task_load_dw_landing_empresas",
        bash_command="python3 -u /opt/airflow/pipelines/rfb/rfb08_dw_landing_empresas.py",
        execution_timeout=timedelta(minutes=260),
    )

    task_load_dw_landing_socios = BashOperator(
        task_id="task_load_dw_landing_socios",
        bash_command="python3 -u /opt/airflow/pipelines/rfb/rfb09_dw_landing_socios.py",
        execution_timeout=timedelta(minutes=260),
    )

    task_load_dw_landing_simples = BashOperator(
        task_id="task_load_dw_landing_simples",
        bash_command="python3 -u /opt/airflow/pipelines/rfb/rfb10_dw_landing_simples.py",
        execution_timeout=timedelta(minutes=260),
    )

    task_load_dw_landing_dimensions = BashOperator(
        task_id="task_load_dw_landing_dimensions",
        bash_command="python3 -u /opt/airflow/pipelines/rfb/rfb11_dw_landing_dimensions.py",
        execution_timeout=timedelta(minutes=260),
    )

    end = PythonOperator(
        task_id="end",
        python_callable=consolidate_dag_audit_logs,
        trigger_rule=TriggerRule.ALL_DONE,
        outlets=[landing_rfb_dataset]
    )

    (
        start
        >> trigger_infra_setup
        >> task_download_files
        >> task_extract_to_bronze
        >> task_load_s3_silver_dims
        >> task_load_s3_silver_estabelecimentos
        >> task_load_s3_silver_empresas
        >> task_load_s3_silver_socios
        >> task_load_dw_landing_estabelecimentos
        >> task_load_dw_landing_empresas
        >> task_load_dw_landing_socios
        >> task_load_dw_landing_simples
        >> task_load_dw_landing_dimensions
        >> end
    )