"""
dag_rfb — Receita Federal (CNPJ open data): WebDAV -> bronze parquet -> silver parquet -> DW landing.

* `ref_month` param (YYYY-MM) replaces the reference month hardcoded in every script.
* `dev_mode` limits big entities to 500k rows (laptop friendly).
* Independent entities run in parallel (max_active_tasks bounds memory usage).
* Emits the `datalabs://landing/rfb` dataset, which triggers dag_rfb_dw_dbt
  (dbt + Elementary + RFB data health checks run there, after gold is rebuilt).
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset  # type: ignore
from airflow.models.param import Param  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipelines.commons.audit_logger import consolidate_dag_audit_logs
from pipelines.observability.airflow_callbacks import OBS_DAG_CALLBACKS, OBS_DEFAULT_ARGS
from pipelines.observability.dag_helpers import observed_script

LANDING_RFB = Dataset("datalabs://landing/rfb")

RFB_ENV = {
    "RFB_REF_MONTH": "{{ params.ref_month }}",
    "DEV_MODE": "{{ params.dev_mode }}",
}
HEAVY = timedelta(hours=4)

with DAG(
    dag_id="dag_rfb",
    doc_md=__doc__,
    default_args={
        "owner": "datalab",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        **OBS_DEFAULT_ARGS,
    },
    description="RFB WebDAV -> bronze/silver parquet (MinIO) -> DW landing",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,  # Polars jobs are memory hungry: at most 2 in parallel
    tags=["rfb", "s3", "polars", "parquet", "observability"],
    params={
        "ref_month": Param("2026-08", type="string", pattern=r"^\d{4}-\d{2}$", description="Reference month (YYYY-MM)"),
        "dev_mode": Param(True, type="boolean", description="Limit big entities to 500k rows"),
    },
    **OBS_DAG_CALLBACKS,
) as dag:

    def step(task_id, script, timeout=HEAVY, **kw):
        return observed_script(task_id, "rfb", f"rfb/{script}", env=RFB_ENV, execution_timeout=timeout, **kw)

    start = EmptyOperator(task_id="start")
    setup_infra = TriggerDagRunOperator(
        task_id="trigger_infra_setup", trigger_dag_id="dag_setup_infrastructure",
        wait_for_completion=True, poke_interval=10,
    )

    download = step("download_files", "rfb01_download_files.py")
    to_bronze = step("extract_to_bronze", "rfb02_s3_unzip_bronze.py")

    silver_dims = step("silver_dimensions", "rfb03_s3_silver_dimensions.py")
    silver_estab = step("silver_estabelecimentos", "rfb04_s3_silver_estabelecimentos.py")
    silver_empresas = step("silver_empresas", "rfb05_s3_silver_empresas.py")
    silver_socios = step("silver_socios", "rfb06_s3_silver_socios.py")

    landing_estab = step("landing_estabelecimentos", "rfb07_dw_landing_estabelecimentos.py")
    landing_empresas = step("landing_empresas", "rfb08_dw_landing_empresas.py")
    landing_socios = step("landing_socios", "rfb09_dw_landing_socios.py")
    landing_simples = step("landing_simples", "rfb10_dw_landing_simples.py")
    landing_dims = step("landing_dimensions", "rfb11_dw_landing_dimensions.py")

    landed = EmptyOperator(task_id="landed", outlets=[LANDING_RFB])
    end = PythonOperator(task_id="end", python_callable=consolidate_dag_audit_logs, trigger_rule=TriggerRule.ALL_DONE)

    start >> setup_infra >> download >> to_bronze
    to_bronze >> [silver_dims, silver_estab, silver_empresas, silver_socios]
    silver_estab >> landing_estab
    silver_empresas >> landing_empresas
    silver_socios >> landing_socios
    silver_dims >> [landing_simples, landing_dims]
    [landing_estab, landing_empresas, landing_socios, landing_simples, landing_dims] >> landed >> end
