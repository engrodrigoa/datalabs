import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipelines.commons.s3_client import ensure_buckets_exist, get_s3_client
from pipelines.commons.env_loader import REQUIRED_BUCKETS
from pipelines.commons.audit_logger import consolidate_dag_audit_logs

logger = logging.getLogger("airflow.task")

DIVIDER_LENGTH = 100
POSTGRES_CONN_ID = "postgres_default"

# Single source of truth for what each step provisions, so the
# before/after existence check and the DDL stay next to each other.
TARGET_SCHEMAS = ["ctrl", "audit", "bronze", "silver", "gold", "landing_rfb"]
CTRL_TABLES = ["ctrl.anp_metadata_mensal", "ctrl.anp_metadata_semanal", "audit.dbt_runs"]
BRONZE_TABLES = ["bronze.anp_landing_mensal", "bronze.anp_landing_semanal"]


def _divider(char: str = "=") -> str:
    return char * DIVIDER_LENGTH


def _log_block(title: str, lines: list) -> None:
    logger.info(_divider("="))
    logger.info(f" {title}")
    logger.info(_divider("="))
    for line in lines:
        logger.info(f" {line}")
    logger.info(_divider("="))


def _postgres_address(hook: PostgresHook) -> str:
    conn = hook.get_connection(POSTGRES_CONN_ID)
    return f"{conn.host}:{conn.port}/{conn.schema}"


def _existing_schemas(hook: PostgresHook, schemas: list) -> set:
    rows = hook.get_records(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ANY(%s)",
        parameters=(schemas,),
    )
    return {row[0] for row in rows}


def _existing_tables(hook: PostgresHook, qualified_names: list) -> set:
    schemas = sorted({name.split(".", 1)[0] for name in qualified_names})
    rows = hook.get_records(
        "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = ANY(%s)",
        parameters=(schemas,),
    )
    found = {f"{schema}.{table}" for schema, table in rows}
    return {name for name in qualified_names if name in found}


def _create_buckets():
    s3 = get_s3_client()
    try:
        existing_before = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    except Exception:
        existing_before = set()

    ensure_buckets_exist(REQUIRED_BUCKETS)

    lines = [
        f"BUCKET s3://{bucket} -> {'already existed' if bucket in existing_before else 'created'}"
        for bucket in REQUIRED_BUCKETS
    ]
    _log_block("STORAGE (MinIO / S3) SETUP", lines)


def _create_schemas():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    address = _postgres_address(hook)
    existing_before = _existing_schemas(hook, TARGET_SCHEMAS)

    hook.run(
        """
        CREATE SCHEMA IF NOT EXISTS ctrl;
        CREATE SCHEMA IF NOT EXISTS audit;
        CREATE SCHEMA IF NOT EXISTS landing_rfb;
        CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS gold;
        """
    )

    lines = [
        f"SCHEMA {address}/{schema} -> {'already existed' if schema in existing_before else 'created'}"
        for schema in TARGET_SCHEMAS
    ]
    
    _log_block("DATABASE SCHEMAS SETUP", lines)


def _create_ctrl_tables():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    address = _postgres_address(hook)
    existing_before = _existing_tables(hook, CTRL_TABLES)

    hook.run(
        """
        CREATE TABLE IF NOT EXISTS ctrl.anp_metadata_mensal (
            cat varchar(20) NOT NULL,
            "ref" varchar(6) NOT NULL,
            "month" int4 NOT NULL,
            "year" int4 NOT NULL,
            file_name varchar(255) NOT NULL,
            url_source text NOT NULL,
            download_date timestamp DEFAULT CURRENT_TIMESTAMP NULL,
            link_name varchar(30) NULL,
            CONSTRAINT pk_control_anp PRIMARY KEY (cat, ref)
        );

        CREATE TABLE IF NOT EXISTS ctrl.anp_metadata_semanal (
            data_ref date NULL,
            data_dag_run timestamp NULL,
            status varchar NULL
        );

        CREATE TABLE IF NOT EXISTS audit.dbt_runs (
            run_timestamp timestamp NULL,
            invocation_id text NOT NULL,
            node_id text NOT NULL,
            status text NULL,
            execution_time_sec float8 NULL,
            failures int4 NULL,
            message text NULL,
            resource_type text NULL,
            "database" text NULL,
            schema_layer text NULL,
            "materialized" text NULL,
            tags text NULL,
            data_carga timestamp NULL,
            CONSTRAINT dbt_runs_pkey PRIMARY KEY (invocation_id, node_id)
        );
        """
    )

    lines = [
        f"TABLE {address}/{table} -> {'already existed' if table in existing_before else 'created'}"
        for table in CTRL_TABLES
    ]
    _log_block("CONTROL TABLES SETUP", lines)


def _create_bronze_tables():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    address = _postgres_address(hook)
    existing_before = _existing_tables(hook, BRONZE_TABLES)

    hook.run(
        """
        CREATE TABLE IF NOT EXISTS bronze.anp_landing_mensal (
            regiao_sigla text NULL,
            estado_sigla text NULL,
            municipio text NULL,
            revenda text NULL,
            cnpj_da_revenda text NULL,
            nome_da_rua text NULL,
            numero_rua text NULL,
            complemento text NULL,
            bairro text NULL,
            cep text NULL,
            produto text NULL,
            data_da_coleta text NULL,
            valor_de_venda text NULL,
            valor_de_compra text NULL,
            unidade_de_medida text NULL,
            bandeira text NULL,
            arquivo text NULL,
            ingestion_timestamp timestamp NULL
        );

        CREATE TABLE IF NOT EXISTS bronze.anp_landing_semanal (
            regiao_sigla text NULL,
            estado_sigla text NULL,
            municipio text NULL,
            revenda text NULL,
            cnpj_da_revenda text NULL,
            nome_da_rua text NULL,
            numero_rua text NULL,
            complemento text NULL,
            bairro text NULL,
            cep text NULL,
            produto text NULL,
            data_da_coleta text NULL,
            valor_de_venda text NULL,
            valor_de_compra text NULL,
            unidade_de_medida text NULL,
            bandeira text NULL,
            arquivo text NULL,
            ingestion_timestamp timestamp NULL
        );
        """
    )

    lines = [
        f"TABLE {table} -> {'already existed' if table in existing_before else 'created'}"
        for table in BRONZE_TABLES
    ]
    _log_block("BRONZE TABLES SETUP", lines)


def _confirm_environment_ready():
    logger.info(_divider("#"))
    logger.info(_divider("="))
    logger.info(" ENVIRONMENT SETUP COMPLETE")
    logger.info(_divider("="))
    logger.info(" Buckets, schemas and tables were checked and provisioned above.")
    logger.info(" Environment is ready for use.")
    logger.info(_divider("="))
    logger.info(_divider("#"))


default_args = {
    'owner': 'datalab',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dag_setup_infrastructure',
    default_args=default_args,
    description='infrastructure setup',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['setup', 'polars', 'ddl', 'anp'],
) as dag:

    # ==========================================
    # object storage (MinIO / S3)
    # ==========================================
    create_buckets = PythonOperator(
        task_id='create_buckets',
        python_callable=_create_buckets,
    )

    # ==========================================
    # schemas
    # ==========================================
    create_schemas = PythonOperator(
        task_id='create_schemas',
        python_callable=_create_schemas,
    )

    # ==========================================
    # control tables
    # ==========================================
    create_ctrl_tables = PythonOperator(
        task_id='create_ctrl_tables',
        python_callable=_create_ctrl_tables,
    )

    # ==========================================
    # bronze tables
    # ==========================================
    create_bronze_tables = PythonOperator(
        task_id='create_bronze_tables',
        python_callable=_create_bronze_tables,
    )

    # ==========================================
    # environment readiness confirmation
    # ==========================================
    confirm_environment_ready = PythonOperator(
        task_id='confirm_environment_ready',
        python_callable=_confirm_environment_ready,
    )

    # ==========================================
    # audit trail consolidation (mesmo padrão da dag_rfb / dag_dbt_anp)
    # ==========================================
    end = PythonOperator(
        task_id="end",
        python_callable=consolidate_dag_audit_logs,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ==========================================
    # flow
    # ==========================================
    (
        create_buckets
        >> create_schemas
        >> create_ctrl_tables
        >> create_bronze_tables
        >> confirm_environment_ready
        >> end
    )