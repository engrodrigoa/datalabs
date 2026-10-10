"""
dag_nfe — NF-e (IBS/CBS/IS): chegada de XML -> lote -> bronze (SQL) -> silver/gold (dbt + Cosmos).

    aguardar_xml        FileSensor na inbox (reschedule: não prende worker enquanto espera)
    reservar_lote       mv inbox/*.xml -> processando/<lote>  (o lote "congela" o que chegou)
    ingerir_bronze      ctrl.fn_nfe_ingerir_lote(): pg_read_file -> landing_nfe.nfe_xml (100% SQL)
    dbt_nfe             seeds, silver (incremental por lote), snapshot, gold, testes (tag:nfe)
    finalizar_lote      CARREGADO -> PROCESSADO        (o "indi_processamento" 1 -> 2, por lote)
    marcar_erro         CARREGADO -> ERRO se o dbt falhar (reprocessar: ctrl.fn_nfe_reprocessar_lote)
    arquivar_lote       processando/<lote> -> processados/<lote>

Python aqui é só orquestração: toda transformação é SQL (função de ingestão + modelos dbt).
Simular chegada de notas:  make nfe-demo   (ou scripts/nfe/gen_nfe.py --inbox datasource/nfe/inbox)
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator  # type: ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # type: ignore
from airflow.sensors.filesystem import FileSensor  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from cosmos import DbtTaskGroup, ExecutionConfig, LoadMode, ProfileConfig, ProjectConfig, RenderConfig  # type: ignore

from pipelines.observability.airflow_callbacks import OBS_DAG_CALLBACKS, OBS_DEFAULT_ARGS
from pipelines.observability.dag_helpers import DBT_BIN, DBT_PROJECT_DIR

NFE_DIR = os.getenv("NFE_DIR", "/mnt/datasource/nfe")      # mesmo caminho no Airflow e no Postgres
MAX_ARQUIVOS_LOTE = int(os.getenv("NFE_MAX_ARQUIVOS_LOTE", "5000"))
LOTE = "{{ ts_nodash }}"
LOTE_ID = "{{ (ti.xcom_pull(task_ids='ingerir_bronze') or [[0]])[0][0] }}"   # 0 = nenhum lote (no-op)

profile_config = ProfileConfig(
    profile_name="datalab",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)

with DAG(
    dag_id="dag_nfe",
    doc_md=__doc__,
    default_args={
        "owner": "datalab",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        **OBS_DEFAULT_ARGS,
    },
    start_date=datetime(2026, 10, 1),
    schedule=timedelta(minutes=10),
    catchup=False,
    max_active_runs=1,          # um lote por vez: incremental simples e determinístico
    tags=["nfe", "dbt", "sql", "incremental", "observability"],
    **OBS_DAG_CALLBACKS,
) as dag:

    aguardar_xml = FileSensor(
        task_id="aguardar_xml",
        fs_conn_id="fs_default",
        filepath=f"{NFE_DIR}/inbox/*.xml",
        poke_interval=30,
        timeout=9 * 60,
        mode="reschedule",
        # sem arquivos -> run SKIPPED (não é falha). ATENÇÃO (Airflow 2.9): soft_fail transforma
        # QUALQUER exceção do poke em SKIPPED (ex.: conexão fs_default inexistente). Skip em ~1 s, em vez
        # de ~9 min, indica erro mascarado: veja o log e a conexão (declarada no docker-compose).
        soft_fail=True,
    )

    reservar_lote = BashOperator(
        task_id="reservar_lote",
        bash_command=f"""
            set -euo pipefail
            DST="{NFE_DIR}/processando/{LOTE}"
            mkdir -p "$DST"
            find "{NFE_DIR}/inbox" -maxdepth 1 -name '*.xml' -print0 \\
              | head -z -n {MAX_ARQUIVOS_LOTE} | xargs -0 -r mv -t "$DST"
            chmod -R a+rX "$DST"     # o Postgres (outro container/usuário) precisa ler
            N=$(find "$DST" -maxdepth 1 -name '*.xml' | wc -l)
            if [ "$N" -eq 0 ]; then rmdir "$DST"; echo "nada a processar"; exit 99; fi
            echo "lote {LOTE}: $N arquivos em $DST"
        """,
        skip_on_exit_code=99,
    )

    ingerir_bronze = SQLExecuteQueryOperator(
        task_id="ingerir_bronze",
        conn_id="postgres_default",
        sql="SELECT ctrl.fn_nfe_ingerir_lote(%(lote)s, %(dir)s, %(run)s)",
        parameters={"lote": LOTE, "dir": f"{NFE_DIR}/processando/{LOTE}", "run": "{{ run_id }}"},
        do_xcom_push=True,       # [[lote_id]] -> teto do processamento no dbt e na finalização
        autocommit=True,
    )

    dbt_nfe = DbtTaskGroup(
        group_id="dbt_nfe",
        project_config=ProjectConfig(DBT_PROJECT_DIR, install_dbt_deps=False),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_BIN),
        render_config=RenderConfig(select=["tag:nfe"], exclude=["package:elementary"],
                                   load_method=LoadMode.DBT_LS, emit_datasets=False),
        operator_args={"vars": {"nfe_lote_max": LOTE_ID}},   # 'vars' é campo templatizado no Cosmos
    )

    finalizar_lote = SQLExecuteQueryOperator(
        task_id="finalizar_lote",
        conn_id="postgres_default",
        sql=f"SELECT ctrl.fn_nfe_finalizar_lotes({LOTE_ID})",
        autocommit=True,
    )

    marcar_erro = SQLExecuteQueryOperator(
        task_id="marcar_erro",
        conn_id="postgres_default",
        sql=f"SELECT ctrl.fn_nfe_finalizar_lotes({LOTE_ID}, 'ERRO', 'falha no dbt: {{{{ run_id }}}}')",
        autocommit=True,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    arquivar_lote = BashOperator(
        task_id="arquivar_lote",
        bash_command=f"""
            set -euo pipefail
            mkdir -p "{NFE_DIR}/processados"
            mv "{NFE_DIR}/processando/{LOTE}" "{NFE_DIR}/processados/{LOTE}"
        """,
    )

    aguardar_xml >> reservar_lote >> ingerir_bronze >> dbt_nfe >> finalizar_lote >> arquivar_lote
    dbt_nfe >> marcar_erro
