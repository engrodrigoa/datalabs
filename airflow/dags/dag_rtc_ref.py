"""
dag_rtc_ref — official CST / cClassTrib / NCM tables (Receita Federal tax calculator) -> ref_rtc -> NF-e adherence.

    fetch_source  -> load_reference -> gate -> dbt_rtc (tag:rtc)                  -> health_checks_rtc -> end
    rtc01 (watch)    rtc02 (one tx)            stg_rtc__* -> silver.rtc_* ->
                                               gold.ft_nfe_item_aderencia_rtc + ag_ (mensal) + testes (dq_nfe)

Source (ADR 0019): github.com/ReceitaFederal/calculadora-rtc-regime-regular (MIT), a mirror of the Receita/Serpro
repository whose Flyway scripts ARE the official tables. rtc01 asks git for the branch head (`git ls-remote`):
same sha as the lake manifest -> SKIPPED. A new sha is fetched (sparse, only flyway/sql), replayed into SQLite
without Java (flyway_replay.py), checked against the source contract and landed; rtc02 loads it into ref_rtc in one
transaction and records the version in ctrl.rtc_carga.

Why dbt runs even when nothing new was fetched (gate = NONE_FAILED)
    The adherence model reads two moving inputs: the reference tables (weekly at most) and the NF-e items (every
    10 min, dag_nfe). "Skipped" upstream means "the reference is up to date", not "stop": the items that arrived
    since the last run still need to be evaluated. A failed step (source, contract, load) does stop everything.

Daily, not weekly: the source check is one `git ls-remote` (~1 s) and the adherence rebuild is seconds at lab
volume. How often the mirror actually moves is measured by the checks (rtc_reference_age_days), not assumed.

Params: force -> rebuild and reload the current sha (e.g. after a fix in flyway_replay.py).
"""
import os
import sys
from datetime import datetime, timedelta

from airflow.providers.standard.operators.empty import EmptyOperator  # type: ignore
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore
from airflow.sdk import DAG, Param, TriggerRule  # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig  # type: ignore
from cosmos.constants import TestBehavior  # type: ignore

from pipelines.commons.audit_logger import consolidate_dag_audit_logs
from pipelines.observability.airflow_callbacks import OBS_DAG_CALLBACKS, OBS_DEFAULT_ARGS
from pipelines.observability.dag_helpers import (
    DBT_BIN, DBT_PROJECT_DIR, dbt_load_config, health_checks, observed_script, run_deadline,
)

FORCE = "{{ '--force' if params.force else '' }}"

profile_config = ProfileConfig(
    profile_name="datalab",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)
DBT_LOAD = dbt_load_config()

with DAG(
    dag_id="dag_rtc_ref",
    doc_md=__doc__,
    default_args={
        "owner": "datalab",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        **OBS_DEFAULT_ARGS,
    },
    start_date=datetime(2026, 11, 1),
    schedule="20 6 * * *",
    catchup=False,
    max_active_runs=1,
    deadline=run_deadline(timedelta(minutes=45), "dag_rtc_ref: reference checked and adherence rebuilt within 45 min"),
    params={"force": Param(False, type="boolean", description="rebuild and reload the current sha")},
    tags=["rtc", "nfe", "reference", "dbt", "observability"],
    **OBS_DAG_CALLBACKS,
) as dag:

    # exit 99 -> SKIPPED (BashOperator's default skip_on_exit_code): "the reference is already up to date"
    fetch_source = observed_script("fetch_source", "rtc", "rtc/rtc01_fetch.py", args=FORCE,
                                   execution_timeout=timedelta(minutes=10))
    load_reference = observed_script("load_reference", "rtc", "rtc/rtc02_load.py", args=FORCE,
                                     trigger_rule=TriggerRule.NONE_FAILED, execution_timeout=timedelta(minutes=10))
    gate = EmptyOperator(task_id="gate", trigger_rule=TriggerRule.NONE_FAILED)

    dbt_rtc = DbtTaskGroup(
        group_id="dbt_rtc",
        project_config=ProjectConfig(DBT_PROJECT_DIR, manifest_path=DBT_LOAD["manifest_path"], install_dbt_deps=False),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_BIN),
        render_config=RenderConfig(
            select=["tag:rtc"], exclude=["package:elementary"], load_method=DBT_LOAD["load_method"],
            emit_datasets=False, test_behavior=TestBehavior.BUILD, should_detach_multiple_parents_tests=True,
        ),
    )

    # NONE_FAILED (not ALL_DONE), as in dag_rag_lc214: a failed step upstream leaves this leaf upstream_failed and
    # the run red; with ALL_DONE a green leaf would hide it.
    data_health = health_checks("rtc", trigger_rule=TriggerRule.NONE_FAILED)
    end = PythonOperator(task_id="end", python_callable=consolidate_dag_audit_logs, trigger_rule=TriggerRule.ALL_DONE)

    fetch_source >> load_reference >> gate >> dbt_rtc >> data_health >> end
