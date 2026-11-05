"""Small factories so every DAG runs scripts the same, observable way."""
from __future__ import annotations

import logging
import os
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator  # type: ignore

# Container paths. Overridable only so the DAG integrity test (tests/test_dag_integrity.py) can parse the
# DAGs outside the image; the compose stack never sets them.
PIPELINES_DIR = os.getenv("DATALABS_PIPELINES_DIR", "/opt/airflow/pipelines")
DBT_PROJECT_DIR = f"{PIPELINES_DIR}/dbt_projects"
DBT_VENV_BIN = os.getenv("DATALABS_DBT_VENV_BIN", "/opt/airflow/dbt_venv/bin")
DBT_BIN = f"{DBT_VENV_BIN}/dbt"
EDR_BIN = f"{DBT_VENV_BIN}/edr"

# Run timestamp that works for EVERY run type. Airflow 3 leaves logical_date empty on manual, asset-triggered
# and TriggerDagRunOperator runs, and then drops the KEYS logical_date/ds/ts/ts_nodash from the template
# context: `{{ ts_nodash }}` and even `{{ logical_date or ... }}` fail with "'logical_date' is undefined".
# dag_run is always in the context; run_after is always set (scheduled time, or trigger time).
RUN_TS = "{{ (dag_run.logical_date or dag_run.run_after) | ts_nodash }}"

# Manifest que o Cosmos lê para montar o grafo das tasks (gerado por `dbt parse`: airflow-init / `make dbt-parse`).
# Sem ele o Cosmos roda `dbt ls` a CADA parse do arquivo da DAG (a cada ~30 s); no bind mount do Windows isso passa
# de 120 s, o DagFileProcessor é morto e a DAG some da UI.
DBT_MANIFEST_DIR = os.getenv("DBT_MANIFEST_DIR", "/opt/airflow/dbt_vol/manifest")
DBT_MANIFEST_PATH = f"{DBT_MANIFEST_DIR}/manifest.json"


def dbt_load_config() -> dict:
    """kwargs de ProjectConfig/RenderConfig: manifest se existir, senão `dbt ls` (lento, mas funciona)."""
    from cosmos import LoadMode  # type: ignore

    if os.path.exists(DBT_MANIFEST_PATH):
        return {"manifest_path": DBT_MANIFEST_PATH, "load_method": LoadMode.DBT_MANIFEST}
    logging.getLogger(__name__).warning(
        "dbt manifest ausente (%s): Cosmos vai usar `dbt ls` no parse. Rode `make dbt-parse`.", DBT_MANIFEST_PATH)
    return {"manifest_path": None, "load_method": LoadMode.DBT_LS}


def run_deadline(within: timedelta, objective: str):
    """`DAG(deadline=...)`: alert when a run is not finished `within` after it was queued.

    Airflow 3 removed SLAs (`sla=`, `sla_miss_callback` are silently ignored). A DeadlineAlert is armed per
    DAG run, cleared when the run finishes, and on a miss the executor runs on_deadline_missed (SyncCallback:
    no triggerer round-trip, same alerting path as task failures).
    """
    from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback  # type: ignore

    from pipelines.observability.airflow_callbacks import on_deadline_missed

    return DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=within,
        callback=SyncCallback(on_deadline_missed, kwargs={"objective": objective}),
    )


def observed_script(task_id: str, pipeline: str, script: str, args: str = "", **kwargs) -> BashOperator:
    """Run `pipelines/<script>` through the observability runner (audit row + structured events).

    Exit code 99 from the script -> task SKIPPED (no new data); anything else != 0 -> FAILED.
    """
    cmd = f"python -u -m pipelines.observability.run {pipeline} {PIPELINES_DIR}/{script}"
    if args:
        cmd += f" -- {args}"
    kwargs.setdefault("execution_timeout", timedelta(minutes=30))
    return BashOperator(task_id=task_id, bash_command=cmd, append_env=True, **kwargs)


def observed_module(task_id: str, pipeline: str, module: str, args: str = "", **kwargs) -> BashOperator:
    cmd = f"python -u -m pipelines.observability.run {pipeline} {module} --module"
    if args:
        cmd += f" -- {args}"
    kwargs.setdefault("execution_timeout", timedelta(minutes=15))
    return BashOperator(task_id=task_id, bash_command=cmd, append_env=True, **kwargs)


def health_checks(pipeline: str, fail_on: str = "error", skip_heavy: bool = False, **kwargs) -> BashOperator:
    """Data health checks for one pipeline (pipelines/observability/checks.yml)."""
    return observed_module(
        task_id=kwargs.pop("task_id", f"health_checks_{pipeline}"),
        pipeline="obs",
        module="pipelines.observability.checks",
        args=f"--pipeline {pipeline} --fail-on {fail_on}" + (" --skip-heavy" if skip_heavy else ""),
        **kwargs,
    )


def dbt_command(task_id: str, command: str, **kwargs) -> BashOperator:
    cmd = f"{DBT_BIN} {command} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --no-version-check"
    kwargs.setdefault("execution_timeout", timedelta(minutes=15))
    return BashOperator(task_id=task_id, bash_command=cmd, append_env=True, **kwargs)


def elementary_report(task_id: str, pipeline: str, **kwargs) -> BashOperator:
    cmd = (
        f"{EDR_BIN} report --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
        f"--file-path {DBT_PROJECT_DIR}/logs/elementary_logs/edr_report_{pipeline}_{RUN_TS}.html"
    )
    kwargs.setdefault("execution_timeout", timedelta(minutes=10))
    return BashOperator(task_id=task_id, bash_command=cmd, append_env=True, **kwargs)
