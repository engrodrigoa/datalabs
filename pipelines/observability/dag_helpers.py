"""Small factories so every DAG runs scripts the same, observable way."""
from __future__ import annotations

from datetime import timedelta

from airflow.operators.bash import BashOperator  # type: ignore

PIPELINES_DIR = "/opt/airflow/pipelines"
DBT_PROJECT_DIR = f"{PIPELINES_DIR}/dbt_projects"
DBT_BIN = "/opt/airflow/dbt_venv/bin/dbt"
EDR_BIN = "/opt/airflow/dbt_venv/bin/edr"


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
        f"--file-path {DBT_PROJECT_DIR}/logs/elementary_logs/edr_report_{pipeline}_{{{{ ts_nodash }}}}.html"
    )
    kwargs.setdefault("execution_timeout", timedelta(minutes=10))
    return BashOperator(task_id=task_id, bash_command=cmd, append_env=True, **kwargs)
