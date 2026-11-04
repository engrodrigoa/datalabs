"""DAG integrity on the real Airflow 3 runtime (CI job `airflow-dags`; skipped where Airflow is not installed).

Parses airflow/dags with the same DagBag the dag-processor uses and checks what a broken deploy would hide until
the first run: import errors, deprecated APIs, missing observability callbacks, the ANP deadline, and that the run
timestamp template renders on runs WITHOUT a logical_date (manual / asset / TriggerDagRun on Airflow 3).

Needs the dbt manifest (`dbt parse`) for the Cosmos DAGs, and DATALABS_PIPELINES_DIR / DATALABS_DBT_VENV_BIN /
DBT_MANIFEST_DIR pointing at the checkout (see .github/workflows/ci.yml).
"""
import os
import warnings
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("airflow.sdk", reason="Airflow 3 not installed (runs in the CI job airflow-dags)")

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"
EXPECTED_DAGS = {
    "dag_dbt_anp", "dag_nfe", "dag_observability_health", "dag_rag_anp", "dag_rag_lc214", "dag_rfb",
    "dag_rfb_dw_dbt", "dag_setup_infrastructure",
}


@pytest.fixture(scope="module")
def dagbag_and_warnings():
    from airflow.dag_processing.dagbag import DagBag  # type: ignore

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        bag = DagBag(str(DAGS_DIR))
    return bag, caught


def test_every_dag_imports(dagbag_and_warnings):
    bag, _ = dagbag_and_warnings
    assert not bag.import_errors, bag.import_errors
    assert set(bag.dag_ids) == EXPECTED_DAGS


def test_no_deprecated_airflow_api(dagbag_and_warnings):
    _, caught = dagbag_and_warnings
    deprecated = sorted({str(w.message).splitlines()[0] for w in caught
                         if "deprecated" in str(w.message).lower() and "airflow" in str(w.message).lower()})
    assert not deprecated, deprecated


def test_every_task_and_dag_has_the_observability_callbacks(dagbag_and_warnings):
    from pipelines.observability.airflow_callbacks import on_dag_failure, on_task_failure, on_task_retry

    def _has(callbacks, fn):
        return fn in (callbacks if isinstance(callbacks, (list, tuple)) else [callbacks])

    bag, _ = dagbag_and_warnings
    for dag in bag.dags.values():
        assert _has(dag.on_failure_callback, on_dag_failure), dag.dag_id
        for task in dag.tasks:
            assert _has(task.on_failure_callback, on_task_failure), f"{dag.dag_id}.{task.task_id}"
            assert _has(task.on_retry_callback, on_task_retry), f"{dag.dag_id}.{task.task_id}"


def test_anp_has_a_run_deadline_instead_of_the_removed_sla(dagbag_and_warnings):
    from datetime import timedelta

    bag, _ = dagbag_and_warnings
    deadline = bag.dags["dag_dbt_anp"].deadline
    alerts = deadline if isinstance(deadline, list) else [deadline]
    assert alerts and alerts[0].interval == timedelta(hours=3)
    assert alerts[0].callback.path.endswith("airflow_callbacks.on_deadline_missed")


def test_assets_wire_the_producer_and_consumer_dags(dagbag_and_warnings):
    bag, _ = dagbag_and_warnings
    produced = {o.uri for t in bag.dags["dag_rfb"].tasks for o in t.outlets}
    assert "datalabs://landing/rfb" in produced
    assert "datalabs://landing/rfb" in str(bag.dags["dag_rfb_dw_dbt"].timetable.asset_condition)


@pytest.mark.parametrize("logical_date", [None, datetime(2026, 11, 4, 9, 0, tzinfo=timezone.utc)],
                         ids=["manual_or_asset_run", "scheduled_run"])
def test_run_ts_renders_with_and_without_logical_date(dagbag_and_warnings, logical_date):
    from pipelines.observability.dag_helpers import RUN_TS

    bag, _ = dagbag_and_warnings
    env = bag.dags["dag_nfe"].get_template_env()          # Airflow's env: StrictUndefined + its filters
    run_after = datetime(2026, 11, 4, 3, 32, 18, tzinfo=timezone.utc)
    rendered = env.from_string(RUN_TS).render(dag_run=SimpleNamespace(logical_date=logical_date, run_after=run_after))
    assert rendered == (logical_date or run_after).strftime("%Y%m%dT%H%M%S")


def test_the_airflow2_template_fails_without_logical_date(dagbag_and_warnings):
    """Documents the trap RUN_TS avoids: Airflow 3 drops ts_nodash from the context of manual/asset runs."""
    import jinja2

    bag, _ = dagbag_and_warnings
    env = bag.dags["dag_nfe"].get_template_env()
    with pytest.raises(jinja2.UndefinedError):
        env.from_string("{{ ts_nodash }}").render(dag_run=SimpleNamespace(logical_date=None))


def test_container_paths_are_overridable_for_ci():
    from pipelines.observability import dag_helpers

    assert dag_helpers.DBT_PROJECT_DIR == f"{os.getenv('DATALABS_PIPELINES_DIR', '/opt/airflow/pipelines')}/dbt_projects"
