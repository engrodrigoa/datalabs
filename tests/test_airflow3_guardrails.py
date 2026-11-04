"""Airflow 3 guardrails that need NO Airflow install (ADR 0017).

ruff AIR3 catches removed imports and parameters. It cannot see the two traps that only fail at runtime on
Airflow 3, and that this project hit during the migration:

  * templates reading logical_date-derived keys ({{ ts_nodash }}, {{ ds }}, {{ logical_date }}...): Airflow 3
    leaves logical_date empty on manual, asset-triggered and TriggerDagRunOperator runs and drops those KEYS from
    the context, so the template fails with "'logical_date' is undefined". Use RUN_TS (dag_helpers).
  * task code reading the metadata DB (dag_run.get_task_instances(), settings.Session): tasks only reach the
    Execution API in Airflow 3. The audit gate uses ti.get_task_states().

Plus unit tests of the pieces rewritten for Airflow 3: the audit gate, the log renderer, the deadline callback.
"""
import ast
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
ORCHESTRATION_CODE = sorted((ROOT / "airflow" / "dags").glob("*.py")) + [
    ROOT / "pipelines" / "observability" / "dag_helpers.py",
    ROOT / "pipelines" / "observability" / "airflow_callbacks.py",
    ROOT / "pipelines" / "commons" / "audit_logger.py",
]

# context keys that only exist when the run has a logical_date
LOGICAL_DATE_KEYS = ("logical_date", "execution_date", "ds", "ds_nodash", "ts", "ts_nodash", "ts_nodash_with_tz",
                     "data_interval_start", "data_interval_end", "prev_ds", "next_ds", "yesterday_ds", "tomorrow_ds")


def _string_literals(path: Path) -> list[str]:
    """Every string constant in the module (f-string parts included), comments excluded by construction."""
    return [n.value for n in ast.walk(ast.parse(path.read_text(encoding="utf-8"))) if isinstance(n, ast.Constant)
            and isinstance(n.value, str)]


def _bare_keys_in_templates(text: str) -> set[str]:
    """Context keys read directly inside {{ ... }} (attribute access like dag_run.logical_date and filters are fine)."""
    found = set()
    for expr in re.findall(r"\{\{(.*?)\}\}", text, flags=re.S):
        expr = re.sub(r"\|\s*\w+", "", expr)                      # drop filters: `| ts_nodash`
        for key in LOGICAL_DATE_KEYS:
            if re.search(rf"(?<![\w.]){key}\b", expr):
                found.add(key)
    return found


@pytest.mark.parametrize("path", ORCHESTRATION_CODE, ids=lambda p: p.name)
def test_templates_do_not_depend_on_logical_date(path):
    offenders = {key for s in _string_literals(path) for key in _bare_keys_in_templates(s)}
    assert not offenders, f"{path.name} reads {sorted(offenders)} in a template: use RUN_TS (dag_helpers) instead"


@pytest.mark.parametrize("path", ORCHESTRATION_CODE, ids=lambda p: p.name)
def test_task_code_does_not_touch_the_metadata_db(path):
    # AST, not text: names in code only (docstrings and comments may explain the rule)
    forbidden = {"get_task_instances", "Session", "create_session", "provide_session"}
    used = {n.attr if isinstance(n, ast.Attribute) else n.id
            for n in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
            if isinstance(n, (ast.Attribute, ast.Name))}
    assert not used & forbidden, f"{path.name}: {sorted(used & forbidden)} -> no metadata DB in Airflow 3 tasks"


def test_template_key_scanner_catches_the_airflow2_patterns():
    assert _bare_keys_in_templates("edr_report_{{ ts_nodash }}.html") == {"ts_nodash"}
    assert _bare_keys_in_templates("{{ (logical_date or dag_run.run_after) | ts_nodash }}") == {"logical_date"}
    assert _bare_keys_in_templates("{{ (dag_run.logical_date or dag_run.run_after) | ts_nodash }}") == set()
    assert _bare_keys_in_templates("{{ params.ref_month }} {{ run_id }}") == set()


# ---------------------------------------------------------------- audit gate (ti.get_task_states)
class _FakeTI:
    def __init__(self, states):
        self.states = states

    def get_task_states(self, dag_id, run_ids):
        return {run_ids[0]: dict(self.states)}


def _context(states):
    return {
        "ti": _FakeTI(states),
        "task": SimpleNamespace(task_id="end"),
        "dag_run": SimpleNamespace(dag_id="dag_x", run_id="manual__1", logical_date=None,
                                   run_after=datetime(2026, 11, 4, 3, 0, tzinfo=timezone.utc),
                                   start_date=datetime(2026, 11, 4, 3, 0, tzinfo=timezone.utc), end_date=None),
        "run_id": "manual__1",
    }


def test_gate_lists_failed_and_upstream_failed_tasks_but_not_itself():
    from pipelines.commons import audit_logger

    ctx = _context({"a": "success", "b": "failed", "c": "upstream_failed", "d": "skipped", "end": "running"})
    assert audit_logger._failed_tasks(ctx) == ["b", "c"]


def test_gate_fails_loud_when_task_states_cannot_be_read():
    from pipelines.commons import audit_logger

    class _Broken(_FakeTI):
        def get_task_states(self, dag_id, run_ids):
            raise RuntimeError("execution api unreachable")

    ctx = _context({})
    ctx["ti"] = _Broken({})
    with pytest.raises(RuntimeError):
        audit_logger._failed_tasks(ctx)   # never a silent [] (that would turn a failed run green)


def test_run_summary_counts_states_from_the_execution_api():
    from pipelines.commons import audit_logger

    lines = audit_logger._build_run_summary(_context({"a": "success", "b": "failed", "c": "skipped", "end": "running"}))
    assert lines[0] == " STATUS: FAILED"
    assert lines[1] == " TASKS: 3 total, 1 success, 1 failed, 1 skipped, 0 other"


def test_audit_timestamp_falls_back_to_run_after_without_logical_date():
    from pipelines.commons import audit_logger

    assert audit_logger._run_logical_date(_context({})) == datetime(2026, 11, 4, 3, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------- task log renderer
def test_airflow3_json_log_line_is_rendered_readable():
    from pipelines.commons.audit_logger import render_task_log_line

    line = json.dumps({"timestamp": "2026-11-04T03:48:01.955123Z", "level": "info", "logger": "task.stdout",
                       "event": "ERROR observability.airflow: x failed | {\"event\": \"task_failed\"}"})
    assert render_task_log_line(line + "\n") == (
        "[2026-11-04 03:48:01.955] INFO task.stdout - ERROR observability.airflow: x failed | {\"event\": \"task_failed\"}")


def test_non_json_log_lines_pass_through():
    from pipelines.commons.audit_logger import render_task_log_line

    assert render_task_log_line("plain text\n") == "plain text"
    assert render_task_log_line("{not json\n") == "{not json"
    assert render_task_log_line('{"a": 1}\n') == '{"a": 1}'


# ---------------------------------------------------------------- deadline callback (replaces SLAs)
def test_deadline_callback_alerts_with_the_run_and_objective(monkeypatch):
    from pipelines.observability import airflow_callbacks

    sent = {}
    monkeypatch.setattr(airflow_callbacks, "send_alert", lambda **kw: sent.update(kw) or True)
    airflow_callbacks.on_deadline_missed(
        context={"dag_run": {"dag_id": "dag_dbt_anp", "dag_run_id": "scheduled__1"},
                 "deadline": {"id": "x", "deadline_time": "2026-11-04 09:00:00+00:00"}},
        objective="gold within 3 h")
    assert sent["severity"] == "warning"
    assert sent["title"] == "Deadline missed: dag_dbt_anp"
    assert "gold within 3 h" in sent["message"] and "scheduled__1" in sent["message"]


def test_deadline_callback_never_raises_on_an_empty_context(monkeypatch):
    from pipelines.observability import airflow_callbacks

    monkeypatch.setattr(airflow_callbacks, "send_alert", lambda **kw: True)
    airflow_callbacks.on_deadline_missed()
