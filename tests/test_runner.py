import textwrap

import pytest

from pipelines.observability import run
from pipelines.observability.tracker import current_step


def _script(tmp_path, body: str) -> str:
    path = tmp_path / "step.py"
    path.write_text(textwrap.dedent(body))
    return str(path)


@pytest.mark.parametrize("body,code", [
    ("print('ok')", 0),
    ("import sys; sys.exit(99)", 99),       # no new data -> Airflow skip
    ("import sys; sys.exit(3)", 3),
    ("raise ValueError('boom')", 1),
])
def test_runner_preserves_exit_codes(tmp_path, body, code):
    assert run.main(["test", _script(tmp_path, body), "--step", "s"]) == code


def test_runner_collects_metrics_and_status(tmp_path):
    script = _script(tmp_path, """
        from pipelines.observability import current_step
        step = current_step()
        step.add(rows_in=10, files_total=2)
        step.add(rows_in=5, rows_out=12, source="unit-test")
    """)
    assert run.main(["test", script, "--step", "metrics"]) == 0
    step = current_step()
    assert step.status == "success"
    assert step.counters == {"rows_in": 15, "files_total": 2, "rows_out": 12}
    assert step.metrics["source"] == "unit-test"


def test_failed_step_records_traceback(tmp_path):
    run.main(["test", _script(tmp_path, "raise RuntimeError('source down')"), "--step", "boom"])
    step = current_step()
    assert step.status == "failed"
    assert "source down" in step.error_message


def test_script_args_are_forwarded(tmp_path):
    script = _script(tmp_path, "import sys; sys.exit(0 if sys.argv[1:] == ['--pipeline', 'anp'] else 2)")
    assert run.main(["test", script, "--", "--pipeline", "anp"]) == 0
