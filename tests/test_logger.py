import json
import logging

from pipelines.commons.logger import JsonFormatter, log_event


def test_json_formatter_includes_airflow_context_and_event(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag_x")
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "task_y")
    logger = logging.getLogger("test.json")
    records = []

    class Capture(logging.Handler):
        def emit(self, record):
            records.append(JsonFormatter().format(record))

    logger.handlers = [Capture()]
    logger.setLevel(logging.INFO)
    logger.propagate = False
    log_event(logger, "file_loaded", "loaded", rows=10, file="a.csv")

    payload = json.loads(records[0])
    assert payload["event"] == "file_loaded"
    assert payload["rows"] == 10
    assert payload["dag_id"] == "dag_x" and payload["task_id"] == "task_y"
    assert payload["level"] == "INFO"


def test_line_format_is_readable_and_keeps_fields_parseable(monkeypatch):
    import re

    from pipelines.commons.logger import LineFormatter

    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag_x")  # context lives in the log path, not on every line
    record = logging.LogRecord("anp_landing", logging.WARNING, __file__, 1, "x.csv | rejected", None, None)
    record.event, record.file = "schema_drift", "x.csv"
    line = LineFormatter().format(record)
    assert line.startswith("WARNING anp_landing: x.csv | rejected | {")
    assert "dag_x" not in line
    # same expression Promtail uses (infra/promtail/promtail-config.yaml)
    m = re.match(r"^(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL) (?P<logger>[^ :{]+): (?P<msg>.*?)"
                 r"(?: \| (?P<payload>\{.*\}))?$", line)
    assert m["level"] == "WARNING" and m["msg"] == "x.csv | rejected"
    assert json.loads(m["payload"]) == {"event": "schema_drift", "file": "x.csv"}


def test_line_format_without_fields_has_no_tail():
    from pipelines.commons.logger import LineFormatter

    record = logging.LogRecord("anp", logging.INFO, __file__, 1, "plain", None, None)
    assert LineFormatter().format(record) == "INFO anp: plain"
