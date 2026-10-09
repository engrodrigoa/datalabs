"""
Structured logging for every pipeline script.

- LOG_FORMAT=line (default inside Docker, i.e. in Airflow task logs): readable by humans AND parseable

      WARNING anp_landing_semanal: x.csv rejected: schema drift | {"event": "schema_drift", "file": "x.csv"}

  Airflow already prefixes the timestamp, and dag_id/run_id/task_id/try are in the log file path
  (Promtail turns them into labels), so the line carries only level, logger, message and a JSON tail
  with the structured fields (`event` + extras). Lines without extra fields have no tail.
- LOG_FORMAT=json: one JSON object per line incl. timestamp and Airflow context (for shippers that
  do not know the Airflow file layout).
- LOG_FORMAT=text (default outside Docker): `[ts] [LEVEL] [name] msg | k=v`.

Backwards compatible: `get_logger(name)` keeps the same signature, so existing
`logger.info("...")` calls keep working. New code should prefer `log_event()`,
which gives every important moment a stable, queryable `event` name.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

_RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", None, None)).keys()) | {"message", "asctime"}

_AIRFLOW_ENV = {
    "dag_id": "AIRFLOW_CTX_DAG_ID",
    "run_id": "AIRFLOW_CTX_DAG_RUN_ID",
    "task_id": "AIRFLOW_CTX_TASK_ID",
    "try_number": "AIRFLOW_CTX_TRY_NUMBER",
}


def airflow_context() -> dict:
    """Airflow context available to BashOperator subprocesses (empty outside Airflow)."""
    return {k: os.environ[v] for k, v in _AIRFLOW_ENV.items() if os.environ.get(v)}


def _default_format() -> str:
    explicit = os.getenv("LOG_FORMAT")
    if explicit:
        return explicit.lower()
    return "line" if os.path.exists("/opt/airflow") else "text"


def _extra_fields(record: logging.LogRecord) -> dict:
    return {k: v for k, v in record.__dict__.items() if k not in _RESERVED and not k.startswith("_")}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(airflow_context())
        payload.update(_extra_fields(record))
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


class LineFormatter(logging.Formatter):
    """`LEVEL logger: message | {json extras}` - see module docstring."""

    def format(self, record: logging.LogRecord) -> str:
        line = f"{record.levelname} {record.name}: {record.getMessage()}"
        extra = _extra_fields(record)
        if extra:
            line += " | " + json.dumps(extra, ensure_ascii=False, default=str)
        if record.exc_info:  # traceback on its own lines, readable in the Airflow UI
            line += "\n" + self.formatException(record.exc_info)
        return line


_FORMATTERS = {"json": JsonFormatter, "line": LineFormatter}


class TextFormatter(logging.Formatter):
    def __init__(self):
        super().__init__(fmt="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extra = _extra_fields(record)
        if extra:
            base += " | " + " ".join(f"{k}={v}" for k, v in extra.items())
        return base


class _CurrentStdoutHandler(logging.StreamHandler):
    """Writes to whatever sys.stdout is *at emit time*.

    Airflow redirects sys.stdout into the task log only while a task runs. A handler bound to
    the stdout captured at import time (DAG parsing) would silently bypass the task log for
    PythonOperators and callbacks.
    """

    def __init__(self):
        super().__init__(sys.stdout)

    def emit(self, record: logging.LogRecord) -> None:
        self.stream = sys.stdout
        super().emit(record)


def get_logger(name: str = "PIPELINE") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
        handler = _CurrentStdoutHandler()
        handler.setFormatter(_FORMATTERS.get(_default_format(), TextFormatter)())
        logger.addHandler(handler)
        logger.propagate = False
    return logger


def log_event(logger: logging.Logger, event: str, msg: str | None = None, level: int = logging.INFO, **fields) -> None:
    """Emit a named, structured event. `event` is the stable key you query in Loki/Grafana."""
    extra = {"event": event}
    extra.update({k: v for k, v in fields.items() if k not in _RESERVED})
    logger.log(level, msg or event, extra=extra)
