"""
dag_rtc_explain — a cited explanation per official cClassTrib, from the LC 214 provisions the bridge links (ADR 0022).

    explain_cclasstrib  -> health_checks_rtc_explain -> end
    rtc04 (Ollama, --gate)

Schedule: the asset `datalabs://gold/rtc`, emitted by dag_rtc_ref (task publish_rtc) once the reference tables, the
bridge and the RTC models are built. Daily, like dag_rtc_ref; most runs end in seconds with exit 99: every row's
fingerprint (prompt with its context and table facts, prompt version, model and digest) is already stored.

Why a DAG of its own
    Explaining is slow and optional: ~1 min per classification on CPU (qwen2.5:3b, profile `ai`), ~3 h for the
    202 rows of a new model or prompt (estimated from lc06's measured rates; measured by the first lab run).
    Inside dag_rtc_ref it would hold the 45-min deadline and the RTC health checks hostage. Here a run spends at most RTC_EXPLAIN_MAX_MINUTES (80) and leaves the rest `pendente` for the next one:
    rows commit one by one, so the backlog drains over a few runs (or one manual run with a larger budget).

Degrades explicitly: no Ollama, no law served (no AI profile), a bridge or silver not built from the reference in
use -> exit 99 (SKIPPED), and the checks `requires: ai` are skipped on the base stack.

Params: force -> regenerate every row (hours of CPU: confirm before triggering).
"""
import os
import sys
from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator  # type: ignore
from airflow.sdk import DAG, Asset, Param, TriggerRule  # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipelines.commons.audit_logger import consolidate_dag_audit_logs
from pipelines.observability.airflow_callbacks import OBS_DAG_CALLBACKS, OBS_DEFAULT_ARGS
from pipelines.observability.dag_helpers import health_checks, observed_script

FORCE = "{{ '--force' if params.force else '' }}"
GOLD_RTC = Asset("datalabs://gold/rtc")              # produced by dag_rtc_ref.publish_rtc

with DAG(
    dag_id="dag_rtc_explain",
    doc_md=__doc__,
    default_args={"owner": "datalab", "depends_on_past": False, "retries": 0, **OBS_DEFAULT_ARGS},
    description="cClassTrib -> LC 214 provisions (bridge) -> cited explanation by the local LLM",
    start_date=datetime(2026, 11, 1),
    schedule=[GOLD_RTC],
    catchup=False,
    max_active_runs=1,
    params={"force": Param(False, type="boolean", description="regenerate every explanation (hours of CPU)")},
    tags=["rtc", "nfe", "ai", "lc214", "observability"],
    **OBS_DAG_CALLBACKS,
) as dag:

    # 80 min of calls (RTC_EXPLAIN_MAX_MINUTES) + one call in flight (LC214_GEN_TIMEOUT_S, 600 s) + margin
    explain = observed_script("explain_cclasstrib", "rtc", "rtc/rtc04_explain.py", args=f"--gate {FORCE}",
                              execution_timeout=timedelta(minutes=100))
    # NONE_FAILED: also after a skipped run (the checks still say how many rows are pending); a failure stays red
    data_health = health_checks("rtc_explain", trigger_rule=TriggerRule.NONE_FAILED)
    end = PythonOperator(task_id="end", python_callable=consolidate_dag_audit_logs, trigger_rule=TriggerRule.ALL_DONE)

    explain >> data_health >> end
