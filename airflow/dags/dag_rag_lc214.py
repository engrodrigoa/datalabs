"""
dag_rag_lc214 — LC 214/2025 (IBS/CBS law) as a governed source for the legislation RAG (ADR 0010).

  check_ai_runtime -> extract_source -> parse_dispositivos -> build_chunks -> health_checks_lc214
     (gate)            lc01 (watch)      lc02 (structure)      lc03 (small-to-big)   (SQL on audit)

Weekly source watch: the Câmara re-publishes the "norma atualizada" PDF whenever the law is amended.
Most weeks nothing changed and the whole run is cheap: every step decides for itself, from its own
inputs and code version, whether there is work to do, and exits 99 (SKIPPED) when there is none.

Why the steps run after a skipped upstream (trigger_rule NONE_FAILED)
    "Skipped" means "my output is up to date", not "stop". Bumping PARSER_VERSION with the same PDF must
    still re-parse: lc01 skips (same sha256), lc02 runs (new parser), lc03 runs (new input). With the
    default ALL_SUCCESS, a code change would only be applied the day the law itself changed.
    A failed step still stops everything after it (NONE_FAILED = no upstream failed or upstream_failed).

Why the AI gate is a ShortCircuitOperator and not an exit 99
    It must stop the whole DAG, overriding the NONE_FAILED rules downstream
    (ignore_downstream_trigger_rules=True). lc03 needs the embedding model's tokenizer: without the AI
    image, the token counts would be estimates, and an estimate cannot prove that no chunk is truncated.

Dataset `datalabs://silver/lc214`
    Emitted by build_chunks only when it SUCCEEDS (Airflow does not emit on skipped), i.e. only when there
    are new chunks. lc04 (load + embed) will be scheduled on it, like dag_rag_anp on datalabs://gold/anp.

Params (manual runs): `force` re-parses and re-chunks the current version; `pdf` uses a PDF already on the
datasource volume (e.g. /mnt/datasource/lc214/inbox/lc214.pdf) when the Câmara site is down.
"""
import importlib.util
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset  # type: ignore
from airflow.models.param import Param  # type: ignore
from airflow.operators.python import ShortCircuitOperator  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipelines.observability.airflow_callbacks import OBS_DAG_CALLBACKS, OBS_DEFAULT_ARGS
from pipelines.observability.dag_helpers import health_checks, observed_script

SILVER_LC214 = Dataset("datalabs://silver/lc214")

FORCE = "{{ '--force' if params.force else '' }}"
PDF = "{{ ('--pdf ' ~ params.pdf) if params.pdf else '' }}"


def ai_runtime_available() -> bool:
    if importlib.util.find_spec("sentence_transformers") is None:
        print("AI dependencies not installed (build with INSTALL_AI=true / make up-ai): skipping the whole DAG.")
        return False
    return True


with DAG(
    dag_id="dag_rag_lc214",
    doc_md=__doc__,
    default_args={"owner": "datalab", "retries": 0, **OBS_DEFAULT_ARGS},
    description="LC 214/2025: versioned PDF -> dispositivos -> chunks (legislation RAG)",
    schedule="40 6 * * 1",  # Mondays 06:40 (America/Sao_Paulo); DAGs start paused
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["rag", "ai", "lc214", "legislation", "observability"],
    params={
        "force": Param(False, type="boolean", description="Re-parse and re-chunk the current version"),
        "pdf": Param("", type="string", description="Local PDF path in the container (empty = download)"),
    },
    **OBS_DAG_CALLBACKS,
) as dag:

    check_ai_runtime = ShortCircuitOperator(
        task_id="check_ai_runtime",
        python_callable=ai_runtime_available,
        ignore_downstream_trigger_rules=True,
    )
    extract = observed_script("extract_source", "lc214", "rag/lc214/lc01_extract.py", args=PDF,
                              execution_timeout=timedelta(minutes=15))
    parse = observed_script("parse_dispositivos", "lc214", "rag/lc214/lc02_parse.py", args=FORCE,
                            trigger_rule=TriggerRule.NONE_FAILED, execution_timeout=timedelta(minutes=10))
    chunk = observed_script("build_chunks", "lc214", "rag/lc214/lc03_chunks.py", args=f"--tokens tokenizer {FORCE}",
                            trigger_rule=TriggerRule.NONE_FAILED, outlets=[SILVER_LC214],
                            execution_timeout=timedelta(minutes=20))
    # NONE_FAILED (not ALL_DONE): checks also run on a week with nothing new, but a failed step upstream
    # leaves this leaf upstream_failed, so the DAG run is red. With ALL_DONE a green leaf would hide it.
    data_health = health_checks("lc214", fail_on="never", trigger_rule=TriggerRule.NONE_FAILED)

    check_ai_runtime >> extract >> parse >> chunk >> data_health
