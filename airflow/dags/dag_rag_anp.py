"""
dag_rag_anp — AI layer as a governed data product, triggered whenever ANP gold changes.

  check_ai_runtime -> build_context (embeddings -> pgvector) -> evaluate (golden set) -> quality gate

The evaluation persists routing/value accuracy per run (ai.rag_eval_*) and fails the task
when accuracy drops below RAG_MIN_ROUTING_ACC / RAG_MIN_VALUE_ACC: a regression in the
RAG is treated exactly like a failing data test.

Requires the AI image (INSTALL_AI=true). Without it, the DAG skips cleanly.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipelines.observability.airflow_callbacks import OBS_DAG_CALLBACKS, OBS_DEFAULT_ARGS
from pipelines.observability.dag_helpers import health_checks, observed_script

GOLD_ANP = Dataset("datalabs://gold/anp")

with DAG(
    dag_id="dag_rag_anp",
    doc_md=__doc__,
    default_args={"owner": "datalab", "retries": 0, **OBS_DEFAULT_ARGS},
    description="ANP RAG: embeddings -> pgvector -> evaluation gate",
    schedule=[GOLD_ANP],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["rag", "ai", "pgvector", "evaluation", "observability"],
    **OBS_DAG_CALLBACKS,
) as dag:

    check_ai_runtime = BashOperator(
        task_id="check_ai_runtime",
        bash_command=(
            "python -c 'import sentence_transformers' 2>/dev/null "
            "|| { echo 'AI dependencies not installed (build with INSTALL_AI=true). Skipping.'; exit 99; }"
        ),
    )
    build_context = observed_script("build_context", "rag", "rag/anp/rag01_context.py",
                                    execution_timeout=timedelta(hours=2))
    evaluate = observed_script("evaluate_retrieval", "rag", "rag/anp/rag04_eval_retrival.py",
                               execution_timeout=timedelta(minutes=30))
    data_health = health_checks("rag", fail_on="never", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    check_ai_runtime >> build_context >> evaluate >> data_health
