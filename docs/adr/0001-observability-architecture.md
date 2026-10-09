# ADR 0001 — Observability: three signals + data health

**Status:** accepted

## Context
Task logs alone answer "did the task crash?". They do not answer "is the data right, complete and fresh?",
which is what consumers of a data platform actually care about.

## Decision
Four complementary signals, all visible in Grafana:

| Signal | Source | Store | Answers |
|---|---|---|---|
| Logs | JSON events from every script (`pipelines/commons/logger.py`) | Loki (Promtail) | what happened, with context (`dag_id`, `run_id`, `event`) |
| Metrics | Airflow StatsD | Prometheus (statsd-exporter) | is the platform healthy (scheduler, queue, durations)? |
| Step facts | `pipelines.observability.run` wrapper | `audit.pipeline_step_runs` | volumes, files, duration, status of every step |
| Data health | dbt + Elementary tests, `checks.yml` | `elementary.*`, `audit.data_quality_checks` | freshness, volume anomalies, reconciliation between layers |

Principles:
* **Zero-touch instrumentation** — the runner wraps any script; scripts only *add* volumes via `current_step()`.
* **Observability never breaks the pipeline** — every audit write is best-effort.
* **The DAG run tells the truth** — the final audit task fails the run when any task failed
  (an `ALL_DONE` leaf would otherwise mark failed runs as success).
* **Alert once, on the root cause** — failures alert from the Airflow callback after the last retry;
  partial loads and failing checks alert from the runner/checks; the final gate never alerts.
* **Low-cardinality labels** — `run_id` stays in the log payload, never as a Loki label.

## Consequences
+ A recruiter (or an on-call engineer) answers "is it healthy?" from one dashboard.
− Two places define quality rules (dbt tests for model contracts, `checks.yml` for cross-layer/time-based rules).
  The split is intentional: dbt runs at build time, checks run on a schedule even when nothing was built.
