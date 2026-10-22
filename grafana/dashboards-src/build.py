"""
Dashboards as code: generates grafana/dashboards/*.json (classic JSON model, Grafana >= 10).

    python grafana/dashboards-src/build.py

Datasource UIDs are fixed by grafana/provisioning/datasources/datasources.yaml:
  dw (DW Postgres), airflow_meta (Airflow metadata DB), loki, prometheus.
Every SQL here is also executed by tests/test_dashboards_sql.py against Postgres.
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "dashboards"

DW = {"type": "grafana-postgresql-datasource", "uid": "dw"}
AF = {"type": "grafana-postgresql-datasource", "uid": "airflow_meta"}
LOKI = {"type": "loki", "uid": "loki"}
PROM = {"type": "prometheus", "uid": "prometheus"}

# Status colors are reserved for state (never reused as a series color) and always come with a text label.
GOOD, WARN, SERIOUS, CRITICAL, NEUTRAL = "green", "yellow", "orange", "red", "text"

STATUS_MAPPINGS = [{
    "type": "value",
    "options": {
        "success": {"text": "✔ success", "color": GOOD, "index": 0},
        "pass": {"text": "✔ pass", "color": GOOD, "index": 1},
        "skipped": {"text": "⏭ skipped", "color": NEUTRAL, "index": 2},
        "running": {"text": "▶ running", "color": "blue", "index": 3},
        "warn": {"text": "▲ warn", "color": WARN, "index": 4},
        "stale": {"text": "⏸ stale", "color": SERIOUS, "index": 5},
        "error": {"text": "✖ error", "color": CRITICAL, "index": 6},
        "fail": {"text": "✖ fail", "color": CRITICAL, "index": 7},
        "failed": {"text": "✖ failed", "color": CRITICAL, "index": 8},
    },
}]

PIPELINE_FILTER = "pipeline IN ($pipeline)"


class Board:
    def __init__(self, uid: str, title: str, description: str, tags: list[str], refresh: str = "1m",
                 time_from: str = "now-7d"):
        self.uid, self.title, self.description, self.tags = uid, title, description, tags
        self.refresh, self.time_from = refresh, time_from
        self.panels: list[dict] = []
        self.templating: list[dict] = []
        self._id = 0
        self._y = 0
        self._x = 0
        self._row_h = 0
        self.links: list[dict] = []

    # ---------------------------------------------------------------- layout
    def _place(self, w: int, h: int) -> dict:
        if self._x + w > 24:
            self._x, self._y = 0, self._y + self._row_h
            self._row_h = 0
        pos = {"x": self._x, "y": self._y, "w": w, "h": h}
        self._x += w
        self._row_h = max(self._row_h, h)
        return pos

    def row(self, title: str):
        if self._x:
            self._x, self._y, self._row_h = 0, self._y + self._row_h, 0
        self._id += 1
        self.panels.append({"id": self._id, "type": "row", "title": title, "collapsed": False,
                            "gridPos": {"x": 0, "y": self._y, "w": 24, "h": 1}, "panels": []})
        self._y += 1

    def add(self, panel: dict, w: int, h: int) -> dict:
        self._id += 1
        panel["id"] = self._id
        panel["gridPos"] = self._place(w, h)
        self.panels.append(panel)
        return panel

    # ---------------------------------------------------------------- output
    def render(self) -> dict:
        return {
            "uid": self.uid, "title": self.title, "description": self.description, "tags": self.tags,
            "timezone": "browser", "editable": True, "graphTooltip": 1, "schemaVersion": 39,
            "version": 1, "refresh": self.refresh, "liveNow": False,
            "time": {"from": self.time_from, "to": "now"},
            "timepicker": {}, "annotations": {"list": [{
                "builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True,
                "hide": True, "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts", "type": "dashboard"}]},
            "links": self.links, "templating": {"list": self.templating}, "panels": self.panels,
        }

    def write(self) -> Path:
        OUT.mkdir(parents=True, exist_ok=True)
        path = OUT / f"{self.uid}.json"
        path.write_text(json.dumps(self.render(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        return path


# ---------------------------------------------------------------- panel factories
def sql_target(sql: str, ds=DW, fmt="table", ref="A") -> dict:
    return {"refId": ref, "datasource": ds, "editorMode": "code", "format": fmt, "rawQuery": True,
            "rawSql": " ".join(sql.split())}


def stat(title, sql, ds=DW, unit="none", thresholds=None, desc="", decimals=None, mappings=None, color_mode="background"):
    steps = thresholds or [{"color": NEUTRAL, "value": None}]
    defaults = {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps}, "color": {"mode": "thresholds"},
                "mappings": mappings or [], "noValue": "–"}
    if decimals is not None:
        defaults["decimals"] = decimals
    return {"type": "stat", "title": title, "description": desc, "datasource": ds,
            "targets": [sql_target(sql, ds)],
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"colorMode": color_mode, "graphMode": "none", "justifyMode": "center", "textMode": "value",
                        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "orientation": "auto", "wideLayout": True, "showPercentChange": False}}


def table(title, sql, ds=DW, desc="", status_cols=("status",), overrides=None, sort=None):
    ov = [{"matcher": {"id": "byName", "options": c},
           "properties": [{"id": "mappings", "value": STATUS_MAPPINGS},
                          {"id": "custom.cellOptions", "value": {"type": "color-text"}}]} for c in status_cols]
    ov += overrides or []
    opts = {"showHeader": True, "cellHeight": "sm", "footer": {"show": False, "reducer": ["sum"], "fields": ""}}
    if sort:
        opts["sortBy"] = [{"displayName": sort, "desc": True}]
    return {"type": "table", "title": title, "description": desc, "datasource": ds,
            "targets": [sql_target(sql, ds)],
            "fieldConfig": {"defaults": {"custom": {"align": "auto", "filterable": True, "cellOptions": {"type": "auto"}},
                                         "mappings": [], "noValue": "–",
                                         "thresholds": {"mode": "absolute", "steps": [{"color": NEUTRAL, "value": None}]}},
                            "overrides": ov},
            "options": opts}


def timeseries(title, targets, unit="none", desc="", draw="line", stack=False, legend="list", overrides=None,
               min_zero=True, color_overrides=None):
    custom = {"drawStyle": draw, "lineWidth": 2, "fillOpacity": 70 if draw == "bars" else 10,
              "pointSize": 8, "showPoints": "auto", "spanNulls": True, "lineInterpolation": "linear",
              "axisPlacement": "auto", "gradientMode": "none",
              "stacking": {"mode": "normal" if stack else "none", "group": "A"},
              "barAlignment": 0, "axisSoftMin": 0 if min_zero else None}
    ov = list(overrides or [])
    for name, color in (color_overrides or {}).items():
        ov.append({"matcher": {"id": "byName", "options": name},
                   "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}]})
    return {"type": "timeseries", "title": title, "description": desc,
            "datasource": targets[0]["datasource"], "targets": targets,
            "fieldConfig": {"defaults": {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"},
                                         "mappings": [], "noValue": "no data yet",
                                         "thresholds": {"mode": "absolute", "steps": [{"color": NEUTRAL, "value": None}]}},
                            "overrides": ov},
            "options": {"legend": {"displayMode": legend, "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "multi", "sort": "desc"}}}


def loki_target(expr, ref="A", legend=None, qtype="range"):
    t = {"refId": ref, "datasource": LOKI, "expr": expr, "queryType": qtype, "editorMode": "code"}
    if legend:
        t["legendFormat"] = legend
    return t


def prom_target(expr, legend="", ref="A", instant=False):
    return {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": legend, "editorMode": "code",
            "range": not instant, "instant": instant}


def logs(title, expr, desc=""):
    return {"type": "logs", "title": title, "description": desc, "datasource": LOKI,
            "targets": [loki_target(expr)],
            "options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending",
                        "enableLogDetails": True, "dedupStrategy": "none", "prettifyLogMessage": False}}


def text(content, title=""):
    return {"type": "text", "title": title, "options": {"mode": "markdown", "content": content}}


# Our log lines (LOG_FORMAT=line) after Airflow's "[ts] {file:line} LEVEL - " prefix:
#     LEVEL logger: message | {"event": ...}
# event/level/pipeline are already labels (Promtail); this only extracts logger + message for display.
LINE_PARSE = '| regexp `- (?P<lvl>[A-Z]+) (?P<logger>[^ :{]+): (?P<msg>.*?)(?: \\| \\{.*\\})?$`'
MSG = '{{ if .msg }}{{ .msg }}{{ else }}{{ __line__ }}{{ end }}'  # older JSON lines: show them raw

GREEN_RED_RATE = [{"color": CRITICAL, "value": None}, {"color": WARN, "value": 90}, {"color": GOOD, "value": 99}]
ZERO_GOOD = [{"color": GOOD, "value": None}, {"color": CRITICAL, "value": 1}]
ZERO_GOOD_WARN = [{"color": GOOD, "value": None}, {"color": WARN, "value": 1}]


# =====================================================================================================
# 1) Pipeline health (main dashboard)
# =====================================================================================================
def pipeline_health() -> Board:
    b = Board("datalabs-pipeline-health", "DataLabs · Pipeline Health",
              "Is the data platform healthy? Step runs, data health checks, dbt/Elementary tests, Airflow runs, RAG quality and error logs.",
              ["datalabs", "observability", "data-quality"])
    b.links = [{"title": "Airflow platform", "type": "link", "url": "/d/datalabs-airflow-platform", "icon": "dashboard"},
               {"title": "Logs explorer", "type": "link", "url": "/d/datalabs-logs", "icon": "doc"},
               {"title": "Airflow UI", "type": "link", "url": "http://localhost:8080", "targetBlank": True, "icon": "external link"}]
    b.templating = [{
        "name": "pipeline", "label": "Pipeline", "type": "query", "datasource": DW,
        "query": "SELECT unnest(ARRAY['anp','rfb','rag','obs']) AS p UNION SELECT DISTINCT pipeline FROM audit.pipeline_step_runs ORDER BY 1",
        "definition": "pipelines", "refresh": 1, "multi": True, "includeAll": True,
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "sort": 1, "hide": 0,
    }]

    # ------------------------------------------------------------- headline
    b.row("Overview — selected time range")
    b.add(stat("Step success rate", f"""
        SELECT 100.0 * COUNT(*) FILTER (WHERE status = 'success')
               / NULLIF(COUNT(*) FILTER (WHERE status IN ('success','failed')), 0) AS rate
        FROM audit.pipeline_step_runs WHERE $__timeFilter(started_at) AND {PIPELINE_FILTER}""",
               unit="percent", decimals=1, thresholds=GREEN_RED_RATE,
               desc="success / (success + failed) over executed steps (skipped = no new data, not counted)"), 4, 4)
    b.add(stat("Failed steps", f"""
        SELECT COUNT(*) FROM audit.pipeline_step_runs
        WHERE status = 'failed' AND $__timeFilter(started_at) AND {PIPELINE_FILTER}""", thresholds=ZERO_GOOD), 3, 4)
    b.add(stat("Rows loaded", f"""
        SELECT COALESCE(SUM(rows_out), 0) FROM audit.pipeline_step_runs
        WHERE status = 'success' AND $__timeFilter(started_at) AND {PIPELINE_FILTER}""", unit="short", color_mode="none"), 3, 4)
    b.add(stat("Checks failing now", """
        SELECT COUNT(*) FROM obs.v_check_last_result WHERE status IN ('fail','error')""", thresholds=ZERO_GOOD,
               desc="latest result of every data health check (pipelines/observability/checks.yml)"), 3, 4)
    b.add(stat("Checks warning now", """
        SELECT COUNT(*) FROM obs.v_check_last_result WHERE status = 'warn'""", thresholds=ZERO_GOOD_WARN), 3, 4)
    b.add(stat("ANP data age", """
        SELECT observed_value FROM obs.v_check_last_result WHERE check_name = 'anp_gold_data_freshness'""",
               unit="h", decimals=0,
               thresholds=[{"color": GOOD, "value": None}, {"color": WARN, "value": 240}, {"color": CRITICAL, "value": 408}],
               desc="hours since the most recent price survey date in gold"), 4, 4)
    b.add(stat("Failed DAG runs", """
        SELECT COUNT(*) FROM dag_run WHERE state = 'failed' AND $__timeFilter(start_date)""",
               ds=AF, thresholds=ZERO_GOOD, desc="from the Airflow metadata database"), 4, 4)

    # ------------------------------------------------------------- steps
    b.row("Pipeline steps — audit.pipeline_step_runs")
    b.add(table("Last run of every step", f"""
        SELECT pipeline, step, health AS status, started_at,
               ROUND(elapsed_s::numeric, 1) AS duration_s, rows_in, rows_out, rows_rejected,
               files_ok, files_failed, dag_id, LEFT(error_message, 160) AS error
        FROM obs.v_step_last_run WHERE {PIPELINE_FILTER} ORDER BY started_at DESC""",
                desc="stale = started more than 6h ago and never finished (killed / OOM)",
                overrides=[{"matcher": {"id": "byName", "options": "files_failed"},
                            "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                                           {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                               {"color": GOOD, "value": None}, {"color": CRITICAL, "value": 1}]}}]}]), 24, 8)
    b.add(timeseries("Rows written per step run", [sql_target(f"""
        SELECT started_at AS time, pipeline || '.' || step AS metric, rows_out AS value
        FROM audit.pipeline_step_runs
        WHERE $__timeFilter(started_at) AND rows_out IS NOT NULL AND status = 'success' AND {PIPELINE_FILTER}
        ORDER BY 1""", fmt="time_series")], unit="short", draw="points",
        desc="a sudden drop is the earliest sign of a broken source"), 12, 8)
    b.add(timeseries("Step duration", [sql_target(f"""
        SELECT started_at AS time, pipeline || '.' || step AS metric, duration_s AS value
        FROM audit.pipeline_step_runs
        WHERE $__timeFilter(started_at) AND duration_s IS NOT NULL AND {PIPELINE_FILTER}
        ORDER BY 1""", fmt="time_series")], unit="s", draw="points"), 12, 8)
    b.add(timeseries("Step outcomes per day", [sql_target(f"""
        SELECT $__timeGroupAlias(started_at, '1d'), status AS metric, COUNT(*) AS value
        FROM audit.pipeline_step_runs WHERE $__timeFilter(started_at) AND {PIPELINE_FILTER}
        GROUP BY 1, 2 ORDER BY 1""", fmt="time_series")], draw="bars", stack=True,
        color_overrides={"success": GOOD, "failed": CRITICAL, "skipped": "#8e8e8e", "running": "blue"}), 12, 8)
    b.add(table("Volume vs. baseline (median of the previous 10 runs)", f"""
        SELECT pipeline, step, last_run_at, last_rows_out, ROUND(median_rows_out::numeric) AS median_rows_out,
               deviation_pct
        FROM obs.v_step_volume_baseline WHERE {PIPELINE_FILTER} ORDER BY ABS(COALESCE(deviation_pct, 0)) DESC""",
                status_cols=(),
                overrides=[{"matcher": {"id": "byName", "options": "deviation_pct"},
                            "properties": [{"id": "unit", "value": "percent"},
                                           {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}},
                                           {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                               {"color": CRITICAL, "value": None}, {"color": WARN, "value": -30},
                                               {"color": GOOD, "value": -10}, {"color": WARN, "value": 10},
                                               {"color": CRITICAL, "value": 30}]}}]}]), 12, 8)

    # ------------------------------------------------------------- data health
    b.row("Data health checks — freshness · volume · reconciliation")
    b.add(table("Current status of every check", """
        SELECT check_name, pipeline, check_type, status, severity, observed_value, expected, message, checked_at
        FROM obs.v_check_last_result
        ORDER BY CASE status WHEN 'fail' THEN 0 WHEN 'error' THEN 1 WHEN 'warn' THEN 2 WHEN 'pass' THEN 3 ELSE 4 END,
                 check_name"""), 24, 9)
    b.add(timeseries("Data age by dataset (freshness checks)", [sql_target("""
        SELECT checked_at AS time, check_name AS metric, observed_value AS value
        FROM audit.data_quality_checks
        WHERE check_type = 'freshness' AND status <> 'skipped' AND $__timeFilter(checked_at)
        ORDER BY 1""", fmt="time_series")], unit="h"), 12, 8)
    b.add(timeseries("Non-passing check results", [sql_target("""
        SELECT $__timeGroupAlias(checked_at, '6h'), status AS metric, COUNT(*) AS value
        FROM audit.data_quality_checks
        WHERE status IN ('warn','fail','error') AND $__timeFilter(checked_at)
        GROUP BY 1, 2 ORDER BY 1""", fmt="time_series")], draw="bars", stack=True,
        color_overrides={"warn": WARN, "fail": CRITICAL, "error": SERIOUS}), 12, 8)

    # ------------------------------------------------------------- dbt / elementary
    b.row("dbt tests & Elementary anomaly detection — schema elementary")
    b.add(stat("dbt tests failing (latest)", """
        SELECT COUNT(*) FROM (
            SELECT DISTINCT ON (test_unique_id, column_name, test_sub_type) status
            FROM elementary.elementary_test_results
            ORDER BY test_unique_id, column_name, test_sub_type, detected_at DESC) t
        WHERE status IN ('fail','error')""", thresholds=ZERO_GOOD), 4, 4)
    b.add(stat("dbt tests warning (latest)", """
        SELECT COUNT(*) FROM (
            SELECT DISTINCT ON (test_unique_id, column_name, test_sub_type) status
            FROM elementary.elementary_test_results
            ORDER BY test_unique_id, column_name, test_sub_type, detected_at DESC) t
        WHERE status = 'warn'""", thresholds=ZERO_GOOD_WARN), 4, 4)
    b.add(stat("dbt model runs failed", """
        SELECT COUNT(*) FROM elementary.dbt_run_results
        WHERE resource_type = 'model' AND status IN ('error','fail') AND $__timeFilter(created_at)""",
               thresholds=ZERO_GOOD), 4, 4)
    b.add(stat("Tests executed", """
        SELECT COUNT(*) FROM elementary.elementary_test_results WHERE $__timeFilter(detected_at)""",
               unit="short", color_mode="none"), 4, 4)
    b.add(stat("Anomaly monitors", """
        SELECT COUNT(DISTINCT test_unique_id) FROM elementary.elementary_test_results
        WHERE test_type = 'anomaly_detection'""", color_mode="none",
               desc="Elementary volume/freshness/column anomaly tests (learn the normal and flag deviations)"), 8, 4)
    b.add(table("Latest result of non-passing tests", """
        SELECT table_name, column_name, COALESCE(test_short_name, test_type) AS test, test_sub_type, status,
               failures, detected_at, LEFT(test_results_description, 200) AS description
        FROM (SELECT DISTINCT ON (test_unique_id, column_name, test_sub_type) *
              FROM elementary.elementary_test_results
              ORDER BY test_unique_id, column_name, test_sub_type, detected_at DESC) t
        WHERE status <> 'pass' ORDER BY detected_at DESC LIMIT 50"""), 12, 9)
    b.add(table("Slowest dbt models", """
        SELECT name AS model, materialization, COUNT(*) AS runs,
               ROUND(AVG(execution_time)::numeric, 2) AS avg_s, ROUND(MAX(execution_time)::numeric, 2) AS max_s,
               MAX(rows_affected) AS last_rows
        FROM elementary.dbt_run_results
        WHERE resource_type = 'model' AND $__timeFilter(created_at)
        GROUP BY name, materialization ORDER BY avg_s DESC LIMIT 15""", status_cols=()), 12, 9)
    b.add(timeseries("Test outcomes over time", [sql_target("""
        SELECT $__timeGroupAlias(detected_at, '1d'), status AS metric, COUNT(*) AS value
        FROM elementary.elementary_test_results WHERE $__timeFilter(detected_at)
        GROUP BY 1, 2 ORDER BY 1""", fmt="time_series")], draw="bars", stack=True,
        color_overrides={"pass": GOOD, "warn": WARN, "fail": CRITICAL, "error": SERIOUS}), 24, 7)

    # ------------------------------------------------------------- airflow
    b.row("Airflow runs — metadata database")
    b.add(timeseries("DAG run duration", [sql_target("""
        SELECT end_date AS time, dag_id AS metric, EXTRACT(EPOCH FROM end_date - start_date) AS value
        FROM dag_run WHERE $__timeFilter(start_date) AND end_date IS NOT NULL ORDER BY 1""", ds=AF, fmt="time_series")],
        unit="s", draw="points"), 12, 8)
    b.add(table("DAG runs by outcome", """
        SELECT dag_id,
               COUNT(*) FILTER (WHERE state = 'success') AS success,
               COUNT(*) FILTER (WHERE state = 'failed')  AS failed,
               COUNT(*) FILTER (WHERE state = 'running') AS running,
               MAX(start_date) AS last_start
        FROM dag_run WHERE $__timeFilter(start_date) GROUP BY dag_id ORDER BY failed DESC, dag_id""", ds=AF,
                status_cols=(),
                overrides=[{"matcher": {"id": "byName", "options": "failed"},
                            "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                                           {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                               {"color": GOOD, "value": None}, {"color": CRITICAL, "value": 1}]}}]}]), 12, 8)
    b.add(table("Recent task failures", """
        SELECT ti.dag_id, ti.task_id, ti.run_id, ti.try_number, ti.start_date,
               ROUND(ti.duration::numeric, 1) AS duration_s, ti.state AS status
        FROM task_instance ti
        WHERE ti.state IN ('failed', 'up_for_retry') AND $__timeFilter(ti.start_date)
        ORDER BY ti.start_date DESC LIMIT 25""", ds=AF,
                overrides=[{"matcher": {"id": "byName", "options": "status"},
                            "properties": [{"id": "mappings", "value": [{"type": "value", "options": {
                                "failed": {"text": "✖ failed", "color": CRITICAL},
                                "up_for_retry": {"text": "↻ retry", "color": WARN}}}]}]}]), 24, 8)

    # ------------------------------------------------------------- rag
    b.row("RAG quality — ai.rag_eval_* (golden set)")
    b.add(timeseries("Evaluation accuracy per run", [sql_target("""
        SELECT executado_em AS time, acuracia_roteamento * 100 AS "routing accuracy",
               acuracia_valor * 100 AS "value accuracy (vs SQL truth)"
        FROM ai.rag_eval_summary WHERE $__timeFilter(executado_em) ORDER BY 1""", fmt="time_series")],
        unit="percent", draw="line",
        desc="the evaluation task fails below RAG_MIN_ROUTING_ACC / RAG_MIN_VALUE_ACC (quality gate)"), 12, 8)
    b.add(stat("Avg retrieval latency (last eval)", """
        SELECT latencia_media_ms FROM ai.rag_eval_summary ORDER BY executado_em DESC LIMIT 1""",
               unit="ms", decimals=0, color_mode="none"), 4, 4)
    b.add(stat("Vector index used", """
        SELECT CASE WHEN plano_execucao_usa_indice THEN 1 ELSE 0 END FROM ai.rag_eval_summary
        ORDER BY executado_em DESC LIMIT 1""",
               mappings=[{"type": "value", "options": {"1": {"text": "✔ HNSW", "color": GOOD},
                                                         "0": {"text": "✖ seq scan", "color": WARN}}}]), 4, 4)
    b.add(stat("Context ↔ gold parity", """
        SELECT CASE WHEN paridade_contexto_gold THEN 1 ELSE 0 END FROM ai.rag_eval_summary
        ORDER BY executado_em DESC LIMIT 1""",
               mappings=[{"type": "value", "options": {"1": {"text": "✔ in sync", "color": GOOD},
                                                         "0": {"text": "✖ diverged", "color": CRITICAL}}}]), 4, 4)
    b.add(table("Last evaluation — per question", """
        SELECT tipo_consulta AS type, pergunta AS question, metodo_esperado AS expected_route,
               metodo_obtido AS route, acerto_roteamento AS route_ok, valor_obtido, valor_esperado,
               acerto_valor AS value_ok, ROUND(latencia_retrieval_ms::numeric) AS latency_ms
        FROM ai.rag_eval_metrics
        WHERE run_id = (SELECT run_id FROM ai.rag_eval_summary ORDER BY executado_em DESC LIMIT 1)
        ORDER BY tipo_consulta""", status_cols=()), 12, 8)

    # ------------------------------------------------------------- logs
    b.row("Logs — Loki")
    b.add(timeseries("Error & warning lines per DAG", [
        loki_target('sum by (dag_id) (count_over_time({job="airflow", source="task", level=~"ERROR|CRITICAL"} [$__auto]))',
                    legend="{{dag_id}} errors"),
        loki_target('sum by (dag_id) (count_over_time({job="airflow", source="task", level="WARNING"} [$__auto]))',
                    ref="B", legend="{{dag_id}} warnings")], draw="bars", stack=True), 12, 8)
    b.add(logs("Alerts, failures and schema drift",
               '{job="airflow", event=~"alert|task_failed|step_finished|file_failed|schema_drift_.*|rag_eval_gate|check_result", level=~"ERROR|WARNING"} '
               + LINE_PARSE + f' | line_format `[{{{{.event}}}}] {MSG}`'), 12, 8)
    return b


# =====================================================================================================
# 2) Airflow platform (Prometheus via statsd-exporter)
# =====================================================================================================
def airflow_platform() -> Board:
    b = Board("datalabs-airflow-platform", "DataLabs · Airflow Platform",
              "Scheduler, executor and task metrics emitted by Airflow (StatsD -> statsd-exporter -> Prometheus).",
              ["datalabs", "airflow", "prometheus"], refresh="30s", time_from="now-6h")
    b.links = [{"title": "Pipeline health", "type": "link", "url": "/d/datalabs-pipeline-health", "icon": "dashboard"}]

    def pstat(title, expr, thresholds=None, unit="none", mappings=None, desc=""):
        p = stat(title, "", ds=PROM, unit=unit, thresholds=thresholds, mappings=mappings, desc=desc)
        p["targets"] = [prom_target(expr, instant=True)]
        return p

    b.row("Health")
    b.add(pstat("Scheduler heartbeat", 'clamp_max(sum(rate(airflow_scheduler_heartbeat[2m])) > bool 0, 1)',
                mappings=[{"type": "value", "options": {"1": {"text": "✔ alive", "color": GOOD},
                                                          "0": {"text": "✖ no heartbeat", "color": CRITICAL}}}],
                desc="rate of scheduler heartbeats over 2 minutes"), 4, 4)
    b.add(pstat("DAG import errors", "max(airflow_dag_processing_import_errors)", thresholds=ZERO_GOOD), 4, 4)
    b.add(pstat("DAGs loaded", "max(airflow_dagbag_size)"), 4, 4)
    b.add(pstat("Running tasks", "sum(airflow_executor_running_tasks)"), 4, 4)
    b.add(pstat("Queued tasks", "sum(airflow_executor_queued_tasks)",
                thresholds=[{"color": GOOD, "value": None}, {"color": WARN, "value": 5}]), 4, 4)
    b.add(pstat("Task failures (1h)", "sum(increase(airflow_ti_failures[1h]))", thresholds=ZERO_GOOD), 4, 4)

    b.row("Tasks")
    b.add(timeseries("Finished task instances by state", [prom_target(
        'sum by (state) (increase(airflow_ti_finish{state=~"success|failed|skipped|up_for_retry|upstream_failed"}[$__rate_interval]))',
        legend="{{state}}")], draw="bars", stack=True,
        color_overrides={"success": GOOD, "failed": CRITICAL, "up_for_retry": WARN, "upstream_failed": SERIOUS,
                         "skipped": "#8e8e8e"}), 12, 8)
    b.add(timeseries("Task duration p95 by DAG", [prom_target(
        'histogram_quantile(0.95, sum by (le, dag_id) (rate(airflow_task_duration_bucket{dag_id!=""}[$__rate_interval])))',
        legend="{{dag_id}}")], unit="s"), 12, 8)
    b.add(timeseries("Executor slots", [
        prom_target("sum(airflow_executor_open_slots)", legend="open slots"),
        prom_target("sum(airflow_executor_running_tasks)", legend="running", ref="B"),
        prom_target("sum(airflow_executor_queued_tasks)", legend="queued", ref="C")]), 12, 8)
    b.add(timeseries("DAG run duration (avg) by DAG", [prom_target(
        'sum by (dag_id) (rate(airflow_dagrun_duration_sum{dag_id!=""}[$__rate_interval])) / '
        'sum by (dag_id) (rate(airflow_dagrun_duration_count{dag_id!=""}[$__rate_interval]))',
        legend="{{dag_id}}")], unit="s"), 12, 8)

    b.row("Scheduler & parsing")
    b.add(timeseries("Schedule delay by DAG", [prom_target(
        'sum by (dag_id) (rate(airflow_dagrun_schedule_delay_sum{dag_id!=""}[$__rate_interval])) / '
        'sum by (dag_id) (rate(airflow_dagrun_schedule_delay_count{dag_id!=""}[$__rate_interval]))',
        legend="{{dag_id}}")], unit="s", desc="time between the scheduled time and the actual start"), 12, 8)
    b.add(timeseries("DAG file parsing time", [prom_target(
        'sum by (dag_file) (rate(airflow_dag_processing_last_duration_sum{dag_file!=""}[$__rate_interval])) / '
        'sum by (dag_file) (rate(airflow_dag_processing_last_duration_count{dag_file!=""}[$__rate_interval]))',
        legend="{{dag_file}}")], unit="s",
        desc="Cosmos DAGs parse dbt projects: keep an eye on them (dbt deps is disabled at parse time)"), 12, 8)
    return b


# =====================================================================================================
# 3) Logs explorer (Loki)
# =====================================================================================================
def logs_explorer() -> Board:
    b = Board("datalabs-logs", "DataLabs · Logs Explorer",
              "Structured pipeline logs (JSON events) shipped by Promtail. Filter by DAG, level, pipeline and event.",
              ["datalabs", "logs", "loki"], refresh="30s", time_from="now-24h")
    b.links = [{"title": "Pipeline health", "type": "link", "url": "/d/datalabs-pipeline-health", "icon": "dashboard"}]

    def lvar(name, label, lbl):
        return {"name": name, "label": label, "type": "query", "datasource": LOKI,
                "query": {"label": lbl, "refId": "LokiVariableQueryEditor-VariableQuery", "stream": '{job="airflow"}', "type": 1},
                "definition": f"label_values({{job=\"airflow\"}}, {lbl})",
                "refresh": 2, "multi": True, "includeAll": True, "allValue": ".*",
                "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "sort": 1, "hide": 0}

    b.templating = [lvar("dag_id", "DAG", "dag_id"), lvar("level", "Level", "level"),
                    lvar("event", "Event", "event"),
                    {"name": "search", "label": "Search", "type": "textbox", "query": "",
                     "current": {"text": "", "value": ""}, "hide": 0}]
    sel = '{job="airflow", source="task", dag_id=~"$dag_id", level=~"$level"}'

    b.add(timeseries("Log volume by level", [loki_target(
        f'sum by (level) (count_over_time({sel} |~ "(?i)$search" [$__auto]))', legend="{{level}}")],
        draw="bars", stack=True, color_overrides={"ERROR": CRITICAL, "WARNING": WARN, "INFO": "#8e8e8e"}), 24, 7)
    b.add(logs("Events", f'{{job="airflow", source="task", dag_id=~"$dag_id", level=~"$level", event=~"$event"}} |~ "(?i)$search" '
               + LINE_PARSE + f' | line_format `[{{{{.level}}}}] {{{{.logger}}}} · {MSG}`'), 24, 14)
    b.add(logs("Raw task logs (everything, incl. Airflow and dbt output)", f'{sel} |~ "(?i)$search"'), 24, 12)
    return b


# =====================================================================================================
# 4) NF-e — operação dos lotes, qualidade e desempenho fiscal (IBS/CBS)
# =====================================================================================================
NFE_SAUDE = [{"type": "value", "options": {
    "processado": {"text": "✔ processado", "color": GOOD, "index": 0},
    "carregado": {"text": "▶ na fila", "color": "blue", "index": 1},
    "recebido": {"text": "… recebido", "color": NEUTRAL, "index": 2},
    "atrasado": {"text": "⏸ atrasado", "color": SERIOUS, "index": 3},
    "erro": {"text": "✖ erro", "color": CRITICAL, "index": 4},
}}]
NFE_TESTS = "test_unique_id ILIKE '%nfe%'"


def nfe_fiscal() -> Board:
    b = Board("datalabs-nfe", "DataLabs · NF-e (IBS/CBS)",
              "Pipeline de NF-e da reforma tributária: lotes (ctrl.nfe_lote), qualidade (dbt/Elementary, dq_nfe) "
              "e desempenho fiscal da camada gold. Dados 100% sintéticos.",
              ["datalabs", "nfe", "dbt", "fiscal"], refresh="1m", time_from="now-1y")
    b.links = [{"title": "Pipeline health", "type": "link", "url": "/d/datalabs-pipeline-health", "icon": "dashboard"}]

    # ------------------------------------------------------------- operação
    b.row("Operação — lotes (ctrl.nfe_lote · obs.v_nfe_lote)")
    b.add(stat("Notas no DW", "SELECT COUNT(*) FROM silver.nfe_nota", unit="short", color_mode="none",
               desc="Notas distintas na silver (reenvios resolvidos)."), 4, 4)
    b.add(stat("Lotes processados", """
        SELECT COUNT(*) FROM obs.v_nfe_lote WHERE status = 'PROCESSADO' AND $__timeFilter(recebido_em)""",
               unit="short", color_mode="none"), 4, 4)
    b.add(stat("Lotes em erro", "SELECT COUNT(*) FROM obs.v_nfe_lote WHERE status = 'ERRO'", thresholds=ZERO_GOOD,
               desc="Saem da fila até correção: SELECT ctrl.fn_nfe_reprocessar_lote(id, 'motivo')."), 4, 4)
    b.add(stat("Lotes atrasados", "SELECT COUNT(*) FROM obs.v_nfe_lote WHERE saude = 'atrasado'",
               thresholds=ZERO_GOOD_WARN, desc="CARREGADO há mais de 1 h sem processamento."), 4, 4)
    b.add(stat("Arquivos rejeitados", """
        SELECT COALESCE(SUM(qtd_rejeitados), 0) FROM obs.v_nfe_lote WHERE $__timeFilter(recebido_em)""",
               thresholds=ZERO_GOOD_WARN, desc="XML malformado/truncado (quarentena na ingestão)."), 4, 4)
    b.add(stat("Latência p50 do lote", """
        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY seg_total) FROM obs.v_nfe_lote
        WHERE status = 'PROCESSADO' AND $__timeFilter(recebido_em)""", unit="s", color_mode="none",
               desc="Da reserva do lote ao fim do dbt."), 4, 4)
    b.add(table("Últimos lotes", """
        SELECT lote_id, nome_lote, saude, qtd_arquivos AS arquivos, qtd_carregados AS carregados,
               qtd_rejeitados AS rejeitados, qtd_reenvios AS reenvios,
               ROUND(seg_ingestao::numeric, 1) AS ingestao_s, ROUND(seg_transformacao::numeric, 1) AS dbt_s,
               recebido_em, mensagem
        FROM obs.v_nfe_lote ORDER BY lote_id DESC LIMIT 30""", status_cols=(),
                overrides=[{"matcher": {"id": "byName", "options": "saude"},
                            "properties": [{"id": "mappings", "value": NFE_SAUDE},
                                           {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}]), 14, 9)
    b.add(timeseries("Arquivos por lote", [sql_target("""
        SELECT recebido_em AS time, qtd_carregados AS carregados, qtd_rejeitados AS rejeitados,
               qtd_reenvios AS reenvios
        FROM obs.v_nfe_lote WHERE $__timeFilter(recebido_em) ORDER BY 1""", fmt="time_series")],
        draw="bars", color_overrides={"rejeitados": CRITICAL, "reenvios": WARN}), 10, 9)

    # ------------------------------------------------------------- qualidade
    b.row("Qualidade de dados — testes dbt (Elementary) e quarentena (dq_nfe)")
    b.add(stat("Testes NF-e com falha", f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT ON (test_unique_id, column_name, test_sub_type) status
            FROM elementary.elementary_test_results WHERE {NFE_TESTS}
            ORDER BY test_unique_id, column_name, test_sub_type, detected_at DESC) t
        WHERE status IN ('fail', 'error')""", thresholds=ZERO_GOOD), 6, 4)
    b.add(stat("Testes NF-e com aviso", f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT ON (test_unique_id, column_name, test_sub_type) status
            FROM elementary.elementary_test_results WHERE {NFE_TESTS}
            ORDER BY test_unique_id, column_name, test_sub_type, detected_at DESC) t
        WHERE status = 'warn'""", thresholds=ZERO_GOOD_WARN,
               desc="Inconsistências de negócio em notas autorizadas: sinalizadas, não rejeitadas."), 6, 4)
    b.add(stat("Registros sinalizados (último run)", f"""
        SELECT COALESCE(SUM(failures), 0) FROM (
            SELECT DISTINCT ON (test_unique_id, column_name, test_sub_type) status, failures
            FROM elementary.elementary_test_results WHERE {NFE_TESTS}
            ORDER BY test_unique_id, column_name, test_sub_type, detected_at DESC) t
        WHERE status <> 'pass'""", thresholds=ZERO_GOOD_WARN), 6, 4)
    b.add(stat("Execuções de teste NF-e", f"""
        SELECT COUNT(*) FROM elementary.elementary_test_results
        WHERE {NFE_TESTS} AND $__timeFilter(detected_at)""", unit="short", color_mode="none"), 6, 4)
    b.add(table("Último resultado dos testes não aprovados", f"""
        SELECT COALESCE(test_short_name, test_type) AS teste, table_name AS modelo, column_name AS coluna,
               status, failures AS registros, detected_at
        FROM (SELECT DISTINCT ON (test_unique_id, column_name, test_sub_type) *
              FROM elementary.elementary_test_results WHERE {NFE_TESTS}
              ORDER BY test_unique_id, column_name, test_sub_type, detected_at DESC) t
        WHERE status <> 'pass' ORDER BY failures DESC NULLS LAST""",
                desc="Detalhe das linhas: schema dq_nfe (store_failures), uma tabela por teste."), 14, 8)
    b.add(table("Arquivos rejeitados na ingestão", """
        SELECT lote_id, arquivo, LEFT(motivo, 80) AS motivo, rejeitado_em
        FROM obs.v_nfe_rejeitados ORDER BY rejeitado_em DESC LIMIT 50""", status_cols=()), 10, 8)

    # ------------------------------------------------------------- negócio
    b.row("Desempenho fiscal — gold (IBS/CBS, ano-teste 2026)")
    b.add(stat("IBS + CBS destacados", """
        SELECT SUM(v_ibs + v_cbs) FROM gold.ag_nfe_ibscbs_mensal""", unit="currencyBRL", color_mode="none"), 5, 4)
    b.add(stat("Líquido de devoluções", """
        SELECT SUM(v_ibscbs_liquido) FROM gold.ag_nfe_ibscbs_mensal""", unit="currencyBRL", color_mode="none",
               desc="Saídas menos devoluções (sinal_arrecadacao da dm_nfe_operacao)."), 5, 4)
    b.add(stat("Tributo diferido", """
        SELECT SUM(v_diferido) FROM gold.ag_nfe_ibscbs_mensal""", unit="currencyBRL", color_mode="none",
               desc="gDif: IBS/CBS cujo recolhimento foi postergado (insumos agropecuários, CST 515)."), 5, 4)
    b.add(stat("Carga efetiva", """
        SELECT 100 * SUM(v_ibs + v_cbs) / NULLIF(SUM(v_base_ibscbs), 0) FROM gold.ag_nfe_ibscbs_mensal""",
               unit="percent", decimals=3, color_mode="none",
               desc="Σ tributo / Σ base (nunca a média de percentuais). Referência 2026: 1% nominal."), 5, 4)
    b.add(stat("Emitentes", "SELECT COUNT(DISTINCT sk_emitente) FROM gold.ft_nfe_documento",
               unit="short", color_mode="none"), 4, 4)
    b.add(timeseries("IBS, CBS e diferimento por mês de emissão", [sql_target("""
        SELECT to_timestamp(ano_mes::text, 'YYYYMM') AS time,
               SUM(v_ibs) AS ibs, SUM(v_cbs) AS cbs, SUM(v_diferido) AS diferido
        FROM gold.ag_nfe_ibscbs_mensal GROUP BY 1 ORDER BY 1""", fmt="time_series")],
        unit="currencyBRL", draw="bars"), 24, 8)
    b.add(table("Por classificação tributária", """
        SELECT a.cst_ibscbs AS cst, a.cclasstrib, COALESCE(t.tipo_tributacao, '? não catalogado') AS tipo,
               SUM(a.qtd_itens) AS itens, ROUND(SUM(a.v_base_ibscbs), 2) AS base,
               ROUND(SUM(a.v_ibs), 2) AS ibs, ROUND(SUM(a.v_cbs), 2) AS cbs, ROUND(SUM(a.v_diferido), 2) AS diferido,
               ROUND(100 * SUM(a.v_ibs + a.v_cbs) / NULLIF(SUM(a.v_base_ibscbs), 0), 4) AS carga_pct
        FROM gold.ag_nfe_ibscbs_mensal a
        LEFT JOIN gold.dm_nfe_tributacao t ON t.sk_tributacao = a.cst_ibscbs || a.cclasstrib
        GROUP BY 1, 2, 3 ORDER BY base DESC""", status_cols=()), 12, 8)
    b.add(table("Top 10 emitentes (IBS + CBS)", """
        SELECT p.doc AS cnpj, p.nome, p.uf, COUNT(DISTINCT f.chave_acesso) AS notas,
               ROUND(SUM(f.v_ibs + f.v_cbs), 2) AS ibs_cbs,
               ROUND(100 * SUM(f.v_ibs + f.v_cbs) / NULLIF(SUM(f.vbc_ibscbs), 0), 4) AS carga_pct
        FROM gold.ft_nfe_item f
        JOIN gold.dm_nfe_participante v ON v.sk_participante = f.sk_emitente
        JOIN gold.dm_nfe_participante p ON p.doc = v.doc AND p.fl_vigente
        GROUP BY 1, 2, 3 ORDER BY ibs_cbs DESC LIMIT 10""", status_cols=()), 12, 8)
    return b


# =====================================================================================================
# 5) ANP — data product (operation · batch profile · quality · prices · RAG downstream)
# =====================================================================================================
ANP_STEPS = "pipeline = 'anp'"
ANP_LANDING = "pipeline = 'anp' AND step IN ('landing_semanal', 'landing_mensal')"
ANP_TESTS = "(test_unique_id ILIKE '%anp%' OR table_name IN ('dm_postos', 'dm_produtos'))"
RAG_BUILD = "pipeline = 'rag' AND step = 'build_context'"
DAY_S = 86400


def anp_produto() -> Board:
    b = Board("datalabs-anp", "DataLabs · ANP (combustíveis)",
              "Produto de dados da ANP: cada carga (audit.pipeline_step_runs), perfil do lote, qualidade "
              "(health checks + dbt/Elementary), preços (gold.ag_anp_preco_semanal) e o RAG que consome a gold.",
              ["datalabs", "anp", "dbt", "rag"], refresh="5m", time_from="now-90d")
    b.links = [{"title": "Pipeline health", "type": "link", "url": "/d/datalabs-pipeline-health", "icon": "dashboard"},
               {"title": "Logs explorer", "type": "link", "url": "/d/datalabs-logs", "icon": "doc"},
               {"title": "Airflow · dag_dbt_anp", "type": "link", "url": "http://localhost:8080/dags/dag_dbt_anp/grid",
                "targetBlank": True, "icon": "external link"}]
    b.templating.append({
        "name": "uf", "label": "UF", "type": "query", "datasource": DW,
        "query": "SELECT DISTINCT uf FROM gold.ag_anp_preco_semanal ORDER BY 1",
        "definition": "SELECT DISTINCT uf FROM gold.ag_anp_preco_semanal ORDER BY 1",
        "multi": True, "includeAll": True, "current": {"text": ["All"], "value": ["$__all"]},
        "refresh": 1, "sort": 1})

    # ------------------------------------------------------------- operação
    b.row("Operação — cada carga (audit.pipeline_step_runs)")
    b.add(stat("Última carga", f"""
        SELECT EXTRACT(EPOCH FROM now() - MAX(finished_at)) FROM audit.pipeline_step_runs
        WHERE {ANP_LANDING} AND status = 'success'""", unit="s", decimals=0,
               thresholds=[{"color": GOOD, "value": None}, {"color": WARN, "value": 8 * DAY_S},
                           {"color": CRITICAL, "value": 15 * DAY_S}],
               desc="Tempo desde o último landing com sucesso. Fonte semanal: alerta em 8 dias, crítico em 15."), 4, 4)
    b.add(stat("Linhas na última carga", f"""
        SELECT rows_out FROM audit.pipeline_step_runs
        WHERE {ANP_LANDING} AND status = 'success' ORDER BY started_at DESC LIMIT 1""",
               unit="short", color_mode="none"), 4, 4)
    b.add(stat("Arquivos com falha", f"""
        SELECT COALESCE(SUM(files_failed), 0) FROM audit.pipeline_step_runs
        WHERE {ANP_LANDING} AND $__timeFilter(started_at)""", thresholds=ZERO_GOOD,
               desc="Arquivos rejeitados no landing (ex.: schema drift) no período."), 4, 4)
    b.add(stat("Sucesso dos steps", f"""
        SELECT 100.0 * COUNT(*) FILTER (WHERE status IN ('success', 'skipped')) / NULLIF(COUNT(*), 0)
        FROM audit.pipeline_step_runs WHERE {ANP_STEPS} AND status <> 'running' AND $__timeFilter(started_at)""",
               unit="percent", decimals=1, thresholds=GREEN_RED_RATE), 4, 4)
    b.add(stat("Última execução da DAG", """
        SELECT EXTRACT(EPOCH FROM end_date - start_date) FROM dag_run
        WHERE dag_id = 'dag_dbt_anp' AND end_date IS NOT NULL ORDER BY end_date DESC LIMIT 1""", ds=AF,
               unit="s", decimals=0, color_mode="none", desc="Duração ponta a ponta do último run da dag_dbt_anp."), 4, 4)
    b.add(stat("Coletas na gold", "SELECT COALESCE(SUM(qtd_coletas), 0) FROM gold.ag_anp_preco_semanal",
               unit="short", color_mode="none",
               desc="Lido do agregado (Σ qtd_coletas = linhas da fato; reconciliado por teste dbt)."), 4, 4)
    b.add(table("Cargas recentes", f"""
        SELECT started_at, step, status, ROUND(duration_s::numeric, 1) AS duracao_s,
               files_ok AS arquivos_ok, files_failed AS arquivos_falha, rows_in AS linhas_lidas,
               rows_out AS linhas_gravadas, rows_rejected AS rejeitadas, LEFT(error_message, 80) AS erro
        FROM audit.pipeline_step_runs WHERE {ANP_LANDING} AND $__timeFilter(started_at)
        ORDER BY started_at DESC LIMIT 30"""), 14, 8)
    b.add(table("Tasks mais lentas da dag_dbt_anp (média no período)", """
        SELECT task_id, COUNT(*) AS execucoes, ROUND(AVG(duration)::numeric, 1) AS media_s,
               ROUND(MAX(duration)::numeric, 1) AS max_s, MAX(end_date) AS ultima
        FROM task_instance
        WHERE dag_id = 'dag_dbt_anp' AND state = 'success' AND duration IS NOT NULL AND $__timeFilter(start_date)
        GROUP BY task_id ORDER BY media_s DESC LIMIT 12""", ds=AF, status_cols=(),
                desc="Onde o tempo da DAG vai: landing, modelos dbt (Cosmos) e checks."), 10, 8)
    b.add(timeseries("Duração por step", [sql_target(f"""
        SELECT started_at AS time, step AS metric, duration_s AS value
        FROM audit.pipeline_step_runs
        WHERE ({ANP_STEPS} OR (pipeline = 'obs' AND step = 'health_checks_anp'))
          AND status IN ('success', 'skipped') AND $__timeFilter(started_at)
        ORDER BY 1""", fmt="time_series")], unit="s", draw="points",
        desc="Cada ponto é uma execução. Ganhos de performance (índices, volumes) aparecem aqui."), 24, 7)

    # ------------------------------------------------------------- perfil do lote
    b.row("Perfil do lote — o dado mudou? (metrics → profile, gravado pelo landing)")
    b.add(timeseries("Linhas por produto em cada carga", [sql_target(f"""
        SELECT r.started_at AS time, p.key AS metric, p.value::numeric AS value
        FROM audit.pipeline_step_runs r
        CROSS JOIN LATERAL jsonb_each_text(r.metrics -> 'profile' -> 'rows_by_product') p
        WHERE {ANP_LANDING} AND r.status = 'success' AND $__timeFilter(r.started_at)
        ORDER BY 1""", fmt="time_series")], unit="short", draw="bars", stack=True), 12, 8)
    b.add(timeseries("Preço médio por produto no lote (antes do dbt)", [sql_target(f"""
        SELECT r.started_at AS time, p.key AS metric, p.value::numeric AS value
        FROM audit.pipeline_step_runs r
        CROSS JOIN LATERAL jsonb_each_text(r.metrics -> 'profile' -> 'avg_price_by_product') p
        WHERE {ANP_LANDING} AND r.status = 'success' AND $__timeFilter(r.started_at)
        ORDER BY 1""", fmt="time_series")], unit="currencyBRL", draw="points", min_zero=False,
        overrides=[{"matcher": {"id": "byName", "options": "GLP"},
                    "properties": [{"id": "custom.axisPlacement", "value": "right"}]}],
        desc="Perfil calculado no landing: um salto aqui, antes de chegar à gold, indica arquivo anômalo. "
             "GLP (botijão) no eixo da direita."), 12, 8)
    b.add(timeseries("Cobertura por carga", [sql_target(f"""
        SELECT r.started_at AS time,
               (r.metrics -> 'profile' ->> 'municipios')::int AS municipios,
               (r.metrics -> 'profile' ->> 'bandeiras')::int  AS bandeiras,
               (r.metrics -> 'profile' ->> 'cnpjs')::int      AS cnpjs
        FROM audit.pipeline_step_runs r
        WHERE {ANP_LANDING} AND r.status = 'success' AND r.metrics ? 'profile' AND $__timeFilter(r.started_at)
        ORDER BY 1""", fmt="time_series")], unit="short", draw="points",
        overrides=[{"matcher": {"id": "byName", "options": "cnpjs"},
                    "properties": [{"id": "custom.axisPlacement", "value": "right"}]}],
        desc="Queda brusca de municípios/postos = arquivo incompleto. CNPJs no eixo da direita."), 12, 8)
    b.add(table("Volume da última carga vs. mediana das 10 anteriores", f"""
        SELECT step, last_run_at AS ultima_carga, last_rows_out AS linhas,
               ROUND(median_rows_out::numeric) AS mediana_anteriores, deviation_pct AS desvio_pct
        FROM obs.v_step_volume_baseline WHERE {ANP_STEPS} ORDER BY step""", status_cols=(),
                overrides=[{"matcher": {"id": "byName", "options": "desvio_pct"},
                            "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                                           {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                               {"color": CRITICAL, "value": None}, {"color": WARN, "value": -50},
                                               {"color": GOOD, "value": -20}, {"color": WARN, "value": 20},
                                               {"color": CRITICAL, "value": 50}]}}]}]), 12, 8)

    # ------------------------------------------------------------- qualidade
    b.row("Qualidade — health checks (audit.data_quality_checks) e testes dbt (Elementary)")
    b.add(stat("Checks com falha/erro", """
        SELECT COUNT(*) FROM obs.v_check_last_result WHERE pipeline = 'anp' AND status IN ('fail', 'error')""",
               thresholds=ZERO_GOOD), 6, 4)
    b.add(stat("Checks em alerta", """
        SELECT COUNT(*) FROM obs.v_check_last_result WHERE pipeline = 'anp' AND status = 'warn'""",
               thresholds=ZERO_GOOD_WARN), 6, 4)
    b.add(stat("Checks suspensos", """
        SELECT COUNT(*) FROM obs.v_check_last_result
        WHERE pipeline = 'anp' AND status = 'skipped' AND message LIKE 'suspended:%'""",
               thresholds=ZERO_GOOD_WARN, desc="Suspensão explícita no checks.yml (com motivo). Dívida a quitar."), 6, 4)
    b.add(stat("Testes dbt ANP com falha", f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT ON (test_unique_id, column_name, test_sub_type) status
            FROM elementary.elementary_test_results WHERE {ANP_TESTS}
            ORDER BY test_unique_id, column_name, test_sub_type, detected_at DESC) t
        WHERE status IN ('fail', 'error')""", thresholds=ZERO_GOOD), 6, 4)
    b.add(table("Health checks — último resultado", """
        SELECT check_name AS check, check_type AS tipo, status, severity AS severidade,
               observed_value AS observado, expected AS esperado, LEFT(message, 90) AS mensagem, checked_at
        FROM obs.v_check_last_result WHERE pipeline = 'anp' ORDER BY status, check_name"""), 14, 8)
    b.add(table("Testes dbt ANP — último resultado não aprovado", f"""
        SELECT COALESCE(test_short_name, test_type) AS teste, table_name AS modelo, column_name AS coluna,
               status, failures AS registros, detected_at
        FROM (SELECT DISTINCT ON (test_unique_id, column_name, test_sub_type) *
              FROM elementary.elementary_test_results WHERE {ANP_TESTS}
              ORDER BY test_unique_id, column_name, test_sub_type, detected_at DESC) t
        WHERE status <> 'pass' ORDER BY failures DESC NULLS LAST"""), 10, 8)

    # ------------------------------------------------------------- negócio
    b.row("Preços — gold.ag_anp_preco_semanal (filtro: UF)")
    b.add(stat("Semana mais recente", """
        SELECT EXTRACT(EPOCH FROM MAX(semana)::timestamp) * 1000 FROM gold.ag_anp_preco_semanal""",
               unit="dateTimeAsLocalNoDateIfToday", color_mode="none"), 6, 4)
    b.add(stat("Postos na última semana", """
        SELECT SUM(qtd_postos) FROM (
            SELECT uf, MAX(qtd_postos) AS qtd_postos FROM gold.ag_anp_preco_semanal
            WHERE uf IN ($uf) AND semana = (SELECT MAX(semana) FROM gold.ag_anp_preco_semanal) GROUP BY uf) t""",
               unit="short", color_mode="none",
               desc="Maior nº de postos entre os produtos de cada UF (um posto vende vários produtos)."), 6, 4)
    b.add(stat("Gasolina — preço médio (última semana)", """
        SELECT SUM(preco_medio * qtd_coletas) / NULLIF(SUM(qtd_coletas), 0) FROM gold.ag_anp_preco_semanal
        WHERE produto = 'GASOLINA' AND uf IN ($uf)
          AND semana = (SELECT MAX(semana) FROM gold.ag_anp_preco_semanal)""",
               unit="currencyBRL", decimals=3, color_mode="none",
               desc="Média ponderada pelas coletas (nunca a média das médias por UF)."), 6, 4)
    b.add(stat("Etanol / gasolina", """
        WITH s AS (SELECT produto, SUM(preco_medio * qtd_coletas) / NULLIF(SUM(qtd_coletas), 0) AS p
                   FROM gold.ag_anp_preco_semanal
                   WHERE produto IN ('ETANOL', 'GASOLINA') AND uf IN ($uf)
                     AND semana = (SELECT MAX(semana) FROM gold.ag_anp_preco_semanal) GROUP BY produto)
        SELECT 100 * MAX(p) FILTER (WHERE produto = 'ETANOL') / NULLIF(MAX(p) FILTER (WHERE produto = 'GASOLINA'), 0)
        FROM s""", unit="percent", decimals=1,
               thresholds=[{"color": GOOD, "value": None}, {"color": NEUTRAL, "value": 70}],
               desc="Regra de bolso: abaixo de 70% o etanol compensa."), 6, 4)
    b.add(timeseries("Preço médio semanal por produto", [sql_target("""
        SELECT semana::timestamp AS time, produto AS metric,
               SUM(preco_medio * qtd_coletas) / NULLIF(SUM(qtd_coletas), 0) AS value
        FROM gold.ag_anp_preco_semanal
        WHERE uf IN ($uf) AND $__timeFilter(semana)
        GROUP BY 1, 2 ORDER BY 1""", fmt="time_series")], unit="currencyBRL", min_zero=False,
        overrides=[{"matcher": {"id": "byName", "options": "GLP"},
                    "properties": [{"id": "custom.axisPlacement", "value": "right"}]}],
        desc="GLP (botijão 13 kg) no eixo da direita."), 24, 8)
    b.add(table("Maiores variações: última semana vs. anterior", """
        WITH s AS (
            SELECT uf, produto, semana, preco_medio,
                   LAG(preco_medio) OVER (PARTITION BY uf, produto ORDER BY semana) AS anterior,
                   ROW_NUMBER() OVER (PARTITION BY uf, produto ORDER BY semana DESC) AS rn
            FROM gold.ag_anp_preco_semanal
            WHERE uf IN ($uf) AND semana >= (SELECT MAX(semana) FROM gold.ag_anp_preco_semanal) - 7)
        SELECT uf, produto, semana, ROUND(preco_medio::numeric, 3) AS preco, ROUND(anterior::numeric, 3) AS anterior,
               ROUND((100 * (preco_medio / anterior - 1))::numeric, 2) AS variacao_pct
        FROM s WHERE rn = 1 AND anterior > 0
        ORDER BY ABS(preco_medio / anterior - 1) DESC LIMIT 15""", status_cols=(),
                overrides=[{"matcher": {"id": "byName", "options": "variacao_pct"},
                            "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                                           {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                               {"color": GOOD, "value": None}, {"color": WARN, "value": 2},
                                               {"color": CRITICAL, "value": 5}]}}]}]), 12, 9)
    b.add(table("Preço médio por UF (última semana)", """
        SELECT uf,
               ROUND(AVG(preco_medio) FILTER (WHERE produto = 'GASOLINA')::numeric, 3)   AS gasolina,
               ROUND(AVG(preco_medio) FILTER (WHERE produto = 'ETANOL')::numeric, 3)     AS etanol,
               ROUND(AVG(preco_medio) FILTER (WHERE produto = 'DIESEL S10')::numeric, 3) AS diesel_s10,
               ROUND(AVG(preco_medio) FILTER (WHERE produto = 'GNV')::numeric, 3)        AS gnv,
               ROUND(AVG(preco_medio) FILTER (WHERE produto = 'GLP')::numeric, 2)        AS glp,
               SUM(qtd_coletas) AS coletas
        FROM gold.ag_anp_preco_semanal
        WHERE uf IN ($uf) AND semana = (SELECT MAX(semana) FROM gold.ag_anp_preco_semanal)
        GROUP BY uf ORDER BY gasolina DESC NULLS LAST""", status_cols=()), 12, 9)

    # ------------------------------------------------------------- RAG
    b.row("Downstream — RAG sobre a gold (ai.rag_context_anp)")
    b.add(stat("Último sync do vector store", f"""
        SELECT COALESCE(metrics ->> 'sync_mode', 'full (legado)') FROM audit.pipeline_step_runs
        WHERE {RAG_BUILD} AND status IN ('success', 'skipped') ORDER BY started_at DESC LIMIT 1""",
               color_mode="none", desc="incremental = só o delta da gold; full = store vazio, troca de modelo ou pedido."), 6, 4)
    b.add(stat("Vetores no store", "SELECT COUNT(*) FROM ai.rag_context_anp", unit="short", color_mode="none"), 6, 4)
    b.add(stat("Paridade gold (GO) ↔ vetores", """
        SELECT (SELECT COUNT(*) FROM ai.rag_context_anp)
             - (SELECT COALESCE(SUM(qtd_coletas), 0) FROM gold.ag_anp_preco_semanal WHERE uf = 'GO')""",
               thresholds=[{"color": WARN, "value": None}, {"color": GOOD, "value": 0}, {"color": WARN, "value": 1}],
               desc="Vetores − coletas de GO na gold. 0 = o RAG enxerga exatamente a gold."), 6, 4)
    b.add(stat("Acurácia do último gate", """
        SELECT 100 * LEAST(acuracia_roteamento, acuracia_valor) FROM ai.rag_eval_summary
        ORDER BY executado_em DESC LIMIT 1""", unit="percent", decimals=1, thresholds=GREEN_RED_RATE,
               desc="Menor entre roteamento e valor no golden set (gate: 90%)."), 6, 4)
    b.add(table("Execuções do build_context", f"""
        SELECT started_at, status, ROUND(duration_s::numeric) AS duracao_s,
               metrics ->> 'sync_mode' AS modo, (metrics ->> 'chunks_new')::int AS novos,
               (metrics ->> 'chunks_stale')::int AS obsoletos, (metrics ->> 'embed_rows_per_s')::numeric AS linhas_por_s,
               metrics ->> 'model' AS modelo, LEFT(error_message, 60) AS erro
        FROM audit.pipeline_step_runs WHERE {RAG_BUILD} AND $__timeFilter(started_at)
        ORDER BY started_at DESC LIMIT 20"""), 14, 8)
    b.add(timeseries("Velocidade de embedding por modelo", [sql_target(f"""
        SELECT started_at AS time, metrics ->> 'model' AS metric, (metrics ->> 'embed_rows_per_s')::numeric AS value
        FROM audit.pipeline_step_runs
        WHERE {RAG_BUILD} AND metrics ? 'embed_rows_per_s' AND $__timeFilter(started_at) ORDER BY 1""",
        fmt="time_series")], draw="points",
        desc="Linhas/s no cálculo dos embeddings (CPU). e5-base ≈ 6-7, e5-small ≈ 26 no lab."), 10, 8)
    return b


# =====================================================================================================
# 6) RAG · LC 214 (ADR 0010-0015): is the gate standing, are the metrics inside the contract, is anything degrading?
# =====================================================================================================
def rag_lc214() -> Board:
    b = Board("datalabs-rag-lc214", "DataLabs · RAG LC 214",
              "LC 214/2025 RAG: data contract, retrieval gate (lc05), answer gate (lc06), drift and cost. "
              "Every series comes from audit.pipeline_step_runs; each gate records its own floor.",
              ["datalabs", "rag", "lc214", "observability"], time_from="now-30d")
    b.links = [{"title": "Pipeline health", "type": "link", "url": "/d/datalabs-pipeline-health", "icon": "dashboard"}]

    run = "pipeline = 'lc214' AND step = '{step}' AND status IN ('success','failed')"
    ev, gen = run.format(step="evaluate_retrieval"), run.format(step="generate_answers")

    def last(step_filter, expr):
        """expr over the metrics of the latest finished run of a gate (NULL until a run records it)."""
        return (f"SELECT {expr} FROM (SELECT metrics AS m, status, exit_code FROM audit.pipeline_step_runs "
                f"WHERE {step_filter} ORDER BY started_at DESC LIMIT 1) l")

    def f(key):
        return f"(m->>'{key}')::float"

    def margin(prefix_keys):
        return "LEAST(" + ", ".join(f"{f(k)} - {f('floor_' + k)}" for k in prefix_keys) + ")"

    def series(step_filter, pairs):
        """value x floor, long format. pairs = [(label, metric key, floor key | None)]."""
        parts = []
        for label, key, floor in pairs:
            parts.append(f"SELECT started_at AS time, '{label}' AS metric, {f2(key)} AS value "
                         f"FROM audit.pipeline_step_runs WHERE {step_filter} AND metrics ? '{key}' AND $__timeFilter(started_at)")
            if floor:
                parts.append(f"SELECT started_at AS time, '{label} · floor' AS metric, {f2(floor)} AS value "
                             f"FROM audit.pipeline_step_runs WHERE {step_filter} AND metrics ? '{floor}' "
                             f"AND $__timeFilter(started_at)")
        return sql_target(" UNION ALL ".join(parts) + " ORDER BY 1", fmt="time_series")

    def f2(key):
        return f"(metrics->>'{key}')::float"

    def ts(title, pairs, step_filter, desc, unit="percentunit", soft_max=None):
        # Runs are weekly and sparse: points always shown (two runs minutes apart draw a zero-length line), no
        # fill (a fill under one point is a vertical stripe to zero). Floors keep their points too, smaller:
        # a floor that is not visible cannot answer "is the gate standing?".
        p = timeseries(title, [series(step_filter, pairs)], unit=unit, desc=desc, min_zero=True,
                       overrides=[{"matcher": {"id": "byRegexp", "options": ".* · floor"},
                                   "properties": [{"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [10, 10]}},
                                                  {"id": "custom.pointSize", "value": 5}]}])
        custom = p["fieldConfig"]["defaults"]["custom"]
        custom.update(showPoints="always", fillOpacity=0)
        if soft_max is None and unit == "percentunit":
            soft_max = 1
        if soft_max is not None:
            custom["axisSoftMax"] = soft_max
        return p

    b.add(text("**How to read.** Solid line = the run's metric; dashed = the gate floor "
               "(baseline − a tolerance of 1 case). Gate standing = the solid line does not cross the dashed one. "
               "*Margin* = the smallest distance to the floor. The model's wording changes between runs; its verdict "
               "does not (text drift is recorded, not a failure).", "Reading guide"), 24, 3)

    b.row("1 · Verdict of the latest run")
    # a stat panel does not show a text field: the verdict is a number with value mappings
    # 1 = passed the gate; 0 = the gate (or the step) failed; 2 = killed from outside (SIGTERM/SIGKILL: a stop, a
    # restart, the OOM killer): says nothing about quality, so it must not read as a failed gate
    verdict = [{"type": "value", "options": {"1": {"text": "✔ passing", "color": GOOD, "index": 0},
                                              "0": {"text": "✖ failed", "color": CRITICAL, "index": 1},
                                              "2": {"text": "⏸ interrupted", "color": SERIOUS, "index": 2}}},
               {"type": "special", "options": {"match": "null", "result": {"text": "no run yet", "color": NEUTRAL,
                                                                           "index": 3}}}]
    verdict_sql = "CASE WHEN status = 'success' THEN 1 WHEN exit_code IN (137, 143) THEN 2 ELSE 0 END"
    waiting = [{"type": "special", "options": {"match": "null", "result": {
        "text": "awaiting first run", "color": NEUTRAL, "index": 0}}}]
    b.add(stat("Retrieval gate (lc05)", last(ev, verdict_sql), mappings=verdict,
               thresholds=[{"color": CRITICAL, "value": None}, {"color": GOOD, "value": 1}],
               desc="Latest evaluate_retrieval result: passing = within the floor; failed = regression or step error; "
                    "interrupted = process killed from outside (SIGTERM), unrelated to quality."), 4, 4)
    b.add(stat("Answer gate (lc06)", last(gen, verdict_sql), mappings=verdict,
               thresholds=[{"color": CRITICAL, "value": None}, {"color": GOOD, "value": 1}],
               desc="Latest generate_answers result (dev subset, ADR 0015). Interrupted = process killed from outside "
                    "(SIGTERM): run it again, quality was not measured."), 4, 4)
    steps_margin = [{"color": CRITICAL, "value": None}, {"color": GOOD, "value": 0}]
    ret_keys = [f"{g}_{m}" for g in ("fiscal", "formal") for m in ("success_at_k", "mrr", "context_recall")]
    b.add(stat("Minimum margin · retrieval", last(ev, margin(ret_keys)), unit="percentunit", decimals=1,
               thresholds=steps_margin, mappings=waiting,
               desc="Smallest (metric − floor) across success@k, MRR and context_recall. Negative = regression. "
                    "A metric exactly on its baseline has a margin of 1 case (the tolerance): that is rest, not an alert. "
                    "Empty until the first run that records the floors."), 4, 4)
    gen_keys = [f"{g}_{m}" for g in ("fiscal", "formal") for m in ("refusal_accuracy", "citation_recall")]
    b.add(stat("Minimum margin · answers", last(gen, margin(gen_keys)), unit="percentunit", decimals=1,
               thresholds=steps_margin, mappings=waiting,
               desc="Smallest (metric − floor) across refusal accuracy and citation recall, per group. On the baseline the "
                    "margin is the tolerance of 1 case (14 pp in the fiscal group, 25 in the formal one)."), 4, 4)
    b.add(stat("False answers",
               last(gen, f"COALESCE({f('fiscal_false_answers')}, 0) + COALESCE({f('formal_false_answers')}, 0)"),
               thresholds=ZERO_GOOD, decimals=0, mappings=waiting,
               desc="An answer shown for a question the law does not answer. The error the 3 barriers exist to stop: "
                    "ceiling = 0, no tolerance."), 4, 4)
    b.add(stat("Data checks (latest round)", """
        SELECT COUNT(*) FILTER (WHERE status NOT IN ('pass','skipped')) AS failures FROM audit.data_quality_checks
        WHERE pipeline = 'lc214' AND check_run_id = (SELECT check_run_id FROM audit.data_quality_checks
                                                     WHERE pipeline = 'lc214' ORDER BY checked_at DESC LIMIT 1)""",
               thresholds=ZERO_GOOD, decimals=0, desc="lc214 health checks that did not pass in the latest round."), 4, 4)

    b.row("2 · Data contract (lc01 → lc04): what each step delivered")
    b.add(table("Latest state of each step", """
        SELECT s.step AS step, s.status, s.started_at AS started, ROUND(s.duration_s::numeric) AS duration_s,
               s.rows_in AS rows_in, s.rows_out AS rows_out,
               (SELECT MAX(started_at) FROM audit.pipeline_step_runs x
                WHERE x.pipeline = 'lc214' AND x.step = s.step AND x.status = 'success') AS last_success
        FROM (SELECT DISTINCT ON (step) * FROM audit.pipeline_step_runs WHERE pipeline = 'lc214'
              ORDER BY step, started_at DESC) s
        ORDER BY array_position(ARRAY['extract_source','parse_dispositivos','build_chunks','load_postgres',
                                      'embed_chunks','golden_contract','evaluate_retrieval','generate_answers'], s.step)""",
        desc="skipped (exit 99) = the output was already up to date for this input and code version; not a failure.",
        overrides=[{"matcher": {"id": "byName", "options": c}, "properties": [{"id": "custom.minWidth", "value": 150}]}
                   for c in ("started", "last_success")]), 12, 9)
    b.add(table("Health checks (latest round)", """
        SELECT check_name AS check, check_type AS type, status, severity AS severity,
               ROUND(observed_value::numeric, 3) AS observed, LEFT(message, 70) AS message
        FROM audit.data_quality_checks
        WHERE pipeline = 'lc214' AND check_run_id = (SELECT check_run_id FROM audit.data_quality_checks
                                                     WHERE pipeline = 'lc214' ORDER BY checked_at DESC LIMIT 1)
        ORDER BY check_name""",
        desc="Law × serving parity, single version, chunks × vectors parity, same law in the vectors, volume and freshness.",
        overrides=[{"matcher": {"id": "byName", "options": "check"}, "properties": [{"id": "custom.width", "value": 250}]},
                   {"matcher": {"id": "byName", "options": "message"}, "properties": [{"id": "custom.minWidth", "value": 260}]}]),
        12, 9)
    b.add(timeseries("Volumes: provisions, chunks", [sql_target("""
        SELECT started_at AS time, step AS metric, rows_out::float AS value FROM audit.pipeline_step_runs
        WHERE pipeline = 'lc214' AND step IN ('parse_dispositivos','build_chunks') AND status = 'success'
          AND $__timeFilter(started_at) ORDER BY 1""", fmt="time_series")], draw="points",
        desc="Parsed provisions (≥ 4,000 by the check) and chunks. An amendment moves these numbers; "
             "a drop with no amendment is a broken parser."), 12, 7)
    b.add(table("Routing and self-citations (golden contract)", """
        SELECT started_at AS started, status, rows_in AS cases, (metrics->>'routing_accuracy')::float AS routing,
               (metrics->>'self_ref_rate')::float AS self_citations
        FROM audit.pipeline_step_runs WHERE pipeline = 'lc214' AND step = 'golden_contract'
          AND status IN ('success','failed') AND $__timeFilter(started_at) ORDER BY started_at DESC LIMIT 10""",
        desc="Do the golden set labels still hold for the current law version? Routing must be 100%; "
             "resolved self-citations ≥ 99%."), 12, 7)

    b.row("3 · Retrieval gate (lc05): does the right provision reach the context?")
    for title, key, desc in (("Success@k", "success_at_k", "Every expected provision in the top-k. Gated."),
                             ("MRR", "mrr", "Position of the first hit (mean of the inverse rank). Gated."),
                             ("Context recall", "context_recall",
                              "Expected provisions in the context built for the LLM (token budget). Gated.")):
        pairs = [(g, f"{g}_{key}", f"floor_{g}_{key}") for g in ("fiscal", "formal")]
        b.add(ts(title, pairs, ev, desc), 8, 8)
    b.add(ts("Fiscal × formal gap (paired)", [("gap", "paired_gap", None)], ev,
             "success@k of the formal phrasing minus the same question in fiscal vocabulary. "
             "Measures the vocabulary the retriever does not cover; informational, not gated."), 12, 7)
    b.add(ts("Hit@k and Recall@k (informational)",
             [(f"{g} {m}", f"{g}_{m}", None) for g in ("fiscal", "formal") for m in ("hit_at_k", "recall_at_k")], ev,
             "Not gated: they help explain a drop in success@k."), 12, 7)

    b.row("4 · Answer gate (lc06): answers right, refuses when it should, cites what exists?")
    b.add(ts("Refusal accuracy",
             [(g, f"{g}_refusal_accuracy", f"floor_{g}_refusal_accuracy") for g in ("fiscal", "formal")], gen,
             "Verdict accuracy: answer when the law has an answer, refuse when it has not. Gated."), 8, 8)
    b.add(ts("Citation recall",
             [(g, f"{g}_citation_recall", f"floor_{g}_citation_recall") for g in ("fiscal", "formal")], gen,
             "Of the expected answers, how many cite the expected provision. Gated."), 8, 8)
    b.add(ts("Citation validity", [(g, f"{g}_citation_validity", None) for g in ("fiscal", "formal")], gen,
             "Citations that exist in the law AND were in the context shown. Below 100% = the model cited what "
             "it did not see."), 8, 8)
    b.add(ts("False answers and false refusals",
             [(f"{g} {k}", f"{g}_{k}", None) for g in ("fiscal", "formal") for k in ("false_answers", "false_refusals")],
             gen, "false_answers: ceiling 0 (strict gate). false_refusals: the cost of being conservative, "
                   "where quality is being lost today.", unit="none"), 12, 7)
    b.add(ts("Share of replies the model says it can answer",
             [(f"{g} model_says_answerable", f"{g}_model_says_answerable", None) for g in ("fiscal", "formal")], gen,
             "How often the model claims it can answer, before the barriers. Stuck near 0 = the model refuses everything."), 12, 7)

    b.row("5 · Drift and cost")
    b.add(ts("Drift since the baseline",
             [("verdict flips", "verdict_flips", None), ("texts reworded", "text_changed", None),
              ("expected citation changed", "cited_expected_changed", None)], gen,
             "A record, not a gate. Stable verdict + reworded text = non-reproducible wording; "
             "a verdict flip = behaviour changed.", unit="none", soft_max=11), 12, 7)
    b.add(ts("Latency per phrasing (s)",
             [(f"{g} p50", f"{g}_latency_p50_s", None) for g in ("fiscal", "formal")]
             + [(f"{g} max", f"{g}_latency_max_s", None) for g in ("fiscal", "formal")], gen,
             "CPU, qwen2.5:3b, num_ctx 6144. DAG budget: 90 min for 11 phrasings.", unit="s"), 12, 7)
    b.add(ts("Tokens per word (budget calibration)", [("measured", "tokens_per_word", None)], gen,
             "The retriever estimates 1.7 tokens/word; lc06 measures it with prompt_eval_count. An estimate below "
             "the measured value = context overflowing num_ctx.", unit="none"), 12, 7)
    b.add(table("Answer gate runs", """
        SELECT started_at AS started, status, ROUND(duration_s::numeric) AS duration_s, metrics->>'variant' AS variant,
               metrics->>'decision' AS rule, (metrics->>'fiscal_refusal_accuracy')::float AS fiscal_refusal_acc,
               (metrics->>'formal_citation_recall')::float AS formal_cit_recall,
               (metrics->>'verdict_flips')::int AS flips, exit_code, LEFT(error_message, 60) AS error
        FROM audit.pipeline_step_runs WHERE pipeline = 'lc214' AND step = 'generate_answers' AND status <> 'running'
          AND $__timeFilter(started_at) ORDER BY started_at DESC LIMIT 20""",
        desc="Includes skipped runs (--once: same fingerprint, nothing to generate). exit_code 143 = killed from "
             "outside (interrupted), 1 = the gate or the step failed."), 12, 7)
    return b


if __name__ == "__main__":
    for board in (pipeline_health(), airflow_platform(), logs_explorer(), nfe_fiscal(), anp_produto(), rag_lc214()):
        print("wrote", board.write())
