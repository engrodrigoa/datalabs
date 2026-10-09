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


if __name__ == "__main__":
    for board in (pipeline_health(), airflow_platform(), logs_explorer()):
        print("wrote", board.write())
