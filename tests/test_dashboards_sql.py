"""
Executes every SQL query of the provisioned Grafana dashboards against Postgres, as the
read-only role Grafana uses (catches typos, missing grants and schema drift in the views).

Runs only when DASHBOARD_SQL_DSN_DW is set, e.g.
  DASHBOARD_SQL_DSN_DW=postgresql://grafana_ro:pwd@localhost:5432/datalab \
  DASHBOARD_SQL_DSN_AIRFLOW=postgresql://grafana_ro:pwd@localhost:5432/airflow  pytest tests/test_dashboards_sql.py
"""
import json
import os
import re
from pathlib import Path

import pytest

DASHBOARDS = sorted((Path(__file__).resolve().parents[1] / "grafana" / "dashboards").glob("*.json"))
DSN = {"dw": os.getenv("DASHBOARD_SQL_DSN_DW"), "airflow_meta": os.getenv("DASHBOARD_SQL_DSN_AIRFLOW")}


def grafana_macros(sql: str) -> str:
    sql = re.sub(r"\$__timeFilter\(([^)]+)\)", r"\1 BETWEEN now() - interval '30 days' AND now()", sql)
    sql = re.sub(r"\$__timeGroupAlias\(([^,]+),\s*'?[^)']+'?\)", r"date_trunc('hour', \1) AS time", sql)
    sql = sql.replace("$pipeline", "'anp','rfb','rag','obs'")
    return sql


def sql_targets():
    for path in DASHBOARDS:
        dash = json.loads(path.read_text(encoding="utf-8"))
        for panel in dash["panels"]:
            for t in panel.get("targets", []):
                if t.get("rawSql"):
                    yield pytest.param(t["datasource"]["uid"], t["rawSql"], id=f"{path.stem}:{panel['title']}")
        for var in dash.get("templating", {}).get("list", []):
            if isinstance(var.get("query"), str) and var.get("datasource", {}).get("uid") in DSN:
                yield pytest.param(var["datasource"]["uid"], var["query"], id=f"{path.stem}:var:{var['name']}")


def test_dashboards_are_valid_json_with_unique_panel_ids():
    assert DASHBOARDS, "no dashboards found"
    for path in DASHBOARDS:
        dash = json.loads(path.read_text(encoding="utf-8"))
        ids = [p["id"] for p in dash["panels"]]
        assert len(ids) == len(set(ids)), path
        assert dash["uid"] == path.stem


@pytest.mark.parametrize("uid,sql", list(sql_targets()))
def test_dashboard_sql_executes(uid, sql):
    dsn = DSN.get(uid)
    if not dsn:
        pytest.skip(f"no DSN for datasource {uid}")
    import psycopg2

    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(grafana_macros(sql))
        cur.fetchall()
