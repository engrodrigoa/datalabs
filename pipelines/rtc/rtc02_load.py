"""Rebuilt calculator database (lake) -> ref_rtc.* tables in the DW, one transaction per version.

Step 2 of the RTC reference pipeline (ADR 0019).

What it does
    Reads the manifest's current version; if ctrl.rtc_carga already has that sha -> exit 99 (SKIPPED).
    Otherwise downloads the database to a temporary file and, in ONE transaction, replaces the content of every
    ref_rtc table (flyway_replay.EXPORT_TABLES) and records the version in ctrl.rtc_carga. Readers see the old
    version or the new one, never a mix: the dbt models of the same run read a consistent set of tables.

Raw on purpose
    Columns keep the source names (lowercase) and the source values: dates as text, flags as 0/1. Typing,
    business names and the point-in-time rules are dbt's job (stg_rtc__* -> silver.rtc_*), where they are tested.

Schema drift
    Same columns -> DELETE + COPY (the stg views that read the table stay valid).
    Columns changed (a new release added or renamed a column) -> DROP ... CASCADE + CREATE, event
    `schema_drift_table_recreated`: the dependent stg views are dropped and rebuilt by the dbt step that runs next in
    dag_rtc_ref; if a renamed column broke a model, that dbt step fails with the column named.

Exit codes: 0 loaded | 99 version already loaded | 1 failed.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sqlite3
import sys
import tempfile

import polars as pl

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.logger import get_logger, log_event  # noqa: E402
from pipelines.observability import current_step  # noqa: E402
from pipelines.rag.lc214.lake import Lake, LakeError, lake_from_env  # noqa: E402
from pipelines.rtc.flyway_replay import EXPORT_TABLES  # noqa: E402
from pipelines.rtc.rtc01_fetch import k_manifest  # noqa: E402

logger = get_logger("rtc02_load")

SCHEMA = "ref_rtc"
EXIT_OK, EXIT_SKIPPED, EXIT_FAILED = 0, 99, 1


class LoadError(Exception):
    def __init__(self, event: str, message: str):
        super().__init__(message)
        self.event = event


# --------------------------------------------------------------------------- pure helpers (unit-tested)
def pg_type(sqlite_decl: str) -> str:
    """SQLite declared type -> Postgres type (SQLite affinity rules, simplified to what the source uses)."""
    d = (sqlite_decl or "").upper()
    if "INT" in d:
        return "bigint"
    if any(t in d for t in ("REAL", "FLOA", "DOUB")):
        return "double precision"
    if any(t in d for t in ("NUMERIC", "DECIMAL")):
        return "numeric"
    return "text"


def source_columns(con: sqlite3.Connection, table: str) -> list[tuple[str, str]]:
    """[(column lowercase, postgres type)] in source order."""
    return [(r[1].lower(), pg_type(r[2])) for r in con.execute(f'PRAGMA table_info("{table}")')]


def table_csv(con: sqlite3.Connection, table: str, columns: list[str]) -> tuple[io.StringIO, int]:
    """The table as CSV for COPY. Read as text: Postgres casts on COPY, so no type is guessed on the way."""
    rows = con.execute(f'SELECT * FROM "{table}"').fetchall()
    df = pl.DataFrame(
        [[None if v is None else str(v) for v in r] for r in rows],
        schema={c: pl.String for c in columns}, orient="row",
    )
    buf = io.StringIO()
    df.write_csv(buf, null_value="")              # COPY CSV: unquoted empty = NULL, quoted "" = empty string
    buf.seek(0)
    return buf, df.height


def create_sql(table: str, cols: list[tuple[str, str]]) -> str:
    body = ", ".join(f'"{c}" {t}' for c, t in cols)
    return f'CREATE TABLE {SCHEMA}."{table.lower()}" ({body})'


# --------------------------------------------------------------------------- the step
def current_columns(cur, table: str) -> list[tuple[str, str]] | None:
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position""",
                (SCHEMA, table.lower()))
    rows = cur.fetchall()
    return [(c, t) for c, t in rows] or None


def load(conn, db_path: str, entry: dict, report: dict | None, step_run_id: str | None = None) -> dict:
    """Replace every ref_rtc table and record the version, in the caller's transaction. Returns row counts."""
    src = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    counts, recreated = {}, []
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            for table in EXPORT_TABLES:
                cols = source_columns(src, table)
                if not cols:
                    raise LoadError("source_table_missing", f"{table} is not in the landed database")
                existing = current_columns(cur, table)
                if existing is None:
                    cur.execute(create_sql(table, cols))
                elif existing != cols:
                    cur.execute(f'DROP TABLE {SCHEMA}."{table.lower()}" CASCADE')
                    cur.execute(create_sql(table, cols))
                    recreated.append(table)
                    log_event(logger, "schema_drift_table_recreated",
                              f"{SCHEMA}.{table.lower()}: columns changed, table recreated (dependent views dropped,"
                              " rebuilt by dbt)", level=30, table=table, before=existing, after=cols)
                else:
                    cur.execute(f'DELETE FROM {SCHEMA}."{table.lower()}"')
                buf, n = table_csv(src, table, [c for c, _ in cols])
                if n:
                    names = ", ".join(f'"{c}"' for c, _ in cols)
                    cur.copy_expert(f'COPY {SCHEMA}."{table.lower()}" ({names}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)',
                                    buf)
                counts[table] = n
            expected = (report or {}).get("row_counts") or {}
            mismatched = {t: (counts[t], expected[t]) for t in counts if t in expected and expected[t] != counts[t]}
            if mismatched:
                raise LoadError("load_count_mismatch", f"rows loaded != rows built: {mismatched}")
            cur.execute("""
                INSERT INTO ctrl.rtc_carga (sha, release, database_version, source_repo, built_at, row_counts,
                                            schema_changes, step_run_id)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)""",
                        (entry["sha"], entry.get("release"), entry.get("database_version"),
                         (report or {}).get("repo", "?"), entry.get("built_at"), json.dumps(counts),
                         json.dumps(recreated), step_run_id))
    finally:
        src.close()
    return {"row_counts": counts, "recreated": recreated}


def already_loaded(conn, sha: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM ctrl.rtc_carga WHERE sha = %s", (sha,))
        return cur.fetchone() is not None


def run(lake: Lake, conn, *, force: bool = False) -> int:
    step = current_step()
    current = (lake.get_json(k_manifest(), {}) or {}).get("current")
    if not current:
        raise LoadError("nothing_landed", "landing/rtc/manifest.json has no current version: run rtc01_fetch first")
    sha = current["sha"]
    step.label(source_sha=sha[:12], release=current.get("release") or "?")
    if already_loaded(conn, sha) and not force:
        log_event(logger, "version_already_loaded", f"{sha[:12]} ({current.get('release')}) is already in ref_rtc",
                  sha=sha)
        return EXIT_SKIPPED
    if force:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ctrl.rtc_carga WHERE sha = %s", (sha,))

    report = lake.get_json(current["db"].rsplit("/", 1)[0] + "/build_report.json", None)
    with tempfile.TemporaryDirectory(prefix="rtc02_") as tmp:
        db_path = os.path.join(tmp, "calculadora-rtc.db")
        with open(db_path, "wb") as f:
            f.write(lake.get(current["db"]))
        try:
            out = load(conn, db_path, current, report, step_run_id=getattr(step, "step_run_id", None))
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    rows = sum(out["row_counts"].values())
    step.add(rows_out=rows)
    step.label(entity=",".join(t.lower() for t in out["row_counts"]))
    log_event(logger, "version_loaded",
              f"{current.get('release')} ({current.get('database_version')}, {sha[:12]}) -> {SCHEMA}: "
              f"{len(out['row_counts'])} tables, {rows} rows, {len(out['recreated'])} recreated",
              sha=sha, row_counts=out["row_counts"], recreated=out["recreated"])
    return EXIT_OK


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--force", action="store_true", help="reload the current version even if already loaded")
    ap.add_argument("--lake", default=None, help="'s3' (default, RTC_LAKE) or file:///dir")
    args = ap.parse_args(argv)
    from pipelines.commons.dw_client import get_pg_connection

    conn = get_pg_connection()
    try:
        return run(lake_from_env(args.lake or os.getenv("RTC_LAKE", "s3")), conn, force=args.force)
    except (LoadError, LakeError) as exc:
        log_event(logger, exc.event, str(exc), level=40)
        return EXIT_FAILED
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
