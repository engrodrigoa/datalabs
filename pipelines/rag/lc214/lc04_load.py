"""LC 214/2025 silver (current version) -> Postgres serving tables, by upsert (ADR 0011).

Step 4a of the legislation RAG. Loads, from silver/lc214/<sha12>/ of the manifest's current version:
    dispositivos.parquet -> ai.lc214_dispositivo   one row per provision: what the router answers by SQL
    artigos.parquet      -> ai.lc214_artigo        one row per article: the "big" of small-to-big

The lake is the system of record; these tables are a projection of the current version, rebuildable at any time.

Why upsert, and not TRUNCATE + INSERT
    TRUNCATE takes an ACCESS EXCLUSIVE lock: every reader (the router answering a citation) would wait for the
    whole load. INSERT ... ON CONFLICT plus the DELETE of orphans, in ONE transaction, takes row locks only, and
    readers keep seeing the previous version (MVCC) until the commit: never an empty or half-loaded table.
    The upsert also writes only what changed (`row_hash` over the content, not over the version stamp), so the
    step reports what an amendment did to the law:
        inserted   provisions that are new in the text
        updated    provisions whose text or status changed (amended, revoked, vetoed)
        deleted    provisions that left the text (renumbered or removed): orphans, deleted
        restamped  same content, new version stamp (`source_sha256`, `parser_version`) or new position
                   (`ordem`, `pagina`): position is not content, an insertion shifts every later provision

Guards (the run fails instead of serving something wrong)
    silver_inconsistent   lc02 and lc03 outputs must be the same law version and the same parser version: a load
                          between a re-parse and its re-chunk would serve provisions and articles that disagree
    mass_delete_blocked   more than LC214_MAX_DELETE_PCT of the served rows leaving at once (a broken parse must not
                          empty the serving table); --allow-mass-delete for a deliberate change

Exit codes: 0 loaded (something changed) | 99 already in sync | 1 failed.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys

import polars as pl

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.logger import get_logger, log_event  # noqa: E402
from pipelines.observability import current_step  # noqa: E402
from pipelines.rag.lc214.lake import Lake, LakeError, k_manifest, k_silver, lake_from_env  # noqa: E402

logger = get_logger("lc04_load")

SCHEMA = "ai"
MAX_DELETE_PCT = float(os.getenv("LC214_MAX_DELETE_PCT", "5"))
EXIT_OK, EXIT_SKIPPED, EXIT_FAILED = 0, 99, 1

JSON_COLS = ("notas", "alterado_por", "tipo_alteracao")
DISP_CONTENT = ("parent_id", "art_id", "tipo", "rotulo", "ordem", "art", "par", "inc", "ali", "titulo", "titulo_nome",
                "capitulo", "capitulo_nome", "secao", "secao_nome", "status", "texto", "notas", "alterado_por",
                "tipo_alteracao", "altera_outra_lei", "pagina")
ART_CONTENT = ("art", "rotulo", "header", "texto", "status", "pagina", "n_dispositivos", "alterado_por",
               "altera_outra_lei")
STAMP = ("row_hash", "source_sha256", "parser_version")

# Position is not content: an amendment that inserts art. 54-A shifts the reading order (`ordem`) of every later
# provision, and the re-flowed PDF shifts their `pagina`. Hashing them would report thousands of "updated"
# provisions for one insertion. They are refreshed with the version stamp instead.
POSITION = ("ordem", "pagina")

# table -> (key, content columns)
TABLES = {
    "lc214_dispositivo": ("dispositivo_id", DISP_CONTENT),
    "lc214_artigo": ("art_id", ART_CONTENT),
}


def hashed(content: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(c for c in content if c not in POSITION)


class LoadError(Exception):
    def __init__(self, event: str, message: str, **fields):
        super().__init__(message)
        self.event, self.fields = event, fields


# --------------------------------------------------------------------------- frames to load (no database)
def _row_hash(df: pl.DataFrame, cols: tuple[str, ...]) -> list[str]:
    """md5 of the content columns, in a fixed order, nulls distinct from empty strings."""
    out = []
    for row in df.select(cols).iter_rows():
        payload = json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=str)
        out.append(hashlib.md5(payload.encode("utf-8")).hexdigest())
    return out


def _with_json_and_stamp(df: pl.DataFrame, key: str, content: tuple[str, ...], sha: str, parser_version: str):
    df = df.with_columns([
        pl.Series(c, [json.dumps(v if v is not None else [], ensure_ascii=False) for v in df[c].to_list()])
        for c in JSON_COLS if c in content
    ])
    df = df.with_columns(pl.Series("row_hash", _row_hash(df, hashed(content))),
                         pl.lit(sha).alias("source_sha256"), pl.lit(parser_version).alias("parser_version"))
    return df.select(key, *content, *STAMP)


def prepare_dispositivos(df: pl.DataFrame, sha: str, parser_version: str) -> pl.DataFrame:
    df = df.with_columns(pl.col("dispositivo_id").str.extract(r"^([^:]+:art-[^:]+)", 1).alias("art_id"))
    return _with_json_and_stamp(df, "dispositivo_id", DISP_CONTENT, sha, parser_version)


def prepare_artigos(df: pl.DataFrame, sha: str, parser_version: str) -> pl.DataFrame:
    return _with_json_and_stamp(df, "art_id", ART_CONTENT, sha, parser_version)


# --------------------------------------------------------------------------- the sync (one transaction, caller commits)
def sync_table(cur, table: str, frame: pl.DataFrame, max_delete_pct: float = MAX_DELETE_PCT,
               allow_mass_delete: bool = False) -> dict:
    """Make SCHEMA.table equal to `frame`, writing only what changed. Runs inside the caller's transaction."""
    key, content = TABLES[table]
    cols = [key, *content, *STAMP]
    collist = ", ".join(cols)
    target = f"{SCHEMA}.{table}"

    cur.execute(f"CREATE TEMP TABLE stage_{table} (LIKE {target} INCLUDING DEFAULTS) ON COMMIT DROP")
    buf = io.StringIO()
    frame.select(cols).write_csv(buf)
    buf.seek(0)
    cur.copy_expert(f"COPY stage_{table} ({collist}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", buf)

    cur.execute(f"SELECT count(*) FROM {target}")
    served = cur.fetchone()[0]
    cur.execute(f"SELECT count(*) FROM {target} t WHERE NOT EXISTS "
                f"(SELECT 1 FROM stage_{table} s WHERE s.{key} = t.{key})")
    leaving = cur.fetchone()[0]
    pct = 100.0 * leaving / served if served else 0.0
    if pct > max_delete_pct and not allow_mass_delete:
        raise LoadError("mass_delete_blocked", f"{target}: {leaving} of {served} served rows would be deleted "
                        f"({pct:.1f}% > {max_delete_pct}%): check the parse, or rerun with --allow-mass-delete",
                        table=table, leaving=leaving, served=served)

    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in (*content, *STAMP))
    cur.execute(f"INSERT INTO {target} ({collist}) SELECT {collist} FROM stage_{table} "
                f"ON CONFLICT ({key}) DO UPDATE SET {updates}, loaded_at = now() "
                f"WHERE {target}.row_hash IS DISTINCT FROM EXCLUDED.row_hash "
                f"RETURNING (xmax = 0)")                     # xmax = 0: the row was inserted, not updated
    written = [r[0] for r in cur.fetchall()]
    inserted = sum(written)
    updated = len(written) - inserted

    refresh = ["source_sha256", "parser_version", *(c for c in POSITION if c in content)]
    cur.execute(f"UPDATE {target} t SET {', '.join(f'{c} = s.{c}' for c in refresh)}, loaded_at = now() "
                f"FROM stage_{table} s WHERE t.{key} = s.{key} "
                f"AND ({', '.join(f't.{c}' for c in refresh)}) IS DISTINCT FROM ({', '.join(f's.{c}' for c in refresh)})")
    restamped = cur.rowcount

    cur.execute(f"DELETE FROM {target} t WHERE NOT EXISTS (SELECT 1 FROM stage_{table} s WHERE s.{key} = t.{key})")
    deleted = cur.rowcount
    return {"rows": frame.height, "inserted": inserted, "updated": updated, "deleted": deleted,
            "restamped": restamped}


# --------------------------------------------------------------------------- the step
def read_silver(lake: Lake) -> tuple[str, str, pl.DataFrame, pl.DataFrame]:
    """The current version's provisions and articles, refusing a silver where lc02 and lc03 disagree."""
    sha = lake.get_json(k_manifest())["current"]
    for name in ("parse_report.json", "chunks_report.json"):
        if not lake.exists(k_silver(sha, name)):
            raise LoadError("input_missing", f"{lake.url(k_silver(sha, name))} not found: run lc02/lc03 first",
                            sha256=sha)
    parse, chunks = lake.get_json(k_silver(sha, "parse_report.json")), lake.get_json(k_silver(sha, "chunks_report.json"))
    disp = lake.get_parquet(k_silver(sha, "dispositivos.parquet"))
    arts = lake.get_parquet(k_silver(sha, "artigos.parquet"))

    versions = {"parse_report": parse.get("parser_version"), "chunks_report": chunks.get("parser_version"),
                "dispositivos": ",".join(sorted(set(disp["parser_version"].to_list())))}
    shas = {chunks.get("source_sha256"), parse.get("source_sha256"), *disp["source_sha256"].unique().to_list()}
    if len(set(versions.values())) != 1 or shas != {sha}:
        raise LoadError("silver_inconsistent", f"silver/lc214/{sha[:12]} mixes outputs: parser versions {versions}, "
                        f"law versions {sorted(s[:12] for s in shas if s)}: wait for lc03 to re-chunk",
                        sha256=sha, **versions)
    return sha, versions["parse_report"], disp, arts


def run(lake: Lake, connect, allow_mass_delete: bool = False) -> int:
    step = current_step()
    sha, parser_version, disp, arts = read_silver(lake)
    frames = {"lc214_dispositivo": prepare_dispositivos(disp, sha, parser_version),
              "lc214_artigo": prepare_artigos(arts, sha, parser_version)}

    conn = connect()
    try:
        with conn:                                   # one transaction: commit on success, rollback on any error
            with conn.cursor() as cur:
                result = {t: sync_table(cur, t, f, allow_mass_delete=allow_mass_delete) for t, f in frames.items()}
    finally:
        conn.close()

    flat = {f"{t.removeprefix('lc214_')}_{k}": v for t, r in result.items() for k, v in r.items() if k != "rows"}
    changed = sum(flat.values())
    step.add(rows_in=sum(r["rows"] for r in result.values()),
             rows_out=sum(r["inserted"] + r["updated"] for r in result.values()), **flat)
    d = result["lc214_dispositivo"]
    summary = (f"version {sha[:12]} (parser v{parser_version}): provisions +{d['inserted']} ~{d['updated']} "
               f"-{d['deleted']} restamped {d['restamped']} of {d['rows']}; articles "
               f"{ {k: v for k, v in result['lc214_artigo'].items() if k != 'rows'} }")
    if not changed:
        log_event(logger, "lc214_load_up_to_date", f"serving tables already match {summary}", sha256=sha)
        return EXIT_SKIPPED
    log_event(logger, "lc214_load_done", summary, sha256=sha, parser_version=parser_version, **flat)
    return EXIT_OK


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lake", default=None, help="'s3' (default, LC214_LAKE) or file:///dir")
    ap.add_argument("--allow-mass-delete", action="store_true",
                    help=f"accept more than {MAX_DELETE_PCT}% of the served rows leaving at once")
    args = ap.parse_args(argv)
    try:
        from pipelines.commons.dw_client import get_pg_connection, test_pg_connection
        test_pg_connection()
        return run(lake_from_env(args.lake), get_pg_connection, allow_mass_delete=args.allow_mass_delete)
    except LoadError as exc:
        log_event(logger, exc.event, str(exc), level=40, **exc.fields)
        return EXIT_FAILED
    except LakeError as exc:
        log_event(logger, exc.event, str(exc), level=40)
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
