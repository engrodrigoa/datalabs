"""LC 214/2025 chunks (lc03) -> ai.lc214_chunk (pgvector), synced incrementally (ADR 0007, ADR 0013).

Step 4c of the legislation RAG. Reads silver/lc214/<sha12>/chunks.parquet of the manifest's current version and makes
ai.lc214_chunk equal to it, paying the embedding cost only for chunks whose TEXT is new:

    new        chunk_id not in the store     -> embedded and inserted
    stale      chunk_id no longer in silver  -> deleted (its text changed: it has a new id, counted in new)
    restamped  same chunk_id, new metadata   -> updated WITHOUT re-embedding (new law version or parser that left the
               (version stamp, provision ids)   chunk's text alone: same text, same vector)

The chunk_id is md5(chunker version | text) (lc03), so "same id" means "same text": the vector stays valid.

Full rebuild only when it must (context_sync.plan_sync): empty store, embedding model changed (vectors of two models
are not comparable), or --full-refresh. Even then the store is replaced by DELETE + INSERT in one transaction, not
TRUNCATE: readers (the retriever) keep the previous version until the commit (ADR 0011).

Guards (the run fails instead of serving something wrong)
    chunks_estimated   chunks sized with the word estimate, not the model tokenizer: a chunk over 512 tokens would be
                       truncated by the model SILENTLY. lc03 proves the limit only with the tokenizer.
    store_shrink       the store would lose more than LC214_MAX_DELETE_PCT of its chunks net (stale - new). A chunker
                       bump replaces every id but keeps the count (churn, allowed); a broken parse loses chunks
                       (shrink, blocked). --allow-shrink for a deliberate change. Checked before loading the model.

Exit codes: 0 synced (something changed) | 99 already in sync (the model is not even loaded) | 1 failed.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Callable

import polars as pl

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.ai.context_sync import SyncPlan, plan_sync  # noqa: E402
from pipelines.commons.logger import get_logger, log_event  # noqa: E402
from pipelines.observability import current_step  # noqa: E402
from pipelines.rag.lc214.lake import Lake, LakeError, k_manifest, k_silver, lake_from_env  # noqa: E402

logger = get_logger("lc04_embed")

SCHEMA, TABLE = "ai", "lc214_chunk"
TARGET = f"{SCHEMA}.{TABLE}"
MAX_DELETE_PCT = float(os.getenv("LC214_MAX_DELETE_PCT", "5"))
BATCH_SIZE = int(os.getenv("RAG_EMBED_BATCH", "64"))
PROGRESS_ROWS = int(os.getenv("LC214_EMBED_PROGRESS_ROWS", "200"))
EXIT_OK, EXIT_SKIPPED, EXIT_FAILED = 0, 99, 1

JSON_COLS = ("dispositivo_ids", "alterado_por")
# what the chunk is (id = text) and what can change without changing the text: refreshed, never re-embedded
META = ("art_id", "parte", "kind", "dispositivo_ids", "n_tokens", "alterado_por", "source_sha256", "parser_version",
        "chunker_version")
COLS = ("chunk_id", "texto", *META)


class EmbedError(Exception):
    def __init__(self, event: str, message: str, **fields):
        super().__init__(message)
        self.event, self.fields = event, fields


@dataclass(frozen=True)
class Encoder:
    """The embedding model as the step needs it: a name (stored per row), a dimension and a batch encode."""
    model: str
    dim: int
    encode: Callable[[list[str]], list[list[float]]]


def load_encoder() -> Encoder:
    from pipelines.commons.ai.embeddings import MODEL_NAME, encode_passages, load_model, model_dim

    m = load_model()
    return Encoder(MODEL_NAME, model_dim(m), lambda texts: encode_passages(m, texts, batch_size=BATCH_SIZE).tolist())


def current_model() -> str:
    from pipelines.commons.ai.embeddings import MODEL_NAME

    return MODEL_NAME


# --------------------------------------------------------------------------- input (lake)
def read_chunks(lake: Lake) -> tuple[str, pl.DataFrame]:
    """The current version's chunks, stamped with the parser version of the report that committed them."""
    sha = lake.get_json(k_manifest())["current"]
    if not lake.exists(k_silver(sha, "chunks_report.json")):
        raise EmbedError("input_missing", f"{lake.url(k_silver(sha, 'chunks_report.json'))} not found: run lc03 first",
                         sha256=sha)
    report = lake.get_json(k_silver(sha, "chunks_report.json"))
    if report.get("token_method") != "tokenizer":
        raise EmbedError("chunks_estimated", f"silver/lc214/{sha[:12]} was chunked with token_method="
                         f"{report.get('token_method')!r}: chunk sizes are estimates, a chunk over "
                         f"{report.get('max_tokens')} tokens would be truncated silently. Re-chunk with --tokens "
                         "tokenizer (the DAG does)", sha256=sha)
    chunks = lake.get_parquet(k_silver(sha, "chunks.parquet"))
    if set(chunks["source_sha256"].unique().to_list()) != {sha} or chunks.height != report.get("chunks"):
        raise EmbedError("silver_inconsistent", f"chunks.parquet does not match its report ({chunks.height} rows, "
                         f"report says {report.get('chunks')}): wait for lc03 to finish", sha256=sha)
    return sha, chunks.with_columns(pl.lit(report.get("parser_version")).cast(pl.Utf8).alias("parser_version"))


def prepare(chunks: pl.DataFrame) -> pl.DataFrame:
    """Rows to load: lists as JSON text (jsonb), only the columns of the table."""
    df = chunks.with_columns([
        pl.Series(c, [json.dumps(v if v is not None else [], ensure_ascii=False) for v in chunks[c].to_list()])
        for c in JSON_COLS
    ])
    return df.select(COLS)


# --------------------------------------------------------------------------- state and plan
def store_state(cur) -> tuple[set[str], set[str], int | None]:
    """chunk_ids and embedding models in the store, and the declared vector size (pgvector typmod)."""
    cur.execute(f"SELECT chunk_id, embedding_model FROM {TARGET}")
    rows = cur.fetchall()
    cur.execute("SELECT NULLIF(atttypmod, -1) FROM pg_attribute WHERE attrelid = %s::regclass AND attname = 'embedding'",
                (TARGET,))
    dim = cur.fetchone()[0]
    return {r[0] for r in rows}, {r[1] for r in rows}, dim


def shrink_pct(plan: SyncPlan, existing: int) -> float:
    """Net loss of chunks, in % of the store. Churn (stale replaced by new) is not a loss."""
    if not existing:
        return 0.0
    return max(0.0, 100.0 * (len(plan.stale_ids) - len(plan.new_ids)) / existing)


def guard_shrink(plan: SyncPlan, existing: int, max_pct: float = MAX_DELETE_PCT, allow: bool = False) -> None:
    pct = shrink_pct(plan, existing)
    if pct > max_pct and not allow:
        raise EmbedError("store_shrink", f"{TARGET} would lose {len(plan.stale_ids) - len(plan.new_ids)} of {existing} "
                         f"chunks net ({pct:.1f}% > {max_pct}%): check the parse and the chunks, or rerun with "
                         "--allow-shrink", stale=len(plan.stale_ids), new=len(plan.new_ids), served=existing)


# --------------------------------------------------------------------------- embedding
def embed(frame: pl.DataFrame, encoder: Encoder, step=None) -> pl.DataFrame:
    """Embeds in blocks of PROGRESS_ROWS with one progress event per block (done/total, rows/s, ETA)."""
    texts, vectors = frame["texto"].to_list(), []
    t0 = time.perf_counter()
    for start in range(0, len(texts), PROGRESS_ROWS):
        vectors.extend(encoder.encode(texts[start:start + PROGRESS_ROWS]))
        done, elapsed = len(vectors), time.perf_counter() - t0
        rate = done / elapsed if elapsed else 0.0
        log_event(logger, "lc214_embed_progress", f"embedding {done}/{len(texts)} · {rate:.1f} chunks/s",
                  done=done, total=len(texts), rows_per_s=round(rate, 1))
    if any(len(v) != encoder.dim for v in vectors):
        raise EmbedError("embedding_dim_mismatch", f"{encoder.model} returned vectors of a size other than {encoder.dim}")
    elapsed = time.perf_counter() - t0
    if step is not None:
        step.set(embed_s=round(elapsed, 2), embed_rows_per_s=round(len(texts) / elapsed, 1) if elapsed else None,
                 embedding_dim=encoder.dim)
    return frame.with_columns(
        pl.Series("embedding", ["[" + ",".join(f"{x:.7g}" for x in v) + "]" for v in vectors], dtype=pl.Utf8),
        pl.lit(encoder.model).alias("embedding_model"),
    )


# --------------------------------------------------------------------------- the sync (caller's transaction)
def _copy(cur, table: str, frame: pl.DataFrame, cols) -> None:
    buf = io.StringIO()
    frame.select(cols).write_csv(buf)
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", buf)


def apply_sync(cur, desired: pl.DataFrame, plan: SyncPlan, embedded: pl.DataFrame, dim_column: int | None,
               dim_model: int | None) -> dict:
    """Make ai.lc214_chunk equal to `desired`. Runs inside the caller's transaction: all or nothing."""
    cur.execute(f"DROP TABLE IF EXISTS pg_temp.stage_{TABLE}")      # a second sync in the same transaction
    cur.execute(f"CREATE TEMP TABLE stage_{TABLE} ({', '.join(f'{c} text' for c in COLS)}) ON COMMIT DROP")
    _copy(cur, f"stage_{TABLE}", desired, COLS)

    if plan.mode == "full":
        cur.execute(f"DELETE FROM {TARGET}")                      # not TRUNCATE: readers keep the old version (MVCC)
        deleted = cur.rowcount
        if dim_model and dim_column != dim_model:                 # e.g. a 768-dim model: the table is empty here
            cur.execute(f"ALTER TABLE {TARGET} ALTER COLUMN embedding TYPE vector({int(dim_model)}) USING NULL")
            log_event(logger, "lc214_vector_resized", f"embedding column: vector({dim_column}) -> vector({dim_model})")
    else:
        deleted = 0
        if plan.stale_ids:
            cur.execute(f"DELETE FROM {TARGET} WHERE chunk_id = ANY(%s)", (sorted(plan.stale_ids),))
            deleted = cur.rowcount

    if embedded.height:
        _copy(cur, TARGET, embedded, (*COLS, "embedding", "embedding_model"))

    casts = {"parte": "integer", "n_tokens": "integer", "dispositivo_ids": "jsonb", "alterado_por": "jsonb"}
    src = {c: f"s.{c}::{casts[c]}" if c in casts else f"s.{c}" for c in META}
    cur.execute(f"UPDATE {TARGET} t SET {', '.join(f'{c} = {src[c]}' for c in META)}, loaded_at = now() "
                f"FROM stage_{TABLE} s WHERE t.chunk_id = s.chunk_id "
                f"AND ({', '.join(f't.{c}' for c in META)}) IS DISTINCT FROM ({', '.join(src[c] for c in META)})")
    restamped = cur.rowcount                       # rows just inserted equal their stage row: never counted here
    return {"rows": desired.height, "inserted": embedded.height, "deleted": deleted, "restamped": restamped}


# --------------------------------------------------------------------------- the step
def run(lake: Lake, connect, encoder_factory: Callable[[], Encoder] = load_encoder, model: str | None = None,
        force_full: bool = False, allow_shrink: bool = False) -> int:
    step = current_step()
    sha, chunks = read_chunks(lake)
    desired = prepare(chunks)
    model = model or current_model()

    conn = connect()
    try:
        with conn.cursor() as cur:
            existing, models, dim_column = store_state(cur)
        conn.rollback()                                            # end the read-only snapshot before embedding
        plan = plan_sync(set(desired["chunk_id"].to_list()), existing, model=model, stored_models=models,
                         force_full=force_full)
        log_event(logger, "lc214_embed_plan", f"{plan.mode}: {plan.reason} | new={len(plan.new_ids)} "
                  f"stale={len(plan.stale_ids)} kept={plan.kept}", mode=plan.mode, new=len(plan.new_ids),
                  stale=len(plan.stale_ids), kept=plan.kept, model=model)
        guard_shrink(plan, len(existing), allow=allow_shrink)

        embedded, dim_model = desired.head(0), None
        if plan.new_ids:
            t0 = time.perf_counter()
            encoder = encoder_factory()
            step.set(model_load_s=round(time.perf_counter() - t0, 2))
            if encoder.model != model:
                raise EmbedError("embedding_model_mismatch", f"planned for {model}, loaded {encoder.model}")
            dim_model = encoder.dim
            embedded = embed(desired.filter(pl.col("chunk_id").is_in(sorted(plan.new_ids))), encoder, step)

        with conn:                                                 # one transaction: commit on success, else rollback
            with conn.cursor() as cur:
                result = apply_sync(cur, desired, plan, embedded, dim_column, dim_model)
    finally:
        conn.close()

    step.set(sync_mode=plan.mode, sync_reason=plan.reason, model=model)
    step.add(rows_in=result["rows"], rows_out=result["inserted"], chunks_inserted=result["inserted"],
             chunks_deleted=result["deleted"], chunks_restamped=result["restamped"])
    summary = (f"version {sha[:12]}: {result['rows']} chunks, +{result['inserted']} embedded, -{result['deleted']} "
               f"removed, {result['restamped']} restamped ({plan.mode}, {model})")
    if not (result["inserted"] or result["deleted"] or result["restamped"]):
        log_event(logger, "lc214_embed_up_to_date", f"vector store already matches {summary}", sha256=sha)
        return EXIT_SKIPPED
    log_event(logger, "lc214_embed_done", summary, sha256=sha, mode=plan.mode, model=model, **{
        f"chunks_{k}": v for k, v in result.items() if k != "rows"})
    return EXIT_OK


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lake", default=None, help="'s3' (default, LC214_LAKE) or file:///dir")
    ap.add_argument("--full-refresh", action="store_true", help="re-embed every chunk (867 chunks: ~1-2 min of CPU)")
    ap.add_argument("--allow-shrink", action="store_true",
                    help=f"accept the store losing more than {MAX_DELETE_PCT}% of its chunks net")
    args = ap.parse_args(argv)
    try:
        from pipelines.commons.dw_client import get_pg_connection, test_pg_connection
        test_pg_connection()
        return run(lake_from_env(args.lake), get_pg_connection, force_full=args.full_refresh,
                   allow_shrink=args.allow_shrink)
    except EmbedError as exc:
        log_event(logger, exc.event, str(exc), level=40, **exc.fields)
        return EXIT_FAILED
    except LakeError as exc:
        log_event(logger, exc.event, str(exc), level=40)
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
