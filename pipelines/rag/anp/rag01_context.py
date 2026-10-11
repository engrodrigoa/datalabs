"""
Gold (ANP) -> semantic chunks -> embeddings -> ai.rag_context_anp (pgvector), INCREMENTAL.

The vector store is a projection of the gold layer, kept in sync by set difference on a
deterministic chunk_id (see pipelines/commons/ai/context_sync.py):
  * new chunks   -> embedded and inserted (only these pay the embedding cost)
  * stale chunks -> deleted (fact removed from gold, or its text changed)
  * nothing new  -> exit 99 (task SKIPPED): the model is not even loaded
A full rebuild happens only when it must: empty store, embedding model changed, or
`--full-refresh` / RAG_FULL_REFRESH=1. Re-running converges to the same state (idempotent).

Why: embedding is CPU-bound (~6-7 rows/s with e5-base on the lab machine). Re-embedding the
whole UF on every gold publication (~32k rows, ~1h40) did not fit the 2h timeout; a weekly
delta is a few thousand rows.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime
from warnings import filterwarnings

import polars as pl
from sqlalchemy import text

filterwarnings("ignore")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.ai.context_sync import build_chunks, plan_sync
from pipelines.commons.ai.embeddings import MODEL_NAME, encode_passages, load_model, model_dim
from pipelines.commons.dw_client import get_sqla_engine, test_pg_connection
from pipelines.commons.env_loader import CONSTRING
from pipelines.commons.logger import get_logger, log_event
from pipelines.commons.pg_copy import copy_dataframe
from pipelines.observability import current_step

logger = get_logger("rag01_context")

SCHEMA = "ai"
TABELA = "rag_context_anp"
INDEX_HNSW = "ix_rag_context_anp_hnsw"
UF = os.getenv("RAG_UF", "GO").upper()
LIMITE_EXTRACAO = int(os.getenv("RAG_MAX_ROWS", "0"))  # 0 = no limit
BATCH_SIZE = int(os.getenv("RAG_EMBED_BATCH", "64"))
# embedding runs in blocks of this many rows, with one progress event per block (rows, rate, ETA)
PROGRESS_ROWS = int(os.getenv("RAG_EMBED_PROGRESS_ROWS", "1000"))
# above this many new rows, drop the HNSW index and rebuild it once (bulk pattern);
# below it, inserting into the live index is cheaper than rebuilding it
INDEX_REBUILD_MIN_ROWS = int(os.getenv("RAG_INDEX_REBUILD_MIN_ROWS", "5000"))


def ler_gold() -> pl.DataFrame:
    limit = f"LIMIT {LIMITE_EXTRACAO}" if LIMITE_EXTRACAO > 0 else ""
    query = f"""
        SELECT a.id_fato, b.cnpj, b.revenda, b.bandeira, b.municipio, b.estado_sigla, b.bairro, b.cep,
               c.produto, a.valor_venda, a.data_coleta
        FROM gold.ft_anp_combustiveis a
        JOIN gold.dm_postos b   ON b.id_posto_sk = a.id_posto_sk
        JOIN gold.dm_produtos c ON c.id_produto_sk = a.id_produto_sk
        WHERE b.estado_sigla = '{UF}'
        ORDER BY a.id_fato
        {limit}
    """
    return pl.read_database_uri(query, uri=CONSTRING)


def estado_atual(engine) -> tuple[set, set, int | None]:
    """chunk_ids and models already in the store, and the declared vector size (pgvector typmod)."""
    with engine.connect() as conn:
        ids = {r[0] for r in conn.execute(text(f"SELECT chunk_id FROM {SCHEMA}.{TABELA}"))}
        modelos = {r[0] for r in conn.execute(
            text(f"SELECT DISTINCT embedding_model FROM {SCHEMA}.{TABELA} WHERE embedding_model IS NOT NULL"))}
        dim = conn.execute(text(
            "SELECT NULLIF(atttypmod, -1) FROM pg_attribute "
            "WHERE attrelid = CAST(:rel AS regclass) AND attname = 'embedding'"),
            {"rel": f"{SCHEMA}.{TABELA}"}).scalar()
    return ids, modelos, dim


def vetorizar(modelo, df: pl.DataFrame, step) -> pl.DataFrame:
    """Embeds in blocks of PROGRESS_ROWS and emits `rag_embed_progress` after each block, so a long
    run is never a black box: the Airflow log, Loki and Grafana show done/total, rows/s and ETA."""
    textos = df["chunk_text"].to_list()
    total = len(textos)
    vetores: list[list[float]] = []
    t1 = time.perf_counter()
    for inicio in range(0, total, PROGRESS_ROWS):
        bloco = encode_passages(modelo, textos[inicio:inicio + PROGRESS_ROWS], batch_size=BATCH_SIZE)
        vetores.extend(bloco.tolist())
        feitos = len(vetores)
        decorrido = time.perf_counter() - t1
        taxa = feitos / decorrido if decorrido else 0.0
        eta_s = (total - feitos) / taxa if taxa else None
        log_event(logger, "rag_embed_progress",
                  f"embedding {feitos:,}/{total:,} ({100 * feitos / total:.0f}%) · {taxa:.1f} linhas/s"
                  + (f" · ETA {eta_s / 60:.0f} min" if eta_s else ""),
                  done=feitos, total=total, pct=round(100 * feitos / total, 1), rows_per_s=round(taxa, 1),
                  eta_s=round(eta_s) if eta_s is not None else None)
    embed_s = time.perf_counter() - t1
    step.set(embedding_dim=len(vetores[0]) if vetores else None, embed_s=round(embed_s, 2),
             embed_rows_per_s=round(total / embed_s, 1) if embed_s else None)
    return df.with_columns(
        pl.Series("embedding", [json.dumps(v) for v in vetores], dtype=pl.Utf8),
        pl.lit(MODEL_NAME).alias("embedding_model"),
    ).select(["chunk_id", "source_id", "chunk_text", "metadata", "embedding", "embedding_model"])


def recriar_indice(engine, step) -> None:
    t = time.perf_counter()
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL maintenance_work_mem = '256MB'"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {INDEX_HNSW} ON {SCHEMA}.{TABELA} "
                          "USING hnsw (embedding vector_cosine_ops)"))
    step.set(index_build_s=round(time.perf_counter() - t, 2))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--full-refresh", action="store_true", help="re-embed everything (also: RAG_FULL_REFRESH=1)")
    args = ap.parse_args(argv)
    force_full = args.full_refresh or os.getenv("RAG_FULL_REFRESH", "0") == "1"

    step = current_step()
    test_pg_connection()
    engine = get_sqla_engine()

    log_event(logger, "rag_build_started", f"vector store sync -> {SCHEMA}.{TABELA} (UF={UF}, model={MODEL_NAME})",
              uf=UF, model=MODEL_NAME)

    gold = ler_gold()
    step.add(rows_in=gold.height)
    if gold.height == 0:
        log_event(logger, "no_new_data", f"no gold rows for UF={UF}", level=30)
        return 99

    desejado = build_chunks(gold)
    step.add(rows_rejected=gold.height - desejado.height)
    if desejado.is_empty():
        log_event(logger, "no_new_data", f"gold rows for UF={UF} produced no valid chunk", level=30)
        return 99
    existentes, modelos, dim_coluna = estado_atual(engine)

    plano = plan_sync(set(desejado["chunk_id"].to_list()), existentes, model=MODEL_NAME,
                      stored_models=modelos, force_full=force_full)
    step.set(sync_mode=plano.mode, sync_reason=plano.reason, chunks_new=len(plano.new_ids),
             chunks_stale=len(plano.stale_ids), chunks_kept=plano.kept, model=MODEL_NAME, uf=UF)
    log_event(logger, "rag_sync_plan",
              f"{plano.mode}: {plano.reason} | new={len(plano.new_ids):,} stale={len(plano.stale_ids):,} "
              f"kept={plano.kept:,}", mode=plano.mode, new=len(plano.new_ids), stale=len(plano.stale_ids),
              kept=plano.kept)

    if plano.mode == "incremental" and not plano.new_ids and not plano.stale_ids:
        log_event(logger, "no_new_data", "vector store already in sync with gold: nothing to embed")
        return 99

    novos = desejado.filter(pl.col("chunk_id").is_in(list(plano.new_ids)))
    modelo = None
    if novos.height:
        t0 = time.perf_counter()
        modelo = load_model()
        step.set(model_load_s=round(time.perf_counter() - t0, 2))
        novos = vetorizar(modelo, novos, step)

    if plano.mode == "full":
        # Bulk-load pattern for pgvector: build the HNSW index once after the load, not row by row.
        dim_modelo = model_dim(modelo)
        with engine.begin() as conn:
            conn.execute(text(f"DROP INDEX IF EXISTS {SCHEMA}.{INDEX_HNSW}"))
            conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{TABELA}"))
            if dim_coluna != dim_modelo:   # e.g. e5-base (768) -> e5-small (384); the table is empty here
                conn.execute(text(f"ALTER TABLE {SCHEMA}.{TABELA} ALTER COLUMN embedding "
                                  f"TYPE vector({dim_modelo}) USING NULL"))
                log_event(logger, "rag_vector_resized",
                          f"embedding column: vector({dim_coluna}) -> vector({dim_modelo})")
        t2 = time.perf_counter()
        escritos = copy_dataframe(novos, engine, SCHEMA, TABELA)
        step.set(copy_s=round(time.perf_counter() - t2, 2))
        recriar_indice(engine, step)
    else:
        if plano.stale_ids:
            with engine.begin() as conn:
                apagados = conn.execute(text(f"DELETE FROM {SCHEMA}.{TABELA} WHERE chunk_id = ANY(:ids)"),
                                        {"ids": list(plano.stale_ids)}).rowcount
            step.set(rows_deleted=apagados)
        escritos = 0
        if novos.height:
            rebuild = novos.height >= INDEX_REBUILD_MIN_ROWS
            if rebuild:
                with engine.begin() as conn:
                    conn.execute(text(f"DROP INDEX IF EXISTS {SCHEMA}.{INDEX_HNSW}"))
            t2 = time.perf_counter()
            escritos = copy_dataframe(novos, engine, SCHEMA, TABELA)
            step.set(copy_s=round(time.perf_counter() - t2, 2))
            if rebuild:
                recriar_indice(engine, step)

    step.add(rows_out=escritos)
    log_event(logger, "rag_build_finished",
              f"{plano.mode}: {escritos:,} vectors inserted, {len(plano.stale_ids):,} removed",
              mode=plano.mode, vectors=escritos, removed=len(plano.stale_ids),
              finished_at=datetime.now().isoformat())
    return 0


if __name__ == "__main__":
    sys.exit(main())
