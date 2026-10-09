"""
Gold (ANP) -> semantic chunks -> embeddings (multilingual-e5-base) -> ai.rag_context_anp (pgvector).

Full refresh by design (small, one UF), but every chunk keeps lineage to the gold fact
(source_id = id_fato) and a deterministic chunk_id, so an incremental strategy can be
added without changing the table. Volumes, model and timings are reported to the step run.
"""
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

from pipelines.commons.dw_client import get_sqla_engine, test_pg_connection
from pipelines.commons.env_loader import CONSTRING
from pipelines.commons.logger import get_logger, log_event
from pipelines.commons.pg_copy import copy_dataframe
from pipelines.observability import current_step

logger = get_logger("rag01_context")

SCHEMA = "ai"
TABELA = "rag_context_anp"
INDEX_HNSW = "ix_rag_context_anp_hnsw"
MODELO_NOME = os.getenv("RAG_EMBEDDING_MODEL", "intfloat/multilingual-e5-base")
UF = os.getenv("RAG_UF", "GO").upper()
LIMITE_EXTRACAO = int(os.getenv("RAG_MAX_ROWS", "0"))  # 0 = no limit
BATCH_SIZE = int(os.getenv("RAG_EMBED_BATCH", "64"))


def main():
    from sentence_transformers import SentenceTransformer  # heavy import: only when the step runs

    step = current_step()
    test_pg_connection()
    engine = get_sqla_engine()

    log_event(logger, "rag_build_started", f"vectorization -> {SCHEMA}.{TABELA} (UF={UF}, model={MODELO_NOME})",
              uf=UF, model=MODELO_NOME)

    t0 = time.perf_counter()
    modelo_ia = SentenceTransformer(MODELO_NOME)
    step.set(model=MODELO_NOME, model_load_s=round(time.perf_counter() - t0, 2), uf=UF)

    limit = f"LIMIT {LIMITE_EXTRACAO}" if LIMITE_EXTRACAO > 0 else ""
    query = f"""
        SELECT a.id_fato, b.cnpj, b.revenda, b.bandeira, b.municipio, b.estado_sigla, b.bairro, b.cep,
               c.produto, a.valor_venda, a.data_coleta
        FROM gold.ft_anp_combustiveis a
        JOIN gold.dm_postos b   ON b.id_posto_sk = a.id_posto_sk
        JOIN gold.dm_produtos c ON c.id_produto_sk = a.id_produto_sk
        WHERE b.estado_sigla = '{UF}'
        {limit}
    """
    df = pl.read_database_uri(query, uri=CONSTRING)
    step.add(rows_in=df.height)
    if df.height == 0:
        log_event(logger, "no_new_data", f"no gold rows for UF={UF}", level=30)
        return 99

    df = df.with_columns(
        pl.concat_str([
            pl.lit("O posto '"), pl.col("revenda"),
            pl.lit("' da bandeira "), pl.col("bandeira"),
            pl.lit(", localizado em "), pl.col("municipio"), pl.lit(" - "), pl.col("estado_sigla"),
            pl.lit(" (Bairro: "), pl.col("bairro"),
            pl.lit(", CEP: "), pl.col("cep"),
            pl.lit(", CNPJ: "), pl.col("cnpj"),
            pl.lit("), vendeu o produto "), pl.col("produto"),
            pl.lit(" pelo valor de R$ "), pl.col("valor_venda").cast(pl.Utf8),
            pl.lit(" na data "), pl.col("data_coleta").cast(pl.Utf8), pl.lit("."),
        ], ignore_nulls=True).alias("chunk_text"),
        pl.col("id_fato").cast(pl.Utf8).alias("source_id"),
    )
    empty = df.filter(pl.col("chunk_text").is_null() | (pl.col("chunk_text") == "")).height
    df = df.filter(pl.col("chunk_text").is_not_null() & (pl.col("chunk_text") != ""))
    step.add(rows_rejected=empty)

    df = df.with_columns(
        pl.col("chunk_text").hash(seed=42).cast(pl.Utf8).alias("_h"),
        pl.struct(["cnpj", "cep", "estado_sigla", "municipio"])
          .map_elements(lambda x: json.dumps(x, ensure_ascii=False), return_dtype=pl.Utf8).alias("metadata"),
    ).with_columns(
        (pl.col("source_id") + pl.lit(":") + pl.col("_h")).alias("chunk_id")
    ).unique(subset=["chunk_id"])

    t1 = time.perf_counter()
    vectors = modelo_ia.encode(
        [f"passage: {t}" for t in df["chunk_text"].to_list()],
        normalize_embeddings=True, batch_size=BATCH_SIZE, show_progress_bar=False,
    )
    embed_s = time.perf_counter() - t1
    step.set(embedding_dim=int(vectors.shape[1]), embed_s=round(embed_s, 2),
             embed_rows_per_s=round(df.height / embed_s, 1) if embed_s else None)

    df_load = df.with_columns(
        pl.Series("embedding", [json.dumps(v) for v in vectors.tolist()], dtype=pl.Utf8),
        pl.lit(MODELO_NOME).alias("embedding_model"),
    ).select(["chunk_id", "source_id", "chunk_text", "metadata", "embedding", "embedding_model"])

    # Bulk-load pattern for pgvector: inserting into a live HNSW index is row-by-row and slow;
    # dropping it, loading, and building it once afterwards is much faster.
    with engine.begin() as conn:
        conn.execute(text(f"DROP INDEX IF EXISTS {SCHEMA}.{INDEX_HNSW}"))
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{TABELA}"))
    t2 = time.perf_counter()
    written = copy_dataframe(df_load, engine, SCHEMA, TABELA)
    load_s = time.perf_counter() - t2

    t3 = time.perf_counter()
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL maintenance_work_mem = '256MB'"))
        conn.execute(text(f"CREATE INDEX {INDEX_HNSW} ON {SCHEMA}.{TABELA} USING hnsw (embedding vector_cosine_ops)"))
    step.add(rows_out=written)
    step.set(copy_s=round(load_s, 2), index_build_s=round(time.perf_counter() - t3, 2))

    log_event(logger, "rag_build_finished", f"{written:,} vectors loaded in {embed_s:.1f}s of embedding",
              vectors=written, embed_s=round(embed_s, 2), finished_at=datetime.now().isoformat())
    return 0


if __name__ == "__main__":
    sys.exit(main())
