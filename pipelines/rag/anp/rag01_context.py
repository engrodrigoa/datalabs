import os
import sys
import gc
import io
from datetime import datetime
from warnings import filterwarnings

import polars as pl
from sqlalchemy import text
from pathlib import Path
import json

from sentence_transformers import SentenceTransformer

filterwarnings("ignore")

# ==========================================
# COMMONS UTILS SETUP
# ==========================================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.env_loader import validate_env, CONSTRING
from pipelines.commons.dw_client import get_sqla_engine, test_pg_connection
from pipelines.commons.logger import get_logger

logger = get_logger("rag01_context")

# ==========================================
# TARGETS & CONFIG
# ==========================================
SCHEMA = "ai"
TABELA = "rag_context_anp"
MODELO_NOME = "intfloat/multilingual-e5-base"
LIMITE_EXTRACAO = 100  # Manter 100 para o MVP local

# ==========================================
# FUNCTIONS
# ==========================================
def copy_to_postgres(df: pl.DataFrame, engine_db, schema: str, tabela: str):
    """Realiza o bulk insert utilizando o comando COPY do PostgreSQL."""
    buffer = io.StringIO()
    df.write_csv(buffer)
    buffer.seek(0)
    colunas = ", ".join(df.columns)

    sql = f"""
        COPY {schema}.{tabela}
        ({colunas})
        FROM STDIN
        WITH (
            FORMAT CSV,
            HEADER TRUE
        )
    """
    conn = engine_db.raw_connection()
    try:
        cur = conn.cursor()
        cur.copy_expert(sql, buffer)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# ==========================================
# PIPELINE
# ==========================================
def main():

    test_pg_connection()
    engine = get_sqla_engine()

    logger.info("=" * 60)
    logger.info(f"[!] starting vectorization pipeline. Target: {SCHEMA}.{TABELA} : {datetime.now()}")
    logger.info("=" * 60)

    # 1. Carregamento do Modelo IA
    logger.info(f"loading AI model: {MODELO_NOME}")
    modelo_ia = SentenceTransformer(MODELO_NOME)

    # 2. Extração
    logger.info(f"extracting data from gold layer (LIMIT {LIMITE_EXTRACAO})")
    query_extracao = f"""
        SELECT 
            b.cnpj, b.revenda, b.bandeira, b.municipio, b.estado_sigla, b.bairro, b.cep, 
            c.produto, a.valor_venda, a.data_coleta  
        FROM gold.ft_anp_combustiveis a 
        LEFT JOIN gold.dm_postos b ON b.id_posto_sk = a.id_posto_sk 
        LEFT JOIN gold.dm_produtos c ON c.id_produto_sk = a.id_produto_sk 
        WHERE 1=1
        AND b.estado_sigla = 'GO' 
        --LIMIT {LIMITE_EXTRACAO}
    """
    
    try:
        df = pl.read_database_uri(query_extracao, uri=CONSTRING)
    except Exception as e:
        logger.error(f"FAILED TO EXTRACT DATA: {e}")
        sys.exit(1)

    if df.height == 0:
        logger.warning("no records found in gold layer to process.")
        sys.exit(0)

    # 3. Transformação
    logger.info("creating semantic chunks and metadata JSON")
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
            pl.lit(" na data "), pl.col("data_coleta").cast(pl.Utf8), pl.lit(".")
        ]).alias("chunk_text")
    )

    df = df.with_columns(
        pl.struct(["cnpj", "cep", "estado_sigla", "municipio"])
        .map_elements(lambda x: json.dumps(x), return_dtype=pl.Utf8)
        .alias("metadata")
    )

    # vectorizing

    logger.info(f">>> creating embeddings for {df.height} rows")
    
    textos_para_vetorizar = [f"passage: {t}" for t in df["chunk_text"].to_list()]

    embeddings_list = modelo_ia.encode(textos_para_vetorizar, normalize_embeddings=True).tolist()

    embeddings_str = [json.dumps(vetor) for vetor in embeddings_list]
    
    df = df.with_columns(
        pl.Series(name="embedding", values=embeddings_str, dtype=pl.Utf8)
    )

    # dw deploy
    df_load = df.select(["chunk_text", "metadata", "embedding"])

    logger.info(f">>> processing load into {SCHEMA}.{TABELA}")
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{TABELA}"))
            logger.info("table truncated for full refresh.")

        copy_to_postgres(df_load, engine, SCHEMA, TABELA)
        logger.info(f">>> successfully loaded vectors into {SCHEMA}.{TABELA}")
        
    except Exception as e:
        logger.error(f"FAILED TO PROCESS LOAD: {e}")
        sys.exit(1)
    finally:
        gc.collect()

    logger.info("=" * 60)
    logger.info("[!] task run complete")
    logger.info(f"total vectors processed: {df.height}")
    logger.info("=" * 60)
    sys.exit(0)

if __name__ == "__main__":
    main()