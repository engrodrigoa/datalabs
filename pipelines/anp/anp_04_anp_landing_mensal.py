import os
import sys
import gc
import io
import glob
from datetime import datetime
from warnings import filterwarnings
import polars as pl
from sqlalchemy import text
from pathlib import Path

filterwarnings("ignore")

# ==========================================
# COMMONS UTILS SETUP
# ==========================================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.env_loader import validate_env, DIR_ANP_LANDING_MONTH
from pipelines.commons.dw_client import get_sqla_engine, test_pg_connection
from pipelines.commons.logger import get_logger
from pipelines.commons.anp_metrics import log_batch_metrics

logger = get_logger("anp_04_anp_landing_mensal")

# ==========================================
# TARGETS & CONFIG
# ==========================================
SCHEMA = "bronze"
TABELA = "anp_landing_mensal"



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
    validate_env({"DIR_ANP_LANDING_MONTH": DIR_ANP_LANDING_MONTH})
    test_pg_connection()

    engine = get_sqla_engine()
    dir_input = Path(DIR_ANP_LANDING_MONTH)

    logger.info("=" * 60)
    logger.info(f"starting ingestion. loading monthly files from {dir_input.as_posix()} : {datetime.now()}")
    logger.info("=" * 60)

    padrao_busca = dir_input / "**" / "*.csv"
    todos_arquivos = glob.glob(str(padrao_busca), recursive=True)

    arquivos_pendentes = sorted([
        f for f in todos_arquivos 
        if not os.path.basename(f).startswith("OK_")
    ])

    if not arquivos_pendentes:
        logger.warning("no pending files found to process")
        sys.exit(0)

    pending_count = len(arquivos_pendentes)
    logger.info(f"difference detected. found {pending_count} pending file(s)")

    timestamp_ingestao = datetime.now()
    processed_count = 0

    
    dfs_lote = []
    for caminho_completo in arquivos_pendentes:
        nome_base = os.path.basename(caminho_completo)
        logger.info(f"reading: {nome_base}")

        try:
            df_bruto = pl.read_csv(
                caminho_completo,
                separator=";",
                encoding="utf8",
                infer_schema_length=0,
                ignore_errors=True
            )

            novas_colunas = [c.lower().replace(" - ", "_").replace(" ", "_") for c in df_bruto.columns]
            df_padronizado = df_bruto.rename(dict(zip(df_bruto.columns, novas_colunas)))

            df_padronizado = df_padronizado.with_columns(
                pl.lit(nome_base).alias("arquivo"),
                pl.lit(timestamp_ingestao).alias("ingestion_timestamp")
            )

            dfs_lote.append((caminho_completo, nome_base, df_padronizado))
        except Exception as e:
            logger.error(f"FAILED TO READ {nome_base}: {e}")

    if not dfs_lote:
        logger.error("no valid dataframes loaded into memory")
        sys.exit(1)

    
    # loop through each dataframe in memory and process it 
    for caminho_completo, nome_base, df_padronizado in dfs_lote:
        logger.info(f"processing load for: {nome_base}")

        try:
            with engine.begin() as conn:
                registros_deletados = conn.execute(
                    text(f"DELETE FROM {SCHEMA}.{TABELA} WHERE arquivo = :arquivo"),
                    {"arquivo": nome_base}
                ).rowcount
                if registros_deletados > 0:
                    logger.info(f"removed {registros_deletados} old record(s)")

            copy_to_postgres(df_padronizado, engine, SCHEMA, TABELA)
            logger.info(f">>> successfully loaded into {SCHEMA}.{TABELA}")

            novo_caminho = os.path.join(os.path.dirname(caminho_completo), f"OK_{nome_base}")
            os.rename(caminho_completo, novo_caminho)
            logger.info(f"renamed file to OK_{nome_base}")

            processed_count += 1

        except Exception as e:
            logger.error(f"FAILED TO PROCESS LOAD FOR {nome_base}: {e}")

        finally:
            gc.collect()

    # batch metrics logging
    df_lote_completo = pl.concat([item[2] for item in dfs_lote])
    log_batch_metrics(df_lote_completo, total_files=len(dfs_lote))

    logger.info("=" * 60)
    logger.info("pipeline run complete")
    logger.info(f"total processed: {processed_count}/{pending_count}")
    logger.info("=" * 60)
    sys.exit(0)


if __name__ == "__main__":
    main()