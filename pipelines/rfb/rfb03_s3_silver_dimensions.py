import os
import sys
import gc
import polars as pl
from warnings import filterwarnings
from botocore.exceptions import ClientError

filterwarnings("ignore")

os.environ["POLARS_MAX_THREADS"] = "1"
os.environ["RAYON_NUM_THREADS"] = "1"

#=============================================================================================================
#===================================
# env loader - connection setup
#===================================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.env_loader import (
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, validate_env,
)
from pipelines.commons.s3_client import get_s3_client
from pipelines.commons.logger import get_logger
from pipelines.commons.rfb_silver_contract import adicionar_colunas_auditoria, validar_schema_basico

logger = get_logger("rfb_silver_dims")

validate_env({
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
})
#=============================================================================================================

REFERENCIA = "2026-08"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
PASTA_TMP = "/mnt/datasource/tmp_silver_auxiliares"

FORCAR_REPROCESSAMENTO = os.getenv("RFB_FORCAR_REPROCESSAMENTO", "0") == "1"

os.makedirs(PASTA_TMP, exist_ok=True)

def arquivo_ja_existe_no_silver(s3_client, chave_destino):
    try:
        s3_client.head_object(Bucket=BUCKET_SILVER, Key=chave_destino)
        return True
    except ClientError as e:
        codigo = e.response.get("Error", {}).get("Code", "")
        if codigo in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def processar_tabela_codigo_descricao(s3_client, nome_entidade):
    ref_partition = REFERENCIA.replace('-', '')
    prefixo_bronze = f"rfb/{nome_entidade}/ref_month={ref_partition}/"
    res = s3_client.list_objects_v2(Bucket=BUCKET_BRONZE, Prefix=prefixo_bronze)
    
    arquivos = [
        obj["Key"] for obj in res.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    processed_count = 0
    skipped_count = 0
    for chave_s3 in arquivos:
        nome_arq = os.path.basename(chave_s3)
        path_local_bronze = os.path.join(PASTA_TMP, f"bronze_{nome_arq}")
        path_local_silver = os.path.join(PASTA_TMP, f"silver_{nome_arq}")
        chave_destino = f"rfb/{nome_entidade}/ref_month={ref_partition}/{nome_arq}"

        if not FORCAR_REPROCESSAMENTO and arquivo_ja_existe_no_silver(s3_client, chave_destino):
            logger.info(f"Skipping dimension {nome_entidade} ({nome_arq}): already exists in s3://{BUCKET_SILVER}/{chave_destino}")
            skipped_count += 1
            continue

        logger.info(f"Processing dimension {nome_entidade} ({nome_arq})...")
        s3_client.download_file(BUCKET_BRONZE, chave_s3, path_local_bronze)

        df = pl.read_parquet(path_local_bronze).rename({"f0": "codigo", "f1": "descricao"})

        if nome_entidade.lower() == "cnaes":
            df_silver = df.with_columns([
                pl.col("codigo").str.strip_chars(),
                pl.col("descricao").str.strip_chars().replace("", None)
            ])
        else:
            df_silver = df.with_columns([
                pl.col("codigo").cast(pl.String).str.strip_chars().cast(pl.Int32, strict=False),
                pl.col("descricao").str.strip_chars().replace("", None)
            ])

        df_silver = adicionar_colunas_auditoria(df_silver, nome_arq)
        
        schema_esperado = {
            "codigo": pl.String if nome_entidade.lower() == "cnaes" else pl.Int32,
            "descricao": pl.String,
            "referencia_mes": pl.Int32,
            "_source_file": pl.String,
            "_inserted_at": pl.Datetime("us", "UTC")
        }
        
        df_silver = df_silver.select(list(schema_esperado.keys()))
        validar_schema_basico(df_silver, schema_esperado)

        logger.info(f"deploying to s3://{BUCKET_SILVER}/{chave_destino}")
        df_silver.write_parquet(path_local_silver, compression="snappy")
        
        s3_client.upload_file(path_local_silver, BUCKET_SILVER, chave_destino)

        del df
        del df_silver
        gc.collect()

        os.remove(path_local_bronze)
        os.remove(path_local_silver)

        logger.info(f"processing done and deployed to {nome_entidade} to s3://{BUCKET_SILVER}/{chave_destino}")
        processed_count += 1

    return processed_count, skipped_count

def processar_simples(s3_client, tamanho_lote=500_000):
    import pyarrow.parquet as pq

    nome_entidade = "simples"
    ref_partition = REFERENCIA.replace('-', '')
    prefixo_bronze = f"rfb/{nome_entidade}/ref_month={ref_partition}/"
    res = s3_client.list_objects_v2(Bucket=BUCKET_BRONZE, Prefix=prefixo_bronze)

    arquivos = [
        obj["Key"] for obj in res.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    mapeamento_simples = {
        "f0": "cnpj_basico",
        "f1": "opcao_simples",
        "f2": "data_opcao_simples",
        "f3": "data_exclusao_simples",
        "f4": "opcao_mei",
        "f5": "data_opcao_mei",
        "f6": "data_exclusao_mei"
    }

    processed_count = 0
    skipped_count = 0
    for chave_s3 in arquivos:
        nome_arq = os.path.basename(chave_s3)
        path_local_bronze = os.path.join(PASTA_TMP, f"bronze_{nome_arq}")
        path_local_silver = os.path.join(PASTA_TMP, f"silver_{nome_arq}")
        chave_destino = f"rfb/{nome_entidade}/ref_month={ref_partition}/{nome_arq}"

        if not FORCAR_REPROCESSAMENTO and arquivo_ja_existe_no_silver(s3_client, chave_destino):
            logger.info(f"skipping dimension simples ({nome_arq}): already exists in s3://{BUCKET_SILVER}/{chave_destino}")
            skipped_count += 1
            continue

        logger.info(f"processing dimension in batched mode ({nome_arq}, batch size {tamanho_lote})...")
        s3_client.download_file(BUCKET_BRONZE, chave_s3, path_local_bronze)

        parquet_file = pq.ParquetFile(path_local_bronze)
        total_linhas = parquet_file.metadata.num_rows
        logger.info(f"Total rows in {nome_arq}: {total_linhas}")

        writer = None
        linhas_processadas = 0

        try:
            for batch in parquet_file.iter_batches(batch_size=tamanho_lote):
                df_lote = pl.from_arrow(batch).rename(mapeamento_simples)

                df_lote = df_lote.with_columns([
                    pl.col("cnpj_basico").str.strip_chars().str.zfill(8),
                    pl.col("opcao_simples").str.strip_chars().replace("", None),
                    pl.col("opcao_mei").str.strip_chars().replace("", None),
                    pl.col("data_opcao_simples").str.to_date(format="%Y%m%d", strict=False),
                    pl.col("data_exclusao_simples").str.to_date(format="%Y%m%d", strict=False),
                    pl.col("data_opcao_mei").str.to_date(format="%Y%m%d", strict=False),
                    pl.col("data_exclusao_mei").str.to_date(format="%Y%m%d", strict=False),
                ])
                
                df_lote = adicionar_colunas_auditoria(df_lote, nome_arq)
                
                schema_esperado = {
                    "cnpj_basico": pl.String,
                    "opcao_simples": pl.String,
                    "data_opcao_simples": pl.Date,
                    "data_exclusao_simples": pl.Date,
                    "opcao_mei": pl.String,
                    "data_opcao_mei": pl.Date,
                    "data_exclusao_mei": pl.Date,
                    "referencia_mes": pl.Int32,
                    "_source_file": pl.String,
                    "_inserted_at": pl.Datetime("us", "UTC")
                }
                
                df_lote = df_lote.select(list(schema_esperado.keys()))
                validar_schema_basico(df_lote, schema_esperado)

                tabela_arrow = df_lote.to_arrow()

                if writer is None:
                    writer = pq.ParquetWriter(path_local_silver, tabela_arrow.schema, compression="snappy")

                writer.write_table(tabela_arrow)

                linhas_processadas += df_lote.height
                logger.info(f"  ... {linhas_processadas}/{total_linhas} rows processed ({nome_arq})")

                del df_lote
                del tabela_arrow
                gc.collect()
        finally:
            if writer is not None:
                writer.close()

        logger.info(f"deploying to s3://{BUCKET_SILVER}/{chave_destino}")
        s3_client.upload_file(path_local_silver, BUCKET_SILVER, chave_destino)

        gc.collect()

        os.remove(path_local_bronze)
        os.remove(path_local_silver)

        logger.info(f"processing done and deployed to s3://{BUCKET_SILVER}/{chave_destino}")
        processed_count += 1

    return processed_count, skipped_count

def executar():
    s3_client = get_s3_client()

    tabelas_dominio = ["cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"]
    total_processed = 0
    total_skipped = 0

    for tab in tabelas_dominio:
        processed, skipped = processar_tabela_codigo_descricao(s3_client, tab)
        total_processed += processed
        total_skipped += skipped

    processed, skipped = processar_simples(s3_client)
    total_processed += processed
    total_skipped += skipped

    logger.info("==================================================================")
    logger.info(f"[!] task complete with  {total_processed} dimension tables processed, {total_skipped} skipped (already in silver bucket).")
    logger.info("==================================================================")

if __name__ == "__main__": 
    executar()
