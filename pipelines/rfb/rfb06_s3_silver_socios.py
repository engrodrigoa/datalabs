import os
import sys
import gc
import polars as pl
import pyarrow.parquet as pq
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

logger = get_logger("rfb_silver_socios")

validate_env({
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
})
#=============================================================================================================


REFERENCIA = "2026-08"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
PASTA_TMP = "/mnt/datasource/tmp_silver"
TAMANHO_LOTE = int(os.getenv("RFB_SOCIOS_BATCH_SIZE", "500000"))

FORCAR_REPROCESSAMENTO = os.getenv("RFB_FORCAR_REPROCESSAMENTO", "0") == "1"

os.makedirs(PASTA_TMP, exist_ok=True)

MAPEAMENTO_COLUNAS = {
    "f0": "cnpj_basico",
    "f1": "identificador_socio",
    "f2": "nome_socio_razao_social",
    "f3": "cnpj_cpf_socio",
    "f4": "qualificacao_socio",
    "f5": "data_entrada_sociedade",
    "f6": "pais",
    "f7": "representante_legal",
    "f8": "nome_representante",
    "f9": "qualificacao_representante_legal",
    "f10": "faixa_etaria"
}

SCHEMA_SOCIOS = {v: pl.String for v in MAPEAMENTO_COLUNAS.values()}
SCHEMA_SOCIOS["identificador_socio"] = pl.Int8
SCHEMA_SOCIOS["qualificacao_socio"] = pl.Int32
SCHEMA_SOCIOS["pais"] = pl.Int32
SCHEMA_SOCIOS["qualificacao_representante_legal"] = pl.Int32
SCHEMA_SOCIOS["faixa_etaria"] = pl.Int8
SCHEMA_SOCIOS["data_entrada_sociedade"] = pl.Date
SCHEMA_SOCIOS["referencia_mes"] = pl.Int32
SCHEMA_SOCIOS["_source_file"] = pl.String
SCHEMA_SOCIOS["_inserted_at"] = pl.Datetime("us", "UTC")


def arquivo_ja_existe_no_silver(s3_client, chave_destino):
    try:
        s3_client.head_object(Bucket=BUCKET_SILVER, Key=chave_destino)
        return True
    except ClientError as e:
        codigo = e.response.get("Error", {}).get("Code", "")
        if codigo in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def transformar_lote(df_lote: pl.DataFrame) -> pl.DataFrame:
    return df_lote.rename(MAPEAMENTO_COLUNAS).with_columns([
        pl.col("cnpj_basico").str.strip_chars().str.zfill(8),
        pl.col("identificador_socio").cast(pl.String).str.strip_chars().cast(pl.Int8, strict=False),
        pl.col("qualificacao_socio").cast(pl.String).str.strip_chars().cast(pl.Int32, strict=False),
        pl.col("pais").cast(pl.String).str.strip_chars().cast(pl.Int32, strict=False),
        pl.col("qualificacao_representante_legal").cast(pl.String).str.strip_chars().cast(pl.Int32, strict=False),
        pl.col("faixa_etaria").cast(pl.String).str.strip_chars().cast(pl.Int8, strict=False),
        pl.col("data_entrada_sociedade").str.to_date(format="%Y%m%d", strict=False),
        pl.col("nome_socio_razao_social").str.strip_chars().replace("", None),
        pl.col("cnpj_cpf_socio").str.strip_chars().replace("", None),
        pl.col("representante_legal").str.strip_chars().replace("", None),
        pl.col("nome_representante").str.strip_chars().replace("", None),
    ])

def baixar_arquivo_verificado(s3_client, bucket, chave_s3, path_local, max_tentativas=3):
    import time
    meta = s3_client.head_object(Bucket=bucket, Key=chave_s3)
    tamanho_esperado = meta["ContentLength"]

    for tentativa in range(1, max_tentativas + 1):
        s3_client.download_file(bucket, chave_s3, path_local)
        tamanho_baixado = os.path.getsize(path_local)

        if tamanho_baixado == tamanho_esperado:
            return

        logger.warning(
            f"Incomplete download of {chave_s3} (attempt {tentativa}/{max_tentativas}): "
            f"expected {tamanho_esperado} bytes, got {tamanho_baixado} bytes. Retrying..."
        )
        if os.path.exists(path_local):
            os.remove(path_local)
        time.sleep(2 ** tentativa)

    raise IOError(f"Failed to download {chave_s3} fully after {max_tentativas} attempts.")

def processar_um_arquivo_com_retry(s3_client, chave_s3, chave_destino, max_tentativas=2):
    nome_arq = os.path.basename(chave_s3)
    path_local_bronze = os.path.join(PASTA_TMP, f"bronze_{nome_arq}")
    path_local_silver = os.path.join(PASTA_TMP, f"silver_{nome_arq}")

    ultimo_erro = None
    for tentativa in range(1, max_tentativas + 1):
        try:
            logger.info(f"processing dimension in batched mode ({nome_arq}, batch size {TAMANHO_LOTE}, attempt {tentativa}/{max_tentativas})...")
            baixar_arquivo_verificado(s3_client, BUCKET_BRONZE, chave_s3, path_local_bronze)
            logger.info(f"download completed and verified: s3://{BUCKET_BRONZE}/{chave_s3} -> {path_local_bronze}")

            parquet_file = pq.ParquetFile(path_local_bronze)
            total_linhas = parquet_file.metadata.num_rows
            logger.info(f"total rows in {nome_arq}: {total_linhas}")

            writer = None
            linhas_processadas = 0

            try:
                for batch in parquet_file.iter_batches(batch_size=TAMANHO_LOTE):
                    df_lote = pl.from_arrow(batch)
                    df_lote = transformar_lote(df_lote)
                    df_lote = adicionar_colunas_auditoria(df_lote, nome_arq)
                    
                    df_lote = df_lote.select(list(SCHEMA_SOCIOS.keys()))
                    validar_schema_basico(df_lote, SCHEMA_SOCIOS)

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
            logger.info(f"processing done and deployed to s3://{BUCKET_SILVER}/{chave_destino}")
            
            return linhas_processadas

        except OSError as e:
            ultimo_erro = e
            logger.error(f"Corrupted data while processing {nome_arq} (attempt {tentativa}/{max_tentativas}): {e}. Discarding local file and retrying.")
        finally:
            if os.path.exists(path_local_bronze):
                os.remove(path_local_bronze)
            if os.path.exists(path_local_silver):
                os.remove(path_local_silver)
            gc.collect()

    logger.exception(f">>> Fatal error processing file {nome_arq} after {max_tentativas} attempts.")
    raise ultimo_erro

def processar_socios_seguro(s3_client):
    logger.info(">>> starting pipeline (bronze -> silver) in batched mode.")

    ref_partition = REFERENCIA.replace('-', '')
    entity = "socios"
    prefixo_bronze = f"rfb/{entity}/ref_month={ref_partition}/"
    
    res = s3_client.list_objects_v2(Bucket=BUCKET_BRONZE, Prefix=prefixo_bronze)
    arquivos = sorted([
        obj["Key"] for obj in res.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ])

    if not arquivos:
        logger.warning(f"No files found in s3://{BUCKET_BRONZE}/{prefixo_bronze}")
        return 0

    logger.info(f"found {len(arquivos)} files to process.")

    total_arquivos_processados = 0
    total_linhas_processadas_geral = 0
    skipped_count = 0

    for chave_s3 in arquivos:
        nome_arq = os.path.basename(chave_s3)
        chave_destino = f"rfb/{entity}/ref_month={ref_partition}/{nome_arq}"

        if not FORCAR_REPROCESSAMENTO and arquivo_ja_existe_no_silver(s3_client, chave_destino):
            logger.info(f"skipping socios ({nome_arq}): already exists in s3://{BUCKET_SILVER}/{chave_destino}")
            skipped_count += 1
            continue

        linhas_processadas = processar_um_arquivo_com_retry(s3_client, chave_s3, chave_destino)

        total_arquivos_processados += 1
        total_linhas_processadas_geral += linhas_processadas

    logger.info("==================================================================")
    logger.info(f"[!] task complete with {total_arquivos_processados} files processed, {skipped_count} skipped (already in silver bucket).")
    logger.info("==================================================================")

    return total_arquivos_processados

def executar():
    s3_client = get_s3_client()
    processar_socios_seguro(s3_client)

if __name__ == "__main__":
    executar()
