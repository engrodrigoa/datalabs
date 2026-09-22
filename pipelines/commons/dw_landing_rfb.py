import os
import gc
import io
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text


def batch_para_large_string(batch: pa.RecordBatch) -> pa.RecordBatch:
    novos_arrays = []
    novos_campos = []
    for i, campo in enumerate(batch.schema):
        arr = batch.column(i)
        if pa.types.is_string(campo.type):
            arr = arr.cast(pa.large_string())
            campo = pa.field(campo.name, pa.large_string(), nullable=campo.nullable)
        elif pa.types.is_binary(campo.type):
            arr = arr.cast(pa.large_binary())
            campo = pa.field(campo.name, pa.large_binary(), nullable=campo.nullable)
        novos_arrays.append(arr)
        novos_campos.append(campo)
    novo_schema = pa.schema(novos_campos)
    return pa.RecordBatch.from_arrays(novos_arrays, schema=novo_schema)


def marcar_linhas_suspeitas(df: pl.DataFrame, limites: dict) -> pl.DataFrame:

    condicao_suspeita = pl.lit(False)
    for coluna, limite in (limites or {}).items():
        # Only attempt validation on columns that exist AND are strings.
        if coluna in df.columns and df.schema[coluna] == pl.Utf8:
            condicao_suspeita = condicao_suspeita | (
                pl.col(coluna).str.len_chars().fill_null(0) > limite
            )
    return df.with_columns(condicao_suspeita.alias("_suspeita_deslocamento"))


def preparar_schema_tabela(engine, schema, tabela, ddl, colunas_para_text, ref_mes_int, logger):
    logger.info(f"Validating schema/table and cleaning partition {ref_mes_int} for {schema}.{tabela}")
    
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '2h'")
            
            logger.info(f"  Creating schema/table via DDL...")
            cur.execute(ddl)

            logger.info(f"  Widening text columns...")
            for coluna in colunas_para_text:
                cur.execute(
                    f"ALTER TABLE {schema}.{tabela} ALTER COLUMN {coluna} TYPE TEXT"
                )
            
            logger.info(f"  Ensuring flag column exists...")
            cur.execute(
                f"ALTER TABLE {schema}.{tabela} "
                f"ADD COLUMN IF NOT EXISTS _suspeita_deslocamento BOOLEAN DEFAULT FALSE"
            )

            logger.info(f"  Clearing partition {ref_mes_int}...")
            cur.execute(
                f"DELETE FROM {schema}.{tabela} WHERE referencia_mes = %s",
                (ref_mes_int,)
            )
            registros_deletados = cur.rowcount
            if registros_deletados > 0:
                logger.info(f"Removed {registros_deletados} existing row(s) from partition {ref_mes_int}.")
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error preparing schema/table {schema}.{tabela}: {e}")
        raise e
    finally:
        try:
            conn.close()
        except Exception as close_err:
            logger.warning(f"Error closing connection after schema prep: {close_err}")


def sanitizar_nul_bytes(df: pl.DataFrame) -> pl.DataFrame:
    colunas_string = [nome for nome, dtype in df.schema.items() if dtype == pl.Utf8]
    if not colunas_string:
        return df
    return df.with_columns([
        pl.col(c).str.replace_all("\x00", "", literal=True) for c in colunas_string
    ])


def inserir_dataframe_bulk_copy(df: pl.DataFrame, engine, schema, tabela, pasta_tmp, logger):
    df = sanitizar_nul_bytes(df)

    buffer = io.BytesIO()
    df.write_csv(buffer, separator='\t', null_value='')
    buffer.seek(0)

    colunas_str = ", ".join(df.columns)
    copy_sql = f"""
        COPY {schema}.{tabela} ({colunas_str})
        FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '', DELIMITER '\t', QUOTE '"', ESCAPE '"')
    """

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '2h'")
            cur.copy_expert(copy_sql, buffer)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error during COPY into {schema}.{tabela}: {e}")
        try:
            debug_path = os.path.join(pasta_tmp, f"failed_batch_{schema}_{tabela}.tsv")
            buffer.seek(0)
            with open(debug_path, "wb") as f:
                f.write(buffer.read())
            logger.error(f"Failed batch saved to: {debug_path} for manual inspection.")
        except Exception as dump_err:
            logger.error(f"Failed to save debug batch: {dump_err}")
        raise e
    finally:
        try:
            conn.close()
        except Exception as close_err:
            logger.warning(f"Error closing connection after COPY: {close_err}")


def processar_arquivos_landing(
    s3_client, engine, bucket_silver, entidade, ref_mes_int,
    schema, tabela, ddl, colunas_para_text, limites_suspeita,
    pasta_tmp, tamanho_lote, logger,
):
    os.makedirs(pasta_tmp, exist_ok=True)
    prefixo_silver = f"rfb/{entidade}/ref_month={ref_mes_int}/"

    res = s3_client.list_objects_v2(Bucket=bucket_silver, Prefix=prefixo_silver)
    arquivos_s3 = sorted([
        obj["Key"] for obj in res.get("Contents", []) if obj["Key"].endswith(".parquet")
    ])

    if not arquivos_s3:
        logger.warning(f"No files found at s3://{bucket_silver}/{prefixo_silver}")
        return 0, 0

    preparar_schema_tabela(engine, schema, tabela, ddl, colunas_para_text, ref_mes_int, logger)

    total_linhas_geral = 0
    total_suspeitas_geral = 0

    for chave_s3 in arquivos_s3:
        nome_arq = os.path.basename(chave_s3)
        path_local = os.path.join(pasta_tmp, f"landing_{nome_arq}")

        logger.info(f">>> downloading {nome_arq} from S3 for local processing")
        s3_client.download_file(bucket_silver, chave_s3, path_local)

        parquet_file = pq.ParquetFile(path_local)
        total_linhas_arquivo = parquet_file.metadata.num_rows
        logger.info(f">>> starting copy of {nome_arq} into dw [total: {total_linhas_arquivo} rows]")

        linhas_processadas = 0
        for batch in parquet_file.iter_batches(batch_size=tamanho_lote):
            batch = batch_para_large_string(batch)
            df_lote = pl.from_arrow(batch)

            df_lote = marcar_linhas_suspeitas(df_lote, limites_suspeita)

            qtd_suspeitas = df_lote.select(pl.col("_suspeita_deslocamento").sum()).item()
            if qtd_suspeitas > 0:
                total_suspeitas_geral += qtd_suspeitas
                logger.warning(
                    f"[!] {qtd_suspeitas} suspicious row(s) in this batch "
                    f"(inserted anyway, flagged _suspeita_deslocamento = TRUE)"
                )

            inserir_dataframe_bulk_copy(df_lote, engine, schema, tabela, pasta_tmp, logger)

            linhas_processadas += df_lote.height
            logger.info(f">>> >>> inserted batch {linhas_processadas}/{total_linhas_arquivo} rows")

            del df_lote
            gc.collect()

        total_linhas_geral += linhas_processadas
        os.remove(path_local)
        logger.info(f"[!] file {nome_arq} done")

    if total_suspeitas_geral > 0:
        logger.warning(
            f">>> {total_suspeitas_geral} suspicious row(s) total for entity '{entidade}' "
            f"(all inserted; filter with _suspeita_deslocamento = false downstream if needed)"
        )

    return total_linhas_geral, total_suspeitas_geral
