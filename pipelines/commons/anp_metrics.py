import polars as pl
from pipelines.commons.logger import get_logger

logger = get_logger("METRICS_OBSERVABILITY")


def log_batch_metrics(df_batch: pl.DataFrame, total_files: int):
    
    try:
        total_linhas = len(df_batch)

       
        cols_map = {c.lower(): c for c in df_batch.columns}

        col_bandeira = next((v for k, v in cols_map.items() if "bandeira" in k), None)
        col_municipio = next((v for k, v in cols_map.items() if "municipio" in k or "município" in k), None)
        col_cnpj = next((v for k, v in cols_map.items() if "cnpj" in k), None)
        col_produto = next((v for k, v in cols_map.items() if "produto" in k), None)
        col_preco = next((v for k, v in cols_map.items() if "preco_de_venda" in k or "valor_de_venda" in k or "preco" in k), None)

        total_bandeiras = df_batch[col_bandeira].n_unique() if col_bandeira else 0
        total_municipios = df_batch[col_municipio].n_unique() if col_municipio else 0
        total_cnpjs = df_batch[col_cnpj].n_unique() if col_cnpj else 0
        lista_produtos = df_batch[col_produto].unique().to_list() if col_produto else []

        logger.info("#" * 90)
        logger.info(f" BATCH OBSERVABILITY REPORT ({total_files} file(s) queued)")
        logger.info("#" * 90)
        logger.info(f" >> total rows           : {total_linhas:,}")
        logger.info(f" >> total flags          : {total_bandeiras:,}")
        logger.info(f" >> total cities         : {total_municipios:,}")
        logger.info(f" >> total tax id (cnpj)  : {total_cnpjs:,}")
        logger.info(f" >> products list        : {lista_produtos}")

        # Preço Médio por Produto
        if col_produto and col_preco:
            df_preco = df_batch.with_columns(
                pl.col(col_preco).str.replace(",", ".").cast(pl.Float64, strict=False).alias("_preco_num")
            )
            
            res_preco = (
                df_preco.group_by(col_produto)
                .agg(
                    pl.col("_preco_num").mean().round(3).alias("preco_medio"),
                    pl.len().alias("volume")
                )
                .sort(col_produto)
                .to_dicts()
            )

            logger.info(" >> average price by product:")
            for item in res_preco:
                prod = item[col_produto]
                p_med = item["preco_medio"]
                vol = item["volume"]
                logger.info(f"    * {prod:<25} | avg: R$ {p_med:.3f} | rows: {vol:,} regs")

        logger.info("#" * 90)

    except Exception as e:
        logger.error(f"fail summarizing batch metrics {e}")