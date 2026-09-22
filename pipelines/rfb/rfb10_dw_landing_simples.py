import os
import sys
from warnings import filterwarnings

filterwarnings("ignore")

os.environ["POLARS_MAX_THREADS"] = "2"
os.environ["RAYON_NUM_THREADS"] = "2"

#=============================================================================================================
#===================================
# env loader - connection setup
#===================================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.s3_client import get_s3_client, test_s3_connection
from pipelines.commons.dw_client import get_sqla_engine, test_pg_connection
from pipelines.commons.logger import get_logger
from pipelines.commons.dw_landing_rfb import processar_arquivos_landing

logger = get_logger("rfb_landing_simples")
#=============================================================================================================


REFERENCIA = "2026-08"
REF_MES_INT = int(REFERENCIA.replace("-", ""))
BUCKET_SILVER = "silver"
SCHEMA_LANDING = "landing_rfb"
TABELA_LANDING = "simples"
ENTIDADE_SILVER = "simples"
PASTA_TMP = "/mnt/datasource/tmp_landing"
TAMANHO_LOTE = 500_000

DDL_SIMPLES = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_LANDING};

CREATE TABLE IF NOT EXISTS {SCHEMA_LANDING}.{TABELA_LANDING} (
    cnpj_basico TEXT,
    opcao_simples TEXT,
    data_opcao_simples DATE,
    data_exclusao_simples DATE,
    opcao_mei TEXT,
    data_opcao_mei DATE,
    data_exclusao_mei DATE,
    referencia_mes INTEGER,
    _source_file TEXT,
    _inserted_at TIMESTAMPTZ,
    _suspeita_deslocamento BOOLEAN DEFAULT FALSE
);
"""

COLUNAS_PARA_TEXT = [
    "cnpj_basico", "opcao_simples", "opcao_mei",
]

LIMITES_SUSPEITA = {
    "cnpj_basico": 8,
}


def processar_landing_simples():
    test_s3_connection()
    test_pg_connection()

    s3_client = get_s3_client()
    engine = get_sqla_engine()

    total_linhas, total_suspeitas = processar_arquivos_landing(
        s3_client=s3_client,
        engine=engine,
        bucket_silver=BUCKET_SILVER,
        entidade=ENTIDADE_SILVER,
        ref_mes_int=REF_MES_INT,
        schema=SCHEMA_LANDING,
        tabela=TABELA_LANDING,
        ddl=DDL_SIMPLES,
        colunas_para_text=COLUNAS_PARA_TEXT,
        limites_suspeita=LIMITES_SUSPEITA,
        pasta_tmp=PASTA_TMP,
        tamanho_lote=TAMANHO_LOTE,
        logger=logger,
    )

    logger.info("==================================================================")
    logger.info(
        f"run complete: {total_linhas} total row(s) inserted into "
        f"{SCHEMA_LANDING}.{TABELA_LANDING} ({total_suspeitas} flagged as suspicious)."
    )
    logger.info("==================================================================")


if __name__ == "__main__":
    processar_landing_simples()