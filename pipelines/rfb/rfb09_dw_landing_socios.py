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

logger = get_logger("rfb_landing_socios")
#=============================================================================================================

REFERENCIA = "2026-08"
REF_MES_INT = int(REFERENCIA.replace("-", ""))
BUCKET_SILVER = "silver"
SCHEMA_LANDING = "landing_rfb"
TABELA_LANDING = "socios"
ENTIDADE_SILVER = "socios"
PASTA_TMP = "/mnt/datasource/tmp_landing"
TAMANHO_LOTE = 500_000

DDL_SOCIOS = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_LANDING};

CREATE TABLE IF NOT EXISTS {SCHEMA_LANDING}.{TABELA_LANDING} (
    cnpj_basico TEXT,
    identificador_socio SMALLINT,
    nome_socio_razao_social TEXT,
    cnpj_cpf_socio TEXT,
    qualificacao_socio INTEGER,
    data_entrada_sociedade DATE,
    pais INTEGER,
    representante_legal TEXT,
    nome_representante TEXT,
    qualificacao_representante_legal INTEGER,
    faixa_etaria SMALLINT,
    referencia_mes INTEGER,
    _source_file TEXT,
    _inserted_at TIMESTAMPTZ,
    _suspeita_deslocamento BOOLEAN DEFAULT FALSE
);
"""

COLUNAS_PARA_TEXT = [
    "cnpj_basico", "cnpj_cpf_socio",
]


LIMITES_SUSPEITA = {
    "cnpj_basico": 8,
    "cnpj_cpf_socio": 14,
}


def processar_landing_socios():
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
        ddl=DDL_SOCIOS,
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
    processar_landing_socios()