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

logger = get_logger("rfb_landing_estabelecimentos")
#=============================================================================================================


REFERENCIA = "2026-08"
REF_MES_INT = int(REFERENCIA.replace("-", ""))
BUCKET_SILVER = "silver"
SCHEMA_LANDING = "landing_rfb"
TABELA_LANDING = "estabelecimentos"
ENTIDADE_SILVER = "estabelecimentos"
PASTA_TMP = "/mnt/datasource/tmp_landing"
TAMANHO_LOTE = 500_000

DDL_ESTABELECIMENTOS = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_LANDING};

CREATE TABLE IF NOT EXISTS {SCHEMA_LANDING}.{TABELA_LANDING} (
    cnpj_basico TEXT,
    cnpj_ordem TEXT,
    cnpj_dv TEXT,
    cnpj_completo TEXT,
    identificador_matriz_filial SMALLINT,
    nome_fantasia TEXT,
    situacao_cadastral SMALLINT,
    data_situacao_cadastral DATE,
    motivo_situacao_cadastral INTEGER,
    nome_cidade_exterior TEXT,
    pais INTEGER,
    data_inicio_atividade DATE,
    cnae_fiscal_principal TEXT,
    cnae_fiscal_secundaria TEXT,
    tipo_logradouro TEXT,
    logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cep TEXT,
    uf TEXT,
    municipio INTEGER,
    ddd_1 TEXT,
    telefone_1 TEXT,
    ddd_2 TEXT,
    telefone_2 TEXT,
    ddd_fax TEXT,
    fax TEXT,
    correio_eletronico TEXT,
    situacao_especial TEXT,
    data_situacao_especial DATE,
    referencia_mes INTEGER,
    _source_file TEXT,
    _inserted_at TIMESTAMPTZ,
    _suspeita_deslocamento BOOLEAN DEFAULT FALSE
);
"""

COLUNAS_PARA_TEXT = [
    "cnpj_basico", "cnpj_ordem", "cnpj_dv", "cnpj_completo",
    "cnae_fiscal_principal", "cep", "uf",
    "ddd_1", "telefone_1", "ddd_2", "telefone_2", "ddd_fax", "fax",
]

LIMITES_SUSPEITA = {
    "cnpj_basico": 8,
    "cnpj_ordem": 4,
    "cnpj_dv": 2,
    "cnpj_completo": 14,
    "cnae_fiscal_principal": 7,
    "cep": 8,
    "uf": 2,
    "ddd_1": 4,
    "ddd_2": 4,
    "ddd_fax": 4,
    "telefone_1": 15,
    "telefone_2": 15,
    "fax": 15,
}


def processar_landing_estabelecimentos():
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
        ddl=DDL_ESTABELECIMENTOS,
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
    processar_landing_estabelecimentos()
