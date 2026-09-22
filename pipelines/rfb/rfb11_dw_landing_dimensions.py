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

logger = get_logger("rfb_landing_dominios")
#=============================================================================================================


REFERENCIA = "2026-08"
REF_MES_INT = int(REFERENCIA.replace("-", ""))
BUCKET_SILVER = "silver"
SCHEMA_LANDING = "landing_rfb"
PASTA_TMP = "/mnt/datasource/tmp_landing"
TAMANHO_LOTE = 500_000

DOMINIOS = [
    {"entidade": "cnaes", "tabela": "dominio_cnae"},
    {"entidade": "municipios", "tabela": "dominio_municipio"},
    {"entidade": "naturezas", "tabela": "dominio_natureza_juridica"},
    {"entidade": "paises", "tabela": "dominio_pais"},
    {"entidade": "qualificacoes", "tabela": "dominio_qualificacao_socio"},
    {"entidade": "motivos", "tabela": "dominio_motivo_situacao_cadastral"},
]


def ddl_dominio(tabela: str) -> str:
    return f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA_LANDING};

    CREATE TABLE IF NOT EXISTS {SCHEMA_LANDING}.{tabela} (
        codigo TEXT,
        descricao TEXT,
        referencia_mes INTEGER,
        _source_file TEXT,
        _inserted_at TIMESTAMPTZ,
        _suspeita_deslocamento BOOLEAN DEFAULT FALSE
    );
    """


def processar_landing_dominios():
    test_s3_connection()
    test_pg_connection()

    s3_client = get_s3_client()
    engine = get_sqla_engine()

    resumo = []
    for dominio in DOMINIOS:
        entidade = dominio["entidade"]
        tabela = dominio["tabela"]

        logger.info(f"processing reference table: {entidade} -> {SCHEMA_LANDING}.{tabela}")

        total_linhas, total_suspeitas = processar_arquivos_landing(
            s3_client=s3_client,
            engine=engine,
            bucket_silver=BUCKET_SILVER,
            entidade=entidade,
            ref_mes_int=REF_MES_INT,
            schema=SCHEMA_LANDING,
            tabela=tabela,
            ddl=ddl_dominio(tabela),
            colunas_para_text=["codigo"],
            limites_suspeita={},
            pasta_tmp=PASTA_TMP,
            tamanho_lote=TAMANHO_LOTE,
            logger=logger,
        )
        resumo.append((tabela, total_linhas, total_suspeitas))

    logger.info("==================================================================")
    logger.info("run complete for all reference (domain) tables:")
    for tabela, total_linhas, total_suspeitas in resumo:
        logger.info(
            f"  {SCHEMA_LANDING}.{tabela}: {total_linhas} row(s) inserted "
            f"({total_suspeitas} flagged as suspicious)."
        )
    logger.info("==================================================================")


if __name__ == "__main__":
    processar_landing_dominios()
