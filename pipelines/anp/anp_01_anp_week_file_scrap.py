import sys
import os
import re
from datetime import datetime
from warnings import filterwarnings
import requests
from bs4 import BeautifulSoup
import polars as pl
from pathlib import Path


filterwarnings("ignore")

# ==========================================
# TARGETS & CONFIGS
# ==========================================
SCHEMA = "ctrl"
TABELA = "anp_metadata_semanal"

URL_ANP = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis"
HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

LINKS_DOWNLOAD = {
    "ultimas-4-semanas-diesel-gnv.csv": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-diesel-gnv.csv",
    "ultimas-4-semanas-gasolina-etanol.csv": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-gasolina-etanol.csv",
    "ultimas-4-semanas-glp.csv": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus/ultimas-4-semanas-glp.csv"
}

# ==========================================
# COMMONS UTILS SETUP
# ==========================================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.env_loader import  DIR_ANP_LANDING_WEEK
from pipelines.commons.dw_client import get_sqla_engine, test_pg_connection
from pipelines.commons.logger import get_logger

# instaciar logger para o pipeline corrente
logger = get_logger("anp_01_anp_week_file_scrap")


# ==========================================
# FUNCTIONS
# ==========================================
def ler_ultima_data_banco(schema: str, tabela: str):
    engine = get_sqla_engine()
    query = f"SELECT MAX(data_ref) AS max_data_ref FROM {schema}.{tabela}"
    try:
        df = pl.read_database(query=query, connection=engine)
        if not df.is_empty() and df["max_data_ref"][0] is not None:
            return df["max_data_ref"][0]
        return None
    except Exception as e:
        logger.error(f'lasdate db read failed {e}')
        raise e

def obter_data_atualizacao_site(url, headers):
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    
    padrao = re.search(
        r"Quatro\s+últimas\s+semanas.*?atualizado\s+em\s*(\d{1,2}/\d{1,2}/\d{4})", 
        soup.get_text(), 
        re.IGNORECASE | re.DOTALL
    )
    if padrao:
        return datetime.strptime(padrao.group(1), "%d/%m/%Y").date()
    raise ValueError("no date param found on site. scraping failed")

def download_csv_local(links: dict, diretorio_destino: str):
    dest_path = Path(diretorio_destino)
    dest_path.mkdir(parents=True, exist_ok=True)
    
    for nome_arquivo, link_url in links.items():
        caminho_arquivo = dest_path / nome_arquivo
        caminho_log = caminho_arquivo.as_posix()
        
        logger.info(f"downloading file {nome_arquivo} to {caminho_log}")
        
        res_file = requests.get(link_url, headers=HEADERS_WEB, stream=True, timeout=30)
        res_file.raise_for_status()
        
        with open(caminho_arquivo, 'wb') as out_file:
            for chunk in res_file.iter_content(chunk_size=8192):
                out_file.write(chunk)

def registrar_metadado_banco(data_site, status, schema, tabela): #, string_conexao):
    engine = get_sqla_engine()
    df_resultado = pl.DataFrame([{
        'data_ref': data_site,
        'data_dag_run': datetime.now(),
        'status': status
    }])

    df_resultado.write_database(
        table_name=f"{schema}.{tabela}",
        connection=engine,
        if_table_exists="append",
        engine="sqlalchemy"
    )
   

# ==========================================
# PIPELINE
# ==========================================
def main():
    test_pg_connection()
    engine = get_sqla_engine()
    dir_input = Path(DIR_ANP_LANDING_WEEK)

    logger.info("=" * 60)
    logger.info(f"web scrapping initiated on https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis : {datetime.now()}")
    logger.info(f"files landing dir  {dir_input.as_posix()} : {datetime.now()}")
    logger.info("=" * 60)
    
    try:
        data_site = obter_data_atualizacao_site(URL_ANP, HEADERS_WEB)
       
        logger.info(f"recovered date param from scrapping >> {data_site}")
    except Exception as e:
        logger.error(f"scrapping fail: {e}")
        sys.exit(1)
        
    ultima_data_banco = ler_ultima_data_banco(SCHEMA, TABELA)
    logger.info(f"last date on ctrl database >> {ultima_data_banco}")

    # Condição de idempotência
    if ultima_data_banco is not None and data_site <= ultima_data_banco:
        logger.warning("lakehouse is already up to date. skipping download")
        logger.warning('pipeline run ended with no new data to process')
        sys.exit(99) 
        
    try:
        logger.info(f"[STEP 1/2] downloading files on {DIR_ANP_LANDING_WEEK}")
        download_csv_local(LINKS_DOWNLOAD, DIR_ANP_LANDING_WEEK)
        
        logger.info("[STEP 2/2] writing successs in ctrl table")
        registrar_metadado_banco(data_site, 'SUCESSO', SCHEMA, TABELA)
        
        logger.info(" --- db updated ---")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"PIPELINE RUN FAIL: {e}")
        try:
            registrar_metadado_banco(data_site, 'FALHOU', SCHEMA, TABELA)
            logger.info("logging fail status")
        except Exception as db_e:
            logger.error(f"log insert fail {db_e}")
            
        sys.exit(1)

if __name__ == "__main__":
    main()