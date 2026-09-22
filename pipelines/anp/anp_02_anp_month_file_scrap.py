import sys
import os
import re
import time
import urllib.parse
from datetime import datetime
from warnings import filterwarnings
import requests
from bs4 import BeautifulSoup
import polars as pl
from pathlib import Path

filterwarnings("ignore")

# ==========================================
# COMMONS UTILS SETUP
# ==========================================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# IMPORTANTE: Certifique-se de que DIR_ANP_LANDING_MONTH esteja mapeado no env_loader.py
from pipelines.commons.env_loader import validate_env, DIR_ANP_LANDING_MONTH
from pipelines.commons.dw_client import get_sqla_engine, test_pg_connection
from pipelines.commons.logger import get_logger

logger = get_logger("anp_02_anp_month_file_scrap")

# ==========================================
# TARGETS & CONFIG
# ==========================================

SCHEMA = "ctrl"
TABELA = "anp_metadata_mensal"


PAGE_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

CATEGORY_FOLDER_MAP = {
    "GLP": "glp",
    "GASOLINA": "combustivel",
    "DIESEL": "combustivel",
}

MONTHS_MAP = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3,
    "abril": 4, "maio": 5, "junho": 6, "julho": 7,
    "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
}

PRODUCT_SLUGS = {
    "óleo diesel": "DIESEL", "diesel": "DIESEL",
    "etanol hidratado + gasolina c": "GASOLINA",
    "gasolina": "GASOLINA",
    "glp p13": "GLP", "glp": "GLP",
}

# ==========================================
# FUNCTIONS 
# ==========================================
def parse_portal_items() -> pl.DataFrame:
    response = requests.get(PAGE_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, "html.parser")
    portal_items = []
    content_area = soup.find("div", {"id": "content"}) or soup
    current_cat = None

    for element in content_area.find_all(["h1", "h2", "h3", "h4", "p", "strong", "ul"]):
        if element.name in ["h1", "h2", "h3", "h4", "p", "strong"]:
            text_clean = element.get_text(strip=True).lower()
            for key, slug_val in PRODUCT_SLUGS.items():
                if key in text_clean:
                    current_cat = slug_val
                    break
                    
        elif element.name == "ul" and current_cat:
            for a_tag in element.find_all("a", href=True):
                link_name = a_tag.get_text(strip=True)
                match = re.search(r"([a-zA-ZçÇ]+)\s+de\s+(\d{4})", link_name, re.IGNORECASE)
                
                if match:
                    month_str = match.group(1).lower()
                    year_num = int(match.group(2))
                    month_num = MONTHS_MAP.get(month_str)

                    if month_num:
                        ref = f"{year_num}{month_num:02d}"
                        download_url = urllib.parse.urljoin(PAGE_URL, a_tag["href"])
                        portal_items.append({
                            "cat": current_cat, "ref": ref, "month": month_num,
                            "year": year_num, "link_name": link_name, "url_source": download_url,
                        })

    schema = {
        "cat": pl.Utf8, "ref": pl.Utf8, "month": pl.Int32,
        "year": pl.Int32, "link_name": pl.Utf8, "url_source": pl.Utf8,
    }

    if not portal_items:
        return pl.DataFrame(schema=schema)

    return pl.DataFrame(portal_items, schema=schema).unique(subset=["cat", "ref", "url_source"], keep="first")

def get_monit_db_dataframe() -> pl.DataFrame:
    engine = get_sqla_engine()
    query = f"SELECT cat, ref FROM {SCHEMA}.{TABELA}"
    schema = {"cat": pl.Utf8, "ref": pl.Utf8}
    try:
        df = pl.read_database(query=query, connection=engine)
        if df.is_empty():
            return pl.DataFrame(schema=schema)
        
        # Garante a tipagem String/Utf8 mesmo se vier como objeto/null do SQL
        return df.with_columns([
            pl.col("cat").cast(pl.Utf8),
            pl.col("ref").cast(pl.Utf8)
        ])
    except Exception as e:
        logger.warning(f"failed to read ctrl table : {e}")
        return pl.DataFrame(schema=schema)

def insert_monitoring_record(record: dict):
    engine = get_sqla_engine()
    df_record = pl.DataFrame([record])
    df_record.write_database(
        table_name=f"{SCHEMA}.{TABELA}",
        connection=engine,
        if_table_exists="append",
        engine="sqlalchemy",
    )

# ==========================================
# PIPELINE
# ==========================================
def main():
    test_pg_connection()
    engine = get_sqla_engine()
    dir_input = Path(DIR_ANP_LANDING_MONTH)

    TEST_ONLY_2026 = True 

    logger.info("=" * 60)
    logger.info(f"web scrapping initiated on https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis : {datetime.now()}")
    logger.info(f"files landing dir  {dir_input.as_posix()} : {datetime.now()}")
    logger.info("=" * 60)
    
    try:
        df_portal = parse_portal_items()
        if df_portal.is_empty():
            logger.warning("invalid structure or no items mapped on portal. skipping")
            sys.exit(99)
    except Exception as e:
        logger.error(f"SCRAPING FAILED: {e}")
        sys.exit(1)

    logger.info("reading ctrl table to identify pending files")
    df_db = get_monit_db_dataframe()
    
    # Garantia extra de tipagem para evitar SchemaError no join
    df_db = df_db.with_columns([
        pl.col("cat").cast(pl.Utf8),
        pl.col("ref").cast(pl.Utf8)
    ])

    df_pending = df_portal.join(df_db, on=["cat", "ref"], how="anti")

    #---------teste data
    if TEST_ONLY_2026:
        logger.info("test mode activated: filtering payload for 2026 files only")
        df_pending = df_pending.filter(pl.col("year") == 2026)
    #------------------------------------------------------

    pending_count = df_pending.height

    if pending_count == 0:
        logger.warning("lakehouse is already up to date. skipping download")
        logger.warning('pipeline run ended with no new data to process')
        sys.exit(99)

    logger.info(f"difference detected. found {pending_count} pending file(s)")
    downloaded_count = 0
    dest_root_path = Path(DIR_ANP_LANDING_MONTH)

    for row in df_pending.iter_rows(named=True):
        cat = row["cat"]
        logger.info(f"processing: {cat} | {row['link_name']} (ref: {row['ref']})")

        subfolder = CATEGORY_FOLDER_MAP.get(cat, "combustivel")
    
        extracted_filename = os.path.basename(urllib.parse.urlparse(row["url_source"]).path)

        if not extracted_filename or not extracted_filename.endswith(".csv"):
            extracted_filename = f"{cat.lower()}_{row['ref']}.csv"
        destination_folder = dest_root_path / subfolder / "mes"
        destination_folder.mkdir(parents=True, exist_ok=True)
        
        destination_local_path = destination_folder / extracted_filename

        try:
            res = requests.get(row["url_source"], headers=HEADERS, stream=True, timeout=30)
            res.raise_for_status()

            with open(destination_local_path, 'wb') as out_file:
                for chunk in res.iter_content(chunk_size=8192):
                    out_file.write(chunk)

            monitoring_record = {
                "cat": cat,
                "ref": row["ref"],
                "month": row["month"],
                "year": row["year"],
                "file_name": f"{subfolder}/mes/{extracted_filename}",
                "url_source": row["url_source"],
                "download_date": datetime.now(),
                "link_name": row["link_name"],
            }

            insert_monitoring_record(monitoring_record)
            logger.info(f"successfully landed at {destination_local_path.as_posix()} ")
            downloaded_count += 1

        except Exception as e:
            logger.error(f"DOWNLOAD FAILED FOR {row['url_source']}: {e}")

        time.sleep(2)

    logger.info("=" * 60)
    logger.info("pipeline run complete")
    logger.info(f"total processed: {downloaded_count}/{pending_count}")
    logger.info(f"main destination: {dest_root_path.as_posix()}")
    logger.info("=" * 60)
    sys.exit(0)

if __name__ == "__main__":
    main()