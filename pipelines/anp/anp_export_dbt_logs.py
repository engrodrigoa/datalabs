import polars as pl
import json
from datetime import datetime, timezone, timedelta
from warnings import filterwarnings
import os
import sys
import glob


filterwarnings("ignore")


# ==========================================
# commons utils: env_loader
# ==========================================

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.env_loader import (
    RUN_RESULTS_PATH, MANIFEST_PATH, CONSTRING,
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKET_DATASOURCE, validate_env,
)
from pipelines.commons.s3_client import get_s3_client
from pipelines.commons.dw_client import get_pg_connection

validate_env({
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "CONSTRING": CONSTRING,
})

# def backup_logs_to_s3(timestamp_str):
#     s3 = get_s3_client()
#     s3.upload_file(RUN_RESULTS_PATH, BUCKET_DATASOURCE, f"dbt/run_results/run_results_{timestamp_str}.json")
#     s3.upload_file(MANIFEST_PATH, BUCKET_DATASOURCE, f"dbt/manifests/manifest_{timestamp_str}.json")
#     print(f">>>>> [SUCCESS] s3 backup done (file sufix: {timestamp_str})")
ELEMENTARY_LOGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dbt_projects/logs/elementary_logs"))

def backup_logs_to_s3(timestamp_str):
    s3 = get_s3_client()
    
    # 1. Backup dos artefatos nativos do dbt
    s3.upload_file(RUN_RESULTS_PATH, BUCKET_DATASOURCE, f"dbt/run_results/run_results_{timestamp_str}.json")
    s3.upload_file(MANIFEST_PATH, BUCKET_DATASOURCE, f"dbt/manifests/manifest_{timestamp_str}.json")
    
    # 2. Captura e Backup do Report do Elementary
    html_files = glob.glob(os.path.join(ELEMENTARY_LOGS_DIR, "edr_report_anp_*.html"))
    if html_files:
        # Identifica o arquivo gerado mais recentemente na pasta
        latest_html = max(html_files, key=os.path.getctime)

        # Captura o POSIX timestamp de criação e converte para GMT-3
        ctime = os.path.getctime(latest_html)
        fuso_br = timezone(timedelta(hours=-3))
        file_dt_gmt3 = datetime.fromtimestamp(ctime, tz=fuso_br)
        file_timestamp_str = file_dt_gmt3.strftime('%Y%m%d_%H%M%S')

        # Faz o upload com o nome ajustado para o fuso correto
        s3.upload_file(latest_html, "audit", f"elementary/edr_report_anp_{file_timestamp_str}.html")
        print(f"s3 elementary backup done ({file_timestamp_str})")
    else:
        print(f">>>>> No Elementary HTML report found in {ELEMENTARY_LOGS_DIR}")

    print(f"[!] s3 backup done (file sufix: {timestamp_str})")


# ==========================================
def process_dbt_artifacts(now):
    """Lê os JSONs usando Polars e retorna um DataFrame desnormalizado enriquecido."""
    with open(RUN_RESULTS_PATH, 'r') as f:
        run_data = json.load(f)

    if not run_data.get('results'):
        print(">>>>> [WARNING] empty .json. no node executed in this run. skipping processing.")
        return pl.DataFrame()

    # Parsing da Tabela Fato
    df_runs = (
        pl.read_json(RUN_RESULTS_PATH)
        .explode('results')
        .unnest('metadata')
        .with_columns(
            (pl.col('generated_at').cast(pl.Datetime) - pl.duration(hours=3)).alias('run_timestamp'),
            pl.col('results').struct.field('unique_id').alias('node_id'),
            pl.col('results').struct.field('status').alias('status'),
            pl.col('results').struct.field('execution_time').alias('execution_time_sec'),
            pl.col('results').struct.field('failures').alias('failures'),
            pl.col('results').struct.field('message').alias('message')
        )
        .select('run_timestamp', 'invocation_id', 'node_id', 'status', 'execution_time_sec', 'failures', 'message')
    )

    # Parsing da Tabela Dimensão
    with open(MANIFEST_PATH, 'r') as f:
        manifest_data = json.load(f)

    parsed_nodes = [
        {
            "node_id": node_id,
            "resource_type": info.get("resource_type"),
            "database": info.get("database"),
            "schema_layer": info.get("schema"),
            "materialized": info.get("config", {}).get("materialized"),
            "tags": info.get("tags", [])
        }
        for node_id, info in manifest_data.get('nodes', {}).items()
    ]
    df_manifest = pl.DataFrame(parsed_nodes)

    # Enriquecimento final
    df_final_log = (
        df_runs
        .join(df_manifest, on='node_id', how='left')
        .with_columns(
            pl.lit(now).cast(pl.Datetime('us')).alias('data_carga'),
            pl.col('tags').list.join(',').alias('tags')
        )
    )
    print(">>> [END] processing done")
    return df_final_log


def load_logs_to_postgres(df_final_log):
    if df_final_log.is_empty():
        print(">>> [END] nothing to load to db. no error exit")
        return

    # 1. Grava na Staging
    df_final_log.write_database(
        table_name="audit.dbt_runs_stg",
        connection=CONSTRING,
        if_table_exists="replace",
        engine="sqlalchemy"
    )
    

    # 2. Cláusula de Merge Idempotente
    merge_query = """
        INSERT INTO audit.dbt_runs (
            run_timestamp, invocation_id, node_id, status, execution_time_sec, 
            failures, message, resource_type, database, schema_layer, 
            materialized, tags, data_carga
        )
        SELECT 
            run_timestamp, invocation_id, node_id, status, execution_time_sec, 
            failures, message, resource_type, database, schema_layer, 
            materialized, tags, data_carga
        FROM audit.dbt_runs_stg
        ON CONFLICT (invocation_id, node_id) DO NOTHING;
        
        DROP TABLE audit.dbt_runs_stg;
    """

    # 3. Execução nativa no banco
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute(merge_query)
    conn.commit()
    cur.close()
    conn.close()
    print(">>> [END] audit table updated")

# ==========================================
# 3. ORQUESTRAÇÃO PRINCIPAL (Ponto de Entrada)
# ==========================================
def main():
    print(">>> [START] observability ")
    
    # Congela o tempo global da execução
    now = datetime.now()
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    
    # 1. Backup Físico
    backup_logs_to_s3(timestamp_str)
    
    # 2. Processamento Analítico
    df_final_log = process_dbt_artifacts(now)
    
    # 3. Carga no Banco (Data Warehouse)
    load_logs_to_postgres(df_final_log)
    
    print(">>> [END] observability ")

if __name__ == "__main__":
    main()