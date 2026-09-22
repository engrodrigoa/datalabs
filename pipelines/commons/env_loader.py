import os
from dotenv import load_dotenv

#=============================================================================================================
#=================================
# .ENV SETUP
#=================================
def get_project_root():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    while current_dir:
        if os.path.exists(os.path.join(current_dir, ".env")):
            return current_dir
        parent = os.path.dirname(current_dir)
        if parent == current_dir:
            break
        current_dir = parent
    return os.getcwd()

is_docker = os.path.exists("/opt/airflow")
ROOT_DIR = "/opt/airflow" if is_docker else get_project_root()
load_dotenv(dotenv_path=os.path.join(ROOT_DIR, ".env"), override=True)
#=============================================================================================================


##################################
### MINIO SETUP
##################################
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_INTERNAL") if is_docker else os.getenv("MINIO_ENDPOINT_EXTERNAL")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_DATASOURCE = os.getenv("MINIO_BUCKET_AUDIT", "audit")
BUCKET_AUDIT = os.getenv("MINIO_BUCKET_AUDIT", "audit")

REQUIRED_BUCKETS = [
    os.getenv("MINIO_BUCKET_LANDING", "landing"),
    os.getenv("MINIO_BUCKET_BRONZE", "bronze"),
    os.getenv("MINIO_BUCKET_SILVER", "silver"),
    os.getenv("MINIO_BUCKET_GOLD", "gold"),
    os.getenv("MINIO_BUCKET_AUDIT", "audit"),
]

##################################
### POSTGRES SETUP
### DB DATALAB
##################################
DB_HOST = os.getenv("DB_HOST_INTERNAL") if is_docker else os.getenv("DB_HOST_EXTERNAL")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_DB")
CONSTRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


##################################
### SOURCE PATH
##################################
TARGET_DIR = os.path.join(ROOT_DIR, "pipelines", "dbt_projects", "target")
RUN_RESULTS_PATH = os.path.join(TARGET_DIR, "run_results.json")
MANIFEST_PATH = os.path.join(TARGET_DIR, "manifest.json")

##################################
### TARGET PATH
##################################
default_landing_week = (
    "/mnt/datasource/anp/ult4" 
    if is_docker 
    else os.path.join(ROOT_DIR, "datasource", "anp", "ult4")
)

default_landing_month = (
    "/mnt/datasource/anp/arquivos_fechados" 
    if is_docker 
    else os.path.join(ROOT_DIR, "datasource", "anp", "arquivos_fechados")
)

DIR_ANP_LANDING_WEEK = os.getenv("DIR_ANP_LANDING_WEEK", default_landing_week)
DIR_ANP_LANDING_MONTH = os.getenv("DIR_ANP_LANDING_MONTH", default_landing_month)



##################################
### PARAMS CALL
##################################
def validate_env(required: dict):
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(f"[env_loader] missing variables: {missing}")