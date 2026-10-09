"""
Single source of configuration for pipelines.

Precedence: process environment (docker-compose `environment:`) > .env file > defaults.
The same code runs inside the containers (DB_HOST=postgres, MINIO_ENDPOINT=http://minio:9000,
injected by docker-compose) and on the host (values from .env, e.g. localhost:5432 / localhost:9005).
"""
import os
from dotenv import load_dotenv


def get_project_root() -> str:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    while True:
        if os.path.exists(os.path.join(current_dir, ".env")) or os.path.exists(os.path.join(current_dir, "docker-compose.yml")):
            return current_dir
        parent = os.path.dirname(current_dir)
        if parent == current_dir:
            return os.getcwd()
        current_dir = parent


IS_DOCKER = os.path.exists("/opt/airflow")
is_docker = IS_DOCKER  # backwards compatibility
ROOT_DIR = "/opt/airflow" if IS_DOCKER else get_project_root()

# override=False: variables set by docker-compose win over the mounted .env
load_dotenv(dotenv_path=os.path.join(ROOT_DIR, ".env"), override=False)

##################################
### MINIO / S3
##################################
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000" if IS_DOCKER else "http://localhost:9005")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_AUDIT = os.getenv("MINIO_BUCKET_AUDIT", "audit")
BUCKET_DATASOURCE = BUCKET_AUDIT  # kept for backwards compatibility

REQUIRED_BUCKETS = [
    os.getenv("MINIO_BUCKET_LANDING", "landing"),
    os.getenv("MINIO_BUCKET_BRONZE", "bronze"),
    os.getenv("MINIO_BUCKET_SILVER", "silver"),
    os.getenv("MINIO_BUCKET_GOLD", "gold"),
    BUCKET_AUDIT,
]

##################################
### POSTGRES (DW)
##################################
DB_HOST = os.getenv("DB_HOST", "postgres" if IS_DOCKER else "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_DB")
CONSTRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

##################################
### DBT
##################################
DBT_PROJECT_DIR = os.path.join(ROOT_DIR, "pipelines", "dbt_projects")
TARGET_DIR = os.path.join(DBT_PROJECT_DIR, "target")
RUN_RESULTS_PATH = os.path.join(TARGET_DIR, "run_results.json")
MANIFEST_PATH = os.path.join(TARGET_DIR, "manifest.json")

##################################
### LANDING PATHS
##################################
DATASOURCE_DIR = "/mnt/datasource" if IS_DOCKER else os.path.join(ROOT_DIR, "datasource")
DIR_ANP_LANDING_WEEK = os.getenv("DIR_ANP_LANDING_WEEK", os.path.join(DATASOURCE_DIR, "anp", "ult4"))
DIR_ANP_LANDING_MONTH = os.getenv("DIR_ANP_LANDING_MONTH", os.path.join(DATASOURCE_DIR, "anp", "arquivos_fechados"))

##################################
### RFB
##################################
RFB_REF_MONTH = os.getenv("RFB_REF_MONTH", "2026-08")  # YYYY-MM, overridden by the dag_rfb param


def rfb_reference() -> str:
    """Reference month (YYYY-MM) for the RFB pipeline. Set by dag_rfb (param ref_month)."""
    value = os.getenv("RFB_REF_MONTH", RFB_REF_MONTH).strip()
    if len(value) != 7 or value[4] != "-":
        raise EnvironmentError(f"[env_loader] RFB_REF_MONTH must be YYYY-MM, got '{value}'")
    return value


##################################
### PARAMS CALL
##################################
def validate_env(required: dict):
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(f"[env_loader] missing variables: {missing}")
