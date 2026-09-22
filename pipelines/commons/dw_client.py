import sys
import psycopg2
from sqlalchemy import create_engine
from functools import wraps

#=============================================================================================================
#===================================
# env loader - connection setup
#===================================
from pipelines.commons.env_loader import (
    validate_env, DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, CONSTRING
)
from pipelines.commons.logger import get_logger
logger = get_logger("DW_CLIENT")
#=============================================================================================================


def get_pg_connection():
    return psycopg2.connect(CONSTRING)

def get_sqla_engine():
    return create_engine(CONSTRING)

def test_pg_connection():
    logger.info("#" * 60)
    db_env_vars = {
        "DB_HOST": DB_HOST, "DB_PORT": DB_PORT, 
        "DB_USER": DB_USER, "DB_PASS": DB_PASS, 
        "DB_NAME": DB_NAME, "CONSTRING": CONSTRING
    }
    
    logger.info("db env vars validation")
    validate_env(db_env_vars)
    logger.info("db env vars found successfully")

    conn = None
    try:
        logger.info(f"attempting to connect to host - [{DB_HOST}]")
        conn = get_pg_connection()
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1;")
            
        logger.info(f"handshake success [{DB_NAME}]")
        return True

    except psycopg2.Error as pg_err:
        logger.error("FAILED CONNECTING ON DB (FAIL-FAST)")
        logger.debug(f"DATAILS {str(pg_err).strip()}")
        logger.critical("ABORTING RUN DUE TO DB CONNECTION FAILURE")
        sys.exit(1)

    except Exception as e:
        logger.error(f"UNEXPECTED ERROR  {e}")
        logger.critical("ABORTING RUN DUE TO DB CONNECTION FAILURE")
        sys.exit(1)

    finally:
        if conn:
            conn.close()
            logger.info("[!] connection test finished")
            logger.info("#" * 60)