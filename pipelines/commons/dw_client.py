import logging
import sys
import time

import psycopg2
from sqlalchemy import create_engine

#=============================================================================================================
#===================================
# env loader - connection setup
#===================================
from pipelines.commons.env_loader import (
    validate_env, DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, CONSTRING
)
from pipelines.commons.logger import get_logger, log_event
logger = get_logger("DW_CLIENT")
#=============================================================================================================


def get_pg_connection():
    return psycopg2.connect(CONSTRING, connect_timeout=10)

def get_sqla_engine():
    # explicit driver: SQLAlchemy 2.1 defaults "postgresql://" to psycopg3, which is not installed
    return create_engine(CONSTRING.replace("postgresql://", "postgresql+psycopg2://", 1), pool_pre_ping=True)

def test_pg_connection():
    """Fail fast before any work: ONE event on success, ONE event (with the real cause) on failure."""
    validate_env({
        "DB_HOST": DB_HOST, "DB_PORT": DB_PORT, "DB_USER": DB_USER,
        "DB_PASS": DB_PASS, "DB_NAME": DB_NAME, "CONSTRING": CONSTRING,
    })
    started = time.perf_counter()
    conn = None
    try:
        conn = get_pg_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1;")
    except Exception as exc:  # noqa: BLE001  (psycopg2.Error or anything unexpected: same outcome)
        # the cause goes in the ERROR line itself: at DEBUG it would never reach the task log
        log_event(logger, "db_connection_failed", f"cannot connect to {DB_HOST}:{DB_PORT}/{DB_NAME}: {str(exc).strip()}",
                  level=logging.ERROR, db_host=DB_HOST, db_name=DB_NAME, error_type=type(exc).__name__)
        sys.exit(1)
    finally:
        if conn:
            conn.close()
    log_event(logger, "db_connected", f"connected to {DB_HOST}/{DB_NAME}", level=logging.DEBUG,
              db_host=DB_HOST, db_name=DB_NAME, latency_ms=round((time.perf_counter() - started) * 1000, 1))
    return True
