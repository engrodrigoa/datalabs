"""Re-loading an RFB landing table must work when dbt staging views already depend on it."""
import logging
import os

import pytest

DSN = os.getenv("CHECKS_TEST_DSN")


@pytest.mark.skipif(not DSN, reason="set CHECKS_TEST_DSN to run against a real Postgres")
def test_second_load_with_dependent_view():
    from sqlalchemy import create_engine, text

    from pipelines.commons.dw_landing_rfb import preparar_schema_tabela

    engine = create_engine(DSN.replace("postgresql://", "postgresql+psycopg2://", 1))
    ddl = ("CREATE SCHEMA IF NOT EXISTS t_landing; CREATE TABLE IF NOT EXISTS t_landing.estab "
           "(cnpj_basico VARCHAR(8), nome VARCHAR(10), referencia_mes INT)")
    log = logging.getLogger("t")
    with engine.begin() as c:
        c.execute(text("DROP SCHEMA IF EXISTS t_landing CASCADE"))
    try:
        preparar_schema_tabela(engine, "t_landing", "estab", ddl, ["cnpj_basico", "nome"], 202608, log)  # 1st load
        with engine.begin() as c:  # dbt creates the staging view
            c.execute(text("CREATE VIEW t_landing.stg AS SELECT cnpj_basico, nome FROM t_landing.estab"))
        preparar_schema_tabela(engine, "t_landing", "estab", ddl, ["cnpj_basico", "nome"], 202609, log)  # 2nd load
        with engine.begin() as c:
            types = dict(c.execute(text("SELECT column_name, data_type FROM information_schema.columns "
                                        "WHERE table_schema='t_landing' AND table_name='estab'")).all())
        assert types["cnpj_basico"] == "text" and types["nome"] == "text"
    finally:
        with engine.begin() as c:
            c.execute(text("DROP SCHEMA IF EXISTS t_landing CASCADE"))
