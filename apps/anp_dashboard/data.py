"""
Data access of the ANP dashboard: reads ONLY the gold serving marts, as the read-only role anp_app_ro.

Configuration (env > repo .env > defaults), on purpose independent of pipelines/commons so the app can
later be deployed on its own:
  ANP_APP_DB_HOST (localhost)            ANP_APP_DB_PORT (DB_PORT, 5432)   ANP_APP_DB_NAME (DB_DB, datalab)
  ANP_APP_DB_USER (anp_app_ro)           ANP_APP_DB_PASSWORD (required)
The host does NOT fall back to DB_HOST: in this repo DB_HOST is often the container-side name ("postgres",
compose network), which the app, running on the host, cannot resolve. DB_PORT is the published host port.
Every value that comes from the UI is a bind parameter (CLAUDE.md: SQL injection rule).
"""
from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine

REPO_ROOT = Path(__file__).resolve().parents[2]

SQL_POSTOS = """
SELECT uf, municipio, bairro, revenda, bandeira, cnpj, endereco, produto, unidade_de_medida,
       data_coleta, valor_venda, data_coleta_anterior, valor_venda_anterior, variacao,
       _source_loaded_at
FROM gold.ag_anp_preco_posto_atual
"""

# Scope = Brasil (both NULL), a UF, or a city. Never below the city (see the mart's header).
SQL_HISTORICO = """
SELECT data_coleta, produto,
       SUM(qtd_postos)::bigint            AS qtd_postos,
       SUM(soma_valor_venda)::float8      AS soma_valor_venda,
       MIN(preco_min)::float8             AS preco_min,
       MAX(preco_max)::float8             AS preco_max
FROM gold.ag_anp_preco_municipio_diario
WHERE produto = ANY(:produtos)
  AND data_coleta >= :desde
  AND (CAST(:uf AS text) IS NULL OR uf = :uf)
  AND (CAST(:municipio AS text) IS NULL OR municipio = :municipio)
GROUP BY data_coleta, produto
ORDER BY data_coleta, produto
"""


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:  # optional: plain env vars work without it
        return
    load_dotenv(REPO_ROOT / ".env", override=False)


def db_url() -> URL:
    _load_dotenv()
    password = os.getenv("ANP_APP_DB_PASSWORD")
    if not password:
        raise RuntimeError("ANP_APP_DB_PASSWORD is not set (make env adds it to .env; then make db-bootstrap)")
    return URL.create(
        "postgresql+psycopg2",
        username=os.getenv("ANP_APP_DB_USER", "anp_app_ro"),
        password=password,
        host=os.getenv("ANP_APP_DB_HOST") or "localhost",
        port=int(os.getenv("ANP_APP_DB_PORT") or os.getenv("DB_PORT") or 5432),
        database=os.getenv("ANP_APP_DB_NAME") or os.getenv("DB_DB") or "datalab",
    )


def get_engine() -> Engine:
    return create_engine(db_url(), pool_pre_ping=True, pool_size=2, max_overflow=2)


def _frame(engine: Engine, sql: str, params: dict | None = None) -> pl.DataFrame:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        rows = result.fetchall()
        cols = list(result.keys())
    if not rows:
        return pl.DataFrame({c: [] for c in cols})
    return pl.DataFrame([tuple(r) for r in rows], schema=cols, orient="row", infer_schema_length=None)


def carregar_postos(engine: Engine) -> pl.DataFrame:
    """The whole 'current picture' (real data: ~20k stations x ~7 products): small enough to filter in memory."""
    df = _frame(engine, SQL_POSTOS)
    if df.is_empty():
        return df
    return df.with_columns(pl.col(["valor_venda", "valor_venda_anterior", "variacao"]).cast(pl.Float64))


def carregar_historico(engine: Engine, produtos: list[str], desde: dt.date,
                       uf: str | None = None, municipio: str | None = None) -> pl.DataFrame:
    return _frame(engine, SQL_HISTORICO,
                  {"produtos": list(produtos), "desde": desde, "uf": uf, "municipio": municipio})
