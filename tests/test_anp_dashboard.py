"""
ANP Streamlit dashboard (apps/anp_dashboard): the Polars transforms, and its SQL run as anp_app_ro.

The SQL tests run only when ANP_APP_SQL_DSN is set (CI, after dbt build tag:anp), e.g.
  ANP_APP_SQL_DSN=postgresql://anp_app_ro:pwd@localhost:5432/datalab pytest tests/test_anp_dashboard.py
"""
import datetime as dt
import os

import polars as pl
import pytest

from apps.anp_dashboard import logic

D = dt.date


def _postos():
    rows = [
        # uf, municipio, bairro, revenda, bandeira, cnpj, produto, data, valor, variacao
        ("GO", "GOIANIA", "CENTRO", "POSTO A", "IPIRANGA", "11111111000111", "GASOLINA", D(2026, 10, 22), 6.00, 0.10),
        ("GO", "GOIANIA", "CENTRO", "POSTO A", "IPIRANGA", "11111111000111", "ETANOL", D(2026, 10, 22), 4.00, None),
        ("GO", "GOIANIA", "SETOR SUL", "POSTO B", "RAIZEN", "22222222000122", "GASOLINA", D(2026, 10, 20), 6.40, -0.10),
        ("GO", "ANAPOLIS", "CENTRO", "POSTO C", "BRANCA", "33333333000133", "GASOLINA", D(2026, 9, 1), 5.00, 0.0),
        ("SP", "SAO PAULO", "SE", "POSTO D", "IPIRANGA", "44444444000144", "GASOLINA", D(2026, 10, 21), 6.80, 0.0),
    ]
    cols = ["uf", "municipio", "bairro", "revenda", "bandeira", "cnpj", "produto", "data_coleta", "valor_venda",
            "variacao"]
    return pl.DataFrame(rows, schema=cols, orient="row").with_columns(pl.lit("RUA A, 1").alias("endereco"))


def test_janela_drops_stale_station_prices():
    atual = logic.aplicar_janela(_postos(), 30)
    assert "33333333000133" not in atual["cnpj"].to_list()          # 01/09, 51 days before 22/10
    assert atual.height == 4


def test_filters_cascade_and_search_by_name_or_cnpj():
    df = _postos()
    assert logic.aplicar_filtros(df, uf="GO", municipio="GOIANIA", bairro="CENTRO")["cnpj"].unique().to_list() \
        == ["11111111000111"]
    assert logic.aplicar_filtros(df, busca="posto b").height == 1
    assert logic.aplicar_filtros(df, busca="4444.4444")["uf"].to_list() == ["SP"]
    assert logic.aplicar_filtros(df, bandeiras=["RAIZEN"]).height == 1


def test_kpis_average_latest_prices_in_fixed_product_order():
    k = logic.kpis(logic.aplicar_janela(_postos(), 30))
    assert k["produto"].to_list() == ["GASOLINA", "ETANOL"]
    gas = k.row(0, named=True)
    assert gas["preco_medio"] == pytest.approx((6.00 + 6.40 + 6.80) / 3)
    assert gas["qtd_postos"] == 3 and gas["preco_min"] == 6.00 and gas["preco_max"] == 6.80


def test_matriz_city_level_pivots_products_and_sorts_cheapest_first():
    m = logic.matriz(logic.aplicar_janela(_postos(), 30), "Cidade", "GASOLINA")
    assert m.columns == ["uf", "municipio", "qtd_postos", "ultima_coleta", "GASOLINA", "ETANOL"]
    assert m["municipio"].to_list() == ["GOIANIA", "SAO PAULO"]
    goiania = m.row(0, named=True)
    assert goiania["GASOLINA"] == pytest.approx(6.20) and goiania["ETANOL"] == 4.00
    assert goiania["qtd_postos"] == 2 and goiania["ultima_coleta"] == D(2026, 10, 22)


def test_matriz_station_level_compares_with_the_city_average():
    m = logic.matriz(logic.aplicar_filtros(_postos(), municipio="GOIANIA"), "Posto", "GASOLINA")
    vs = dict(zip(m["revenda"], m["vs_media_cidade"]))
    assert vs["POSTO A"] == pytest.approx((6.00 / 6.20 - 1) * 100)
    assert vs["POSTO B"] == pytest.approx((6.40 / 6.20 - 1) * 100)


def test_historico_weekly_rollup_is_exact_not_an_average_of_averages():
    diario = pl.DataFrame({
        "data_coleta": [D(2026, 10, 19), D(2026, 10, 20)],           # same ISO week (Mon, Tue)
        "produto": ["GASOLINA", "GASOLINA"],
        "qtd_postos": [1, 3],
        "soma_valor_venda": [6.0, 3 * 7.0],
        "preco_min": [6.0, 6.5],
        "preco_max": [6.0, 7.5],
    })
    sem = logic.historico(diario, "Semana")
    assert sem.height == 1
    r = sem.row(0, named=True)
    assert r["preco_medio"] == pytest.approx(27.0 / 4)                 # not (6 + 7) / 2
    assert (r["preco_min"], r["preco_max"], r["qtd_postos"]) == (6.0, 7.5, 4)
    assert logic.historico(diario, "Dia")["preco_medio"].to_list() == [6.0, 7.0]


def test_colour_follows_the_product_not_its_rank():
    assert logic.cor_produto("ETANOL") == logic.CORES[logic.PRODUTOS.index("ETANOL")]
    assert logic.cor_produto("PRODUTO NOVO") == logic.COR_OUTROS
    assert logic.ordenar_produtos(["GNV", "XYZ", "GASOLINA"]) == ["GASOLINA", "GNV", "XYZ"]


# ------------------------------------------------------------------ SQL as the app's read-only role
DSN = os.getenv("ANP_APP_SQL_DSN")


@pytest.fixture(scope="module")
def engine():
    if not DSN:
        pytest.skip("ANP_APP_SQL_DSN not set")
    from sqlalchemy import create_engine
    # same driver as the app (psycopg2); recent SQLAlchemy maps a bare postgresql:// to psycopg 3
    return create_engine(DSN.replace("postgresql://", "postgresql+psycopg2://", 1))


def test_app_reads_the_current_picture_as_anp_app_ro(engine):
    from apps.anp_dashboard import data
    postos = data.carregar_postos(engine)
    assert postos.height > 0 and postos["valor_venda"].dtype == pl.Float64
    m = logic.matriz(logic.aplicar_janela(postos, 30), "Bairro", "GASOLINA")
    assert m.height > 0


def test_app_history_query_scopes_brasil_uf_and_city(engine):
    from apps.anp_dashboard import data
    desde = D(2000, 1, 1)
    brasil = data.carregar_historico(engine, ["GASOLINA"], desde)
    go = data.carregar_historico(engine, ["GASOLINA"], desde, uf="GO")
    goiania = data.carregar_historico(engine, ["GASOLINA"], desde, uf="GO", municipio="GOIANIA")
    assert brasil["qtd_postos"].sum() > go["qtd_postos"].sum() > goiania["qtd_postos"].sum() > 0
    assert logic.historico(goiania, "Semana")["preco_medio"].null_count() == 0


def test_anp_app_ro_cannot_read_outside_the_serving_marts(engine):
    from sqlalchemy import text
    from sqlalchemy.exc import ProgrammingError
    with pytest.raises(ProgrammingError, match="permission denied"):
        with engine.connect() as conn:
            conn.execute(text("SELECT 1 FROM gold.ft_anp_combustiveis LIMIT 1"))


# ------------------------------------------------------------------ configuration and the sample guard
def test_app_host_ignores_the_container_side_db_host(monkeypatch):
    from apps.anp_dashboard import data
    monkeypatch.setattr(data, "_load_dotenv", lambda: None)
    monkeypatch.setenv("ANP_APP_DB_PASSWORD", "x")
    monkeypatch.setenv("DB_HOST", "postgres")          # what the services see inside the compose network
    monkeypatch.delenv("ANP_APP_DB_HOST", raising=False)
    monkeypatch.setenv("DB_PORT", "5433")
    url = data.db_url()
    assert (url.host, url.port, url.username) == ("localhost", 5433, "anp_app_ro")
    monkeypatch.setenv("ANP_APP_DB_HOST", "lambda4")
    assert data.db_url().host == "lambda4"


def test_sample_guard_refuses_over_real_loads_unless_forced():
    import importlib.util
    from pathlib import Path
    path = Path(__file__).resolve().parents[1] / "scripts" / "anp_sample_guard.py"
    spec = importlib.util.spec_from_file_location("anp_sample_guard", path)
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    assert guard.decide(0, 0, force=False)[0] is True
    ok, msg = guard.decide(9, 0, force=False)
    assert ok is False and "REPLACE the real weekly load" in msg
    assert guard.decide(0, 3, force=False)[0] is False
    assert guard.decide(9, 3, force=True)[0] is True
