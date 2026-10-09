import logging

import polars as pl

from pipelines.anp.anp_landing import EXPECTED_COLUMNS, check_schema, normalize_columns

LOG = logging.getLogger("test")


def test_normalize_columns_handles_bom_and_separators():
    assert normalize_columns(["﻿Regiao - Sigla", "CNPJ da Revenda", "Valor de Venda"]) == [
        "regiao_sigla", "cnpj_da_revenda", "valor_de_venda"]


def test_schema_drift_missing_columns_rejects_file():
    df = pl.DataFrame({"regiao_sigla": ["CO"], "produto": ["GLP"]})
    assert check_schema(df, "f.csv", LOG) is None


def test_schema_drift_extra_columns_are_dropped():
    df = pl.DataFrame({c: ["x"] for c in EXPECTED_COLUMNS + ["nova_coluna"]})
    out = check_schema(df, "f.csv", LOG)
    assert out is not None and out.columns == EXPECTED_COLUMNS
