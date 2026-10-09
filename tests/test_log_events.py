"""The legacy multi-line banners became single structured events."""
import logging

import polars as pl
import pytest

from pipelines.commons import anp_metrics


def test_batch_profile_is_one_event_with_numbers(monkeypatch):
    events = []
    monkeypatch.setattr(anp_metrics, "log_event", lambda _l, event, msg, **kw: events.append((event, msg, kw)))
    df = pl.DataFrame({
        "bandeira": ["A", "B", "A"], "municipio": ["X", "Y", "Y"], "cnpj": ["1", "2", "2"],
        "produto": ["GASOLINA", "GASOLINA", "ETANOL"], "valor_de_venda": ["6,50", "6,70", "4,30"],
    })
    profile = anp_metrics.log_batch_metrics(df, total_files=1)
    assert [e[0] for e in events] == ["batch_profile"]
    assert profile["rows"] == 3 and profile["cnpjs"] == 2
    assert profile["avg_price_by_product"] == {"ETANOL": 4.3, "GASOLINA": 6.6}


def test_profile_failure_never_breaks_the_load(monkeypatch):
    monkeypatch.setattr(anp_metrics, "build_batch_profile", lambda *a, **k: 1 / 0)
    assert anp_metrics.log_batch_metrics(pl.DataFrame({"a": [1]}), 1) is None


def test_db_failure_logs_the_real_cause_at_error(monkeypatch, caplog):
    from pipelines.commons import dw_client

    def boom():
        raise RuntimeError('database "nope" does not exist')

    monkeypatch.setattr(dw_client, "get_pg_connection", boom)
    monkeypatch.setattr(dw_client, "validate_env", lambda _vars: None)
    seen = []
    monkeypatch.setattr(dw_client, "log_event", lambda _l, event, msg, level=logging.INFO, **kw: seen.append((event, msg, level)))
    with pytest.raises(SystemExit):
        dw_client.test_pg_connection()
    assert seen[0][0] == "db_connection_failed" and seen[0][2] == logging.ERROR
    assert 'database "nope" does not exist' in seen[0][1]
