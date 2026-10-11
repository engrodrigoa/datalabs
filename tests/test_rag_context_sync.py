"""Incremental vector store sync: deterministic ids + set-difference plan (no DB, no model)."""
from datetime import date

import polars as pl

from pipelines.commons.ai.context_sync import build_chunks, chunk_id, plan_sync

MODEL = "intfloat/multilingual-e5-small"


def _gold(**over):
    row = dict(id_fato=1, cnpj="00.000.000/0001-00", revenda="POSTO A", bandeira="BRANCA", municipio="GOIANIA",
               estado_sigla="GO", bairro="CENTRO", cep="74000-000", produto="GASOLINA", valor_venda=6.29,
               data_coleta=date(2026, 10, 1))
    row.update(over)
    return pl.DataFrame([row])


def test_chunk_id_is_deterministic_and_tracks_the_text():
    assert chunk_id("1", "abc") == chunk_id("1", "abc")
    assert chunk_id("1", "abc") != chunk_id("1", "abd")        # text changed -> new id (old one becomes stale)
    assert chunk_id("1", "abc").startswith("1:")               # lineage to the gold fact


def test_build_chunks_same_input_same_ids():
    a = build_chunks(_gold())["chunk_id"].to_list()
    b = build_chunks(_gold())["chunk_id"].to_list()
    assert a == b
    assert build_chunks(_gold(valor_venda=6.39))["chunk_id"].to_list() != a


def test_incremental_plan_embeds_only_the_delta_and_removes_stale():
    p = plan_sync({"a", "b", "c"}, {"a", "b", "x"}, model=MODEL, stored_models={MODEL})
    assert p.mode == "incremental"
    assert p.new_ids == {"c"} and p.stale_ids == {"x"} and p.kept == 2


def test_in_sync_store_is_a_noop():
    p = plan_sync({"a", "b"}, {"a", "b"}, model=MODEL, stored_models={MODEL})
    assert p.mode == "incremental" and not p.new_ids and not p.stale_ids


def test_model_change_forces_full_rebuild():
    p = plan_sync({"a"}, {"a"}, model=MODEL, stored_models={"intfloat/multilingual-e5-base"})
    assert p.mode == "full" and p.new_ids == {"a"} and "model changed" in p.reason


def test_empty_store_and_explicit_flag_are_full():
    assert plan_sync({"a"}, set(), model=MODEL, stored_models=set()).mode == "full"
    assert plan_sync({"a"}, {"a"}, model=MODEL, stored_models={MODEL}, force_full=True).mode == "full"
