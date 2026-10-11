"""Pure logic of the incremental vector store sync (no DB, no model: unit-testable).

The vector store is a projection of the gold layer. Each chunk has a deterministic id,
    chunk_id = <id_fato>:<md5(chunk_text)[:16]>
so the sync is a set difference between "what gold says should exist" and "what the store has":
    new   = desired - existing  -> embed + insert
    stale = existing - desired  -> delete (fact removed, or its text changed and got a new id)
Re-running it converges to the same state (idempotent): a run that dies half-way is fixed
by the next one, with no flags or checkpoints.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

import polars as pl

CHUNK_TEMPLATE_VERSION = "v1"   # bump when the chunk text changes on purpose (forces new ids)


def chunk_id(source_id: str, text: str) -> str:
    # md5 is stable across Polars/Python versions (pl.hash is not): ids survive upgrades
    digest = hashlib.md5(f"{CHUNK_TEMPLATE_VERSION}|{text}".encode("utf-8")).hexdigest()[:16]
    return f"{source_id}:{digest}"


def build_chunks(df: pl.DataFrame) -> pl.DataFrame:
    """Gold rows -> one chunk per fact with text, lineage, metadata and deterministic id."""
    out = df.with_columns(
        pl.concat_str([
            pl.lit("O posto '"), pl.col("revenda"),
            pl.lit("' da bandeira "), pl.col("bandeira"),
            pl.lit(", localizado em "), pl.col("municipio"), pl.lit(" - "), pl.col("estado_sigla"),
            pl.lit(" (Bairro: "), pl.col("bairro"),
            pl.lit(", CEP: "), pl.col("cep"),
            pl.lit(", CNPJ: "), pl.col("cnpj"),
            pl.lit("), vendeu o produto "), pl.col("produto"),
            pl.lit(" pelo valor de R$ "), pl.col("valor_venda").cast(pl.Utf8),
            pl.lit(" na data "), pl.col("data_coleta").cast(pl.Utf8), pl.lit("."),
        ], ignore_nulls=True).alias("chunk_text"),
        pl.col("id_fato").cast(pl.Utf8).alias("source_id"),
    ).filter(pl.col("chunk_text").is_not_null() & (pl.col("chunk_text") != ""))

    return out.with_columns(
        pl.struct(["source_id", "chunk_text"])
          .map_elements(lambda r: chunk_id(r["source_id"], r["chunk_text"]), return_dtype=pl.Utf8)
          .alias("chunk_id"),
        pl.struct(["cnpj", "cep", "estado_sigla", "municipio"])
          .map_elements(lambda x: json.dumps(x, ensure_ascii=False), return_dtype=pl.Utf8)
          .alias("metadata"),
    ).unique(subset=["chunk_id"], keep="first", maintain_order=True)


@dataclass(frozen=True)
class SyncPlan:
    mode: str                 # "full" | "incremental"
    reason: str
    new_ids: frozenset
    stale_ids: frozenset
    kept: int


def plan_sync(desired_ids: set, existing_ids: set, *, model: str, stored_models: set,
              force_full: bool = False) -> SyncPlan:
    """Decide what to embed/delete. A model change forces a full rebuild: vectors from different
    models are not comparable, so they can never share a table (and may not even have the same size)."""
    reason = None
    if force_full:
        reason = "full refresh requested"
    elif stored_models - {model}:
        reason = f"embedding model changed ({', '.join(sorted(stored_models))} -> {model})"
    elif not existing_ids:
        reason = "empty vector store"

    if reason:
        return SyncPlan("full", reason, frozenset(desired_ids), frozenset(existing_ids), 0)
    new = frozenset(desired_ids - existing_ids)
    stale = frozenset(existing_ids - desired_ids)
    return SyncPlan("incremental", "delta against the current store", new, stale,
                    len(existing_ids) - len(stale))
