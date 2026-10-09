"""Batch profile of an ANP landing load: ONE structured event + step metrics (audit.pipeline_step_runs.metrics).

Replaces the old multi-line banner report: in Loki every banner line was a separate, unqueryable entry.
Now `event="batch_profile"` carries the numbers as fields, and the same profile is persisted with the
step run, so price/coverage drift can be checked in SQL later.
"""
import polars as pl

from pipelines.commons.logger import get_logger, log_event
from pipelines.observability import current_step

logger = get_logger("anp_batch_profile")


def _find(cols: dict, *needles: str):
    return next((v for k, v in cols.items() if any(n in k for n in needles)), None)


def build_batch_profile(df_batch: pl.DataFrame, total_files: int) -> dict:
    cols = {c.lower(): c for c in df_batch.columns}
    col_bandeira = _find(cols, "bandeira")
    col_municipio = _find(cols, "municipio", "município")
    col_cnpj = _find(cols, "cnpj")
    col_produto = _find(cols, "produto")
    col_preco = _find(cols, "preco_de_venda", "valor_de_venda", "preco")

    profile = {
        "files": total_files,
        "rows": df_batch.height,
        "bandeiras": df_batch[col_bandeira].n_unique() if col_bandeira else None,
        "municipios": df_batch[col_municipio].n_unique() if col_municipio else None,
        "cnpjs": df_batch[col_cnpj].n_unique() if col_cnpj else None,
        "avg_price_by_product": {},
        "rows_by_product": {},
    }
    if col_produto and col_preco:
        res = (
            df_batch.with_columns(
                pl.col(col_preco).cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64, strict=False).alias("_preco")
            )
            .group_by(col_produto)
            .agg(pl.col("_preco").mean().round(3).alias("avg"), pl.len().alias("rows"))
            .sort(col_produto)
        )
        for row in res.iter_rows(named=True):
            profile["avg_price_by_product"][str(row[col_produto])] = row["avg"]
            profile["rows_by_product"][str(row[col_produto])] = row["rows"]
    return profile


def log_batch_metrics(df_batch: pl.DataFrame, total_files: int) -> dict | None:
    """Best effort: a profiling failure must never fail the load."""
    try:
        profile = build_batch_profile(df_batch, total_files)
    except Exception as exc:  # noqa: BLE001
        log_event(logger, "batch_profile_failed", f"could not profile batch: {exc}", level=30)
        return None
    prices = ", ".join(f"{p}={v:.3f}" for p, v in profile["avg_price_by_product"].items() if v is not None)
    log_event(
        logger, "batch_profile",
        f"{profile['rows']:,} rows | {profile['municipios']} cities | {profile['cnpjs']} cnpjs | avg price: {prices}",
        **profile,
    )
    current_step().add(profile=profile)
    return profile
