"""
Shared landing logic for ANP weekly/monthly CSVs -> bronze.anp_landing_* (Postgres).

Observability contract of this step:
  * schema drift check per file (missing columns -> file rejected; extra columns -> dropped + warned)
  * volumes reported to the step run: files_total/ok/failed, rows_in, rows_out
  * exit codes: 0 ok (or partial, alerted by the runner) | 1 nothing could be loaded | 99 nothing to do
  * idempotent: re-loading a file first deletes the rows previously loaded from it
"""
from __future__ import annotations

import glob
import gc
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
from sqlalchemy import text

from pipelines.commons.anp_metrics import log_batch_metrics
from pipelines.commons.dw_client import get_sqla_engine, test_pg_connection
from pipelines.commons.logger import get_logger, log_event
from pipelines.commons.pg_copy import copy_dataframe
from pipelines.observability import current_step

SCHEMA = "bronze"

EXPECTED_COLUMNS = [
    "regiao_sigla", "estado_sigla", "municipio", "revenda", "cnpj_da_revenda", "nome_da_rua",
    "numero_rua", "complemento", "bairro", "cep", "produto", "data_da_coleta", "valor_de_venda",
    "valor_de_compra", "unidade_de_medida", "bandeira",
]


def normalize_columns(columns: list[str]) -> list[str]:
    return [c.lstrip("﻿").strip().lower().replace(" - ", "_").replace(" ", "_") for c in columns]


def read_anp_csv(path: str, logger: logging.Logger) -> pl.DataFrame:
    kwargs = dict(separator=";", infer_schema_length=0, ignore_errors=True)
    try:
        df = pl.read_csv(path, encoding="utf8", **kwargs)
    except Exception as exc:  # ANP files are not always valid UTF-8
        log_event(logger, "encoding_fallback", f"{os.path.basename(path)}: {exc}", level=logging.WARNING,
                  file=os.path.basename(path))
        df = pl.read_csv(path, encoding="utf8-lossy", **kwargs)
    return df.rename(dict(zip(df.columns, normalize_columns(df.columns))))


def check_schema(df: pl.DataFrame, file_name: str, logger: logging.Logger) -> pl.DataFrame | None:
    """Returns the frame restricted to the contract columns, or None when required columns are missing."""
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    extra = [c for c in df.columns if c not in EXPECTED_COLUMNS]
    if missing:
        log_event(logger, "schema_drift_missing_columns", f"{file_name}: missing {missing} -> file rejected",
                  level=logging.ERROR, file=file_name, missing=missing, found=df.columns)
        return None
    if extra:
        log_event(logger, "schema_drift_extra_columns", f"{file_name}: extra columns ignored {extra}",
                  level=logging.WARNING, file=file_name, extra=extra)
    return df.select(EXPECTED_COLUMNS)


def latest_weekly_reference(engine) -> str | None:
    with engine.connect() as conn:
        value = conn.execute(text(
            "SELECT MAX(data_ref) FROM ctrl.anp_metadata_semanal WHERE status = 'SUCESSO'")).scalar()
    return value.isoformat() if value else None


def run_landing(kind: str) -> int:
    """kind: 'semanal' | 'mensal'. Returns the process exit code."""
    from pipelines.commons.env_loader import DIR_ANP_LANDING_MONTH, DIR_ANP_LANDING_WEEK, validate_env

    table = f"anp_landing_{kind}"
    logger = get_logger(f"anp_landing_{kind}")
    step = current_step()

    input_dir = Path(DIR_ANP_LANDING_WEEK if kind == "semanal" else DIR_ANP_LANDING_MONTH)
    validate_env({"input_dir": str(input_dir)})
    test_pg_connection()
    engine = get_sqla_engine()

    pattern = str(input_dir / "*.csv") if kind == "semanal" else str(input_dir / "**" / "*.csv")
    pending = sorted(f for f in glob.glob(pattern, recursive=True) if not os.path.basename(f).startswith("OK_"))
    step.set(files_total=len(pending), source_dir=input_dir.as_posix())

    if not pending:
        log_event(logger, "no_new_data", f"no pending files in {input_dir.as_posix()}", level=logging.WARNING)
        return 99

    # Weekly files keep the same name every week ("ultimas-4-semanas-*.csv"). Tagging the
    # load with the survey reference keeps previous weeks in bronze instead of overwriting them.
    weekly_ref = latest_weekly_reference(engine) if kind == "semanal" else None
    ingestion_ts = datetime.now()
    log_event(logger, "landing_started", f"{len(pending)} pending file(s) -> {SCHEMA}.{table}",
              files=len(pending), weekly_ref=weekly_ref)

    loaded_frames = []
    for path in pending:
        name = os.path.basename(path)
        load_key = f"{name}#ref={weekly_ref}" if weekly_ref else name
        try:
            raw = read_anp_csv(path, logger)
            step.add(rows_in=raw.height)
            df = check_schema(raw, name, logger)
            if df is None:
                step.add(files_failed=1, rows_rejected=raw.height)
                continue

            df = df.with_columns(pl.lit(load_key).alias("arquivo"), pl.lit(ingestion_ts).alias("ingestion_timestamp"))

            with engine.begin() as conn:
                deleted = conn.execute(text(f"DELETE FROM {SCHEMA}.{table} WHERE arquivo = :k"), {"k": load_key}).rowcount
            written = copy_dataframe(df, engine, SCHEMA, table)

            os.rename(path, os.path.join(os.path.dirname(path), f"OK_{name}"))
            step.add(files_ok=1, rows_out=written)
            log_event(logger, "file_loaded", f"{name}: {written:,} rows (replaced {deleted:,})",
                      file=name, rows=written, replaced=deleted, load_key=load_key)
            loaded_frames.append(df)
        except Exception as exc:
            step.add(files_failed=1)
            log_event(logger, "file_failed", f"{name}: {exc}", level=logging.ERROR, file=name, error=str(exc)[:500])
        finally:
            gc.collect()

    if loaded_frames:
        log_batch_metrics(pl.concat(loaded_frames), total_files=len(loaded_frames))

    ok, failed = step.counters.get("files_ok", 0), step.counters.get("files_failed", 0)
    log_event(logger, "landing_finished", f"loaded {ok}/{len(pending)} file(s), {failed} failed",
              files_ok=ok, files_failed=failed, rows_out=step.counters.get("rows_out", 0))
    return 1 if ok == 0 else 0


if __name__ == "__main__":
    sys.exit(run_landing(sys.argv[1] if len(sys.argv) > 1 else "semanal"))
