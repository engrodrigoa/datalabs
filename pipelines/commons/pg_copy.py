"""Bulk load a Polars DataFrame into Postgres with COPY (single shared implementation)."""
from __future__ import annotations

import io

import polars as pl


def copy_dataframe(df: pl.DataFrame, engine, schema: str, table: str) -> int:
    """COPY `df` into schema.table. Returns the number of rows written. Raises on failure (after rollback)."""
    if df.is_empty():
        return 0

    buffer = io.StringIO()
    df.write_csv(buffer)
    buffer.seek(0)

    columns = ", ".join(f'"{c}"' for c in df.columns)
    sql = f"COPY {schema}.{table} ({columns}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"

    conn = engine.raw_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.copy_expert(sql, buffer)
        conn.commit()
        return df.height
    except Exception:
        conn.rollback()
        raise
    finally:
        if cur is not None:
            cur.close()
        conn.close()
