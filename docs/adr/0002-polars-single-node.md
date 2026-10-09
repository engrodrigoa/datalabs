# ADR 0002 — Polars on a single node instead of Spark

**Status:** accepted

The largest dataset (RFB, ~60 M establishments) fits a single machine when processed in batches
(`iter_batches` + Parquet). Polars gives columnar speed without a cluster to operate, which mirrors the
reality of most companies' data volumes. Memory is bounded with batch sizes, `POLARS_MAX_THREADS` and
`max_active_tasks` in the DAG. Spark remains an option if a dataset outgrows one node.
