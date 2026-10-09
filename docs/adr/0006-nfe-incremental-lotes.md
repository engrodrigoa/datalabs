# ADR 0006 — NF-e: batch-level control, immutable bronze, SQL-only transformation

**Status:** accepted

**Context.** State tax DWs typically land each NF-e XML in a column and run stored procedures that set a
per-row flag (`indi_processamento` 1 → 2) after transforming it. At tens of billions of rows, that `UPDATE`
on the raw table becomes part of the I/O problem, the raw layer stops being a reliable replay source, and
nothing in between is testable.

**Decisions.**

1. **Control per batch, not per row.** Files arriving together form a batch (`ctrl.nfe_lote`).
   `CARREGADO` is the old "1", `PROCESSADO` the old "2", and there is one `UPDATE` per batch. The bronze table
   (`landing_nfe.nfe_xml`) is insert-only. A failed or corrected batch goes back to the queue with
   `ctrl.fn_nfe_reprocessar_lote()`, and the `merge` models make that rerun idempotent.
2. **Ingestion in SQL** (`pg_ls_dir` / `pg_read_file` / `XMLPARSE`). Malformed files are quarantined in
   `ctrl.nfe_arquivo_rejeitado` and do not fail the batch. Resends of the same access key are kept in bronze,
   and silver keeps the latest file.
3. **Parsing with `XMLTABLE` in dbt**, with contracts enforced on silver. Python only orchestrates (Airflow)
   and simulates the source system (`scripts/nfe/gen_nfe.py`).
4. **Participant SCD2 keyed by the note's own attributes.** A `dbt snapshot` would date versions by
   *processing* time, and late notes would create false versions. Instead, `sk = md5(doc | attributes hash)`
   is computed both in the dimension history and in the fact. Each note carries the attributes the taxpayer
   had when it was issued, so the fact gets the correct version without a date-range join, including for
   late arrivals. The snapshot is used where processing time is the right semantics: the cClassTrib reference
   table, which is revised by technical notes.
5. **Tests scoped to the batch in flight** (`where: lote_id in CARREGADO`, and `nfe_filtro_lote()` in singular
   tests). Re-testing billions of historical rows on every run is not viable. Business anomalies are `warn` with
   `store_failures` into `dq_nfe`, because an authorized NF-e is a legal fact: the DW flags it, it does not reject it.

**Consequences.** Full rebuild = `dbt build --full-refresh` (replay from bronze). At production scale, bronze
and facts would be range-partitioned by month (not done here: dbt-postgres has no native partitioning
materialization), and the participant dimension would be rebuilt only for affected documents.
