# ADR 0011 — LC 214 serving tables: upsert by content hash, a projection of the lake

**Status:** accepted (step 4a: `pipelines/rag/lc214/lc04_load.py`, task `load_postgres` in `dag_rag_lc214`);
the router (4b) is [ADR 0012](0012-lc214-question-router.md), the embeddings (4c) [ADR 0013](0013-lc214-vector-search-and-context.md).

**Context.** ADR 0010 made the lake the system of record and Postgres a serving projection of the current law
version. The router answers citations ("o que diz o art. 54?") and revoked/vetoed provisions by SQL, so the
provisions (4,223) and the articles (580) must be in Postgres, and stay equal to silver when the law changes.
Two ways to keep a table equal to a source: replace it (`TRUNCATE` + `INSERT`) or reconcile it (upsert + delete
the rows that left). The table is read while it is loaded, and an amendment changes a few dozen provisions, not
the whole law.

**Decisions.**

1. **Upsert plus orphan delete, in one transaction.** `INSERT ... ON CONFLICT DO UPDATE` from a staging table,
   then `DELETE` of the rows absent from silver. Readers keep seeing the previous version until the commit (MVCC)
   and are never blocked. `TRUNCATE` takes an `ACCESS EXCLUSIVE` lock for the whole load. Measured on the lab
   Postgres 16 with the 4,223 provisions, a reader with `lock_timeout = 2s` during an uncommitted load: upsert,
   read the previous 4,222 rows in 2 ms; truncate, blocked until the timeout (`LockNotAvailable`).
2. **`row_hash` = md5 of the content, not of the version stamp nor of the position.** Only rows whose content
   changed are rewritten, so the step measures what an amendment did: `inserted`, `updated` (text or status),
   `deleted` (left the text), `restamped` (same content, new `source_sha256`/`parser_version` or new
   `ordem`/`pagina`). Position is not content: inserting an art. 54-A shifts the reading order of every later
   provision. Measured on the real table: the insertion shifted the `ordem` of 3,590 provisions and the load
   reported `+1 ~0 -0` (with position in the hash it would have been ~3,590 "updated").
3. **Guards before writing.** `silver_inconsistent`: lc02 and lc03 outputs must carry the same law and parser
   version (a load between a re-parse and its re-chunk would serve provisions and articles that disagree).
   `mass_delete_blocked`: more than `LC214_MAX_DELETE_PCT` (5%) of the served rows leaving at once fails the run
   and rolls back; `--allow-mass-delete` for a deliberate change. Measured: a parse that lost 10% of the
   provisions was blocked (424 of 4,222) and the table stayed intact.
4. **Exit 99 when nothing changed**, like every step of the DAG: a weekly run with the same law and parser
   writes nothing and says so (`lc214_load_up_to_date`).
5. **Two checks after the load** (`checks.yml`): `lc214_serving_parity` (served provisions = rows of the last
   successful parse, tolerance 0) and `lc214_serving_single_version` (at most one law version in the table: the
   router must never answer from a mix of two laws). Both verified to fail on a missing row and on a mixed table.
6. **Lists as `jsonb`** (`notas`, `alterado_por`, `tipo_alteracao`): COPY in CSV carries them as JSON text, and
   the router can still filter (`alterado_por ? 'LC 227/2026'`).

**Consequences.** The serving tables can be dropped and rebuilt from silver at any time (first load: +4,223 /
+580). Changing the hashed columns (or how lists are serialized) changes every `row_hash`: the next load rewrites
the whole table once, by design, like `CHUNKER_VERSION` for the chunks. History stays out of Postgres (ADR 0010):
an older wording is in the lake, not in the serving table. The load does not read the chunks: they go to the
vector store in 4c, with the incremental sync of ADR 0007.
