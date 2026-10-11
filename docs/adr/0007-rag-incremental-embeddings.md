# ADR 0007 — RAG: incremental embeddings and a CPU-sized model

**Status:** accepted

**Context.** `rag01_context.py` rebuilt the whole vector store on every gold publication
(`TRUNCATE` + embed ~32k GO rows). Embedding is CPU-bound: on the lab machine
`multilingual-e5-base` ran at **~6-7 rows/s** (measured in isolation, 3-6 torch threads),
so a rebuild took ~1h40 and every run since the gold grew hit the 2h task timeout. The cost
grew with history, not with what changed: a weekly ANP file adds a few thousand GO rows.

**Decisions.**

1. **The vector store is a projection of gold, synced by set difference.** Each chunk has a
   deterministic id `id_fato:md5(text)[:16]` (md5, not `pl.hash`, so ids survive library
   upgrades). Each run computes `new = gold - store` (embed + insert) and
   `stale = store - gold` (delete). Nothing new means exit 99 (skipped) without loading the model.
   The sync is idempotent: a run that dies half-way is completed by the next one.
2. **Full rebuild only when required:** empty store, embedding model changed (vectors from
   different models are not comparable), or explicit `--full-refresh` / `RAG_FULL_REFRESH=1`.
   On a model change the `embedding` column is resized to the model dimension (`vector(n)`).
3. **HNSW maintenance by delta size:** small deltas go into the live index; at
   `RAG_INDEX_REBUILD_MIN_ROWS` (default 5000) or more, drop, load and rebuild once.
4. **Default model `multilingual-e5-small` (384 dims)** instead of e5-base (768): several times faster
   on CPU, ~470 MB instead of ~1.1 GB to download, half the storage per vector. One module
   (`pipelines/commons/ai/embeddings.py`) defines the model for writing *and* querying.
   The change is governed by the existing evaluation gate (ADR 0005): if routing/value
   accuracy drops below the thresholds, the DAG fails and the model goes back.

5. **One transaction per sync.** Truncate/resize/delete, `COPY` and the HNSW build commit together:
   a failure rolls back to the previous store instead of leaving it empty or without its index.
   If the data is in sync but the index is missing, the run rebuilds only the index (self-heal).
   The Postgres container gets `shm_size: 1g` (parallel HNSW builds use `/dev/shm`; Docker's 64 MB
   default failed with *No space left on device*).

**Measured (lab, full rebuild of 32,097 GO chunks with e5-small):** embedding 2,430 s (13.2 rows/s:
real chunks are ~2x longer than the benchmark sentence, which ran at 26.5 rows/s), `COPY` 336 s.

**Consequences.** A normal run embeds only the weekly delta (minutes). Changing the model
or the chunk template (`CHUNK_TEMPLATE_VERSION`) costs one full rebuild, by design. The
`rag_context_parity` check (gold rows = context rows) keeps validating the projection.
