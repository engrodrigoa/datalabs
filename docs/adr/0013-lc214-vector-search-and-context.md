# ADR 0013 — LC 214 vector search: exact cosine over an incremental store, context under an LLM budget

**Status:** accepted (step 4c: `pipelines/rag/lc214/lc04_embed.py`, task `embed_chunks` in `dag_rag_lc214`, and
`pipelines/rag/lc214/retriever.py`); the evaluation gate is [ADR 0014](0014-lc214-retrieval-evaluation-gate.md), the
generation is next.

**Context.** The router (ADR 0012) sends open questions to `vector` and citation + question to `combined`, but
nothing answered them yet. Three things were missing: the 867 chunks of lc03 as vectors, a search over them, and the
assembly of what the LLM reads. Two constraints shape the design. The chunk is what is searched, but the article is
what the auditor cites (small-to-big, ADR 0010). And the LLM is `llama3.2:1b` with `num_ctx` 8192: the context has a
hard ceiling, counted in tokens of a tokenizer this step does not have (llama), while lc03 counts tokens of another
one (e5, limit 512).

**Decisions.**

1. **The vector store is a projection of silver, synced by set difference (ADR 0007).** `ai.lc214_chunk`, one row
   per chunk, key = lc03's `chunk_id` (md5 of chunker version + text). New ids are embedded, ids that left are
   deleted, and an id that stays keeps its vector: same id = same text. A new law version or parser that leaves a
   chunk's text alone only **restamps** it (`source_sha256`, `parser_version`, provision ids) without re-embedding.
   Exit 99 when in sync, without loading the model.
2. **DELETE + INSERT in one transaction, never TRUNCATE**, even for a full rebuild (model change, `--full-refresh`):
   the retriever keeps reading the previous version until the commit (ADR 0011). A model with another dimension
   resizes the column inside the same transaction.
3. **Guards before the model loads.** `chunks_estimated`: chunks sized with the word estimate are refused (a chunk
   over 512 e5 tokens is truncated by the model silently; only the tokenizer proves the limit). `store_shrink`: the
   store may not lose more than `LC214_MAX_DELETE_PCT` (5%) of its chunks **net** (stale − new). A chunker bump
   replaces every id but keeps the count (churn, allowed); a broken parse loses chunks (shrink, blocked).
4. **Exact search, no ANN index.** 867 × 384 dims by sequential scan and top-N sort: 1-3 ms per query (measured on
   Postgres 16 + pgvector 0.6, 867 random vectors, 2026-10-17), against tens of ms to embed the question. HNSW is
   approximate: its misses would mix with the retriever's in the lc05 baseline. Revisit at ~50k vectors (a second
   corpus) or when the scan shows in the latency, and measure the index's recall against the exact search then.
5. **Option C, the same rule for anchors and hits.** One block per article, in this order: the cited provisions
   (`combined`, never dropped), then the articles of the top-k hits by rank. An article goes **whole** when it fits
   `LC214_ANCHOR_MAX_TOKENS` (2,000, see below); a bigger one goes **in layers**: header, caput, the cited provision's subtree,
   then the article's own chunks ranked by similarity to the question while they fit, then the labels of what was
   left out ("não incluídos: § 4º; § 6º"). A window of a huge provision (art. 517) is shown as the window, never the
   whole provision. Revoked/vetoed provisions show their note, like the SQL answer.
6. **A greedy budget, in LLM tokens, estimated until it is measured.**
   `budget = (num_ctx − answer reserve − fixed prompt − question) × (1 − margin)` = (8192 − 1024 − 500 − ~30) × 0.8
   ≈ 5,300. Each block takes min(ceiling, what is left); hits that no longer fit are reported as `dropped`. LLM
   tokens are words × `LC214_LLM_TOKENS_PER_WORD` (1.7, conservative for legal Portuguese, with its numbers, "§"
   and "º"): an **estimate**, labelled as such in the CLI, corrected by the generation step, which logs Ollama's
   `prompt_eval_count` for the same prompt.
7. **Two checks** (`checks.yml`): `lc214_vector_parity` (chunks of the last successful lc03 = vectors, tolerance 0)
   and `lc214_vector_same_law` (vectors and the provisions the retriever shows come from one law version and parser,
   and the vectors from one model and one chunker). `lc214_vector_same_law` verified on Postgres: 0 on empty tables,
   2 (> max 1) with vectors of one law version and provisions of another.

**Measured (2026-10-17).** First embedding in the lab: 867 chunks in ~3 min (4.7 chunks/s on CPU, slower than the
ANP store's ~13/s: a legal chunk is up to 512 tokens, an ANP chunk is one sentence); the second run exited 99 without loading the
model; `health_checks_lc214` 6/6. The ceiling comes from the real articles (`ai.lc214_artigo`, 575 live):

| words per article | p50 | p90 | p95 | p99 | max | > 1,000 |
|---|---|---|---|---|---|---|
| | 160 | 516 | 634 | 1,135 | 2,656 | 9 |

1,500 LLM tokens ≈ 880 words: ~98% of the live articles go whole. The 10 largest are all above 880 words and go in
layers (art. 517, which quotes the LC 123; art. 542, a revoking article; arts. 26, 174, 341-G, 516, 11, 22, 349 and
5); counted in the lab on 2026-10-17: 11 live articles above 880 words, so 564 of 575 (98.1%) go whole. A worst-case anchor takes 30% of
the budget; five median articles (~270 tokens each) take ~1,350.

**The ceiling, chosen by measurement (2026-10-18, ADR 0014).** 1,500 was a first guess from the word distribution. The
lc05 baseline showed what it cost: two cases where the right article was retrieved but the expected provision was
left out of its layered block (art. 5º, 948 words; art. 22, 1,041 words). The ceiling acts after the search, so only
context_recall can move, and only context_recall did:

| ceiling | fiscal context_recall | formal context_recall | external-02 fiscal (art. 5º) | content-15 (art. 22) |
|---|---|---|---|---|
| 1,500 | 0.601 | 0.740 | 0/1 | 1/3 |
| 1,800 | 0.631 | 0.740 | 1/1 | 1/3 |
| 2,000 | **0.651** | **0.767** | 1/1 | 3/3 |
| 2,600 | 0.651 | 0.767 | 1/1 | 3/3 |

success@k, hit@k, recall@k and MRR stayed identical to the 4th decimal in every run, and no case lost context to the
budget (contexts use ~1.3-2.3k of ~5.3k tokens). The default is **2,000**, the smallest ceiling that keeps the whole
gain: more context than the measured cases need only gives `llama3.2:1b` more to read past (lost in the middle), a
cost the retrieval metrics cannot see and the generation step will measure. At 2,000 (~1,170 words), only the largest
articles go in layers (517, 542, 26, 174, 341-G by the word estimate).

**Consequences.** `retrieve()` returns the global top-k (what lc05 scores: hit@k, coverage@k, MRR on the chunks'
`dispositivo_ids`) and, separately, the blocks the LLM reads with the provision ids each one shows (what faithfulness
will check). No similarity threshold yet: abstention (`no_answer`) and the `cite_external` cases need a threshold
calibrated on the golden set, which is lc05's job, not a guess here. The ceiling, the ratio and the budget are env
settings: the generation step will measure the ratio, and lc05 can compare ceilings as an experiment of one variable.
Changing the embedding model or `CHUNKER_VERSION` costs one full re-embedding of 867 chunks (minutes on CPU), by
design.
