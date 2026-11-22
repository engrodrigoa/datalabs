# ADR 0020 — Retire the ANP RAG: one AI product, the LC 214

**Status:** accepted. Supersedes [ADR 0005](0005-rag-router.md) and [ADR 0007](0007-rag-incremental-embeddings.md)
as running code; both stay as the record of what was learned.

**Context.** The lab had two RAGs. The ANP one (`dag_rag_anp`, `pipelines/rag/anp/`, `ai.rag_context_anp`) answered
fuel price questions ("which station is cheapest?") over the ANP gold. Reviewed on 2026-11-22 against the rest of
the repo:

1. **The question is SQL, not retrieval.** Its own router sent every CNPJ and aggregation question to parameterised
   SQL on gold (ADR 0005). The vector path embedded one sentence per row of the price fact: tabular data written
   as text, where similarity has nothing to find that a `WHERE` does not find better. The roadmap already recorded
   the result: name queries at precision@5 = 0.
2. **The reader's question has a better answer in the repo.** The Streamlit app over two serving marts
   (ADR 0016) answers "how much does fuel cost now, down to a station?" exactly, auditably and in milliseconds.
3. **What it taught is already in the LC 214.** The shared embedding model (`commons/ai/embeddings.py`), the
   set-difference sync (`commons/ai/context_sync.plan_sync`, used by `lc04_embed`), routing before retrieval
   (`rag/lc214/router.py`: sql / combined / vector / clarify, checked exactly by the golden contract), the
   evaluation as a gate in the DAG (lc05, lc06) and the LLM usage per call (ADR 0018). The ANP code was the
   first draft of each of these.
4. **Two AI products split the story.** About 1,300 lines (four scripts and a 405-line router that was
   ANP-specific but lived in `commons/`), a DAG, three health checks, two dashboard rows, three tables, a second
   Ollama model (llama3.2:1b, ~1.3 GB) and roadmap items (hybrid search per station) pulling effort away from the
   RAG where retrieval is the right tool: legislation, unstructured text.

**Decision.** Remove the ANP RAG entirely; keep the ANP pipeline (landing → dbt → gold → Streamlit) as a data
engineering and observability product. The LC 214 is the lab's only RAG. Its link to the rest of the platform is
the NF-e (tax reform data, ADR 0006) through the cClassTrib code: the official table (ADR 0019) carries the
legal reference of each code (`fundamentacao_legal`, e.g. `Art. 412, I`), which the LC 214 router parses; wiring
it is the next study case.

Removed:

| Layer | What |
|---|---|
| Code | `airflow/dags/dag_rag_anp.py`, `pipelines/rag/anp/` (rag01-rag04), `pipelines/commons/ai/rag_query_router.py`, `context_sync.build_chunks` / `chunk_id` |
| Data | `ai.rag_context_anp`, `ai.rag_eval_metrics`, `ai.rag_eval_summary`; audit rows of pipeline `rag` / `dag_rag_anp`; `ai.llm_call` rows of app `anp_assistant` |
| Quality | checks `rag_context_parity`, `rag_routing_accuracy_pct`, `rag_value_accuracy_pct` (26 → 23) |
| Grafana | Pipeline Health row "RAG quality" (replaced by a link to the RAG LC 214 board; the log panel now lists the LC 214 gate failures); ANP row "Downstream — RAG sobre a gold" |
| Config | `RAG_UF`, `RAG_MIN_ROUTING_ACC`, `RAG_MIN_VALUE_ACC`, `RAG_FULL_REFRESH`, `OLLAMA_MODEL`; `ollama-pull` fetches only `LC214_GEN_MODEL` |
| Tests | `tests/test_rag_anp_usage.py`; the `build_chunks` cases of `tests/test_rag_context_sync.py` |

Kept, because the LC 214 uses them: `commons/ai/embeddings.py` (`RAG_EMBEDDING_MODEL`, `RAG_TORCH_THREADS`),
`commons/ai/context_sync.plan_sync`, `commons/ai/llm_usage.py`, the Ollama service, the `ai` schema and its grant.

**How the data goes.** `infra/postgres/init/sql/04_ai.sql` became `04_retire_anp_rag.sql`: idempotent drops and
deletes, applied like any DDL file (`dag_setup_infrastructure`, `make db-bootstrap`). On a fresh volume it does
nothing. Scope checked before writing it: pipeline `rag` and app `anp_assistant` were written only by the ANP RAG
(the LC 214 writes `lc214` and `lc214_generate`). The file can be deleted once no volume from before this ADR is
left. The Airflow metadata of the removed DAG is not DDL: on an existing lab, `airflow dags delete dag_rag_anp -y`
once.

**Consequences.**
- One AI story to explain and to measure: a governed source (the law), a deterministic router, retrieval and answer
  gates, token usage per call.
- The lab no longer shows "RAG over structured data". The lesson stays in ADR 0005: SQL for exact questions,
  vectors only where similarity is the question. The LC 214 router applies it (citations by SQL).
- `make up-ai` downloads one model fewer. An existing `.env` keeps the removed keys; they are ignored.
- `docs/images/pipeline-health.png` still shows the old RAG row until the next screenshot.
