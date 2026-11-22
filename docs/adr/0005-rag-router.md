# ADR 0005 — RAG over tabular data: route to SQL first, vectors second

**Status:** superseded by [ADR 0020](0020-retire-anp-rag.md) (the ANP RAG was retired; the principle lives on in
the LC 214 router, [ADR 0012](0012-lc214-question-router.md))

Embeddings retrieve *similar text*, not exact identifiers or aggregates. Questions with a CNPJ or an
aggregation (count, average, min/max, list) go to parameterised SQL on the gold layer; everything else goes
to pgvector. Both paths return the same shape, and the generator is told which lines are exact.

Quality is measured, not assumed: a golden set with SQL ground truth runs after every context rebuild
(`dag_rag_anp`) and the task fails below `RAG_MIN_ROUTING_ACC` / `RAG_MIN_VALUE_ACC`.

Next: add an unstructured corpus (ANP resolutions, survey methodology, RFB layout manuals), where RAG is the
right tool, and answer-level evaluation (faithfulness).
