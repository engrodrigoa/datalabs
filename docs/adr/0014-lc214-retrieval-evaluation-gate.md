# ADR 0014 — LC 214 retrieval gate: golden set through the real retriever, thresholds from a committed baseline

**Status:** accepted (step 5: `pipelines/rag/lc214/lc05_eval.py`, task `evaluate_retrieval` in `dag_rag_lc214`,
thresholds in `pipelines/rag/lc214/eval_baseline.yml`).

**Context.** The retriever (ADR 0013) works end to end, but the first manual tries said nothing about its quality.
The auditor's phrasing of `content-03` ("transferiu mercadoria da matriz para a filial") did not bring art. 6º, II
into the top 5: articles on imports and financial services came first, at distances 0.108-0.115. The law says
"transferência de bens entre estabelecimentos pertencentes ao mesmo contribuinte". The two phrasings share no
discriminative word, and the Portuguese stemmer does not join them either (`transferiu` → `transfer`,
`transferência` → `transferent`). One case is an anecdote, though. Hybrid search, query expansion with a fiscal
glossary, another embedding model: each idea needs a number to beat, measured the same way every time, before it
is built. The ANP RAG has a gate with fixed thresholds (0.9, ADR 0005). Nobody knows the right level for this
corpus yet.

**Decisions.**

1. **Evaluate the product, not a component.** Every phrasing of every measured case goes through
   `retriever.retrieve()`, the same router → exact search → context assembly a user question goes through. Measured
   cases: routes `vector` and `combined`, scope `now`, with labels. `no_answer` questions also run, but only for
   their distances.
2. **Two levels, because small-to-big makes them differ.**
   - On the top-k chunks (route `vector` only; a `combined` case gets its provision from the SQL anchor, so the
     vectors cannot fail it): **success@k** (mode `all`: every expected provision; `any`: one), **hit@k**,
     **recall@k**, **MRR**. A provision is retrieved when a top-k chunk carries it in `dispositivo_ids`, so a window
     of art. 517 retrieves the provision it was cut from.
   - On the assembled blocks (routes `vector` and `combined`): **context_recall**, the share of the expected
     provisions the LLM will actually read. A hit on an article's caput chunk brings the whole article. That gap
     (context_recall > recall@k) is what small-to-big buys, and now it is measured.
3. **Grouped by vocabulary, not averaged away.** Metrics per phrasing: `fiscal` (31 phrasings with chunk metrics,
   33 with context) and `formal` (25). The **paired gap** compares the two on the same 25 cases: the cost of asking
   like an auditor instead of like the law. A blended average would hide the gap the next experiments are meant
   to close.
4. **Thresholds from the first measurement, committed.** `--write-baseline` writes `eval_baseline.yml` with the
   metrics and the fingerprint of what was measured (law version, parser, chunker, model, golden set md5, k,
   retriever settings). The file is reviewed and merged like a label change. A run fails when a gated metric
   (success@k, MRR, context_recall, for each group) drops below `baseline − LC214_EVAL_TOLERANCE_CASES / n`.
   - **Tolerance in cases, not in percent**, because a person can read it: 1 case = 3.2 pp for the fiscal group,
     4.0 pp for the formal one. The search is exact (ADR 0013) and the model fixed, so the same inputs give the same
     scores; the tolerance absorbs a near tie flipping on another CPU (the lab has two machines). With very few
     cases it makes the gate toothless (n = 1 sets the threshold at 0), so it is meant for tens of cases, and the
     tests run the gate strict.
   - **Improvements never fail.** They are logged (`lc214_eval_improved`), and raising the baseline is a deliberate
     commit: a ratchet operated by a person.
   - **No baseline means failure**, with the command to create one: a gate without thresholds is not a gate.
   - **A metric definition change** (`EVAL_VERSION`) refuses the old baseline.
5. **Labels first.** The evaluation refuses a golden set labelled against another law version (`eval_labels_stale`),
   and in the DAG it runs after `golden_contract`: a wrong label would punish the right answer.
6. **No exit 99.** The evaluation runs every week even when nothing changed (~60 queries + model load, about a
   minute). It is a check: an embedding model or retriever change with the same law shows up here.
7. **Where the results go.** The full report (every phrasing: first rank, provisions found, top-k articles, top-1
   distance, context size) goes to the lake at `silver/lc214/<sha12>/eval/<timestamp>.json`. The metrics go to the
   step's `metrics` in `audit.pipeline_step_runs` (trend for Grafana).
8. **Data for abstention, not a threshold yet.** The report holds the spread of the top-1 distance of solved,
   missed and `no_answer` questions, plus `separable` (the closest unanswerable question sits farther than the
   farthest solved one). With 2 `no_answer` cases, it is a first look, not a calibration.

**Consequences.** Every retrieval change (hybrid search, a fiscal → legal glossary, e5-base, removing the
citation from the `combined` query vector, the ceiling of option C) becomes an experiment of one variable: run
lc05, compare with the baseline, keep the change only if the gated metrics hold and the target metric moves. The
generation (llama3.2:1b) is gated on retrieval, and faithfulness will be a separate measure later. The golden set
is small (33 measured cases), so a 1-case change is ~3 pp: differences of that size are noise, and the gate is
built to catch drops, not to rank close variants. Growing the golden set is the way to sharper numbers.
