# ADR 0015 — LC 214 answer generation: cited JSON from a local LLM, three barriers to "não consta"

**Status:** accepted, step 6a (`pipelines/rag/lc214/lc06_generate.py`, task `generate_answers` in `dag_rag_lc214`):
generation and measurement. Step 6b, the gate against a committed answer baseline, comes after the first lab run.

**Context.** Retrieval is measured and gated (ADR 0014), but nothing produces an answer yet: the R and the A of RAG
exist, the G does not. Primary user: a SEFAZ tax auditor. For that reader, an answer that sounds right and is not in
the law is worse than no answer, so the design starts from when NOT to answer. The model is `llama3.2:1b` on CPU
(Ollama 0.6.5, profile `ai`): small, local, free and weak. The aim of this step is to measure how weak, with the
same discipline as lc05, not to hide it. Comparing models is a later experiment, and it needs this measurement to
compare against.

**Decisions.**

1. **Structured output, not free text.** Ollama's `format` takes a JSON Schema (Ollama ≥ 0.5):
   `{answerable: bool, answer: str, citations: [{artigo, dispositivo}]}`. A citation is split into article and
   provision because a 1B model reliably fills two short fields. It does not reliably write a whole legal path.
   A reply off the schema is refused (`invalid_json`), or `truncated` when Ollama says `done_reason=length`.
2. **Three barriers decide "não consta"**, two of them deterministic:
   1. *Before the LLM.* Route `clarify` (ambiguous or nonexistent citation: ask, ADR 0012), or no context block.
      The LLM is not called.
   2. *The model.* `answerable=false` when the context does not support an answer.
   3. *After the LLM.* Every citation goes through the router's own parser (`parse_citations` + `Index.resolve`,
      the code that resolves 99.6% of the law's 799 self-citations) and is checked against the provisions the context
      SHOWED (`Block.dispositivo_ids`): `valid`, `outside_context` (a real provision the model did not read here: it
      answered from memory), `not_found` (invented), `foreign` (another law: neither valid nor a violation).
      Bad citations are dropped and counted. The answer is refused (`no_valid_citation`) only when no valid citation
      is left. One bad citation next to two good ones is a violation to measure, not a reason to hide the answer.

   No similarity threshold: 2 `no_answer` cases cannot calibrate one (ADR 0014 measures `separable`). It is a
   future experiment, once the golden set has more unanswerable questions.
3. **Routes without the LLM.** `sql` answers with the cited provision as structure (`router.render_structure`, as in
   the router CLI). The LLM would only add a chance to misquote. `clarify` returns the router's question.
4. **Another law is not a refusal.** A citation of another law (`foreign`, ADR 0012) still goes to vectors, and the
   golden set has 6 `cite_external` cases that must answer ("the rule is in art. 14 of the CTN, as art. X of the
   LC 214 says"). The prompt gets a note naming the other law and is told not to invent its content.
5. **Repeatable, so a re-run is a no-op.** `temperature 0`, `seed` (`LC214_GEN_SEED`), and `num_ctx` explicit:
   Ollama's default of 2048 would silently cut a context budgeted for 8192. The golden run writes
   `silver/lc214/<sha12>/answers/<fingerprint>.json` and exits 99 when that report already exists. The fingerprint
   covers the law, chunks and embedding model (lc05's fingerprint), the golden set, the LLM model AND its digest
   (a re-pulled tag is another model), the prompt version (hash of system prompt, template, schema) and the
   retriever settings. `answers_digest` hashes every answer: two runs with `force` must print the same digest. That
   is how determinism is verified, not assumed. Ollama not running → exit 99 (an optional layer, like the AI image).
   Ollama running without the model → failure (a misconfiguration).
6. **Measured per phrasing group, in two recalls.** Over the golden phrasings of routes `vector`/`combined`, scope
   `now` (60 phrasings): `refusal_accuracy` (abstain cases refused, the others answered), `false_refusals` /
   `false_answers`, `citation_validity` (valid / all citations), `citation_recall` (an expected provision cited, same
   branch: the article or its parts) and `citation_recall_in_context` (the same, only where the expected provision
   reached the context). The two recalls separate the causes. A miss where the context had the provision is the
   model's. A miss without it is the retriever's, and lc05 already counts that one. Also `json_valid`, latency p50/max,
   the refusal reasons.
7. **Calibrating the token estimate (open since ADR 0013).** Each call logs `prompt_eval_count` against the prompt's
   words. Ollama reuses the KV cache of a shared prefix (the system prompt), so a later call may count fewer tokens.
   A ratio below 1.0 token/word cannot happen in Portuguese, so it is flagged `cache_suspect` and left out. The
   report gives the median of the rest. The default (1.7) changes only in a separate commit, measured by lc05 like
   any retrieval change.
8. **A read-only session in autocommit.** The run holds a cursor for up to an hour. In a transaction, that would be an
   `idle in transaction` session next to lc04a's upsert.
9. **CI tests the code, the lab measures the model.** CI has no LLM. The unit tests replay Ollama bodies
   (`tests/fixtures/lc214_ollama_replies.json`) through the real parsing, citation check, barriers and metrics. A
   model regression can only show up in the lab, in this step (and in 6b's gate).

**Consequences.** The RAG answers end to end: question → router → retrieval → cited JSON or "não consta".
Every refusal names its barrier. The first numbers will be low (1B model), and that is accepted: the gate of
step 6b catches drops, not levels, like lc05. Next: (6b) run in the lab, verify the digest is stable across two
runs, record real replies as the CI fixtures, write `answer_baseline.yml` and gate `refusal_accuracy`,
`citation_validity` and `citation_recall_in_context`. Then experiments, one variable each: labelled context lines
(the `[art. 5, § 2º]` of each provision in the prompt: it changes the context size, so lc05 runs too), field order
in the schema (verdict after the answer), larger models (a project of its own), the similarity threshold. The
calibrated tokens-per-word ratio feeds back into the retriever budget.
