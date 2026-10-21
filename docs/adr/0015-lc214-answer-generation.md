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
   Ollama's default of 2048 would silently cut the budgeted context. The golden run writes
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
9. **Latency: speed is bought with hardware, not with context (measured 2026-10-19).** On CPU the prompt is ~90%
   of the time: one question with a 3,144-token prompt took 130 s, and a 5,498-token one took 204 s (~25 tok/s of
   prompt, 12 tok/s of answer). Sixty phrasings at `num_ctx` 8192 meant ~2 h. A smaller context is cheaper only if
   lc05 says what it costs. It is a retrieval change, so it was swept as one variable against the baseline (lc05
   runs in ~15 s, the LLM never ran):

   | `num_ctx` | context budget | fiscal / formal context_recall | lost (cases) | context tokens, 60 phrasings |
   |---|---|---|---|---|
   | 8192 | 5,310 | 0.651 / 0.767 | — | 168k (~129 min) |
   | **6144** | 3,672 | 0.651 / 0.767 | 0 | 157k (~122 min) |
   | 4096 | 2,033 | 0.556 / 0.767 | fiscal −3 | 110k (~92 min) |
   | 3072 | 1,214 | 0.495 / 0.667 | fiscal −5, formal −2.5 | 69k (~66 min) |

   A second sweep, of the whole-article ceiling under the smaller budgets (2,000 → 1,200 → 800 → 600), refuted the
   idea that more, smaller blocks would win the recall back. It got worse at every step (4096: fiscal 0.556 → 0.424),
   because the answer usually sits in the hit's own article (the hit is art. 3's caput, the answer its incisos):
   small-to-big works by showing the article whole. The ceiling stays at 2,000.
   The criterion was fixed before the choice, in the gate's unit (cases), not in a relative percentage: a speed
   trade may cost at most **2 fiscal cases and 1 formal case** (the fiscal phrasing is the primary user's and already
   the weakest). Only 6144 passes: it is the default now, for ~5% less time and less KV cache, at the same recall.
   The real gain came from the machine: WSL was capped at 4 of the 8 logical cores (`.wslconfig processors=4`). With
   8, the same prompt took 88 s instead of 130 s (×1.48: hyper-threading, not ×2) and gave the **same answer and
   citations**. A full run is ~80 min (task timeout 150), only when the fingerprint changes. Prompt experiments
   iterate on a fixed **dev subset** (`dev: true` in the golden set, ~11 phrasings: answer, the vocabulary miss, a
   false refusal, abstain, cite_external, combined; `--golden --dev`, a few minutes), and the full run confirms only
   the winner. Each answer records Ollama's split of its latency (load, prompt, answer).
10. **CI tests the code, the lab measures the model.** CI has no LLM. The unit tests replay Ollama bodies
   (`tests/fixtures/lc214_ollama_replies.json`) through the real parsing, citation check, barriers and metrics. A
   model regression can only show up in the lab, in this step (and in 6b's gate).

**First observations (two CLI questions, 2026-10-19).** The unanswerable one (EFD fine) was refused by the model,
as it should be. The other ("quem é o adquirente", by conta e ordem) was refused although art. 3, III-V was in
the context (5th block) and the model cited two valid provisions: a false refusal, the failure this step exists to
count. Without context, the same model says IBS is "Intestino Irritável": the case for grounding in one
line. The real ratio is ~1.79 LLM tokens per word (+5% over the 1.7 estimate, inside the 20% margin).

**The first dev run: refusal collapse (2026-10-19, baseline prompt, 11 phrasings, 15 min).** The model refused all
11. Only the 2 `no_answer` refusals were right (fiscal refusal accuracy 0.286, formal 0.000, citation recall 0).
In `content-02/formal` the context held 3 of the 3 expected provisions, and the model still refused. One reply ran
into `num_predict` (truncated, 167 s). Five replies cited valid provisions next to `answerable=false`. The single
question that isolates it ("diferença entre fornecedor, adquirente e destinatário", art. 3 as the FIRST block)
came back as `answerable=false` with `answer` = one word ("destinatário") and no citations. The model commits to the
verdict on its first token and the rest only follows it. A refusal-everything baseline would gate a floor, so the
full run waits for a prompt that answers. Prompt eval went from ~26 to 46 tok/s with 8 cores, and the ratio held at 1.795
tokens per word.

11. **Prompt experiments are named variants, one change each.** `LC214_GEN_VARIANT` / `--variant`: `baseline`,
    `verdict_last` (H1: the JSON asks for `answer` and `citations` before `answerable`; Ollama turns the schema
    into a grammar, so property order is the order the model writes in), `answer_default` (H2: answering is the
    default, refusing the exception, stated last). They compose with `+`, so `verdict_last+answer_default` is
    tested only after each one alone. The variant and the hash of the prompt are in the fingerprint. The hash
    used to sort the schema's keys, which made `verdict_last` hash like `baseline` (fixed: the order is part of
    the prompt). A subset run (`--dev`, `--case`) writes `answers/dev/<fingerprint>-<subset>-<time>.json`, never
    skips and keeps every run, so variants are compared side by side and the same variant twice must give the same
    digest. Every answer keeps the raw reply: the recorded replies become the CI fixtures. The winner becomes the
    default in a commit of its own, after the full run confirms it. Failing both H1 and H2 means `llama3.2:1b`
    cannot do this task. Then the LLM comparison stops being a later project and becomes the next step.

**H1 and H2 (2026-10-20, dev subset, 11 phrasings each).** Neither moved refusal accuracy (fiscal 0.286, formal
0.000, like the baseline). Both changed what the model wrote. `verdict_last` wrote real sentences (content-02
explains adquirente and destinatário) but hardly cited, and still ended in `false`. `answer_default` cited valid
provisions (fiscal citation validity 0.50 → 0.77; content-02/fiscal: 3 valid citations, with 3 of 3 expected in the
context) and still said `false`. Across 33 replies under 3 prompts, `answerable` was `false` every time. Warning
signs about the model itself: under `verdict_last` it invented a DIFAL calculation for `no_answer-02` (no citation,
so barrier 3 would have stopped it), it wrote full provision texts and "Capítulo III" into the citation fields,
and two replies looped into `num_predict`.

12. **The decision is separate from the generation; a constant flag does not decide.** `LC214_GEN_DECISION`:
    `model` (barrier 2 obeys `answerable`, then barrier 3) or `citations` (`answerable` is recorded but not obeyed,
    and a reply with at least one valid citation is shown). One function (`decide`) applies the rule, live and
    offline. The rule changes nothing the model writes, so it is not in the generation fingerprint. Every report
    scores BOTH rules (`metrics_by_decision`), plus `model_says_answerable`, the share of replies where the model
    claims it can answer (a value stuck at 0 or 1 is not a signal). `--rescore <reports | dev>` re-decides recorded
    replies with no LLM, Postgres or embedding model: the experiments already paid for answer the question. The
    principle: with a small model, trust what can be checked (a citation resolved against the context) over what
    the model says about itself. The risk it brings is a cited reply that does not answer the question, or answers
    it wrongly. `citation_recall` (the EXPECTED provision cited) measures the first part. Only a human read
    measures the second, and no metric here replaces it. The default stays `model` until the rescore and that read
    agree. Then `citations` becomes the default in a commit of its own.

**H4: qwen2.5:3b (2026-10-20, dev subset, `answer_default`, 39 min, 213 s per phrasing, prompt 15.9 tok/s).** The
largest prompt was 4,338 tokens (no silent cut at `num_ctx` 6144; 1.83 tokens per word for this tokenizer). The
failure changed in kind. `answerable` carries signal again (0.143 fiscal, 0.500 formal). Both `no_answer` questions
were refused with a correct reason ("nenhum trecho... trata do DIFAL"). The refusals are coherent sentences, and
there was no visible invention. Two things remain. (a) Literalism: 8 of 9 answerable phrasings were refused with
"o contexto não trata especificamente...", three of them with the expected provision in the context. This is the
fiscal vocabulary gap again, now on the generation side. (b) The model cites in the prose ("conforme § 2º do art.
4º da LC 214/2025") and leaves the citation field empty: two replies flagged answerable were stopped by barrier 3
for that alone. Strictly, 1 of 9 phrasings was shown with the expected provision cited.

13. **Citations are read where the model writes them.** Decision rule `model_text`: barrier 2 obeys `answerable`,
    and barrier 3 accepts citations from the field AND from the answer's text, parsed and checked by the same
    router code (`parse_citations` + `Index.resolve`, valid only if the context showed the provision). The grounding
    check is as strict as before, but it reads the prose too. It is never used without the flag: a refusal
    names provisions too (qwen's DIFAL refusal cites art. 69, §§ 1º-2º), and reading its text would turn it into an
    answer. Reports from GEN_VERSION 4 on store `text_citations`. `--rescore` computes them for older reports from
    the recorded answer and context, with the law's provision ids from the lake (`dispositivos.parquet` of the
    report's law version).

**Consequences.** The RAG answers end to end: question → router → retrieval → cited JSON or "não consta".
Every refusal names its barrier. The first numbers will be low (1B model), and that is accepted: the gate of
step 6b catches drops, not levels, like lc05. Next: (6b) run in the lab, verify the digest is stable across two
runs, record real replies as the CI fixtures, write `answer_baseline.yml` and gate `refusal_accuracy`,
`citation_validity` and `citation_recall_in_context`. Then experiments, one variable each: labelled context lines
(the `[art. 5, § 2º]` of each provision in the prompt: it changes the context size, so lc05 runs too), field order
in the schema (verdict after the answer), larger models (a project of its own), the similarity threshold. The
calibrated tokens-per-word ratio feeds back into the retriever budget.
