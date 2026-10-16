# ADR 0010 — Legislation RAG (LC 214/2025): source contract, structural parser, small-to-big chunks

**Status:** accepted (steps 1-3: `pipelines/rag/lc214/lc01..lc03`, orchestrated by `dag_rag_lc214`); retrieval and
evaluation are the next steps.

**Context.** ADR 0005 said RAG is the right tool for unstructured text, and the ANP store is tabular. The
first unstructured corpus is the LC 214/2025 (IBS/CBS/IS), from the "norma atualizada" PDF the Câmara publishes
(298 pages, text layer from LibreOffice, no OCR needed). A PDF has no structure, only text at positions, and the
legal structure (Livro → Título → Capítulo → Seção → Art. → § → inciso → alínea) is a drafting convention. The
consolidation also mixes the law with notes ("Parágrafo acrescido pela LC 227/2026"), and the source has defects.

**Decisions.**

1. **Source contract is structural, the hash detects versions (lc01).** The PDF is re-published whenever the law
   is amended, so a pinned sha256 would break on legitimate changes. The contract checks what must stay true: the
   bytes are a PDF (checked before extraction: an HTML error page fails as `not_a_pdf`), at least 250 pages, the
   title and the last article are in the text. The sha256 makes the step idempotent: same file → exit 99; amended
   law → stored next to the previous versions (decision 8 says where).
2. **`pdftotext -layout` (poppler), because the layout is a signal.** The first line of a provision is indented
   (11-13 spaces) and wrapped lines start at column 0 (5,711 lines). A marker at column 0 is a wrapped reference
   (62 of them: "nos termos do\n§ 3º"), unless it is the next number of its sequence and the previous line closed a
   sentence (arts. 542-543 lost their indentation). Numbering follows LC 95/1998 art. 10: ordinal up to the ninth
   (`§ 9º`), cardinal from the tenth (`§ 10.`).
3. **One row per provision, ids from the printed labels (lc02).** `lc214-2025:art-7-A:par-1:inc-II:ali-a`, so a
   citation maps to one row. The consolidation notes leave the text and become columns (`notas`, `status`,
   `alterado_por`, `tipo_alteracao`). Revoked and vetoed provisions keep only their label: they are rows, never
   vectors, and "it was revoked" is an answer by lookup, not "I don't know". Text of another law quoted inside an
   amending article is content of that article (`altera_outra_lei`), not structure of the LC 214.
4. **Source defects are a registry, not code (`known_anomalies.yml`).** Measured in the 2026-10 version: Art. 260
   printed twice (new caput with the LC 227 note, then the old one unmarked) → `annotated_wins`, pending
   confirmation against planalto.gov.br; § 4º of arts. 444 and 462 prints incisos I, II, II → kept as printed with an
   id suffix (`inc-II#2`), because a citation must match the published text. Any anomaly not in the registry fails
   the run: a new defect needs a human decision.
5. **A data contract on the parse.** I1 articles 1..N without gaps, I2 unique ids, I3 contiguous sequences under
   each parent, I4 ≥ 98% of the body words inside a provision. The invariants found three parser bugs (`§ 10.`,
   heading levels, column-0 articles) and both source defects before any table was written. I5 (parser v2):
   revoked/vetoed ⇔ no body. The status comes from the marker right after the provision's own label ("§ 5º
   (VETADO).", "b) (Revogada ...)"), the body from the text without notes: two rules that check each other.
6. **Small-to-big chunks (lc03).** The model reads 512 tokens and sentence-transformers truncates the rest
   silently. An article that fits is one chunk; a longer one is split along its own structure (caput block, each §,
   each item, word windows as a last resort), every piece with a header (Título, Capítulo, Art.) and the start of
   the caput, packed while they fit. The answer shows the whole article (`artigos.parquet`). With the model's
   tokenizer a chunk over the limit fails the run; without it, counts are estimates and say so (`token_method`).
7. **Orchestration: every step decides for itself (`dag_rag_lc214`).** A weekly source watch (Mon 06:40). Each step
   compares its own input and code version with what it last wrote and exits 99 when there is nothing to do: lc01
   the sha256, lc02 sha + `PARSER_VERSION`, lc03 sha + parser version of its input + `CHUNKER_VERSION` + token
   method. Steps after lc01 use `trigger_rule=NONE_FAILED`, so "skipped" means "up to date", not "stop": a parser
   bump with the same PDF still re-parses and re-chunks (with the default ALL_SUCCESS it would wait for the next
   amendment of the law). A failure still stops the chain and turns the run red. The AI-image gate is a
   `ShortCircuitOperator` that overrides those rules: lc03 runs only with the real tokenizer (`--tokens tokenizer`),
   because an estimate cannot prove that no chunk is truncated. `datalabs://silver/lc214` is emitted only when
   lc03 succeeds (new chunks), to schedule lc04. Two health checks read the step audit, since the law is not in
   Postgres yet: the source watch ran in the last 8 days, and the last parse kept ≥ 4,000 provisions.
   Building the DAG exposed a bug: lc03 keyed "up to date" on the law version only, so a re-parse under the same
   sha would have left stale chunks (fixed, with a regression test).

8. **Everything lives in the lake, in medallion layers (`lake.py`).** The first version wrote to a loose folder
   under `datasource/` while ANP/RFB used object storage: the artifacts were invisible to the lake, tied to one
   machine's volume, and "the history" was a directory listing. Now:

   | layer | key | what |
   |---|---|---|
   | landing | `landing/lc214/raw/lc214_<sha12>.pdf`, `manifest.json`, `rejected/` | the file as published, every version |
   | bronze | `bronze/lc214/text/lc214_<sha12>.txt` | `pdftotext -layout`: a lossless technical derivation |
   | silver | `silver/lc214/<sha12>/{dispositivos,chunks,artigos}.parquet` + reports | structure, one prefix per version |

   S3 has no rename, so atomicity comes from ordering: data objects first, the report (lc02, lc03) or the manifest
   (lc01) LAST. A run killed in between leaves an orphan object nobody points to, never a "current" pointing to
   missing data (tested). Processing still uses local disk (pdftotext reads a file), in a temporary directory that
   is deleted. `LC214_LAKE=file:///dir` runs the same keys on a folder (tests, a laptop without the stack).
   The lake is the system of record; the Postgres tables of lc04 are a serving projection of the current version,
   rebuildable from silver at any time.

9. **The golden set is labelled data with a contract (`golden_set.yml`, `golden.py`).** The primary user is a
   SEFAZ tax auditor checking a taxpayer that issues NF-e, so every content case has the auditor's phrasing
   (`fiscal`, primary) and the legal one (`formal`): the gap between the two measures how much the retriever
   depends on the law's own vocabulary. The cases cover every route of the router, not only similarity: citations
   and revoked/vetoed provisions by SQL, amended provisions, the Art. 260 anomaly, abstention (previous wordings,
   ICMS questions) and references to other laws (`cite_external`: retrieve the anchor in the LC 214, say the rule
   lives elsewhere, never invent it). Labels are provision ids of one law version, so they are checked like data
   (G1 schema, G2 version, G3 exists, G4 status, G5 amended, G6 retrievable within k chunks) by the
   `golden_contract` task on every run: an amendment that touches a labelled provision fails the run with the
   case named. A known failure is a `known_issue`, reported but not fatal, and strict: once it passes, the
   contract fails until the marker is removed.

**Measured (version `0cf36f4433ce`; parser v2 and chunks re-run in the lab on 2026-10-16).**

| | |
|---|---|
| provisions | 4,223: 580 articles, 1,228 §, 1,792 incisos, 623 alíneas; coverage 98.25% |
| status | 4,165 in force, 33 revoked, 25 vetoed (parser v2; v1 counted 26: see the golden set below) |
| notes | 371 provisions amended by LC 227/2026; 41 articles quote another law |
| chunks | 867 (450 whole articles, 351 groups, 39 single provisions, 27 windows); 125 articles split. Parser v1 gave 854 / 14 windows / 124: the 13 extra windows are art. 517 |
| tokens | `multilingual-e5-small` tokenizer, `passage: ` prefix and special tokens included: p50 267, p95 497, max 512 (0 over the limit) |
| estimate vs real | the 1.5 tokens/word estimate gave 863 chunks, max 511: close in total, but the chunk boundaries differ, so it is a development aid, never the gate |

**Golden set v1 (2026-10-16, version `0cf36f4433ce`).** 46 cases: 22 content (21 with both phrasings),
4 citation, 4 status, 2 amended, 1 anomaly, 1 out of scope, 2 no answer, 10 external references (6 `now`,
2 `annexes`, 2 `future_project`). Routes: 35 vector, 9 SQL, 2 none. Vector cases need 1-3 chunks (17 need more
than one), within k = 5. Labelling them found a parser bug: lc02 marks art. 517 as `vetado` because the LC 123
text it quotes contains "b) (VETADO)", so the whole article (23k characters of amendments to the Simples
Nacional) is missing from the chunks. The check "vetoed rows = `(VETADO)` markers in the text" (26 = 26) could not
see it: one marker is inside the quote, so a genuine count of 25 plus the false positive still adds up to 26.
Fixed in parser v2 (`status_of`: the marker must follow the provision's own label) and guarded by I5, which on
the v1 table flags exactly art. 517 and nothing else. Re-applying the v2 rule to the 4,223 provisions of version
`0cf36f4433ce` changes one status (art. 517: vetado → vigente); the article becomes 13 word windows (the token
estimate predicted 11; the lab run with the tokenizer measured 13, and the golden contract passed with 0 known issues). The lesson for checks: a count
reconciliation cannot see a swap; a rule that ties two independent derivations of the same row can.

**Consequences.** Next: lc04 loads the provisions and articles into Postgres (the router answers citations and
revoked provisions by SQL) and embeds the chunks with the incremental sync of ADR 0007; lc05 adds a legal golden
set with recall@k / MRR to the evaluation gate. A new law version re-runs the chain from lc01; a parser or chunker
change bumps `PARSER_VERSION` / `CHUNKER_VERSION` and the next run (or a manual run with `force`) applies it. The Annexes (39 pages of tables with NCM codes) are out of scope
here: they need a table extractor and prefix lookups, not text chunks.
