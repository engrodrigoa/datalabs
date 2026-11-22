# ADR 0021 — NF-e × LC 214: the legal reference of each cClassTrib resolved into provisions of the law

**Status:** accepted (`pipelines/rtc/rtc03_lc214.py`, task `link_lc214` in `dag_rtc_ref`, table
`ai.rtc_lc214_fundamento` in `infra/postgres/init/sql/09_rtc_lc214.sql`, dbt `gold.br_cclasstrib_lc214` +
`gold.vw_nfe_item_fundamento_lc214`, checks `rtc_lc214_*`, NF-e board row "Fundamento legal"). Router extension
in `pipelines/rag/lc214/router.py`.

**Context.** The lab had the two halves of one question and no join between them. The NF-e side knows each item's
cClassTrib and checks it against the official table (ADR 0019). The law side serves the LC 214 provision by
provision (ADR 0010/0011). An auditor looking at an item asks *what in the law grounds this classification?* The
official table already answers it in a column: `FUNDAMENTACAO_LEGAL.FDLG_TEXTO_CURTO` ("Art. 412, I",
"Art. 128, I, II, VIII, IX, X, XI, XII e XIII") plus a transcription of the cited text (`FDLG_TEXTO`).

Probe before building (release 1.7.2, V0073, 2026-11-22): 202 references for 202 classifications; every one but
"LC 214/2025 / Regra Geral" parses to at least one citation with the LC 214 router. **5 of 202 lost list items**:
the router read the chat order ("art. 12, II") and the legal order ("incisos I e II" lost them all), but not the
table's compact lists and ranges ("I, II e XIII", "I,III e VII", "§ 1º, I e II", "I a XVIII"), and article ranges
("arts. 99 a 102") only by their ends. Measuring against the law found two more gaps of the same kind: plural
paragraphs ("§§ 3º e 4º", "parágrafos 3° e 4°") were read as the bare article, so the bridge resolved
"Art 28, parágrafos 3° e 4°" to the caput of art. 28, a wrong provision caught by the transcription check (48% of
its words); and a legal-order citation did not close at its article ("incisos I e II do caput do art. 485, § 1º do
art. 486" gave art. 485 the § 1º).

**Decisions.**

1. **The router reads lists and ranges** (`_list_tail`, `_expand`). After a bare roman in chat order, every
   ", X" / " e X" / " a X" continues the list; after the plural "incisos" the same on the lower-case text; " a "
   between two numerals of the same kind is a range, expanded (≤ 60 items, ascending, well-formed numerals only;
   "31-A a 31-E" keeps its two ends). The same for plural paragraphs ("§§", "parágrafos"). Each item becomes its
   own segment, so the existing rules give it the article and the § of the list. A legal-order segment (one that
   starts below the article) closes at its article: nothing lower joins it afterwards. What must not change, does not: "§ 3º, IV, a" (an alínea), "art. 9º, I e § 1º", another
   law's list. Golden contract: 5 new `citation` cases in the table's own spelling (53 → 58 cases, routing 82/82 → 87/87
   phrasings, 0 violations); R3 (the law citing itself) now reads **1,352 self-citations instead of 799** (553 sat
   in lists, ranges and plural paragraphs and were silently dropped) and resolves 1,350 (99.85%, was 796/799 =
   99.62%), 0 new misses. Gates lc05/lc06 score the vector and combined routes only: their baselines do not move.
2. **The bridge is deterministic and lives in Python**, as a step of `dag_rtc_ref` after the load (`link_lc214`):
   the parser is Python, and it is the same code the golden contract checks, so a parser change is tested once and
   used twice. No LLM: a classification's legal basis is a lookup, not a generation (ADR 0005's lesson).
3. **Grain: one row per citation of a reference**, with the outcome named (resolved | ambiguous | not_found |
   foreign | unparsed). A miss is a row, never an exception: the business sees which references the law served
   cannot ground, and the step's events name them (`reference_not_found`, `reference_ambiguous`).
4. **Two triggers, one skip rule.** Either side can move: a new official version (daily cron, rtc01 sees a new sha)
   or a new law version (an amendment, a parser fix). `load_postgres` in `dag_rag_lc214` emits
   `datalabs://serving/lc214` when the served law changed, and `dag_rtc_ref` runs on its cron **or** that asset
   (`AssetOrTimeSchedule`). The step skips (exit 99) when the table already holds the bridge of the same
   (rtc sha, law sha256, `BRIDGE_VERSION`), or when the law is not served (base stack, no AI profile).
5. **The transcription is a second source, reconciled.** For each resolved provision, `texto_check` compares the
   law's current text with the Receita's transcription by words (accents, case, punctuation, both kinds of
   newline ignored): `match` (contiguous), `near` (≥ 90% of the words), `diverges`, `no_transcription`. A
   divergence is a fact about two sources, not a verdict: the table can be behind an amendment or carry a slip.
6. **Signals, not failures** (checks, all `requires: ai`, severity warn): `rtc_lc214_bridge_current` (built from
   the versions in use), `rtc_lc214_citations_resolved_pct`, `rtc_lc214_cites_inactive_provision` (a cClassTrib
   grounded in a revoked/vetoed provision), `rtc_lc214_transcription_diverges`.
7. **Consumption in dbt.** `gold.br_cclasstrib_lc214`: cClassTrib × citation with the provision's text and status;
   `gold.vw_nfe_item_fundamento_lc214`: item → provision text, through `cltr_id`, now exposed by
   `gold.ft_nfe_item_aderencia_rtc` (rebuilt whole every run: a new column is safe).

**Measured** (2026-11-22, official table 1.7.2 / V0073, sha `f1eebd7` × LC 214 version `0cf36f4433ce`, 4,223
provisions; the same bridge computed with the router of `master` for comparison):

| | router before | router after |
|---|---|---|
| citations read from 202 references | 209 | **238** |
| resolved to one provision | 206 (99.52%) | **235 (99.58%)** |
| ambiguous / not found / unparsed ("LC 214/2025, Regra Geral") | 1 / 0 / 2 | 1 / 0 / 2 |
| cited provision revoked or vetoed | 0 | 0 |
| transcription match / near / diverges | 193 / 9 / 4 | 222 / 10 / 3 |

- **29 provisions** cited by the official table were invisible before (list items, ranges, plural paragraphs).
- The **ambiguous** one is in the source: "Art. 80 II" (fdlg 169). Art. 80 has an inciso II in § 1º and in § 1º-A
  and no inciso in the caput; the Receita's transcription shows which one ("o fornecimento dos seguintes bens e
  serviços... vinculados à exportação"). The bridge keeps both candidates and does not pick: choosing by the
  transcription is a separate decision.
- The **3 divergences** are real: arts. 485 I, 485 II and 487 § 2º. The transcription says "ficará sujeita" where
  the law says "será sujeita ao pagamento de IBS e de CBS", and drops the rates written out ("dois inteiros e oito
  centésimos por cento"): an older wording or a simplification, a question for whoever maintains the table.
- Checks on these numbers: `rtc_lc214_citations_resolved_pct` 99.58 (floor 99.0: one more miss),
  `rtc_lc214_cites_inactive_provision` 0, `rtc_lc214_transcription_diverges` 3 (baseline: a 4th warns),
  `rtc_lc214_bridge_current` 0. `dbt build tag:rtc`: `br_cclasstrib_lc214` 238 rows, its tests pass.
- Skip rule: a second run with nothing changed exits 99 (`bridge_up_to_date`); `--force` rebuilds.

Not covered yet: bare article numbers after a list of paragraphs ("arts. 446, 450, §§ 1º e 5º, **464, 465 e
474**") stay unread, as before.

**Consequences.**

- Every NF-e item with an adherent cClassTrib points at the text of the law that grounds it, in SQL, with no
  model in between. That is the anchor a later step can hand to the RAG's `combined` route ("explain why this
  item has a 60% reduction"): the provision is known before any retrieval.
- The router contract grew with a real second source: the table's references are the compact form a fiscal
  analyst writes, which the chat phrasings of the golden set did not cover.
- The bridge needs the law served, i.e. the AI profile. On the base stack it is skipped and its checks too.
- `FDLG_TEXTO` is compared, not stored again: the transcription stays in `ref_rtc`, the verdict in the bridge.
