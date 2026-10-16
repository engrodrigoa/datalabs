# ADR 0012 — LC 214 question router: citations by SQL, questions by vectors, both when both

**Status:** accepted (step 4b: `pipelines/rag/lc214/router.py`, checked by `golden_contract`); the vector search
and the generation are step 4c.

**Context.** ADR 0005 routes the ANP questions: identifiers to SQL, open questions to vectors. Legislation has the
same split with a twist: the identifier is a legal citation ("art. 12, § 2º, III") and people write it in two
orders, the chat order (big to small) and the legal order the law itself uses (small to big: "inciso III do § 2º
do art. 12"). An embedding does not retrieve an identifier: "art. 6º" in a question does not bring art. 6º back.
The primary user is a SEFAZ tax auditor (ADR 0010, golden set), who often cites a provision AND asks about it.

**Decisions.**

1. **Four routes, decided before any retrieval.**

   | route | when | answer |
   |---|---|---|
   | `sql` | only a citation, or asking its text / status / previous wording | the provision and its structure, literal, no LLM |
   | `combined` | a citation + a question ("o art. 6º se aplica a transferência entre filiais?") | the cited provision is a mandatory anchor in the context; the vectors complete it |
   | `vector` | no citation of the LC 214 (a citation of another law does not count) | similarity search |
   | `clarify` | the citation matches several provisions, or none | list the candidates, or say it does not exist; never guess |

2. **One citation grammar for both orders.** Parts (art., §, inciso, alínea, caput) next to each other form a
   citation; "e" splits a list, and a citation without an article takes it from the one it is listed with (the next
   one in the legal order, the previous one in the chat order), with the levels above its own. Arabic incisos
   become roman ("inc. 3" → III), lettered provisions are kept ("art. 7º-A", "§ 5º-A").
3. **Resolution allows unstated levels, and asks when that is ambiguous.** "inciso III do art. 12" matches the
   incisos III of three §§: `clarify`. One drafting convention is applied, and only when it leaves one candidate:
   an inciso cited right after the article is of the caput ("art. 6º, II" ≠ "art. 6º, § 2º, II"). An alínea without
   its inciso is never guessed. A provision printed twice in the source (`inc-II#2`, ADR 0010) is ambiguous on
   purpose: the law itself is.
4. **Another law is never an anchor.** "art. 14 do Código Tributário Nacional", "arts. 31-A a 31-E da Lei nº
   4.591", "art. 155, II ... ambos da Constituição Federal": reported as `foreign`, routed to vectors, so the answer
   can say the rule lives elsewhere (`cite_external` cases of the golden set).
5. **The router is deterministic, so its quality gate is exact, not a threshold.** The golden contract adds
   R1 (every phrasing takes the case's route), R2 (anchors / options / missing match the labels) and R3 (the law's
   citations of itself resolve, ≥ `LC214_MIN_SELF_REF` = 99%). The `route` of a golden case is now the router's
   decision: `no_answer` goes to `vector` (abstaining is the behavior after retrieval, not a route). The router
   needs only the provision ids, so the same code runs in the DAG, the tests and the CLI
   (`python -m pipelines.rag.lc214.router "pergunta" --db`).
6. **The SQL answer is the structure**, indented by the id depth (an inciso of the caput is one level below the
   article, one of a § two levels), revoked/vetoed provisions show their note instead of an empty label, and a
   provision longer than `LC214_ANSWER_PAGE_WORDS` (400) is paged, never cut (art. 517: 7 parts).

**Measured (version `0cf36f4433ce`, 2026-10-16).**

| | |
|---|---|
| golden set v2 | 53 cases, 82 phrasings: 82 routed right (37 vector, 11 sql, 2 combined, 3 clarify) |
| the law citing itself | 799 citations of the LC 214 in its own text: 796 resolved (99.62%); 198 citations of other laws recognised; revoking articles 540, 542, 543 excluded (their items cite laws named in the item above) |
| known misses | 3: two in a long mixed list of art. 472 ("..., 246, incisos II e III do § 4º do art. 293, ..."), one implicit reference to the LC 123 in art. 26 |

The corpus found four parser bugs that no hand-written case had: "alíneas do" read as alínea "s", ranges
("31-A a 31-E"), a law named once for a whole list ("todos da Constituição Federal") and, in the measure itself,
every article's own label counted as a citation (1,335 instead of 799: a rate inflated by 536 trivial hits).

**Consequences.** Step 4c plugs the vector search into `combined` and `vector` (incremental embeddings, ADR 0007)
and the generation with an explicit context budget (`num_ctx` 8192 for `llama3.2:1b`, prompt measured before it is
sent and `prompt_eval_count` logged after, so a truncation is visible). Intent detection is a keyword list on
purpose: it is cheap and testable; a miss becomes a golden case before it becomes a rule. Lists of incisos in the
chat order ("art. 5º, I e IV") resolve the first one only, recorded here as a limit.
