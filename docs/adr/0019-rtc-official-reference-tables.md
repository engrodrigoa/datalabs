# ADR 0019 — Official CST / cClassTrib / NCM tables from the Receita Federal tax calculator, and NF-e adherence

**Status:** accepted, amended 2026-11-18 (contract v2, below) (`pipelines/rtc/`, `dag_rtc_ref`, schema `ref_rtc` + `ctrl.rtc_carga` in
`infra/postgres/init/sql/08_rtc.sql`, dbt tag `rtc`, checks `rtc_*`).

**Context.** The NF-e domain classified items with a 3-row seed (`nfe_ref_classtrib.csv`, "simplified descriptions,
check against the official table"). The official table exists, changes by technical note every few weeks, and had
no machine-readable source: the Portal NF-e publishes it as spreadsheets. On 2026-11-13 (Portaria Cotec nº 342/2026)
the Receita Federal published the source of its tax calculator on GitHub under MIT
(`ReceitaFederal/calculadora-rtc-regime-regular`). Its reference tables are not a dataset but Flyway migrations for
SQLite: a 2 MB baseline (`B0001`) and one `V<n>` script per change of the official tables, named by publication
date ("nova tabela cClassTrib 15/04/2026"). That makes the repository a versioned, auditable changelog of the
regulation — and the first automatable source of it.

The question for the lab: *can the items of a note be checked against the official table, point in time, and can
the case the authorising server does not block — a valid classification used on a product it does not cover — be
found?*

**Measured before deciding (2026-11-17, release 1.6.1, sha `150cccbe414d`).**

| | |
|---|---|
| Mirror history | 8 commits up to 1.6.1 (squashed: the mirror receives releases, not the internal history) |
| Internal cadence (CHANGELOG) | 1.5.0 2026-09-08, 1.5.1 09-10, 1.5.4 09-30, 1.6.0 10-13 |
| `git ls-remote` + sparse `git fetch --depth 1` | works; `api.github.com` and `codeload.github.com` answered 403 on the test network |
| Replay without Java | 61 scripts, 0.6-0.7 s, 0 foreign key violations |
| Contents | 193 cClassTrib (46 with an NCM list), 1,372 NCM rules, 76 exceptions, 821 annex items |
| Code is not a key | 14 codes repeat (`000001` exists for IBS/CBS from 2026 and for IS from 2027) |
| Generator catalogue | all 8 agro items eligible (Annex IX items 2, 5, 6, 7): the synthetic data was right |

**Decisions.**

1. **Replay the Flyway scripts with Python, do not run the calculator.** The tables are what the lab needs; Java,
   Maven and a Spring service are not. `flyway_replay.py` follows the two Flyway behaviours the scripts depend on,
   each found by a failing script: every `V<n>` runs in its own transaction (`PRAGMA defer_foreign_keys` in V0011
   needs one), and a script that contains `PRAGMA foreign_keys` runs outside a transaction (`flyway.mixed=true`;
   V0044 adds a `REFERENCES` column with a default, accepted only with foreign keys off). Order is numeric.
2. **Source check = the request the step makes: `git ls-remote`.** Same sha as the manifest → exit 99. A new sha is
   fetched by that sha (sparse: `flyway/sql`, `pom.xml`, `CHANGELOG.md`), so what is built is what was checked.
   git and not the REST API: no token, no 60/h limit, and it worked where the API did not.
3. **Structural source contract** (`CONTRACT_MIN_ROWS`, ~70% of 1.6.1): every table the pipeline reads exists with
   a floor of rows, and the replay has no foreign key violation. A rejected release is not landed (the manifest is
   the commit point, as in ADR 0010). Overlapping validity inside one tax group is reported, not fatal: the source
   is authoritative, and the dbt test `assert_rtc_cclasstrib_vigencia_sem_sobreposicao` makes it visible.
4. **Raw copy in `ref_rtc`, typing in dbt.** `rtc02_load` replaces every table in ONE transaction and records the
   version in `ctrl.rtc_carga` (release, `V00nn`, sha, row counts): every reference row has a lineage. Same columns →
   DELETE + COPY (dependent views survive); changed columns → DROP CASCADE + CREATE, event
   `schema_drift_table_recreated`, and the dbt step that follows rebuilds the views or fails naming the column.
5. **Natural key = (tax group, cClassTrib, start of validity).** The group comes from the CST's taxes
   (`tributo_situacao_tributaria`): IS or IBSCBS. Joins with items are point in time (`dt_emissao` within validity).
6. **One status per item, the first rule that fails** (`gold.ft_nfe_item_aderencia_rtc`): `sem_cclasstrib` →
   `cclasstrib_inexistente` → `cclasstrib_fora_vigencia` → `cst_incompativel` → `ncm_inelegivel` → `aderente`.
   NCM eligibility is the calculator's own rule (`NcmAplicavelRepository`: the rule's NCM is a PREFIX of the item's
   NCM, minus prefix exceptions, both with validity). `ncm_inelegivel` is a signal, never a pipeline failure: dbt
   `warn` + `store_failures` in `dq_nfe`, like the other business anomalies (ADR 0006).
7. **The generator gets the case the rules exist for.** New defect `beneficio_ncm_inelegivel`: an electronics item
   with the agro inputs treatment (CST 200, cClassTrib 200038, 60% reduction), coherent in CST and values — only
   the NCM is outside Annex IX. The manifest (`--manifesto`) is the answer key.
8. **Daily DAG, dbt even when nothing new was fetched.** The adherence reads two moving inputs (reference and items);
   "skipped" upstream means "reference up to date". In the lab the adherence is rebuilt whole (seconds); production
   would make it incremental by batch and re-evaluate only when `ctrl.rtc_carga` gets a row.

**Result on the lab (3,000 notes, 9,291 items, `--defect-rate 0.04`, seed 214).**

| Injected (manifest) | Notes | Detected as | Notes |
|---|---|---|---|
| `beneficio_ncm_inelegivel` | 30 | `ncm_inelegivel` | 30 |
| `cclasstrib_incoerente` | 23 | `cst_incompativel` | 23 |
| `tot_cbs_divergente`, `vbc_ibscbs_divergente` | 68 | — (totals, other tests) | 0 |
| none | 2,879 | — | 0 false positives |

Pipeline: rtc01 2.2 s (fetch + replay + land), rtc02 0.5 s (3,538 rows), dbt `tag:rtc` 6.8 s (28 nodes, 2 warns =
the injected anomalies). Checks: `rtc_reference_loaded`, `rtc_reference_age_days` (warn > 45 days: measures the
mirror's real cadence), `rtc_serving_parity`, `rtc_nfe_adherence_coverage`, `rtc_nfe_ncm_ineligible_pct`
(0.30% measured, warn > 1%).

**Consequences.**

- The seed `nfe_ref_classtrib` and `dm_nfe_tributacao` stay as they are (the star schema's dimension); switching the
  dimension to `silver.rtc_cclasstrib` is a separate change (it alters a published gold model).
- `fundamentacao_legal` carries the short legal reference (`Art. 138, § 1º`): the same ids the LC 214 RAG uses
  (ADR 0010), so a cClassTrib can be explained by its provision. Not wired yet.
- IS has its own cClassTrib list (29 classifications, 391 NCM rules, from 2027): the same model applies to
  `cclasstrib_is` when the generator emits IS items.
- The `Lake` classes are imported from `pipelines.rag.lc214.lake`; they are generic and should move to
  `pipelines/commons` when a third consumer appears.
- Open: how often the mirror follows the internal releases (the age check measures it); a Grafana row for the
  adherence (`ag_nfe_aderencia_rtc_mensal`) is not built yet.
- The same design is the proposal for a production DW (PoC outside this repository): official table as a governed,
  versioned dimension, point-in-time join with the items, a signal for the business rather than a verdict.

## Amendment 1 (2026-11-18) — contract v2: official reductions, treatments, rule `reducao_divergente`

**Context.** The adherence checked *which* classification an item used, not *how much* discount it took. The
official percentage lives in `PERCENTUAL_REDUCAO`, outside the v1 contract.

**Measured (release 1.6.1).** 492 rows = 164 cClassTrib x (CBS, IBSUF, IBSMun); values 0, 30, 40, 50, 60, 70, 80,
100. CBS and IBS can differ for the same classification (200025: CBS 100%, IBS 60%). Reference rates 2026: CBS 0.9%,
IBSUF 0.1%, IBSMun 0%. `TRATAMENTO_CLASSIFICACAO` (193) links each cClassTrib to one of 43 treatments
(`TRATAMENTO_TRIBUTARIO`: "Alíquota reduzida em 60%" + the JEXL expressions of the calculation).

**Decisions.**

1. **Contract v2** adds `PERCENTUAL_REDUCAO`, `TRATAMENTO_CLASSIFICACAO`, `TRATAMENTO_TRIBUTARIO`.
   `flyway_replay.CONTRACT_VERSION` enters the skip rule of both steps: rtc01 skips only when sha AND contract are
   unchanged (event `contract_changed` otherwise), rtc02 only when `(sha, contract_version)` is in `ctrl.rtc_carga`
   (unique index replaces the unique sha; `08_rtc.sql` migrates a v1 table in place). Without it, a new contract
   would wait for the Receita's next release. Manifests without the field read as v1.
2. **Long format** `silver.rtc_percentual_reducao` (cClassTrib x tributo x validity), not one column: the taxes
   differ and each has its own validity.
3. **Declared reduction without touching `silver.nfe_item`.** The item carries `pRedAliq` only for IBSUF; the CBS
   reduction is derived from the effective rate, `100 x (1 - pAliqEfet / pCBS)`. Parsing the other `gRed` groups
   would add columns to an incremental model with `on_schema_change='fail'` (full refresh of every lab).
   Tolerances: IBSUF 0.01 p.p.; CBS 1 p.p. (pAliqEfet rounded to 2 decimals: 0.005 / 0.9 = 0.56 p.p. worst case).
   IBSMun is not evaluated: a 0% reference rate has no reduction to measure.
4. **Rule 6 `reducao_divergente`**, after `ncm_inelegivel` (classification and NCM right, discount wrong). Generator
   defect `reducao_divergente`: eligible agro input, CST 200 / 200038, `pRedAliq` 30 instead of 60, values
   recalculated with 30% so the totals still reconcile.

**Result (7,000 notes in two batches, 22,038 items).**

| Injected | Notes | Detected as | Notes |
|---|---|---|---|
| `beneficio_ncm_inelegivel` | 64 | `ncm_inelegivel` | 64 |
| `cclasstrib_incoerente` | 63 | `cst_incompativel` | 63 |
| `reducao_divergente` | 27 | `reducao_divergente` | 27 |
| totals (`tot_cbs_*`, `vbc_ibscbs_*`) | 154 | — (other tests) | 0 |
| none | — | — | 0 false positives |

Max |declared − official| CBS among adherent items: 0.00 p.p. Upgrade v1 → v2 on a loaded lab: rtc01
`contract_changed` → landed, rtc02 17 tables / 4,266 rows, second run exit 99.
