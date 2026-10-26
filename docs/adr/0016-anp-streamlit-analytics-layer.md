# ADR 0016 — ANP analytics layer: Streamlit over two gold serving marts, read-only role

**Status:** proposed (first version: `apps/anp_dashboard/`, marts `ag_anp_preco_posto_atual` and
`ag_anp_preco_municipio_diario`).

**Context.** The ANP gold (`ft_anp_combustiveis` + `dm_postos` + `dm_produtos`, and the weekly UF aggregate
`ag_anp_preco_semanal` used by Grafana) answers pipeline questions well. It does not answer a reader's two
questions, modelled on combustivelbr.com.br (dark UI, product cards, cheapest stations table, 26-week line):
*how much does fuel cost now, from a UF down to a station?* and *how did it move, down to the city?*
Computing either from the fact on every page view means a window function over ~790k rows (real 2026 load)
per click, and the Grafana role (`grafana_ro`) reads every schema, which a public-facing app must not.

**Decisions.**

1. **Two serving marts, one per question.**
   - `ag_anp_preco_posto_atual`: one row per station × product, the latest collection and the previous one
     (change). ANP surveys each station on its own day, so the model does NOT filter by date: the app applies a
     *recency window* (default 30 days from the newest collection) and shows how much of the selection it
     keeps ("Sobre os dados"). Bairro and brand are trimmed/upper-cased (free text in the source).
   - `ag_anp_preco_municipio_diario`: collection day × city × product with `qtd_postos` and
     `soma_valor_venda`, so UF/Brasil and weekly roll-ups are exact (`sum / count`), never an average of
     averages. The line stops at the city on purpose: a bairro has one or two collections per day.
   Both are reconciled with the fact by singular tests (row count and summed price; latest price per key).
2. **Least privilege by dbt grants.** Role `anp_app_ro` (created by `00_bootstrap.sh`, `NOLOGIN` until
   `ANP_APP_DB_PASSWORD` is set) gets `USAGE` on `gold` and `SELECT` only on the two marts, through
   `grants={'select': ['anp_app_ro']}` in the models: re-applied on every build, so a rebuilt table never loses it
   and no other gold table is readable (CI asserts `permission denied` on the fact).
3. **The app is decoupled from the pipelines.** Own venv (`make dashboard-anp`, `apps/anp_dashboard/requirements.txt`,
   Streamlit 1.66.1), own config (`ANP_APP_DB_*`; host defaults to `localhost`, never `DB_HOST`, which is the container name `postgres` for the services; port/database fall back to `DB_PORT`/`DB_DB`), no import of `pipelines.commons`.
   Transforms are pure Polars in `logic.py` (unit-tested in CI without Streamlit); `data.py` holds the only SQL,
   with bind parameters for every UI value; `app.py` is layout only.
4. **Visual contract.** Palette from the reference site (surface `#0f0f13`/`#17171d`, accent `#1d9e75`); product
   colours follow the product, never its rank (fixed order in `logic.PRODUTOS`, validated for a dark surface:
   adjacent CVD ΔE ≥ 8.4, contrast ≥ 3:1). One y-axis, min–max band only for a single series.

**Consequences.** Two more tables per ANP build (synthetic sample: 2,764 and 13,214 rows, ~1-2 MB; real data
estimated at ~140k and a few hundred thousand). New models need `make dbt-parse`. An existing volume needs
`make env` (adds `ANP_APP_DB_PASSWORD`), `docker compose up -d postgres` (new env var) and `make db-bootstrap`.
Not decided yet: how it is published (Streamlit Community Cloud cannot reach the lab Postgres; options are a
Parquet snapshot of the two marts exported by the DAG, or a hosted read replica), and whether the app becomes a
compose service.
