# ANP dashboard (Streamlit)

Analytics layer over the ANP gold ([ADR 0016](../../docs/adr/0016-anp-streamlit-analytics-layer.md)).

```bash
make env && docker compose up -d postgres && make db-bootstrap   # once: creates ANP_APP_DB_PASSWORD + role anp_app_ro
make demo-offline        # or the real dag_dbt_anp: builds the two serving marts
make dashboard-anp       # creates .venv-anp on first run, then http://localhost:8501
```

| File | Role |
|---|---|
| `app.py` | layout only: sidebar filters (UF → cidade → bairro, bandeira, busca, janela), product cards, tabs |
| `logic.py` | pure Polars: filters, recency window, KPIs, matrix pivot, exact weekly roll-up (tested in `tests/test_anp_dashboard.py`) |
| `data.py` | the only SQL, bind parameters, role `anp_app_ro` (`ANP_APP_DB_*`, falls back to `DB_*`) |
| `.streamlit/config.toml` | theme (palette of the reference site) |

| Tab | Source | Grain |
|---|---|---|
| Cards + Matriz de preços | `gold.ag_anp_preco_posto_atual` | latest price per station × product, filtered by the recency window |
| Histórico | `gold.ag_anp_preco_municipio_diario` | collection day (or week) × product, Brasil / UF / city |
