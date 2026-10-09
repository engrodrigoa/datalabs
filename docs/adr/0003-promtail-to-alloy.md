# ADR 0003 — Log shipping: Promtail now, Grafana Alloy next

**Status:** accepted (migration planned)

Promtail is in long-term-support / end-of-life and Grafana Alloy is its successor. Promtail 2.9 is kept for
now because the pipeline stages (regex → json → labels) are validated against Loki 2.9. Next step: port
`infra/promtail/promtail-config.yaml` to Alloy (`loki.source.file` + `loki.process`) and upgrade Loki to 3.x
(structured metadata for `run_id` without label cardinality).
