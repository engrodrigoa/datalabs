{{ config(tags=['anp']) }}
-- O agregado não pode perder nem duplicar coletas: Σ qtd_coletas = linhas da fato.
SELECT a.total_agregado, f.total_fato
FROM (SELECT COALESCE(SUM(qtd_coletas), 0) AS total_agregado FROM {{ ref('ag_anp_preco_semanal') }}) a
CROSS JOIN (SELECT COUNT(*) AS total_fato FROM {{ ref('ft_anp_combustiveis') }}) f
WHERE a.total_agregado <> f.total_fato
