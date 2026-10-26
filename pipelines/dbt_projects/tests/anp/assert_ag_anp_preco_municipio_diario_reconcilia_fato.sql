{{ config(tags=['anp']) }}
-- O agregado diário não pode perder nem duplicar coletas, nem mudar o valor somado.
SELECT a.total_agregado, f.total_fato, a.soma_agregado, f.soma_fato
FROM (SELECT COALESCE(SUM(qtd_postos), 0) AS total_agregado, COALESCE(SUM(soma_valor_venda), 0) AS soma_agregado
      FROM {{ ref('ag_anp_preco_municipio_diario') }}) a
CROSS JOIN (SELECT COUNT(*) AS total_fato, COALESCE(SUM(valor_venda), 0) AS soma_fato
            FROM {{ ref('ft_anp_combustiveis') }}) f
WHERE a.total_agregado <> f.total_fato
   OR ABS(a.soma_agregado - f.soma_fato) > 0.01
