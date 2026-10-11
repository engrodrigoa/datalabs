{#- Agregado semanal de preços (UF × produto × semana) para BI e para o dashboard Grafana da ANP.
    Lê a fato uma vez por run e entrega uma tabela pequena: os painéis não varrem a fato inteira. -#}
{{ config(materialized='table', tags=['gold', 'agregado'], indexes=[{'columns': ['semana']}]) }}

SELECT
    DATE_TRUNC('week', f.data_coleta)::date      AS semana,
    d.estado_sigla                                AS uf,
    p.produto,
    COUNT(*)                                      AS qtd_coletas,
    COUNT(DISTINCT f.id_posto_sk)                 AS qtd_postos,
    COUNT(DISTINCT d.municipio)                   AS qtd_municipios,
    AVG(f.valor_venda)                            AS preco_medio,
    MIN(f.valor_venda)                            AS preco_min,
    MAX(f.valor_venda)                            AS preco_max,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.valor_venda) AS preco_mediano,
    MAX(f._source_loaded_at)                      AS _source_loaded_at
FROM {{ ref('ft_anp_combustiveis') }} f
JOIN {{ ref('dm_postos') }}   d ON d.id_posto_sk = f.id_posto_sk
JOIN {{ ref('dm_produtos') }} p ON p.id_produto_sk = f.id_produto_sk
GROUP BY 1, 2, 3
