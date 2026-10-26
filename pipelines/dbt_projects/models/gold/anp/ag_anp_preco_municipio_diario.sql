{#- Serving: price per collection day x city x product, the source of the dashboard's history line
    (Brasil, UF or city; never below the city, the bairro sample is too thin per day).
    soma_valor_venda + qtd_postos let the app roll up to UF/Brasil and to weeks EXACTLY
    (avg = sum / count); an average of averages would weight a 2-station town like a capital. -#}
{{ config(materialized='table', tags=['gold', 'agregado', 'serving'], grants={'select': ['anp_app_ro']},
          indexes=[{'columns': ['data_coleta']}, {'columns': ['uf', 'municipio']}]) }}

SELECT
    f.data_coleta,
    d.estado_sigla                     AS uf,
    UPPER(TRIM(d.municipio))           AS municipio,
    p.produto,
    COUNT(*)                           AS qtd_postos,   -- grain of the fact: one collection per station/product/day
    SUM(f.valor_venda)                 AS soma_valor_venda,
    AVG(f.valor_venda)                 AS preco_medio,
    MIN(f.valor_venda)                 AS preco_min,
    MAX(f.valor_venda)                 AS preco_max,
    MAX(f._source_loaded_at)           AS _source_loaded_at
FROM {{ ref('ft_anp_combustiveis') }} f
JOIN {{ ref('dm_postos') }}   d ON d.id_posto_sk = f.id_posto_sk
JOIN {{ ref('dm_produtos') }} p ON p.id_produto_sk = f.id_produto_sk
GROUP BY 1, 2, 3, 4
