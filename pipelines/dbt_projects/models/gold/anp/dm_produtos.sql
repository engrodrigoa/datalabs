{{
    config(
        materialized='table',
        tags=['gold', 'dimensao']
    )
}}

-- One row per product (the SK is the hash of the product name only)
SELECT
    MD5(CAST(produto AS VARCHAR))   AS id_produto_sk,
    MAX(id_produto)                 AS id_produto_origem,
    produto,
    MAX(unidade_de_medida)          AS unidade_de_medida
FROM {{ ref('anp_combustivel') }}
WHERE produto IS NOT NULL
GROUP BY produto
