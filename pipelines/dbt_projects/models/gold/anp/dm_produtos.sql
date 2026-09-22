{{ 
    config(
        materialized='table',
        tags=['gold', 'dimensao']
    ) 
}}

WITH base_silver AS (
    SELECT DISTINCT
        produto,
        unidade_de_medida,
        id_produto
    FROM {{ ref('anp_combustivel') }}
    WHERE produto IS NOT NULL
)

SELECT
    -- Geração da Surrogate Key via Hash determinístico
    MD5(CAST(produto AS VARCHAR)) AS id_produto_sk,
    id_produto AS id_produto_origem,
    produto,
    unidade_de_medida
FROM base_silver