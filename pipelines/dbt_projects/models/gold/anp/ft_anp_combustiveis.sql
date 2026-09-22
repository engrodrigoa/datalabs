{{ 
    config(
        materialized='incremental',
        unique_key='id_fato',
        incremental_strategy='delete+insert', 
        tags=['gold', 'fato']
    ) 
}}

WITH base_silver AS (
    SELECT
        chave,
        cnpj,
        produto,
        valor_venda,
        valor_compra,
        data_coleta
    FROM {{ ref('anp_combustivel') }}
    
    {% if is_incremental() %}
        WHERE data_coleta >= (
            SELECT COALESCE(MAX(data_coleta) - INTERVAL '3 days', '1900-01-01'::DATE) 
            FROM {{ this }}
        )
    {% endif %}
)

SELECT
    chave AS id_fato,
    MD5(CAST(cnpj AS VARCHAR)) AS id_posto_sk,
    MD5(CAST(produto AS VARCHAR)) AS id_produto_sk,
    data_coleta,
    valor_venda,
    valor_compra
FROM base_silver