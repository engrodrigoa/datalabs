{{
    config(
        materialized='incremental',
        unique_key='id_fato',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns',
        tags=['gold', 'fato']
    )
}}

-- Watermark on INGESTION time, not on the business date: the monthly closed file arrives
-- weeks later with old `data_coleta` values and would be skipped by a date-based filter
-- (late-arriving data). Reprocessing a key is safe thanks to delete+insert on id_fato.
WITH base_silver AS (
    SELECT chave, cnpj, produto, valor_venda, valor_compra, data_coleta, ingestion_timestamp
    FROM {{ ref('anp_combustivel') }}
    {% if is_incremental() %}
    WHERE ingestion_timestamp >= (
        SELECT COALESCE(MAX(_source_loaded_at), '1900-01-01'::timestamp) FROM {{ this }}
    )
    {% endif %}
)

SELECT
    chave                           AS id_fato,
    MD5(CAST(cnpj AS VARCHAR))      AS id_posto_sk,
    MD5(CAST(produto AS VARCHAR))   AS id_produto_sk,
    data_coleta,
    valor_venda,
    valor_compra,
    ingestion_timestamp             AS _source_loaded_at,
    NOW()                           AS _dbt_loaded_at
FROM base_silver
