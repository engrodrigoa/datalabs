{{ config(
    materialized='view',
    tags=['rfb']
) }}

WITH source AS (
    SELECT * FROM {{ source('rfb', 'dominio_natureza_juridica') }}
)

SELECT
    CAST(codigo AS INTEGER) AS codg_natureza_juridica,
    descricao AS desc_natureza_juridica
FROM source