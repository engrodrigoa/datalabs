{{ config(
    materialized='view',
    tags=['rfb']
) }}

WITH source AS (
    SELECT * FROM {{ source('rfb', 'dominio_pais') }}
)

SELECT
    CAST(codigo AS INTEGER) AS codg_pais,
    descricao AS desc_pais
FROM source