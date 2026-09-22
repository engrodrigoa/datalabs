{{ config(
    materialized='view',
    tags=['rfb']
) }}

WITH source AS (
    SELECT * FROM {{ source('rfb', 'dominio_municipio') }}
)

SELECT
    CAST(codigo AS INTEGER) AS codg_municipio,
    descricao AS desc_municipio
FROM source