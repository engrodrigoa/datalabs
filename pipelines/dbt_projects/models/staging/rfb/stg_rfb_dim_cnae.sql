{{ config(
    materialized='view',
    tags=['rfb']
) }}

WITH source AS (
    SELECT * FROM {{ source('rfb', 'dominio_cnae') }}
)

SELECT
    codigo AS codg_cnae,
    descricao AS desc_cnae
FROM source