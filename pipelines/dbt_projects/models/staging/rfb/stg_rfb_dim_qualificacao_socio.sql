{{ config(
    materialized='view',
    tags=['rfb']
) }}

WITH source AS (
    SELECT * FROM {{ source('rfb', 'dominio_qualificacao_socio') }}
)

SELECT
     CAST(codigo AS INTEGER) AS codg_qualificacao,
    descricao AS desc_qualificacao
FROM source