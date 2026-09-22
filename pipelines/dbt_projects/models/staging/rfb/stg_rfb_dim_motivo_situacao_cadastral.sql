{{ config(
    materialized='view',
    tags=['rfb']
) }}

WITH source AS (
    SELECT * FROM {{ source('rfb', 'dominio_motivo_situacao_cadastral') }}
)

SELECT
     CAST(codigo AS INTEGER) AS codg_motivo_sit_cadast,
    descricao AS desc_motivo_sit_cadast
FROM source