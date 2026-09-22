{{ config(
    materialized='view',
    tags=['rfb']
) }}

WITH source AS (
    SELECT * FROM {{ source('rfb', 'simples') }}
    WHERE cnpj_basico IS NOT NULL
),

hashed AS (
    SELECT
        -- Geração da Chave MD5 Composta para unicidade da linha
        {{ dbt_utils.generate_surrogate_key(['cnpj_basico']) }} AS sk_simples,
        cnpj_basico,
        opcao_simples,
        CAST(data_opcao_simples AS DATE) AS data_opcao_simples,
        CAST(data_exclusao_simples AS DATE) AS data_exclusao_simples,
        opcao_mei,
        CAST(data_opcao_mei AS DATE) AS data_opcao_mei,
        CAST(data_exclusao_mei AS DATE) AS data_exclusao_mei,
        referencia_mes
        
    FROM source
)

SELECT * FROM hashed
