{{ config(
    materialized='view',
    tags=['rfb']
) }}

WITH source AS (
    SELECT * FROM {{ source('rfb', 'empresas') }}
    WHERE cnpj_basico IS NOT NULL
),

hashed AS (
    SELECT
        -- Geração das chaves MD5
        {{ dbt_utils.generate_surrogate_key(['cnpj_basico']) }} AS sk_empresa,
        
        -- Business Keys Originais
        cnpj_basico AS cnpj_basico,
        
        -- Atributos com Casts
        razao_social,
        CAST(natureza_juridica AS INTEGER) AS codg_natureza_juridica,
        CAST(qualificacao_responsavel AS INTEGER) AS codg_qualificacao_responsavel,
        
        -- Tratamento de valor monetário (se vier como string com vírgula da RFB)
        CAST(capital_social AS NUMERIC(18,2)) AS capital_social,
        
        CAST(porte_empresa AS INTEGER) AS codg_porte_empresa,
        ente_federativo_responsavel,
        
        -- Auditoria
        referencia_mes,
        _inserted_at AS data_carga

    FROM source
)

SELECT * FROM hashed
