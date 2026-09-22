{{ config(
    materialized='view',
    tags=['rfb']
) }}

WITH source AS (
    SELECT * FROM {{ source('rfb', 'socios') }}
    WHERE cnpj_basico IS NOT NULL
),

hashed AS (
    SELECT
        -- Geração da Chave MD5 Composta para unicidade da linha
        {{ dbt_utils.generate_surrogate_key(['cnpj_basico', 'cnpj_cpf_socio', 'nome_socio_razao_social']) }} AS sk_socio_empresa,
        
        -- Foreign Key para join com Empresas
        {{ dbt_utils.generate_surrogate_key(['cnpj_basico']) }} AS sk_empresa,
        
        -- Business Keys
        cnpj_basico AS cnpj_basico,
        cnpj_cpf_socio,
        
        -- Atributos
        CAST(identificador_socio AS INTEGER) AS identificador_socio,
        nome_socio_razao_social,
        CAST(qualificacao_socio AS INTEGER) AS codg_qualificacao_socio,
        CAST(data_entrada_sociedade AS DATE) AS data_entrada_sociedade,
        CAST(pais AS INTEGER) AS codg_pais,
        representante_legal,
        nome_representante,
        CAST(qualificacao_representante_legal AS INTEGER) AS codg_qualificacao_representante_legal,
        CAST(faixa_etaria AS INTEGER) AS codigo_faixa_etaria,
        
        -- Auditoria
        referencia_mes,
        _inserted_at AS data_carga

    FROM source
)

SELECT * FROM hashed
