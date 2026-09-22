{{ config(
    materialized='view', 
    tags=['rfb'] 
) }}

WITH source AS (
    SELECT * FROM {{ source('rfb', 'estabelecimentos') }}
    WHERE cnpj_completo IS NOT NULL
),

hashed AS (
    SELECT
        -- Geração das chaves MD5 (Surrogate Keys)
        {{ dbt_utils.generate_surrogate_key(['cnpj_completo']) }} AS sk_estabelecimento,
        {{ dbt_utils.generate_surrogate_key(['cnpj_basico']) }} AS sk_empresa,
        
        -- Business Keys Originais (Mantidas para referência)
        cnpj_completo AS cnpj_completo,
        cnpj_basico AS cnpj_basico,
        
        -- Colunas brutas e Casts (Atributos)
        cnpj_ordem,
        cnpj_dv,
        identificador_matriz_filial,
        nome_fantasia,
        
        CAST(situacao_cadastral AS INTEGER) AS situacao_cadastral_codigo,
        CAST(data_situacao_cadastral AS DATE) AS data_situacao_cadastral,
        CAST(motivo_situacao_cadastral AS INTEGER) AS motivo_situacao_cadastral,
        
        nome_cidade_exterior,
        CAST(pais AS INTEGER) AS codg_pais,
        CAST(data_inicio_atividade AS DATE) AS data_inicio_atividade,
        
        cnae_fiscal_principal,
        cnae_fiscal_secundaria,
        
        tipo_logradouro,
        logradouro,
        numero,
        complemento,
        bairro,
        cep,
        uf,
        CAST(municipio AS INTEGER) AS codg_municipio,
        
        ddd_1,
        telefone_1,
        ddd_2,
        telefone_2,
        ddd_fax,
        fax,
        correio_eletronico,
        situacao_especial,
        CAST(data_situacao_especial AS DATE) AS data_situacao_especial,
        
        -- Auditoria
        referencia_mes,
        _inserted_at as data_carga

    FROM source
)

SELECT * FROM hashed