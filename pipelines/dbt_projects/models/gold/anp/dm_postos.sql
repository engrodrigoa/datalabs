{{ 
    config(
        materialized='table',
        unique_key='id_posto_sk',
        tags=['gold', 'dimensao']
    ) 
}}

WITH base_silver AS (
    SELECT 
        cnpj,
        revenda,
        bandeira,
        regiao_sigla,
        estado_sigla,
        municipio,
        bairro,
        cep,
        nome_da_rua,
        numero_rua,
        complemento,
        data_coleta,
        ROW_NUMBER() OVER (
            PARTITION BY cnpj 
            ORDER BY data_coleta DESC 
        ) as rn
    FROM {{ ref('anp_combustivel') }}
    WHERE cnpj IS NOT NULL
)

SELECT
    MD5(CAST(cnpj AS VARCHAR)) AS id_posto_sk,
    cnpj,
    revenda,
    bandeira,
    regiao_sigla,
    estado_sigla,
    municipio,
    bairro,
    cep,
    nome_da_rua,
    numero_rua,
    complemento
FROM base_silver
WHERE rn = 1