{{ config(materialized='table') }}

-- Weekly + monthly loads overlap: keep the most recent ingestion of each natural key.
WITH uniao_bases AS (
    SELECT * FROM {{ ref('stg_anp_mensal') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_anp_semanal') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY chave ORDER BY ingestion_timestamp DESC, arquivo DESC) AS rn
    FROM uniao_bases
)

SELECT
    CAST(chave AS VARCHAR)              AS chave,
    CAST(produto AS VARCHAR)            AS produto,
    CAST(id_produto AS INTEGER)         AS id_produto,
    CAST(valor_venda AS DOUBLE PRECISION)  AS valor_venda,
    CAST(valor_compra AS DOUBLE PRECISION) AS valor_compra,
    CAST(unidade_de_medida AS VARCHAR)  AS unidade_de_medida,
    data_coleta,
    CAST(regiao_sigla AS VARCHAR)       AS regiao_sigla,
    CAST(estado_sigla AS VARCHAR)       AS estado_sigla,
    CAST(municipio AS VARCHAR)          AS municipio,
    CAST(revenda AS VARCHAR)            AS revenda,
    CAST(cnpj AS VARCHAR)               AS cnpj,
    CAST(nome_da_rua AS VARCHAR)        AS nome_da_rua,
    CAST(numero_rua AS VARCHAR)         AS numero_rua,
    CAST(complemento AS VARCHAR)        AS complemento,
    CAST(bairro AS VARCHAR)             AS bairro,
    CAST(cep AS VARCHAR)                AS cep,
    CAST(bandeira AS VARCHAR)           AS bandeira,
    CAST(arquivo AS VARCHAR)            AS arquivo,
    ingestion_timestamp
FROM deduplicated
WHERE rn = 1
