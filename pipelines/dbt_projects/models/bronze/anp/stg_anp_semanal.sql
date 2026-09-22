{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'anp_landing_semanal') }}
),

cleaned AS (
    SELECT
        produto,
        CASE 
            WHEN produto = 'GASOLINA' THEN 1
            WHEN produto = 'ETANOL' THEN 2
            WHEN produto = 'DIESEL' THEN 3
            WHEN produto = 'DIESEL S10' THEN 4
            WHEN produto = 'GNV' THEN 5
            WHEN produto = 'GASOLINA ADITIVADA' THEN 6
            WHEN produto = 'DIESEL S50' THEN 7
            WHEN produto = 'GLP' THEN 8
            ELSE 0 
        END AS id_produto_calc,
        
        -- Clean Code: NULLIF evita erros de parse caso o campo venha como string vazia ('') do CSV
        CAST(REPLACE(NULLIF(valor_de_venda, ''), ',', '.') AS DOUBLE PRECISION) AS valor_venda_cast,
        CAST(REPLACE(NULLIF(valor_de_compra, ''), ',', '.') AS DOUBLE PRECISION) AS valor_compra_cast,
        
        unidade_de_medida,
        
        -- A coluna já é texto na landing, então o CAST não é necessário no TO_DATE
        TO_DATE(data_da_coleta, 'DD/MM/YYYY') AS data_coleta_cast,
        
        regiao_sigla,
        estado_sigla,
        municipio,
        revenda,
        REGEXP_REPLACE(cnpj_da_revenda, '[-./ ]', '', 'g') AS cnpj_clean,
        nome_da_rua,
        numero_rua,
        complemento,
        bairro,
        REGEXP_REPLACE(cep, '[-./ ]', '', 'g') AS cep_clean,
        arquivo, 
        bandeira,
        ingestion_timestamp
    FROM source
)

SELECT 
    produto,
    id_produto_calc AS id_produto,
    valor_venda_cast AS valor_venda,
    valor_compra_cast AS valor_compra,
    unidade_de_medida,
    data_coleta_cast AS data_coleta,
    regiao_sigla,
    estado_sigla,
    municipio,
    revenda,
    cnpj_clean AS cnpj,
    nome_da_rua,
    numero_rua,
    complemento,
    bairro,
    cep_clean AS cep,
    arquivo,
    bandeira,
    ingestion_timestamp,
    
    -- Uso do TO_CHAR para padronizar o formato da data na Surrogate Key
    COALESCE(TO_CHAR(data_coleta_cast, 'YYYYMMDD'), '') || 
    COALESCE(CAST(id_produto_calc AS VARCHAR), '') || 
    COALESCE(cnpj_clean, '') AS chave
FROM cleaned