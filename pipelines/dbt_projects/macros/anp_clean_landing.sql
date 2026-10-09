{#
  Shared cleaning for the ANP landing tables (weekly and monthly files have the same layout).
  Natural key: data da coleta | produto | cnpj  -> no collisions for unmapped products.
#}
{% macro anp_clean_landing(source_relation) %}
WITH source AS (
    SELECT * FROM {{ source_relation }}
),

cleaned AS (
    SELECT
        produto,
        CASE produto
            WHEN 'GASOLINA'           THEN 1
            WHEN 'ETANOL'             THEN 2
            WHEN 'DIESEL'             THEN 3
            WHEN 'DIESEL S10'         THEN 4
            WHEN 'GNV'                THEN 5
            WHEN 'GASOLINA ADITIVADA' THEN 6
            WHEN 'DIESEL S50'         THEN 7
            WHEN 'GLP'                THEN 8
            ELSE 0
        END                                                                         AS id_produto,
        CAST(REPLACE(NULLIF(valor_de_venda, ''), ',', '.') AS DOUBLE PRECISION)    AS valor_venda,
        CAST(REPLACE(NULLIF(valor_de_compra, ''), ',', '.') AS DOUBLE PRECISION)   AS valor_compra,
        unidade_de_medida,
        TO_DATE(data_da_coleta, 'DD/MM/YYYY')                                       AS data_coleta,
        regiao_sigla,
        estado_sigla,
        municipio,
        revenda,
        REGEXP_REPLACE(cnpj_da_revenda, '[-./ ]', '', 'g')                          AS cnpj,
        nome_da_rua,
        numero_rua,
        complemento,
        bairro,
        REGEXP_REPLACE(cep, '[-./ ]', '', 'g')                                      AS cep,
        arquivo,
        bandeira,
        ingestion_timestamp
    FROM source
)

SELECT
    *,
    COALESCE(TO_CHAR(data_coleta, 'YYYYMMDD'), '') || '|' ||
    COALESCE(produto, '') || '|' ||
    COALESCE(cnpj, '')                                                              AS chave
FROM cleaned
{% endmacro %}
