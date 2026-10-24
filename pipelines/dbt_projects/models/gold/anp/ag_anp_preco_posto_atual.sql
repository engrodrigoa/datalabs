{#- Serving: the latest price of every station x product, the "current picture" the Streamlit price
    matrix reads (UF -> city -> bairro -> station). One row per (station, product), with the previous
    collection for the change. Stations are surveyed on different days, so each row carries its own
    data_coleta; the app decides how old a price may be (recency window), the model does not filter. -#}
{{ config(materialized='table', tags=['gold', 'agregado', 'serving'], grants={'select': ['anp_app_ro']},
          indexes=[{'columns': ['uf', 'municipio']}, {'columns': ['data_coleta']}]) }}

WITH coletas AS (
    SELECT
        f.id_posto_sk,
        f.id_produto_sk,
        f.data_coleta,
        f.valor_venda,
        f._source_loaded_at,
        ROW_NUMBER() OVER (PARTITION BY f.id_posto_sk, f.id_produto_sk ORDER BY f.data_coleta DESC) AS rn,
        COUNT(*)     OVER (PARTITION BY f.id_posto_sk, f.id_produto_sk)                           AS qtd_coletas
    FROM {{ ref('ft_anp_combustiveis') }} f
),

ultimas AS (
    SELECT
        id_posto_sk,
        id_produto_sk,
        MAX(data_coleta) FILTER (WHERE rn = 1) AS data_coleta,
        MAX(valor_venda) FILTER (WHERE rn = 1) AS valor_venda,
        MAX(data_coleta) FILTER (WHERE rn = 2) AS data_coleta_anterior,
        MAX(valor_venda) FILTER (WHERE rn = 2) AS valor_venda_anterior,
        MAX(qtd_coletas)                       AS qtd_coletas,
        MAX(_source_loaded_at)                 AS _source_loaded_at
    FROM coletas
    WHERE rn <= 2
    GROUP BY id_posto_sk, id_produto_sk
)

SELECT
    u.id_posto_sk,
    u.id_produto_sk,
    p.produto,
    p.unidade_de_medida,
    d.cnpj,
    d.revenda,
    COALESCE(NULLIF(UPPER(TRIM(d.bandeira)), ''), 'NAO INFORMADA')                AS bandeira,
    d.regiao_sigla,
    d.estado_sigla                                                                 AS uf,
    UPPER(TRIM(d.municipio))                                                       AS municipio,
    -- ANP bairro is free text: trimmed/upper-cased so the drill-down does not split one bairro in two
    COALESCE(NULLIF(UPPER(TRIM(d.bairro)), ''), 'NAO INFORMADO')                  AS bairro,
    CONCAT_WS(', ', NULLIF(TRIM(d.nome_da_rua), ''), NULLIF(TRIM(d.numero_rua), '')) AS endereco,
    d.cep,
    u.data_coleta,
    u.valor_venda,
    u.data_coleta_anterior,
    u.valor_venda_anterior,
    u.valor_venda - u.valor_venda_anterior                                         AS variacao,
    u.qtd_coletas,
    u._source_loaded_at
FROM ultimas u
JOIN {{ ref('dm_postos') }}   d ON d.id_posto_sk = u.id_posto_sk
JOIN {{ ref('dm_produtos') }} p ON p.id_produto_sk = u.id_produto_sk
