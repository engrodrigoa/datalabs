{# 
  Mapeamento de dependências explícitas para o dbt/Cosmos reconhecer no DAG:
  {{ ref('stg_rfb_empresas') }}
  {{ ref('stg_rfb_estabelecimentos') }}
  {{ ref('stg_rfb_socios') }}
  {{ ref('stg_rfb_simples') }}
  {{ ref('stg_rfb_dim_cnae') }}
  {{ ref('stg_rfb_dim_motivo_situacao_cadastral') }}
  {{ ref('stg_rfb_dim_municipio') }}
  {{ ref('stg_rfb_dim_natureza_juridica') }}
  {{ ref('stg_rfb_dim_pais') }}
  {{ ref('stg_rfb_dim_qualificacao_socio') }}
#}

{{ config(
    materialized='incremental',
    unique_key='sk_empresa',
    schema='gold',
    tags=['rfb'],
    indexes=[
      {'columns': ['sk_empresa'], 'type': 'btree', 'unique': True},
      {'columns': ['cnpj_basico'], 'type': 'btree'}
    ]
) }}

WITH stg_empresas AS (
    SELECT * FROM {{ ref('stg_rfb_empresas') }}
    {% if is_incremental() %}
        WHERE data_carga >= (SELECT MAX(data_carga) FROM {{ this }})
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['e.cnpj_basico']) }} AS sk_empresa,
    e.cnpj_basico,
    e.razao_social,
    e.codg_natureza_juridica,
    e.codg_qualificacao_responsavel,
    e.capital_social,
    e.codg_porte_empresa,
    e.ente_federativo_responsavel,
    NOW() AS data_carga
FROM stg_empresas e
WHERE e.cnpj_basico IS NOT NULL