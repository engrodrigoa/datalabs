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
    materialized='table',
    schema='gold',
    tags=['rfb'],
    indexes=[
      {'columns': ['codg_pais'], 'type': 'btree', 'unique': True}
    ]
) }}

WITH stg_dm AS (
    SELECT * FROM {{ ref('stg_rfb_dim_pais') }}
)

SELECT 
    codg_pais,
    desc_pais,
    NOW() AS data_carga 
FROM stg_dm
WHERE codg_pais IS NOT NULL