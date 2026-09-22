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
      {'columns': ['sk_simples'], 'type': 'btree', 'unique': True},
      {'columns': ['cnpj_basico'], 'type': 'btree'}
    ]
) }}

WITH stg_simples AS (
    SELECT * FROM {{ ref('stg_rfb_simples') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['s.cnpj_basico']) }} AS sk_simples,
    s.cnpj_basico,
    s.opcao_simples,
    s.data_opcao_simples,
    s.data_exclusao_simples,
    s.opcao_mei,
    s.data_opcao_mei,
    s.data_exclusao_mei,
    NOW() AS data_carga
FROM stg_simples s
WHERE s.cnpj_basico IS NOT NULL