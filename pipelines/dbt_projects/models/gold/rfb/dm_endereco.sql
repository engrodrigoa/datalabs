{# 
  Mapeamento de dependências para o Cosmos/dbt aguardar TODAS as stagings:
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
      {'columns': ['sk_estabelecimento'], 'type': 'btree', 'unique': True},
      {'columns': ['sk_empresa'], 'type': 'btree'},
      {'columns': ['uf', 'codg_municipio'], 'type': 'btree'}
    ]
) }}

WITH stg_estabelecimentos AS (
    SELECT * FROM {{ ref('stg_rfb_estabelecimentos') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['e.cnpj_basico', 'e.cnpj_ordem', 'e.cnpj_dv']) }} AS sk_estabelecimento,
    {{ dbt_utils.generate_surrogate_key(['e.cnpj_basico']) }} AS sk_empresa,

    e.tipo_logradouro,
    e.logradouro,
    e.numero,
    e.complemento,
    e.bairro,
    e.cep,
    e.uf,
    e.codg_municipio,
    e.ddd_1,
    e.telefone_1,
    e.correio_eletronico,
    NOW() AS data_carga

FROM stg_estabelecimentos e