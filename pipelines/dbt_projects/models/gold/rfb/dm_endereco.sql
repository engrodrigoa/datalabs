-- Explicit dependencies so the whole staging layer is built first.
-- (refs inside a Jinja comment block are NOT parsed by dbt; `-- depends_on:` is the supported way)
-- depends_on: {{ ref('stg_rfb_empresas') }}
-- depends_on: {{ ref('stg_rfb_estabelecimentos') }}
-- depends_on: {{ ref('stg_rfb_socios') }}
-- depends_on: {{ ref('stg_rfb_simples') }}
-- depends_on: {{ ref('stg_rfb_dim_cnae') }}
-- depends_on: {{ ref('stg_rfb_dim_motivo_situacao_cadastral') }}
-- depends_on: {{ ref('stg_rfb_dim_municipio') }}
-- depends_on: {{ ref('stg_rfb_dim_natureza_juridica') }}
-- depends_on: {{ ref('stg_rfb_dim_pais') }}
-- depends_on: {{ ref('stg_rfb_dim_qualificacao_socio') }}

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