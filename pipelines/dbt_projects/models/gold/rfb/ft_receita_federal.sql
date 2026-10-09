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
    materialized='incremental',
    on_schema_change='append_new_columns',
    unique_key='sk_estabelecimento',
    schema='gold',
    tags=['rfb'],
    indexes=[
      {'columns': ['sk_estabelecimento'], 'type': 'btree', 'unique': True},
      {'columns': ['sk_empresa'], 'type': 'btree'},
      {'columns': ['codg_municipio'], 'type': 'btree'},
      {'columns': ['codg_cnae'], 'type': 'btree'}
    ]
) }}

WITH stg_estabelecimentos AS (
    SELECT * FROM {{ ref('stg_rfb_estabelecimentos') }}

    {% if is_incremental() %}
    WHERE data_carga > (SELECT COALESCE(MAX(data_carga), '1900-01-01'::timestamptz) FROM {{ this }})
    {% endif %}
)

SELECT
    e.sk_estabelecimento,
    e.sk_empresa,
    e.cnpj_completo,
    e.cnpj_basico,
    e.cnae_fiscal_principal AS codg_cnae,
    e.codg_municipio,
    e.motivo_situacao_cadastral AS codg_motivo_situacao_cadastral,
    e.codg_pais,
    e.identificador_matriz_filial,
    e.nome_fantasia,
    e.situacao_cadastral_codigo,
    e.data_situacao_cadastral,
    e.data_inicio_atividade,
    e.cnae_fiscal_secundaria,
    1 AS qtd_estabelecimentos,
    e.data_carga AS data_carga,
    NOW() AS dbt_loaded_at
FROM stg_estabelecimentos e
WHERE e.cnpj_completo IS NOT NULL