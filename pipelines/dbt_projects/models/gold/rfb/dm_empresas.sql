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
        WHERE data_carga > (SELECT COALESCE(MAX(data_carga), '1900-01-01'::timestamptz) FROM {{ this }})
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
    e.data_carga AS data_carga,
    NOW() AS dbt_loaded_at
FROM stg_empresas e
WHERE e.cnpj_basico IS NOT NULL