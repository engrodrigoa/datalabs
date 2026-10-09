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
      {'columns': ['codg_motivo_sit_cadast'], 'type': 'btree', 'unique': True}
    ]
) }}

WITH stg_dm AS (
    SELECT * FROM {{ ref('stg_rfb_dim_motivo_situacao_cadastral') }}
)

SELECT
    codg_motivo_sit_cadast,
    desc_motivo_sit_cadast,
    NOW() AS data_carga
FROM stg_dm
WHERE codg_motivo_sit_cadast IS NOT NULL