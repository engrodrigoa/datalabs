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
    unique_key='sk_socio_empresa',
    schema='gold',
    tags=['rfb'],
    indexes=[
      {'columns': ['sk_socio_empresa'], 'type': 'btree', 'unique': True},
      {'columns': ['sk_empresa'], 'type': 'btree'},
      {'columns': ['cnpj_basico'], 'type': 'btree'}
    ]
) }}

WITH stg_socios AS (
    SELECT * FROM {{ ref('stg_rfb_socios') }}
    {% if is_incremental() %}
        WHERE data_carga > (SELECT COALESCE(MAX(data_carga), '1900-01-01'::timestamptz) FROM {{ this }})
    {% endif %}
),

stg_qualificacao AS (
    SELECT * FROM {{ ref('stg_rfb_dim_qualificacao_socio') }}
),

socios_enriquecidos AS (
    SELECT
        s.sk_socio_empresa,
        s.sk_empresa,
        s.cnpj_basico,
        s.identificador_socio,
        s.nome_socio_razao_social,
        s.cnpj_cpf_socio,
        s.codg_qualificacao_socio,
        q.desc_qualificacao,
        s.data_entrada_sociedade,
        s.codg_pais,
        s.data_carga AS data_carga,
        NOW() AS dbt_loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY s.sk_socio_empresa 
            ORDER BY s.data_entrada_sociedade DESC
        ) AS rn

    FROM stg_socios s
    LEFT JOIN stg_qualificacao q 
        ON s.codg_qualificacao_socio = q.codg_qualificacao
    WHERE s.cnpj_basico IS NOT NULL
)

SELECT
    sk_socio_empresa,
    sk_empresa,
    cnpj_basico,
    identificador_socio,
    nome_socio_razao_social,
    cnpj_cpf_socio,
    codg_qualificacao_socio,
    desc_qualificacao,
    data_entrada_sociedade,
    codg_pais,
    data_carga,
    dbt_loaded_at
FROM socios_enriquecidos
WHERE rn = 1