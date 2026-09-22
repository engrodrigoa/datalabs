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
        WHERE data_carga >= (SELECT MAX(data_carga) FROM {{ this }})
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
        NOW() AS data_carga,
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
    data_carga
FROM socios_enriquecidos
WHERE rn = 1