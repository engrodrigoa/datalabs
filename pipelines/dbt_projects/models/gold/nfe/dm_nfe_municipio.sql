{{ config(materialized='table') }}

-- Municípios (código IBGE, 7 dígitos). Seed com subconjunto; em produção: tabela IBGE completa.
select
    m.cod_municipio,
    m.nome_municipio,
    m.sigla_uf,
    u.cod_uf,
    u.nome_uf,
    u.regiao
from {{ ref('nfe_ref_municipio') }} as m
join {{ ref('nfe_ref_uf') }} as u using (sigla_uf)
