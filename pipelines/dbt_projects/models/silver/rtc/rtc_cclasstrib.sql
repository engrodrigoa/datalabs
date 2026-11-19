{{ config(indexes=[{'columns': ['cclasstrib']}, {'columns': ['grupo_tributo', 'cclasstrib', 'dt_inicio_vigencia'], 'unique': true}]) }}

-- cClassTrib oficial (Calculadora RTC da Receita), com vigência e linhagem da versão carregada.
-- Alimenta gold.dm_nfe_tributacao (que antes vinha de uma seed de 3 linhas simplificadas).
-- Junção ponto-no-tempo: dt_emissao between dt_inicio_vigencia and coalesce(dt_fim_vigencia, '9999-12-31').
with versao as (
    select sha, release, database_version, loaded_at
    from {{ source('ctrl_rtc', 'rtc_carga') }}
    order by loaded_at desc, carga_id desc
    limit 1
)

select
    c.*,
    coalesce(c.dt_fim_vigencia, date '9999-12-31')  as dt_fim_vigencia_efetiva,
    v.release                                       as rtc_release,
    v.database_version                              as rtc_versao_base,
    v.sha                                           as rtc_sha,
    v.loaded_at                                     as rtc_carregado_em
from {{ ref('stg_rtc__classificacao') }} as c
cross join versao as v
