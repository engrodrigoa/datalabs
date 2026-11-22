{{ config(materialized='table', tags=['rtc']) }}

-- CST x cClassTrib do IBS/CBS a partir da tabela OFICIAL (Calculadora RTC da Receita, ADR 0019, emenda 2).
-- Antes vinha de uma seed de 3 linhas simplificadas; agora traz todas as classificações do grupo IBS/CBS.
--
-- Tipo 1 de propósito: uma linha por CST + cClassTrib (a chave que a fato já usa, cst || cclasstrib), com os
-- atributos da linha vigente hoje. O histórico NÃO se perde: fica versionado em silver.rtc_cclasstrib /
-- ctrl.rtc_carga, e a avaliação ponto-no-tempo de cada item (vigência na data de emissão) é trabalho de
-- gold.ft_nfe_item_aderencia_rtc. Uma dimensão SCD2 aqui obrigaria a trocar a chave da fato incremental
-- (full refresh de ft_nfe_item) para responder uma pergunta que a fato de aderência já responde.
--
-- Tag rtc: o dag_rtc_ref reconstrói esta dimensão logo depois de carregar uma versão nova da tabela oficial.
-- Clone novo (dag_rtc_ref ainda não rodou): sai vazia, com as colunas certas; o dag_nfe não quebra e o teste de
-- relacionamento da fato (warn) mostra o que ficou sem dimensão.
{#- namespace: um set dentro do for não sai do escopo do loop (Jinja) -#}
{%- set ns = namespace(ref_pronta=true) -%}
{%- if execute -%}
    {%- for m in ['rtc_cclasstrib', 'rtc_percentual_reducao'] -%}
        {%- set rel = adapter.get_relation(database=ref(m).database, schema=ref(m).schema, identifier=ref(m).identifier) -%}
        {%- if rel is none -%}{%- set ns.ref_pronta = false -%}{%- endif -%}
    {%- endfor -%}
{%- endif %}
{%- if ns.ref_pronta %}
with classif as (
    select c.*,
           row_number() over (
               partition by c.cst, c.cclasstrib
               order by (c.dt_inicio_vigencia <= current_date) desc, c.dt_inicio_vigencia desc
           ) as rn                                     -- a linha vigente hoje; senão a mais recente
    from {{ ref('rtc_cclasstrib') }} as c
    where c.grupo_tributo = 'IBSCBS'
),

reducao as (
    select r.cltr_id,
           max(r.p_reducao) filter (where r.tributo = 'CBS')      as p_reducao_cbs,
           max(r.p_reducao) filter (where r.tributo = 'IBSUF')    as p_reducao_ibs_uf,
           max(r.p_reducao) filter (where r.tributo = 'IBSMun')   as p_reducao_ibs_mun
    from {{ ref('rtc_percentual_reducao') }} as r
    where current_date between r.dt_inicio_vigencia and r.dt_fim_vigencia_efetiva
    group by r.cltr_id
)

select
    c.cst || c.cclasstrib                 as sk_tributacao,
    c.cst,
    c.cclasstrib,
    c.descricao_cst,
    c.descricao                           as descricao_classtrib,
    c.tratamento                          as tipo_tributacao,      -- ex.: 'Alíquota reduzida em 60%'
    c.tipo_aliquota,
    r.p_reducao_ibs_uf                    as p_reducao_aliquota,   -- mantido por compatibilidade: = IBSUF (pRedAliq)
    r.p_reducao_cbs,
    r.p_reducao_ibs_uf,
    r.p_reducao_ibs_mun,
    c.tem_lista_ncm,
    c.fundamentacao_legal,
    c.dt_inicio_vigencia,
    c.dt_fim_vigencia,
    c.rtc_release,
    c.rtc_versao_base
from classif as c
left join reducao as r on r.cltr_id = c.cltr_id
where c.rn = 1
{%- else %}
select null::text as sk_tributacao, null::text as cst, null::text as cclasstrib, null::text as descricao_cst,
       null::text as descricao_classtrib, null::text as tipo_tributacao, null::text as tipo_aliquota,
       null::numeric(7,4) as p_reducao_aliquota, null::numeric(7,4) as p_reducao_cbs,
       null::numeric(7,4) as p_reducao_ibs_uf, null::numeric(7,4) as p_reducao_ibs_mun, null::boolean as tem_lista_ncm,
       null::text as fundamentacao_legal, null::date as dt_inicio_vigencia, null::date as dt_fim_vigencia,
       null::text as rtc_release, null::text as rtc_versao_base
where false
{%- endif %}
