{{ config(indexes=[{'columns': ['status_aderencia']}, {'columns': ['dt_emissao']}, {'columns': ['lote_id']}]) }}

-- Aderência de cada item de NF-e às tabelas oficiais da Calculadora RTC (ADR 0019). 1 linha por chave + nItem.
--
-- Regras, avaliadas em ordem; o status é a PRIMEIRA que falha (uma causa por item, a mais básica):
--   sem_cclasstrib            item sem grupo IBSCBS (nada a avaliar)
--   cclasstrib_inexistente    o código não existe na tabela oficial (grupo IBS/CBS)
--   cclasstrib_fora_vigencia  existe, mas nenhuma linha vigente na data de emissão
--   cst_incompativel          vigente, mas o CST do item não é o CST oficial da cClassTrib
--   ncm_inelegivel            a cClassTrib tem lista de NCM (ex.: Anexo IX, insumos agro) e o NCM do item
--                             não casa com nenhuma regra vigente (prefixo), ou casa só com uma exceção
--   aderente                  passou em todas
--
-- "ncm_inelegivel" é INDÍCIO, não infração: a classificação existe e é coerente com o CST, mas o produto não
-- está na lista que dá direito ao tratamento (redução, alíquota zero...). É o caso que a autorizadora não barra.
-- Lab: recalculada inteira a cada run (volume pequeno). Em produção seria incremental por lote, com reavaliação
-- quando a tabela oficial muda (nova linha em ctrl.rtc_carga).
--
-- Clone novo, dag_nfe ainda não rodou: silver.nfe_item não existe. O modelo sai vazio (com as colunas certas) em vez
-- de falhar o dag_rtc_ref; a referência oficial carrega do mesmo jeito.
{%- set nfe_pronta = true -%}
{%- if execute -%}
    {%- set rel_item = adapter.get_relation(database=ref('nfe_item').database, schema=ref('nfe_item').schema,
                                            identifier=ref('nfe_item').identifier) -%}
    {%- set rel_nota = adapter.get_relation(database=ref('nfe_nota').database, schema=ref('nfe_nota').schema,
                                            identifier=ref('nfe_nota').identifier) -%}
    {%- set nfe_pronta = rel_item is not none and rel_nota is not none -%}
{%- endif %}
with item as (
{%- if nfe_pronta %}
    select
        i.chave_acesso, i.n_item, i.lote_id, n.dt_emissao, n.emit_cnpj, n.emit_uf,
        i.ncm, i.xprod, i.cst_ibscbs as cst, i.cclasstrib,
        i.vprod, i.vbc_ibscbs, i.p_red_aliq, coalesce(i.v_ibs, 0) + coalesce(i.v_cbs, 0) as v_ibscbs
    from {{ ref('nfe_item') }} as i
    join {{ ref('nfe_nota') }} as n using (chave_acesso)
{%- else %}
    select null::text as chave_acesso, null::int as n_item, null::bigint as lote_id, null::date as dt_emissao,
           null::text as emit_cnpj, null::text as emit_uf, null::text as ncm, null::text as xprod, null::text as cst,
           null::text as cclasstrib, null::numeric(15,2) as vprod, null::numeric(15,2) as vbc_ibscbs,
           null::numeric(7,4) as p_red_aliq, null::numeric(15,2) as v_ibscbs
    where false
{%- endif %}
),

classif as (
    select * from {{ ref('rtc_cclasstrib') }} where grupo_tributo = 'IBSCBS'
),

avaliacao as (
    select
        it.chave_acesso, it.n_item,
        count(c.cltr_id) > 0                                                        as existe,
        coalesce(bool_or(it.dt_emissao between c.dt_inicio_vigencia and c.dt_fim_vigencia_efetiva), false)
                                                                                    as vigente,
        string_agg(distinct c.cst, ',') filter (
            where it.dt_emissao between c.dt_inicio_vigencia and c.dt_fim_vigencia_efetiva)  as cst_oficial,
        min(c.cltr_id) filter (where it.dt_emissao between c.dt_inicio_vigencia and c.dt_fim_vigencia_efetiva
                                 and c.cst = it.cst)                                as cltr_id
    from item as it
    left join classif as c on c.cclasstrib = it.cclasstrib
    group by it.chave_acesso, it.n_item
),

elegibilidade as (
    -- só para os itens com classificação vigente e CST coerente cuja cClassTrib tem lista de NCM
    select a.chave_acesso, a.n_item,
           min(r.anexo_lc214 || '-' || r.anexo_item || ' (' || r.ncm_prefixo || ')')   as regra_casada,
           count(r.regra_id) > 0                                                       as elegivel
    from avaliacao as a
    join item as it using (chave_acesso, n_item)
    join classif as c on c.cltr_id = a.cltr_id and c.tem_lista_ncm
    left join {{ ref('rtc_ncm_regra') }} as r
           on r.cltr_id = a.cltr_id
          and r.tipo_regra = 'elegivel'
          and it.ncm like r.ncm_prefixo || '%'
          and it.dt_emissao between r.dt_inicio_vigencia and r.dt_fim_vigencia_efetiva
          and not exists (
                select 1 from {{ ref('rtc_ncm_regra') }} as e
                where e.tipo_regra = 'excecao' and e.regra_id = r.regra_id
                  and it.ncm like e.ncm_prefixo || '%'
                  and it.dt_emissao between e.dt_inicio_vigencia and e.dt_fim_vigencia_efetiva)
    group by a.chave_acesso, a.n_item
)

select
    it.chave_acesso,
    it.n_item,
    it.lote_id,
    it.dt_emissao,
    it.emit_cnpj,
    it.emit_uf,
    it.ncm,
    it.xprod,
    it.cst,
    a.cst_oficial,                              -- CST da tabela oficial para a cClassTrib (diagnóstico)
    it.cclasstrib,
    c.descricao                                 as cclasstrib_descricao,
    c.tipo_aliquota,
    c.tem_lista_ncm,
    c.fundamentacao_legal,
    el.regra_casada,
    case
        when it.cclasstrib is null              then 'sem_cclasstrib'
        when not a.existe                       then 'cclasstrib_inexistente'
        when not a.vigente                      then 'cclasstrib_fora_vigencia'
        when a.cltr_id is null                  then 'cst_incompativel'
        when c.tem_lista_ncm and not el.elegivel then 'ncm_inelegivel'
        else 'aderente'
    end                                         as status_aderencia,
    it.vprod,
    it.vbc_ibscbs,
    it.p_red_aliq,
    it.v_ibscbs,
    c.rtc_release,
    c.rtc_versao_base,
    c.rtc_sha,
    now()                                       as dbt_processado_em
from item as it
join avaliacao as a using (chave_acesso, n_item)
left join classif as c on c.cltr_id = a.cltr_id
left join elegibilidade as el using (chave_acesso, n_item)
