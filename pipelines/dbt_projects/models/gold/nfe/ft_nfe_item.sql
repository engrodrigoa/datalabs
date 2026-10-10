{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['chave_acesso', 'n_item'],
        on_schema_change='append_new_columns',
        indexes=[{'columns': ['sk_data']}, {'columns': ['lote_id']}, {'columns': ['sk_emitente']}],
    )
}}

-- Fato no grão ITEM da NF-e. Chaves para as dimensões + medidas aditivas (valores).
-- Percentuais não entram aqui (não são aditivos): calcule carga efetiva = Σ tributo / Σ base.
with itens as (
    select * from {{ ref('nfe_item') }}
    where {{ nfe_filtro_lote('lote_id') }}
),

notas as (
    select * from {{ ref('nfe_nota') }}
    where {{ nfe_filtro_lote('lote_id') }}
)

select
    i.chave_acesso,
    i.n_item,
    i.lote_id,
    to_char(n.dt_emissao, 'YYYYMMDD')::int                            as sk_data,
    n.dh_emissao,
    {{ nfe_sk_participante('emit') }} as sk_emitente,
    {{ nfe_sk_participante('dest') }} as sk_destinatario,
    n.tp_nf || n.fin_nfe || n.id_dest || n.ind_final || n.ind_pres    as sk_operacao,
    i.cst_ibscbs || i.cclasstrib                                      as sk_tributacao,
    n.emit_uf                                                         as uf_emitente,
    n.emit_cod_municipio                                              as cod_mun_emitente,
    n.dest_uf                                                         as uf_destinatario,
    n.dest_cod_municipio                                              as cod_mun_destinatario,
    i.cfop,
    i.ncm,
    i.cst_ibscbs,
    i.cclasstrib,
    i.qcom,
    i.vprod,
    i.v_icms,
    i.v_icms_deson,
    i.v_ipi,
    i.v_pis,
    i.v_cofins,
    i.vbc_ibscbs,
    i.v_ibs_uf,
    i.v_ibs_mun,
    i.v_ibs,
    i.v_cbs,
    coalesce(i.v_dif_ibs_uf, 0) + coalesce(i.v_dif_ibs_mun, 0)       as v_dif_ibs,
    coalesce(i.v_dif_cbs, 0)                                          as v_dif_cbs,
    coalesce(i.v_devtrib_ibs_uf, 0) + coalesce(i.v_devtrib_ibs_mun, 0) as v_devtrib_ibs,
    coalesce(i.v_devtrib_cbs, 0)                                      as v_devtrib_cbs,
    coalesce(i.v_is, 0)                                               as v_is,
    now()                                                             as dbt_processado_em
from itens as i
join notas as n using (chave_acesso)
