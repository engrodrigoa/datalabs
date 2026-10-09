{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='chave_acesso',
        on_schema_change='append_new_columns',
        indexes=[{'columns': ['sk_data']}, {'columns': ['lote_id']}],
    )
}}

-- Fato no grão DOCUMENTO (1 linha por NF-e): contagem de notas, totais do cabeçalho,
-- vínculo com a nota referenciada (devoluções) e protocolo.
with notas as (
    select * from {{ ref('nfe_nota') }}
    where {{ nfe_filtro_lote('lote_id') }}
)

select
    n.chave_acesso,
    n.lote_id,
    to_char(n.dt_emissao, 'YYYYMMDD')::int                            as sk_data,
    n.dh_emissao,
    n.dh_autorizacao,
    {{ nfe_sk_participante('emit') }} as sk_emitente,
    {{ nfe_sk_participante('dest') }} as sk_destinatario,
    n.tp_nf || n.fin_nfe || n.id_dest || n.ind_final || n.ind_pres    as sk_operacao,
    n.emit_uf                                                         as uf_emitente,
    n.emit_cod_municipio                                              as cod_mun_emitente,
    n.dest_uf                                                         as uf_destinatario,
    n.serie,
    n.nnf,
    n.chave_referenciada,
    n.qtd_itens,
    n.v_prod,
    n.v_nf,
    n.v_icms,
    n.v_bc_ibscbs,
    n.v_ibs,
    n.v_cbs,
    coalesce(n.v_dif_ibs_uf, 0) + coalesce(n.v_dif_ibs_mun, 0) + coalesce(n.v_dif_cbs, 0)             as v_diferido_total,
    coalesce(n.v_devtrib_ibs_uf, 0) + coalesce(n.v_devtrib_ibs_mun, 0) + coalesce(n.v_devtrib_cbs, 0) as v_devtrib_total,
    coalesce(n.v_is, 0)                                               as v_is,
    now()                                                             as dbt_processado_em
from notas as n
