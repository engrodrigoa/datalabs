-- Aderência à tabela oficial por mês x cClassTrib x status: a visão que se apresenta (sem CNPJ, agregada).
with agregado as (
    select
        date_trunc('month', dt_emissao)::date                   as mes_emissao,
        cclasstrib,
        max(cclasstrib_descricao)                               as cclasstrib_descricao,
        status_aderencia,
        count(*)                                                as itens,
        count(distinct chave_acesso)                            as notas,
        count(distinct emit_cnpj)                               as emitentes,
        sum(vprod)                                              as valor_produtos,
        sum(vbc_ibscbs)                                         as base_ibscbs,
        sum(v_ibscbs)                                           as valor_ibscbs,
        max(rtc_release)                                        as rtc_release
    from {{ ref('ft_nfe_item_aderencia_rtc') }}
    group by 1, 2, 4
)

select
    *,
    round(100.0 * itens / sum(itens) over (partition by mes_emissao, cclasstrib), 2)  as pct_itens_na_cclasstrib
from agregado
