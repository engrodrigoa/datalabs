{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='ano_mes',
    )
}}

-- Agregado mensal para análise de desempenho fiscal (consumo direto por BI).
-- Incremental por MÊS AFETADO: só recalcula os meses que receberam itens nos lotes em
-- processamento (delete+insert do mês inteiro). Notas atrasadas reabrem o mês correto.
{% if is_incremental() %}
with meses_afetados as (
    select distinct sk_data / 100 as ano_mes
    from {{ ref('ft_nfe_item') }}
    where {{ nfe_filtro_lote('lote_id') }}
)
{% endif %}

select
    f.sk_data / 100                                        as ano_mes,
    f.uf_emitente,
    f.cod_mun_emitente,
    f.cst_ibscbs,
    f.cclasstrib,
    o.natureza,
    count(distinct f.chave_acesso)                         as qtd_notas,
    count(*)                                               as qtd_itens,
    count(distinct f.sk_emitente)                          as qtd_emitentes,
    sum(f.vprod)                                           as v_produtos,
    sum(f.vbc_ibscbs)                                      as v_base_ibscbs,
    sum(f.v_ibs)                                           as v_ibs,
    sum(f.v_cbs)                                           as v_cbs,
    sum(f.v_dif_ibs + f.v_dif_cbs)                         as v_diferido,
    sum(f.v_devtrib_ibs + f.v_devtrib_cbs)                 as v_devolucao_tributo,
    sum(f.v_is)                                            as v_is,
    sum((f.v_ibs + f.v_cbs) * o.sinal_arrecadacao)         as v_ibscbs_liquido,
    round(100 * sum(f.v_ibs + f.v_cbs) / nullif(sum(f.vbc_ibscbs), 0), 4) as pct_carga_efetiva,
    now()                                                  as dbt_processado_em
from {{ ref('ft_nfe_item') }} as f
join {{ ref('dm_nfe_operacao') }} as o using (sk_operacao)
{% if is_incremental() %}
where f.sk_data / 100 in (select ano_mes from meses_afetados)
{% endif %}
group by 1, 2, 3, 4, 5, 6
