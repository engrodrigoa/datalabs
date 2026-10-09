{{ config(severity='warn', tags=['nfe', 'reconciliacao']) }}
-- Total de CBS do cabeçalho (IBSCBSTot/gCBS/vCBS) = soma da CBS dos itens.
-- Escopo incremental: só notas dos lotes em processamento (no full-refresh, todas).
with itens as (
    select chave_acesso, sum(v_cbs) as soma_itens
    from {{ ref('nfe_item') }}
    where {{ nfe_filtro_lote('lote_id') }}
    group by chave_acesso
)
select n.chave_acesso, n.lote_id, n.v_cbs as total_cabecalho, i.soma_itens, n.v_cbs - i.soma_itens as diferenca
from {{ ref('nfe_nota') }} as n
join itens as i using (chave_acesso)
where n.v_cbs <> i.soma_itens
