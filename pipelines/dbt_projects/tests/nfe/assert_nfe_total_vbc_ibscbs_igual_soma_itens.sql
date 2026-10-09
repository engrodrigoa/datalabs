{{ config(severity='warn', tags=['nfe', 'reconciliacao']) }}
-- Base IBS/CBS do cabeçalho (vBCIBSCBS) = soma das bases dos itens.
with itens as (
    select chave_acesso, sum(vbc_ibscbs) as soma_itens
    from {{ ref('nfe_item') }}
    where {{ nfe_filtro_lote('lote_id') }}
    group by chave_acesso
)
select n.chave_acesso, n.lote_id, n.v_bc_ibscbs as total_cabecalho, i.soma_itens,
       n.v_bc_ibscbs - i.soma_itens as diferenca
from {{ ref('nfe_nota') }} as n
join itens as i using (chave_acesso)
where n.v_bc_ibscbs <> i.soma_itens
