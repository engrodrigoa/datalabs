{{ config(severity='warn', tags=['nfe', 'regra_negocio']) }}
-- Regra observada em 2026: base IBS/CBS do item = vProd - ICMS - PIS - COFINS.
-- Falha aqui não é erro de parse: é nota fora do padrão esperado (ou mudança de regra).
select chave_acesso, n_item, lote_id, vprod, vbc_ibscbs,
       vprod - coalesce(v_icms, 0) - coalesce(v_pis, 0) - coalesce(v_cofins, 0) as base_esperada
from {{ ref('nfe_item') }}
where {{ nfe_filtro_lote('lote_id') }}
  and vbc_ibscbs <> vprod - coalesce(v_icms, 0) - coalesce(v_pis, 0) - coalesce(v_cofins, 0)
