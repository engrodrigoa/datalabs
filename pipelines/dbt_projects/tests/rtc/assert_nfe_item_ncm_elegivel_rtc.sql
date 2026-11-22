{{ config(severity='warn', tags=['rtc', 'regra_negocio']) }}
-- Indício de tratamento indevido: a cClassTrib existe, está vigente e bate com o CST, mas exige lista de NCM e
-- o NCM do item não está nela (ex.: smartphone com a redução de 60% dos insumos agropecuários, Anexo IX).
-- É o caso que a autorizadora não barra. Falha = aviso + amostra em dq_nfe, nunca bloqueio do pipeline.
select chave_acesso, n_item, lote_id, dt_emissao, ncm, xprod, cst, cclasstrib, cclasstrib_descricao,
       fundamentacao_legal, vprod, vbc_ibscbs, p_red_aliq
from {{ ref('ft_nfe_item_aderencia_rtc') }}
where status_aderencia = 'ncm_inelegivel'
