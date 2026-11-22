{{ config(severity='warn', tags=['rtc', 'regra_negocio']) }}
-- % de redução declarado no item diferente do % oficial da cClassTrib na data de emissão (ref_rtc.percentual_reducao).
-- Classificação e NCM em ordem, mas o desconto aplicado não é o da lei: ex.: cClassTrib 200038 (redução de 60%,
-- Anexo IX) com pRedAliq 30. Afeta o valor do tributo diretamente; a autorizadora não confere o percentual.
select chave_acesso, n_item, lote_id, dt_emissao, ncm, xprod, cst, cclasstrib, cclasstrib_descricao,
       p_red_decl_ibs_uf, p_red_ofic_ibs_uf, p_red_decl_cbs, p_red_ofic_cbs, vbc_ibscbs, v_ibscbs
from {{ ref('ft_nfe_item_aderencia_rtc') }}
where status_aderencia = 'reducao_divergente'
