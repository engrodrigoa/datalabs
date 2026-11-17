{{ config(severity='warn', tags=['rtc', 'regra_negocio']) }}
-- Item com cClassTrib fora da tabela oficial: código inexistente, fora de vigência na data de emissão, ou com CST
-- diferente do CST oficial. A autorizadora tende a rejeitar parte disso: o que aparece aqui merece a pergunta
-- "como esta nota foi autorizada?".
select chave_acesso, n_item, lote_id, dt_emissao, ncm, cst, cst_oficial, cclasstrib, status_aderencia
from {{ ref('ft_nfe_item_aderencia_rtc') }}
where status_aderencia in ('cclasstrib_inexistente', 'cclasstrib_fora_vigencia', 'cst_incompativel')
