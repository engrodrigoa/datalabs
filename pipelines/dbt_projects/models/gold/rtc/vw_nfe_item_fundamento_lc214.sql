{{ config(materialized='view') }}

-- Item de NF-e -> texto da LC 214 que fundamenta a sua cClassTrib (ADR 0021). Uma linha por item x dispositivo
-- citado. Só itens com a cClassTrib casada na tabela oficial (vigente na emissão e CST coerente): um item
-- 'cclasstrib_inexistente' não tem fundamentação a mostrar, e isso já é o status de aderência dele.
--
--   select * from gold.vw_nfe_item_fundamento_lc214 where chave_acesso = '...' order by n_item, ordem;
select
    a.chave_acesso,
    a.n_item,
    a.dt_emissao,
    a.ncm,
    a.xprod,
    a.cst,
    a.cclasstrib,
    a.status_aderencia,
    b.referencia,
    b.ordem,
    b.citacao,
    b.outcome,
    b.dispositivo_id,
    b.rotulo,
    b.status_lei,
    b.texto_lei
from {{ ref('ft_nfe_item_aderencia_rtc') }} as a
join {{ ref('br_cclasstrib_lc214') }} as b on b.cltr_id = a.cltr_id
