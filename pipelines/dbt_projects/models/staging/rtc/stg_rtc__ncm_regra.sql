-- Regras de elegibilidade NCM x cClassTrib, tipadas. Uma linha por regra (ncm_aplicavel) ou exceção.
--   tipo_regra = 'elegivel'  -> o NCM do item que COMEÇA com ncm_prefixo é elegível à cClassTrib (no período)
--   tipo_regra = 'excecao'   -> ...exceto o NCM que começa com este prefixo (regra_id aponta a regra que restringe)
-- Mesmo critério da calculadora (NcmAplicavelRepository): n.ncm = SUBSTRING(:ncm, 1, LENGTH(n.ncm)).
select
    n.ncma_id                                   as regra_id,
    'elegivel'                                  as tipo_regra,
    n.ncma_cltr_id                              as cltr_id,
    n.ncma_ncm_cd                               as ncm_prefixo,
    length(n.ncma_ncm_cd)                       as nivel_ncm,      -- 2 capítulo, 4 posição, 6 subposição, 8 item
    a.anxo_numero                               as anexo_lc214,
    a.anxo_numero_item                          as anexo_item,
    left(a.anxo_texto_item, 200)                as anexo_texto,
    n.ncma_inicio_vigencia::date                as dt_inicio_vigencia,
    n.ncma_fim_vigencia::date                   as dt_fim_vigencia
from {{ source('ref_rtc', 'ncm_aplicavel') }} as n
left join {{ source('ref_rtc', 'anexo') }} as a on a.anxo_id = n.ncma_anxo_id

union all

select
    e.encm_ncma_id                              as regra_id,
    'excecao'                                   as tipo_regra,
    n.ncma_cltr_id                              as cltr_id,
    e.encm_ncm_cd                               as ncm_prefixo,
    length(e.encm_ncm_cd)                       as nivel_ncm,
    null                                        as anexo_lc214,
    null                                        as anexo_item,
    null                                        as anexo_texto,
    e.encm_inicio_vigencia::date                as dt_inicio_vigencia,
    e.encm_fim_vigencia::date                   as dt_fim_vigencia
from {{ source('ref_rtc', 'excecao_ncm_aplicavel') }} as e
join {{ source('ref_rtc', 'ncm_aplicavel') }} as n on n.ncma_id = e.encm_ncma_id
