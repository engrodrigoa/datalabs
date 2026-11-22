{{ config(indexes=[{'columns': ['cltr_id']}, {'columns': ['dispositivo_id']}]) }}

-- Ponte cClassTrib oficial -> dispositivos da LC 214 (ADR 0021). Uma linha por cClassTrib x citação da sua
-- fundamentação legal: "Art. 146, § 1º, I e II" vira duas linhas, uma por inciso.
--
-- A resolução (citação -> dispositivo_id) é feita em Python pelo rtc03_lc214, com o parser do router da LC 214
-- (o mesmo código determinístico que o golden contract confere). Aqui só se junta cada cClassTrib à sua
-- fundamentação e ao texto vigente da lei servida. Tabela vazia (com as colunas certas) enquanto a lei não está
-- carregada (perfil AI): o join interno com a ponte não devolve linhas.
select
    c.cltr_id,
    c.grupo_tributo,
    c.cst,
    c.cclasstrib,
    c.descricao                     as cclasstrib_descricao,
    c.dt_inicio_vigencia,
    c.dt_fim_vigencia,
    f.fdlg_id,
    f.referencia,                   -- como a Receita publica: "Art. 412, I"
    f.ordem,
    f.citacao,                      -- como o parser leu: "art. 412, inciso I"
    f.outcome,                      -- resolved | ambiguous | not_found | foreign | unparsed
    f.dispositivo_id,
    f.candidatos,
    d.rotulo,
    f.status_lei,                   -- vigente | revogado | vetado
    d.texto                         as texto_lei,
    f.texto_check,                  -- transcrição da Receita x texto da lei: match | near | diverges | no_transcription
    f.cobertura_texto,
    f.rtc_sha,
    f.law_sha256
from {{ ref('rtc_cclasstrib') }} as c
join {{ source('ref_rtc', 'fundamentacao_classificacao') }} as fc on fc.fdcl_cltr_id = c.cltr_id
join {{ source('ai_lc214', 'rtc_lc214_fundamento') }} as f on f.fdlg_id = fc.fdcl_fdlg_id
left join {{ source('ai_lc214', 'lc214_dispositivo') }} as d on d.dispositivo_id = f.dispositivo_id
