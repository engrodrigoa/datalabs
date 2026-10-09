{{ config(materialized='table', indexes=[{'columns': ['doc', 'dt_inicio_vigencia']}]) }}

-- Contribuinte/participante SCD tipo 2 por DATA DE EMISSÃO (não por data de carga).
-- Cada conjunto distinto de atributos vira uma versão (sk = md5(doc | hash dos atributos)).
-- A fato NÃO procura a versão por intervalo de datas: calcula a mesma sk a partir dos atributos
-- da própria nota (macro nfe_sk_participante). A vigência aqui serve para consultas "as-of" e
-- para o flag de versão atual; a 1a versão começa em 1900-01-01 por convenção.
with versoes as (
    select
        doc,
        hash_atributos,
        max(tipo_doc)          as tipo_doc,
        max(nome)              as nome,
        max(ie)                as ie,
        max(uf)                as uf,
        max(cod_municipio)     as cod_municipio,
        max(municipio)         as municipio,
        max(logradouro)        as logradouro,
        max(bairro)            as bairro,
        max(cep)               as cep,
        min(dh_primeira_nota)  as dh_primeira_nota,
        max(dh_ultima_nota)    as dh_ultima_nota,
        sum(qtd_notas)         as qtd_notas
    from {{ ref('nfe_participante_hist') }}
    group by doc, hash_atributos
)

select
    md5(doc || '|' || hash_atributos)                                   as sk_participante,
    doc,
    tipo_doc,
    nome,
    ie,
    uf,
    cod_municipio,
    municipio,
    logradouro,
    bairro,
    cep,
    row_number() over w                                                 as nr_versao,
    case when row_number() over w = 1 then '1900-01-01'::timestamptz
         else dh_primeira_nota end                                      as dt_inicio_vigencia,
    coalesce(lead(dh_primeira_nota) over w, '9999-12-31'::timestamptz)  as dt_fim_vigencia,
    lead(dh_primeira_nota) over w is null                               as fl_vigente,
    dh_primeira_nota,
    dh_ultima_nota,
    qtd_notas
from versoes
window w as (partition by doc order by dh_primeira_nota)
