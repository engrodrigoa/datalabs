{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['doc', 'hash_atributos', 'lote_id'],
        indexes=[{'columns': ['doc']}],
    )
}}

-- Histórico de atributos cadastrais observados nas notas (emitente e destinatário).
-- 1 linha por documento x conjunto de atributos x lote. Base da dimensão SCD2 por DATA DE EMISSÃO:
-- nota que chega atrasada só amplia o intervalo da versão a que pertence (não cria versão falsa).
with notas as (
    select * from {{ ref('nfe_nota') }}
    where {{ nfe_filtro_lote('lote_id') }}
),

papeis as (
    select emit_cnpj as doc, 'CNPJ' as tipo_doc, emit_nome as nome, emit_ie as ie, emit_uf as uf,
           emit_cod_municipio as cod_municipio, emit_municipio as municipio,
           emit_logradouro as logradouro, emit_bairro as bairro, emit_cep as cep,
           dh_emissao, lote_id
    from notas
    union all
    select dest_doc, dest_tipo_doc, dest_nome, dest_ie, dest_uf,
           dest_cod_municipio, dest_municipio, dest_logradouro, dest_bairro, dest_cep,
           dh_emissao, lote_id
    from notas
)

select
    doc,
    {{ nfe_hash_atributos('nome', 'ie', 'uf', 'cod_municipio', 'municipio', 'logradouro', 'bairro', 'cep') }} as hash_atributos,
    lote_id,
    max(tipo_doc)       as tipo_doc,
    max(nome)           as nome,
    max(ie)             as ie,
    max(uf)             as uf,
    max(cod_municipio)  as cod_municipio,
    max(municipio)      as municipio,
    max(logradouro)     as logradouro,
    max(bairro)         as bairro,
    max(cep)            as cep,
    min(dh_emissao)     as dh_primeira_nota,
    max(dh_emissao)     as dh_ultima_nota,
    count(*)            as qtd_notas,
    now()               as dbt_processado_em
from papeis
group by 1, 2, 3
