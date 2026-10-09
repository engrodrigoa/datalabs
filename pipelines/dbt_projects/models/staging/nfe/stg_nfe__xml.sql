{{ config(materialized='ephemeral') }}

-- Staging 1:1 com a bronze: só seleção/renomeação. Ephemeral = vira CTE dentro de quem usa.
select
    lote_id,
    arquivo,
    chave_acesso,
    hash_conteudo,
    tamanho_bytes,
    conteudo,
    carregado_em
from {{ source('landing_nfe', 'nfe_xml') }}
