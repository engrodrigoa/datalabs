{{ config(materialized='table') }}

-- NCM observados no domínio. NCM ausente aqui é sinalizado pelo teste de relacionamento da fato.
select ncm, descricao_ncm, capitulo, descricao_capitulo
from {{ ref('nfe_ref_ncm') }}
