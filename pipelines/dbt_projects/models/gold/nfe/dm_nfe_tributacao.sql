{{ config(materialized='table') }}

-- CST x cClassTrib do IBS/CBS. Vem do snapshot: a tabela oficial muda por nota técnica e o
-- snapshot guarda o histórico de alterações (validade por processamento, que aqui é o correto).
select
    cst || cclasstrib                 as sk_tributacao,
    cst,
    cclasstrib,
    descricao_cst,
    descricao_classtrib,
    tipo_tributacao,
    p_reducao_aliquota,
    dbt_valid_from                    as dt_inicio_vigencia,
    dbt_valid_to                      as dt_fim_vigencia
from {{ ref('snap_nfe_ref_classtrib') }}
where dbt_valid_to is null
