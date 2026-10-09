{{ config(materialized='table') }}

select cfop, descricao, sentido, abrangencia, grupo
from {{ ref('nfe_ref_cfop') }}
