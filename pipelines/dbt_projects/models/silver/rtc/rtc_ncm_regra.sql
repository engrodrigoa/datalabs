{{ config(indexes=[{'columns': ['cltr_id', 'tipo_regra']}, {'columns': ['regra_id']}]) }}

-- Regras NCM x cClassTrib (elegível + exceção), materializadas para a junção com os itens de NF-e.
select
    r.*,
    coalesce(r.dt_fim_vigencia, date '9999-12-31')  as dt_fim_vigencia_efetiva
from {{ ref('stg_rtc__ncm_regra') }} as r
