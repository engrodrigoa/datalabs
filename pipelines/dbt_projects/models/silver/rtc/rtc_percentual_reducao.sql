{{ config(indexes=[{'columns': ['cltr_id', 'tributo']}]) }}

-- % oficial de redução por cClassTrib x tributo, com vigência efetiva para a junção ponto-no-tempo.
select
    r.*,
    coalesce(r.dt_fim_vigencia, date '9999-12-31')  as dt_fim_vigencia_efetiva
from {{ ref('stg_rtc__percentual_reducao') }} as r
