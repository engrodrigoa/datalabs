-- % oficial de redução da alíquota, uma linha por cClassTrib x tributo x vigência (formato longo).
-- Longo e não pivotado: CBS e IBS podem divergir (200025: CBS 100%, IBS 60%) e cada tributo tem vigência própria.
select
    p.pere_id                                   as reducao_id,
    p.pere_cltr_id                              as cltr_id,
    t.tbto_sigla                                as tributo,          -- CBS | IBSUF | IBSMun
    p.pere_valor::numeric(7,4)                  as p_reducao,
    p.pere_inicio_vigencia::date                as dt_inicio_vigencia,
    p.pere_fim_vigencia::date                   as dt_fim_vigencia
from {{ source('ref_rtc', 'percentual_reducao') }} as p
join {{ source('ref_rtc', 'tributo') }} as t on t.tbto_id = p.pere_tbto_id
