{{ config(tags=['rtc']) }}
-- Contrato da fonte: no mesmo grupo de tributo, um código de cClassTrib não pode ter duas linhas vigentes no
-- mesmo dia. Se tiver, a junção ponto-no-tempo com o item fica ambígua (o item casaria com duas classificações).
select a.grupo_tributo, a.cclasstrib, a.cltr_id as cltr_id_a, b.cltr_id as cltr_id_b,
       a.dt_inicio_vigencia as ini_a, a.dt_fim_vigencia_efetiva as fim_a,
       b.dt_inicio_vigencia as ini_b, b.dt_fim_vigencia_efetiva as fim_b
from {{ ref('rtc_cclasstrib') }} as a
join {{ ref('rtc_cclasstrib') }} as b
  on a.grupo_tributo = b.grupo_tributo and a.cclasstrib = b.cclasstrib and a.cltr_id < b.cltr_id
 and a.dt_inicio_vigencia <= b.dt_fim_vigencia_efetiva and b.dt_inicio_vigencia <= a.dt_fim_vigencia_efetiva
