{#
  Testes genéricos do domínio NF-e (reutilizáveis em qualquer modelo/coluna).
  Retornam as linhas que FALHAM (convenção dbt).
#}

{# Chave de acesso: 44 posições e DV (posição 44) = módulo 11 das 43 anteriores #}
{% test nfe_chave_valida(model, column_name) %}
select {{ column_name }} as chave_acesso
from {{ model }}
where {{ column_name }} is not null
  and (length({{ column_name }}) <> 44
       or right({{ column_name }}, 1) <> {{ nfe_dv_mod11('left(' ~ column_name ~ ', 43)') }})
{% endtest %}


{# CNPJ numérico ou alfanumérico (IN RFB 2.229/2024): 12 posições [0-9A-Z] + 2 DVs numéricos #}
{% test nfe_cnpj_valido(model, column_name) %}
select {{ column_name }} as cnpj
from {{ model }}
where {{ column_name }} is not null
  and length({{ column_name }}) = 14
  and ({{ column_name }} !~ '^[0-9A-Z]{12}[0-9]{2}$'
       or substr({{ column_name }}, 13, 1) <> {{ nfe_dv_mod11('left(' ~ column_name ~ ', 12)') }}
       or substr({{ column_name }}, 14, 1) <> {{ nfe_dv_mod11('left(' ~ column_name ~ ', 13)') }})
{% endtest %}


{# cClassTrib deve pertencer ao CST informado (ex.: CST 200 -> cClassTrib 200xxx) #}
{% test nfe_cclasstrib_coerente(model, column_name, cst_column) %}
select {{ cst_column }} as cst, {{ column_name }} as cclasstrib
from {{ model }}
where {{ cst_column }} is not null
  and left({{ column_name }}, 3) <> {{ cst_column }}
{% endtest %}
