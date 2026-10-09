{#
  Macros do domínio NF-e.
  ---------------------------------------------------------------------------------------------
  nfe_ns()                  -> cláusula XMLNAMESPACES da NF-e (usar dentro de XMLTABLE)
  nfe_lotes_pendentes()     -> SELECT dos lote_id que este run deve processar
  nfe_filtro_lote(col)      -> WHERE incremental por lote (no full-refresh: tudo)
  nfe_dv_mod11(expr)        -> DV módulo 11 (pesos 2..9), aceita CNPJ alfanumérico (ASCII - 48)
#}

{% macro nfe_ns() -%}
xmlnamespaces('http://www.portalfiscal.inf.br/nfe' as n)
{%- endmacro %}


{#
  Fila de processamento = lotes com status CARREGADO ("indi_processamento = 1").
  Vars:
    nfe_lote_max : teto passado pelo Airflow (o lote que acabou de ser ingerido) -> run determinístico
    nfe_lotes    : lista explícita para correção pontual, ex.: --vars '{nfe_lotes: [12, 15]}'
#}
{% macro nfe_lotes_pendentes() -%}
    {%- set lotes = var('nfe_lotes', none) -%}
    {%- set lote_max = var('nfe_lote_max', none) -%}
    {%- if lotes -%}
        select unnest(array[{{ lotes | join(', ') }}]::bigint[])
    {%- else -%}
        select lote_id from {{ source('ctrl_nfe', 'nfe_lote') }} where status = 'CARREGADO'
        {%- if lote_max not in (none, '', 'None') %} and lote_id <= {{ lote_max }}{% endif %}
    {%- endif -%}
{%- endmacro %}


{% macro nfe_filtro_lote(coluna='lote_id') -%}
    {%- if flags.FULL_REFRESH or var('nfe_reprocessar_tudo', false) -%}
        true
    {%- else -%}
        {{ coluna }} in ({{ nfe_lotes_pendentes() }})
    {%- endif -%}
{%- endmacro %}


{% macro nfe_dv_mod11(expr) -%}
(select case when s % 11 < 2 then '0' else (11 - s % 11)::text end
   from (select sum((ascii(substr({{ expr }}, length({{ expr }}) - i + 1, 1)) - 48) * (2 + (i - 1) % 8)) as s
           from generate_series(1, length({{ expr }})) as i) as _dv)
{%- endmacro %}
