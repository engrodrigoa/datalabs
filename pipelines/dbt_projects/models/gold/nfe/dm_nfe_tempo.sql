{{ config(materialized='table') }}

-- Dimensão de datas (2025-2027). sk_data = AAAAMMDD (inteiro: chave pequena e legível).
with dias as (
    {{ dbt_utils.date_spine(datepart='day',
                            start_date="cast('2025-01-01' as date)",
                            end_date="cast('2028-01-01' as date)") }}
)

select
    to_char(date_day, 'YYYYMMDD')::int                      as sk_data,
    date_day::date                                          as data,
    extract(year from date_day)::int                        as ano,
    extract(quarter from date_day)::int                     as trimestre,
    extract(month from date_day)::int                       as mes,
    to_char(date_day, 'YYYYMM')::int                        as ano_mes,
    (array['janeiro','fevereiro','marco','abril','maio','junho','julho',
           'agosto','setembro','outubro','novembro','dezembro'])[extract(month from date_day)::int] as nome_mes,
    extract(isodow from date_day)::int                      as dia_semana,
    extract(isodow from date_day) in (6, 7)                 as fl_fim_de_semana
from dias
