{{ config(materialized='view') }}

{{ anp_clean_landing(source('bronze', 'anp_landing_semanal')) }}
