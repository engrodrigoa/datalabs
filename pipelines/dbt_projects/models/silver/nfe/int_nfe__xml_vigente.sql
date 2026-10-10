{{ config(materialized='ephemeral') }}

-- Recorte incremental + resolução de reenvios.
--   * só lotes pendentes (CARREGADO) — ou tudo, em --full-refresh (a bronze imutável permite replay)
--   * se a mesma chave chegou mais de uma vez, vale o arquivo mais recente
select distinct on (chave_acesso)
    lote_id,
    arquivo,
    chave_acesso,
    hash_conteudo,
    conteudo,
    carregado_em
from {{ ref('stg_nfe__xml') }}
where {{ nfe_filtro_lote('lote_id') }}
order by chave_acesso, lote_id desc, arquivo desc
