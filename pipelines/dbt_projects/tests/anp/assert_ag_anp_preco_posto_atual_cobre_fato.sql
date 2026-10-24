{{ config(tags=['anp']) }}
-- Uma linha por (posto, produto) presente na fato, e o preço é o da coleta mais recente.
WITH esperado AS (
    SELECT DISTINCT ON (id_posto_sk, id_produto_sk) id_posto_sk, id_produto_sk, data_coleta, valor_venda
    FROM {{ ref('ft_anp_combustiveis') }}
    ORDER BY id_posto_sk, id_produto_sk, data_coleta DESC
)
SELECT e.id_posto_sk, e.id_produto_sk, e.data_coleta, e.valor_venda, a.data_coleta AS data_atual, a.valor_venda AS valor_atual
FROM esperado e
FULL JOIN {{ ref('ag_anp_preco_posto_atual') }} a USING (id_posto_sk, id_produto_sk)
WHERE a.id_posto_sk IS NULL OR e.id_posto_sk IS NULL
   OR a.data_coleta <> e.data_coleta OR a.valor_venda <> e.valor_venda
