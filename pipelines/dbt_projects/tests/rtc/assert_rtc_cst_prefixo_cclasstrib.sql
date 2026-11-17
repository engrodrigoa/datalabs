{{ config(severity='warn', tags=['rtc']) }}
-- A cClassTrib começa com o seu CST (CST 200 -> 200038). É a regra que o teste nfe_cclasstrib_coerente aplica
-- aos itens; aqui ela é conferida na própria tabela oficial: se a Receita quebrar a convenção, o teste do item
-- passa a dar falso positivo, e este aviso diz por quê.
select grupo_tributo, cst, cclasstrib, cltr_id
from {{ ref('rtc_cclasstrib') }}
where left(cclasstrib, 3) <> cst
