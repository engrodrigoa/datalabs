{{ config(materialized='table') }}

-- Dimensão "junk": combina os indicadores de baixa cardinalidade do ide em uma chave só,
-- em vez de 5 colunas soltas na fato. sk_operacao = tpNF|finNFe|idDest|indFinal|indPres.
with tp_nf(tp_nf, ds_tp_nf) as (values ('0', 'Entrada'), ('1', 'Saida')),
fin(fin_nfe, ds_finalidade) as (
    values ('1', 'Normal'), ('2', 'Complementar'), ('3', 'Ajuste'),
           ('4', 'Devolucao'), ('5', 'Nota de credito'), ('6', 'Nota de debito')),
dest(id_dest, ds_destino) as (values ('1', 'Interna'), ('2', 'Interestadual'), ('3', 'Exterior')),
fin_cons(ind_final, ds_consumidor) as (values ('0', 'Normal'), ('1', 'Consumidor final')),
pres(ind_pres, ds_presenca) as (
    values ('0', 'Nao se aplica'), ('1', 'Presencial'), ('2', 'Internet'), ('3', 'Teleatendimento'),
           ('4', 'Entrega em domicilio'), ('5', 'Presencial fora do estabelecimento'), ('9', 'Outros'))

select
    tp_nf || fin_nfe || id_dest || ind_final || ind_pres     as sk_operacao,
    tp_nf, ds_tp_nf,
    fin_nfe, ds_finalidade,
    id_dest, ds_destino,
    ind_final, ds_consumidor,
    ind_pres, ds_presenca,
    case when fin_nfe = '4' then 'devolucao' else 'venda' end as natureza,
    case when fin_nfe = '4' then -1 else 1 end               as sinal_arrecadacao
from tp_nf
cross join fin
cross join dest
cross join fin_cons
cross join pres
