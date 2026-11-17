-- cClassTrib oficial, tipada, com o CST e o grupo de tributo (IBSCBS | IS) resolvidos.
-- Chave natural: (grupo_tributo, cclasstrib, dt_inicio_vigencia). O código sozinho repete entre IBS/CBS e IS.
with cst_grupo as (
    select ts.trst_sitr_id                                                    as sitr_id,
           case when bool_or(t.tbto_sigla = 'IS') and not bool_or(t.tbto_sigla <> 'IS')
                then 'IS' else 'IBSCBS' end                                   as grupo_tributo
    from {{ source('ref_rtc', 'tributo_situacao_tributaria') }} as ts
    join {{ source('ref_rtc', 'tributo') }} as t on t.tbto_id = ts.trst_tbto_id
    group by ts.trst_sitr_id
),

fundamentacao as (
    select fc.fdcl_cltr_id                                                    as cltr_id,
           string_agg(distinct fl.fdlg_texto_curto, '; ')                     as fundamentacao_legal
    from {{ source('ref_rtc', 'fundamentacao_classificacao') }} as fc
    join {{ source('ref_rtc', 'fundamentacao_legal') }} as fl on fl.fdlg_id = fc.fdcl_fdlg_id
    group by fc.fdcl_cltr_id
)

select
    c.cltr_id,
    coalesce(g.grupo_tributo, 'IBSCBS')                                       as grupo_tributo,
    s.sitr_cd                                                                 as cst,
    c.cltr_cd                                                                 as cclasstrib,
    c.cltr_descricao                                                          as descricao,
    c.cltr_tipo_aliquota                                                      as tipo_aliquota,
    c.cltr_nomenclatura                                                       as nomenclatura,
    c.cltr_anexo                                                              as anexo_lc214,
    exists (select 1 from {{ source('ref_rtc', 'ncm_aplicavel') }} as n
            where n.ncma_cltr_id = c.cltr_id)                                 as tem_lista_ncm,
    f.fundamentacao_legal,                                                    -- ex.: 'Art. 138, § 1º' (LC 214)
    c.cltr_inicio_vigencia::date                                              as dt_inicio_vigencia,
    c.cltr_fim_vigencia::date                                                 as dt_fim_vigencia
from {{ source('ref_rtc', 'classificacao_tributaria') }} as c
join {{ source('ref_rtc', 'situacao_tributaria') }} as s on s.sitr_id = c.cltr_sitr_id
left join cst_grupo as g on g.sitr_id = c.cltr_sitr_id
left join fundamentacao as f on f.cltr_id = c.cltr_id
