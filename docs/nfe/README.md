# NF-e — DW fiscal da reforma tributária (IBS/CBS/IS)

Pipeline incremental de NF-e, do XML à camada analítica, só com **SQL + dbt + Airflow**.
Os dados são 100% sintéticos. A estrutura é validada contra os XSDs oficiais (`leiauteNFe_v4.00` +
`DFeTiposBasicos_v1.00`, com os grupos IBS/CBS/IS e o CNPJ alfanumérico).

```
gen_nfe.py (sistema de origem simulado)
   │  escrita atômica
   ▼
datasource/nfe/inbox/*.xml
   │  dag_nfe: FileSensor ─► reservar_lote (mv → processando/<lote>)
   ▼
ctrl.fn_nfe_ingerir_lote()  ── SQL: pg_read_file + XMLPARSE ──►  landing_nfe.nfe_xml   (BRONZE, imutável)
   │                                                     └─►  ctrl.nfe_arquivo_rejeitado
   ▼  dbt (Cosmos, tag:nfe) — só lotes CARREGADO
stg_nfe__xml ─► int_nfe__xml_vigente (reenvio: vale o mais recente)
   ├─► silver.nfe_nota            XMLTABLE, contrato, merge por chave        (SILVER)
   ├─► silver.nfe_item            XMLTABLE, contrato, merge por chave+item
   └─► silver.nfe_participante_hist
            ▼
gold.dm_nfe_participante (SCD2) · dm_nfe_tempo · dm_nfe_municipio · dm_nfe_cfop · dm_nfe_ncm
gold.dm_nfe_tributacao (← snapshot) · dm_nfe_operacao (junk)                    (GOLD)
gold.ft_nfe_item · ft_nfe_documento · ag_nfe_ibscbs_mensal
   │  testes → dq_nfe.* (quarentena) · Elementary
   ▼
ctrl.fn_nfe_finalizar_lotes()  CARREGADO → PROCESSADO  ·  obs.v_nfe_lote → Grafana
```

## Rodando

```bash
make up                 # stack do lab (o 05_nfe.sql é aplicado no bootstrap / dag_setup_infrastructure)
# Airflow: despausar dag_nfe
make nfe-backfill       # histórico jan–set/2026 (5000 notas)
make nfe-demo           # +500 notas "de agora" (com anomalias)
make nfe-stream         # 12 lotes, 1 a cada 5 min (chegada contínua)
```

Ao vivo, sem Airflow:

```sql
SELECT ctrl.fn_nfe_ingerir_lote('manual-1', '/mnt/datasource/nfe/processando/manual-1');
-- dbt build --select tag:nfe --vars '{nfe_lote_max: <id>}'
SELECT ctrl.fn_nfe_finalizar_lotes(<id>);
```

Correção fora do fluxo principal:

```sql
SELECT ctrl.fn_nfe_reprocessar_lote(42, 'cClassTrib corrigido na tabela de referência');
-- próximo run do dag_nfe reprocessa o lote 42 (merge idempotente)
-- ou pontual: dbt build --select tag:nfe --vars '{nfe_lotes: [42]}'
```

## Camadas

| Camada | Objetos | Materialização | Por quê |
|---|---|---|---|
| Controle | `ctrl.nfe_lote`, `ctrl.nfe_arquivo_rejeitado` | tabela SQL | fila por lote (o "1/2" por lote, não por linha) |
| Bronze | `landing_nfe.nfe_xml` | tabela SQL, só INSERT | replay, auditoria, reenvios preservados |
| Staging | `stg_nfe__xml`, `int_nfe__xml_vigente` | ephemeral | recorte do lote + resolução de reenvio |
| Silver | `nfe_nota`, `nfe_item`, `nfe_participante_hist` | incremental `merge` + contrato | tipado, 1 linha por chave, auditável |
| Gold | dimensões, `ft_nfe_item`, `ft_nfe_documento` | table / incremental `merge` | estrela para BI |
| Agregado | `ag_nfe_ibscbs_mensal` | incremental `delete+insert` por mês | só recalcula meses afetados (notas atrasadas) |
| Qualidade | `dq_nfe.*` | `store_failures` | quarentena consultável |

## Modelo dimensional (gold)

Grão da fato principal: **item da NF-e**.

| Dimensão | Chave | Observação |
|---|---|---|
| `dm_nfe_tempo` | `sk_data` (AAAAMMDD) | data de emissão |
| `dm_nfe_participante` | `sk_participante` | emitente e destinatário (role-playing), **SCD2 por data de emissão** |
| `dm_nfe_operacao` | `sk_operacao` | *junk dimension* (tpNF, finNFe, idDest, indFinal, indPres) + `sinal_arrecadacao` |
| `dm_nfe_tributacao` | `sk_tributacao` (CST+cClassTrib) | vem do **snapshot** (histórico da tabela oficial) |
| `dm_nfe_cfop`, `dm_nfe_ncm`, `dm_nfe_municipio` | código natural | seeds; código ausente → teste `warn` |

`ag_nfe_ibscbs_mensal` responde às perguntas típicas de desempenho fiscal: arrecadação IBS/CBS, diferimento,
devolução de tributo e carga efetiva por mês, UF/município, CST/cClassTrib e venda/devolução.

## Recursos do dbt usados (e onde)

| Recurso | Onde |
|---|---|
| `sources` + `freshness` | `staging/nfe/sources_nfe.yml` |
| `ephemeral` | staging e `int_nfe__xml_vigente` |
| incremental `merge` / `delete+insert` | silver e fatos / agregado mensal |
| `contract: enforced` | `silver/nfe/schema_silver_nfe.yml` |
| `seeds` com `column_types` | `seeds/nfe/` (códigos com zero à esquerda) |
| `snapshot` (YAML, `check`, `hard_deletes`) | `snapshots/nfe/snapshots_nfe.yml` |
| macros | `macros/nfe/` (namespace, fila de lotes, DV módulo 11, sk do participante) |
| testes genéricos próprios | `nfe_chave_valida`, `nfe_cnpj_valido` (alfanumérico), `nfe_cclasstrib_coerente` |
| testes singulares | `tests/nfe/` (reconciliação cabeçalho × itens, regra da base 2026) |
| `where` em testes | escopo incremental (só o lote em processamento) |
| `store_failures` | `dq_nfe.*` |
| `vars` | `nfe_lote_max` (vem do Airflow via XCom), `nfe_lotes` (correção pontual) |
| `exposures` | `gold/nfe/exposures_nfe.yml` |
| Elementary | `volume_anomalies` em `nfe_nota` |

## Anomalias simuladas (e quem pega)

| Anomalia | Onde é detectada |
|---|---|
| XML truncado no transporte | ingestão → `ctrl.nfe_arquivo_rejeitado` |
| Reenvio da mesma chave | ingestão (`qtd_reenvios`) + silver (vale o mais recente) |
| Total CBS ≠ Σ itens | `assert_nfe_total_cbs_igual_soma_itens` (warn) |
| vBC IBS/CBS total ≠ Σ itens | `assert_nfe_total_vbc_ibscbs_igual_soma_itens` (warn) |
| cClassTrib incompatível com o CST | `nfe_cclasstrib_coerente` + relacionamento com `dm_nfe_tributacao` (warn) |
| Nota com data antiga chegando hoje | agregado reabre o mês certo; SCD2 aponta a versão certa |
| Alteração cadastral do contribuinte | nova versão em `dm_nfe_participante` |

As decisões de arquitetura estão em [ADR 0006](../adr/0006-nfe-incremental-lotes.md).

## Plano / próximos passos

1. ✅ Gerador validado no XSD, ingestão SQL, silver, gold, testes, DAG, CI
2. ✅ Painel Grafana **DataLabs · NF-e (IBS/CBS)** (`/d/datalabs-nfe`): operação dos lotes, qualidade (Elementary + `dq_nfe`) e desempenho fiscal da gold
3. Trilha "legado" para contraste: procedure PL/pgSQL com flag por linha + `postgres_fdw` (simula dblink) e reconciliação com a gold dbt
4. Eventos (cancelamento, CC-e) como fato própria
5. Gold exportada em Parquet (MinIO) + DuckDB: primeiro passo para lakehouse
