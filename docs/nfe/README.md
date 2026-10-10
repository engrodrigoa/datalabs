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

## Primeira execução (runbook)

Ambiente limpo, do clone ao dashboard. **Linux/macOS/WSL: um comando.**

```bash
git clone https://github.com/engrodrigoa/datalabs && cd datalabs
make nfe-quickstart
```

O que ele faz, em ordem:

| # | Passo | Por quê |
|---|---|---|
| 1 | `make up`: cria o `.env` com senhas aleatórias e sobe a stack | no volume novo, o Postgres roda `infra/postgres/init` (inclusive `05_nfe.sql`: schemas, `ctrl.nfe_lote`, funções, grants) |
| 2 | `dbt run --select elementary` | cria as tabelas do Elementary (histórico de testes); sem elas o bloco de qualidade do dashboard fica vazio. Uma vez só |
| 3 | `gen_nfe.py --n 2000` (jan–set/2026, com anomalias) | o "sistema de origem" deposita os XMLs na inbox |
| 4 | `airflow dags unpause dag_nfe` | DAGs nascem pausados no lab (`DAGS_ARE_PAUSED_AT_CREATION`) |
| 5 | `airflow dags trigger dag_nfe` | 1ª execução imediata (sem esperar o agendamento de 10 min) |

Acompanhe em http://localhost:8080/dags/dag_nfe/grid e veja o resultado em http://localhost:3000/d/datalabs-nfe.

**Windows (PowerShell, sem `make`):**

```powershell
Copy-Item .env.example .env            # e troque as senhas
New-Item -ItemType Directory -Force -Path datasource\nfe\inbox, datasource\nfe\processando, datasource\nfe\processados
docker compose up -d --build
docker compose exec -T airflow-scheduler bash -c "cd /opt/airflow/pipelines/dbt_projects && /opt/airflow/dbt_venv/bin/dbt run --select elementary --profiles-dir ."
docker compose exec -T airflow-scheduler python /opt/airflow/scripts/nfe/gen_nfe.py --inbox /mnt/datasource/nfe/inbox `
  --n 2000 --data-ini 2026-01-01 --data-fim 2026-09-30 --defect-rate 0.02 --dup-rate 0.01 --corrupt-rate 0.005
docker compose exec -T airflow-scheduler airflow dags unpause dag_nfe
docker compose exec -T airflow-scheduler airflow dags trigger dag_nfe
```

**Volume já existente** (stack rodando antes deste projeto): aplique o DDL uma vez com `make db-bootstrap`
(PowerShell: `docker compose exec -T postgres bash /docker-entrypoint-initdb.d/00_bootstrap.sh`).

**Como saber que deu certo:**

```sql
SELECT lote_id, status, qtd_arquivos, qtd_carregados, qtd_rejeitados, qtd_reenvios FROM obs.v_nfe_lote;
-- 1 lote PROCESSADO; rejeitados = arquivos truncados; reenvios = duplicatas
SELECT count(*) FROM silver.nfe_nota;   -- 2000
```

Testes em amarelo (WARN) no grupo `dbt_nfe` são esperados: são as inconsistências injetadas pelo gerador.

**Gerar notas a qualquer momento (atalho)** — roda no host (só biblioteca padrão do Python) e grava
direto na inbox do lab; o sensor do `dag_nfe` pega na execução corrente ou na próxima (≤ 10 min):

```bash
python scripts/nfe/gen_nfe.py 100                  # 100 notas "de agora"
python scripts/nfe/gen_nfe.py 100 --sujo           # + anomalias: defeitos 2%, reenvios 1%, truncados 0,5%
python scripts/nfe/gen_nfe.py 100 --sujo --agora   # + dispara o dag_nfe na hora
```

No Windows, gerar pelo host é bem mais rápido que via `docker compose exec` (evita a escrita arquivo a
arquivo pelo bind mount). `make nfe-demo` / `make nfe-stream` continuam disponíveis.

**Executar uma etapa à mão, sem Airflow** (útil para depurar):

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
