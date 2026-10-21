# Estudo de caso: RAG da LC 214/2025, de um PDF a um gate de respostas

> Documento de estudo (pt-BR). A fonte técnica de cada decisão são as ADRs 0010-0015; aqui está o fio que liga
> todas elas, o porquê de cada passo e como ler as métricas. Todo número tem data e origem. Onde o resultado
> ficou abaixo do esperado, está dito.

## 0. Como estudar isto

Ordem sugerida: §1-2 (o desenho), §4 (como medimos, é o coração), §3 (cada passo), §5 (resultados), §6
(observabilidade), §7 (lições). Os §8-9 são para publicar e para se testar.

Glossário mínimo:

| termo | significa |
|---|---|
| dispositivo / provisão | uma unidade da lei com id próprio: artigo, parágrafo, inciso, alínea |
| chunk | trecho de texto vetorizado (≤ 512 tokens do modelo de embedding) |
| small-to-big | busca no chunk pequeno, mostra o artigo inteiro ao LLM |
| golden set | perguntas rotuladas com a(s) provisão(ões) esperada(s): o "gabarito" |
| gate | verificação que **falha a execução** se a qualidade cair abaixo de um piso |
| baseline | as métricas da primeira medição boa, commitadas: o piso nasce delas |
| fingerprint | hash de tudo que muda o resultado (lei, modelo, prompt, golden set...) |
| exit 99 | "nada a fazer, a saída já está atualizada" (skipped, não é erro) |

## 1. O problema e quem é o usuário

O usuário-alvo é um **auditor fiscal da SEFAZ** que fiscaliza contribuinte emissor de NF-e e pergunta sobre a
LC 214/2025 (IBS/CBS/IS). Para esse leitor, **uma resposta que soa certa e não está na lei é pior que nenhuma
resposta**. Isso decide o desenho: o sistema começa pensando em *quando não responder*, e só depois em como responder.

Três propriedades que o projeto exige e mede:

1. **Fundamentada:** toda resposta cita dispositivos que existem na lei **e** que estavam no contexto mostrado ao modelo.
2. **Abstenção correta:** "não consta" quando a lei não trata do assunto (e dizer qual barreira decidiu).
3. **Reprodutível e observável:** dá para saber, por execução, se a qualidade subiu, caiu ou ficou igual, e por quê.

## 2. Visão ponta a ponta

```
 PDF da Câmara ──► lc01 extract ──► lc02 parse ──► lc03 chunks ──┬─► lc04a load  (Postgres: provisões, artigos)
 (norma atualizada)  contrato        1 linha por     small-to-big │
                     da fonte +      dispositivo     ≤ 512 tokens ├─► lc04c embed (pgvector: 867 vetores)
                     sha256          + invariantes                │
                                                                  └─► golden_contract (rótulos ainda valem?)
                                                                              │
   health checks (6) ◄── load + embed                                         ▼
          └──────────────────────────────────────────────►  lc05 evaluate_retrieval  ── GATE 1 (retrieval)
                                                                              │
                                                                              ▼
                                  pergunta ─► router ─► retriever ─► lc06 generate_answers ── GATE 2 (respostas)
                                             sql|vector|            3 barreiras a "não consta"
                                             combined|clarify
```

Camadas de armazenamento (o *lake* é o sistema de registro; o Postgres é uma projeção reconstruível):
`landing/lc214` (PDF como publicado, toda versão) → `bronze/lc214/text` (texto extraído) →
`silver/lc214/<sha12>/` (dispositivos, chunks, artigos, relatórios).

**Princípio de orquestração:** cada passo decide sozinho se há trabalho (entrada + versão do código iguais às da
última vez → exit 99). Por isso o DAG semanal (segunda 06:40) é barato quando a lei não mudou, e uma mudança de
`PARSER_VERSION` é aplicada sem esperar uma emenda (`trigger_rule=NONE_FAILED`: "skipped" significa "atualizado",
não "pare").

## 3. Passo a passo: o que, por quê, como mede, como falha

### lc01: extrair (ADR 0010)
- **O quê:** baixa o PDF "norma atualizada", valida e guarda por versão (sha256).
- **Por quê:** a lei é republicada a cada emenda. Um hash fixo quebraria em mudança legítima; então o *contrato é
  estrutural* (é PDF mesmo, ≥ 250 páginas, título e último artigo presentes) e o hash só **detecta versão**.
- **Mede/observa:** `files_total`, `rows_out` = páginas. Check `lc214_source_watch_freshness` (rodou nos últimos 8 dias).
- **Falha como:** `not_a_pdf` (página HTML de erro), arquivo truncado. Mesmo sha → exit 99.

### lc02: parsear (ADR 0010)
- **O quê:** transforma texto em **uma linha por dispositivo**, com id construído do rótulo impresso
  (`lc214-2025:art-7-A:par-1:inc-II:ali-a`). Notas da consolidação saem do texto e viram colunas.
- **Por quê:** PDF não tem estrutura; a estrutura jurídica é convenção de redação. A indentação é sinal (1ª linha do
  dispositivo indentada; linha quebrada começa na coluna 0). Revogado/vetado vira **linha**, nunca vetor:
  "foi revogado" é resposta por consulta, não "não sei".
- **Qualidade (contrato de dados):** I1 artigos 1..N sem buracos; I2 ids únicos; I3 sequências contíguas;
  I4 ≥ 98% das palavras dentro de algum dispositivo; I5 revogado/vetado ⇔ sem corpo. Defeito conhecido da fonte
  só passa se estiver em `known_anomalies.yml`; qualquer outro derruba a execução e exige decisão humana.
- **Resultado (v. `0cf36f4433ce`):** 4.223 dispositivos (580 artigos, 1.228 §, 1.792 incisos, 623 alíneas), cobertura
  98,25%; 4.165 vigentes, 33 revogados, 25 vetados.
- **Lição concreta:** o invariante I5 achou o art. 517 marcado como vetado por engano (o texto da LC 123 citado
  dentro dele contém "(VETADO)"). A contagem "26 marcadores = 26 vetados" **não via**: um falso positivo mais um
  genuíno somam o mesmo. *Reconciliação por contagem não enxerga troca; regra que amarra duas derivações
  independentes da mesma linha enxerga.*
- **Observa:** `rows_out`, check `lc214_parse_volume` (≥ 4.000).

### lc03: chunks small-to-big (ADR 0010)
- **O quê:** artigo que cabe é 1 chunk; maior é dividido pela própria estrutura (caput, §, incisos, janela de palavras
  como último recurso), cada pedaço com cabeçalho (Título, Capítulo, Art.).
- **Por quê:** o modelo de embedding lê 512 tokens e o sentence-transformers **trunca em silêncio** o resto. Com o
  tokenizer real, chunk acima do limite derruba a execução; sem ele a contagem é estimativa, e estimativa não
  prova ausência de truncamento (por isso o gate de IA é um `ShortCircuit`).
- **Resultado:** 867 chunks (450 artigos inteiros, 351 grupos, 39 dispositivos únicos, 27 janelas); tokens p50 267,
  p95 497, máx 512, **0 acima do limite**.
- **Observa:** `rows_in` (artigos) → `rows_out` (chunks); check `lc214_vector_parity` mais adiante.

### lc04a: carga de serving (ADR 0011)
- **O quê:** projeta a versão atual em `ai.lc214_dispositivo` / `ai.lc214_artigo` por **upsert + delete de órfãos em
  uma transação**.
- **Por quê:** `TRUNCATE` bloqueia leitores (medido: leitor com `lock_timeout` de 2 s bloqueado; com upsert leu as
  4.222 linhas em 2 ms). `row_hash` só do **conteúdo** (não do carimbo de versão nem da posição): inserir o art. 54-A
  desloca a `ordem` de 3.590 provisões; com posição no hash seriam ~3.590 "atualizadas", e a carga reportou `+1 ~0 -0`.
- **Guardas:** `silver_inconsistent` (lc02 e lc03 de versões diferentes), `mass_delete_blocked` (> 5% saindo de uma vez;
  medido: parse que perdeu 10% foi barrado e a tabela ficou intacta).
- **Observa:** por tabela, `inserted/updated/deleted/restamped`: o que uma emenda fez. Checks
  `lc214_serving_parity` (tolerância 0) e `lc214_serving_single_version` (nunca duas versões da lei misturadas).

### lc04c: embeddings e busca vetorial (ADR 0013)
- **O quê:** `ai.lc214_chunk` sincronizada por **diferença de conjuntos** no `chunk_id` (mesmo id = mesmo texto = mesmo
  vetor): só embeda o novo, apaga o obsoleto, "restampa" o inalterado. Busca **exata** (sem HNSW).
- **Por quê (exata):** 867 vetores × 384 dims = 1-3 ms por consulta. Um índice aproximado só acrescentaria erros de
  recall à baseline do lc05.
- **Retriever:** âncoras do SQL primeiro, depois os acertos por rank, um bloco por artigo, **inteiro** até 2.000
  tokens-LLM (senão em camadas), orçamento guloso em tokens do LLM (estimado 1,7 token/palavra até o lc06 medir).
- **Observa:** `sync_mode`, `chunks_inserted/deleted/restamped`, `model_load_s`, `embed_rows_per_s`; checks
  `lc214_vector_parity`, `lc214_vector_same_law`. Guardas `chunks_estimated` e `store_shrink` (> 5% líquido).

### Router (ADR 0012): decide antes de recuperar
- **Rotas:** `sql` (só citação: texto, status, redação anterior), `combined` (citação + pergunta: a provisão citada
  é âncora, vetores completam), `vector` (sem citação), `clarify` (ambíguo ou inexistente: pergunta de volta, **nunca chuta**).
  Outra lei é `foreign`: nunca âncora.
- **Por quê:** parte das perguntas é consulta, não semântica. Responder "o que diz o art. 7º" por similaridade é
  introduzir risco onde havia certeza. E o roteador é **determinístico**, então o golden contract o checa *exatamente*.
- **Resultado:** 82 frases do golden set roteadas certo (37 vector, 11 sql, 2 combined, 3 clarify); a lei cita a si
  mesma 799 vezes e 796 resolveram (99,62%). Achou 4 bugs de parser que nenhum caso escrito à mão achou, inclusive
  uma taxa **inflada** (1.335 em vez de 799: o rótulo do próprio artigo contava como citação).

### golden_contract (ADR 0010, decisão 9)
- **O quê:** os rótulos do golden set são ids de **uma versão** da lei; todo run re-checa contra a versão atual
  (G1 esquema, G2 versão, G3 existe, G4 status, G5 alterado, G6 recuperável em k chunks) + roteamento + auto-citações.
- **Por quê:** uma emenda que revoga ou renumera uma provisão rotulada tem de ficar **vermelha aqui, com o caso
  nomeado**, e não baixar o recall@k em silêncio no lc05. Rótulo errado pune a resposta certa.
- **Observa:** `routing_accuracy` (exige 100%), `self_ref_rate` (≥ 99%).

### lc05: gate de retrieval (ADR 0014)
- **O quê:** passa cada frase do golden set pelo `retrieve()` real (router → busca → contexto) e **falha** se
  success@k, MRR ou context_recall caírem abaixo de `baseline − 1 caso / n`.
- **Por quê:** "alguém disse que melhorou" não é evidência. Cada ideia (busca híbrida, glossário fiscal→legal,
  modelo de embedding maior) precisa de **um número a bater**, medido sempre do mesmo jeito. Roda toda semana
  (~15 s): uma troca de modelo com a mesma lei aparece aqui.
- **Detalhes em §4.**

### lc06: geração com três barreiras (ADR 0015)
- **O quê:** Ollama local (`qwen2.5:3b`, CPU), saída em **JSON Schema** `{answerable, answer, citations[]}`,
  `temperature 0` + `seed`, `num_ctx` 6144.
- **Três barreiras a "não consta"** (duas determinísticas):
  1. *Antes do LLM:* rota `clarify` ou nenhum bloco de contexto → o LLM nem é chamado.
  2. *O modelo:* `answerable=false`.
  3. *Depois do LLM:* cada citação passa pelo `parse_citations` + `Index.resolve` do router e é conferida contra as
     provisões **que o contexto mostrou**: `valid`, `outside_context` (existe, mas o modelo respondeu de memória),
     `not_found` (inventada), `foreign` (outra lei). Sem nenhuma válida → recusa.
- **Por quê:** com modelo pequeno, **confie no que dá para verificar** (citação resolvida contra o contexto) mais que
  no que o modelo diz de si mesmo.
- **Regra de decisão separada da geração** (`model`, `citations`, `model_text`): não está no fingerprint; cada
  relatório pontua todas, e `--rescore` re-decide respostas gravadas **sem chamar o LLM**.
- **Gate 2:** refusal accuracy e citation recall podem cair ≤ 1 caso; **false answers não podem crescer nada**.
  Drift (flips de veredito, textos reescritos) é **registrado, não gated**.

## 4. Como medimos (o coração do projeto)

### 4.1 O golden set
53 casos / 82 frases (v2). Cada caso de conteúdo tem **duas formulações do mesmo assunto**: `fiscal` (como o auditor
fala) e `formal` (como a lei fala). A diferença entre as duas mede o quanto o sistema depende do vocabulário da lei.
Cobre todas as rotas, abstenção (redação anterior, ICMS), anomalia do art. 260 e referências a outras leis.

### 4.2 Métricas de retrieval (lc05), com definição
- **success@k:** todas as provisões esperadas apareceram nos k=5 chunks recuperados (modo `all`) ou ao menos uma (`any`).
- **hit@k / recall@k:** pelo menos uma / fração das esperadas nos top-k.
- **MRR:** média de 1/rank do primeiro acerto (1ª posição = 1; 2ª = 0,5...).
- **context_recall:** das esperadas, quantas o **LLM efetivamente lerá** no contexto montado. É maior que o recall@k
  porque acertar o caput traz o artigo todo: esse ganho é exatamente o que o small-to-big compra, e agora é medido.
- **paired gap:** success@k formal − fiscal nos mesmos 25 casos: o custo de perguntar como auditor.

### 4.3 Métricas de geração (lc06)
- **refusal_accuracy:** respondeu quando devia e recusou quando devia.
- **false_answers:** respondeu a pergunta **sem** resposta na lei. É o erro que as barreiras existem para impedir. **Teto 0.**
- **false_refusals:** recusou pergunta respondível. É o custo de ser conservador, e onde a qualidade perde hoje.
- **citation_validity:** citações que existem **e** estavam no contexto / todas as citações.
- **citation_recall** e **citation_recall_in_context:** citou a provisão esperada; a segunda só onde ela chegou ao
  contexto. Separam a causa: erro com a provisão no contexto é **do modelo**; sem ela é **do retriever** (que o lc05 já conta).
- **model_says_answerable:** quanto o modelo afirma poder responder, antes das barreiras. **Preso em 0 ou 1 não é sinal.**

### 4.4 O gate: por que "casos" e não "pontos percentuais"
Piso = `baseline − tolerância_em_casos / n`. Um caso do grupo fiscal do lc05 vale 3,2 p.p.; do formal, 4,0 p.p.
Uma pessoa lê "1 caso" e entende; "−3 p.p." não diz se é ruído. Melhorias **nunca falham** (viram log
`improved`), e subir a baseline é um commit deliberado: uma catraca operada por humano. **Sem baseline = falha**
(um gate sem piso não é gate). Mudou a definição da métrica (`EVAL_VERSION`)? A baseline antiga é recusada.

**Limite honesto:** com 7 frases fiscais e 4 formais no gate do lc06, 1 caso = 14 e 25 p.p. É um gate **grosso**:
pega colapso, não nuance. E o piso do citation recall fiscal é negativo (baseline 0,0 − 1/5): ali o gate não pode
reprovar nada. Crescer o subconjunto dev é o caminho (cada frase custa ~3,5 min de CPU).

## 5. Resultados e o que deu errado

### 5.1 Retrieval (baseline commitada, 2026-10-19)

| grupo | n | success@k | hit@k | recall@k | MRR | context_recall |
|---|---|---|---|---|---|---|
| fiscal | 31 | 0,387 | 0,581 | 0,484 | 0,454 | 0,652 |
| formal | 25 | 0,480 | 0,840 | 0,633 | 0,753 | 0,767 |

Lacuna fiscal × formal (success@k, 25 casos pareados): **0,16**. O auditor é pior servido que a lei, porque
"transferiu da matriz para a filial" e "transferência entre estabelecimentos do mesmo contribuinte" não dividem
nenhuma palavra discriminativa (nem o stemmer junta: `transferiu`→`transfer`, `transferência`→`transferent`).

### 5.2 Experimentos de uma variável só
- **`num_ctx` (velocidade × recall):** 8192 → 6144 manteve o context_recall (0,651 / 0,767) e cortou ~5% do tempo;
  4096 perdeu 3 casos fiscais; 3072 perdeu 5 fiscais e 2,5 formais. Critério fixado **antes**: no máximo 2 casos
  fiscais e 1 formal. Só o 6144 passou. A hipótese "blocos menores recuperam o recall" foi **refutada** (piorou a cada
  passo): a resposta costuma estar nos incisos do artigo do acerto, e o small-to-big funciona mostrando o artigo inteiro.
- **Hardware, não contexto, deu a velocidade:** o WSL estava limitado a 4 dos 8 núcleos lógicos. Com 8, o mesmo
  prompt caiu de 130 s para 88 s (×1,48) com a **mesma resposta e citações**.
- **Estimativa errada, corrigida pela medição:** achei que cada geração levaria 20-60 s; foram 130-200 s. Isso gerou o
  subconjunto dev (11 frases em vez de 60) e a decisão de rodar o conjunto completo (~3,5 h) fora do DAG.

### 5.3 Geração: modelo pequeno demais

| modelo / prompt | acertos de 9 respondíveis | observação |
|---|---|---|
| llama3.2:1b, baseline | 0 | recusou 11 de 11; `answerable=false` em 33/33 respostas sob 3 prompts; inventou um cálculo de DIFAL |
| llama3.2:1b, `verdict_last` / `answer_default` | 0 | mudou o que escreve, não o veredito |
| qwen2.5:3b, `answer_default` + `model_text` | **2** (barra: 3) | não inventou nada; recusou as 2 sem-resposta com o motivo certo |

Leitura humana (você, especialista de domínio) com critério fixado antes da execução: 2 corretas
(content-01/formal, art. 4º; content-02/formal, art. 3º), 1 parcial (external-01/fiscal aponta o art. 14 do CTN,
mas enquadra instituição de ensino como assistência social e cita § 2º e § 4º onde se esperava o § 3º).
**O critério não foi relido depois.** O que falhou foi **cobertura** (6 falsas recusas: "o contexto não trata
especificamente..."), não **confiabilidade** (nada mostrado foi inventado).

### 5.4 A última execução do DAG (2026-10-21, `force=true`, todos os passos verdes)
- 6/6 health checks; golden contract 82/82; retrieval com métricas idênticas à baseline.
- `generate_answers` (2.523 s): fiscal n=7 refusal 0,429; formal n=4 refusal 0,500, citation recall 0,500,
  citation validity 1,000; **false answers 0/0**; tokens/palavra medido 1,828 (estimado 1,7); p50 de 253 s / 284 s.
- **Drift contra a baseline: 0 flips, 0 mudanças de citação esperada, 0/11 textos reescritos.** Esta execução
  reproduziu a baseline **byte a byte**. Na noite anterior, dois runs concorrentes tinham sido iniciados por engano e
  mortos: o run seguinte reescreveu 11/11 textos. A hipótese é contaminação do Ollama por requisições paralelas
  (cache/lote), não indeterminismo do modelo. **Não está provada** (uma observação de cada lado); por isso o gate é
  tolerante e o drift fica registrado.

## 6. Observabilidade: onde cada pergunta é respondida

Quatro sinais (ADR 0001): logs estruturados (Loki), métricas de plataforma (Prometheus), **fatos por passo**
(`audit.pipeline_step_runs`) e **saúde de dados** (`audit.data_quality_checks`). Observabilidade nunca derruba o
pipeline (escrita de auditoria é best-effort), e alerta-se **uma vez, na causa raiz**: o lc06 roda depois do gate do
lc05, então um retriever pior falha **lá**.

| pergunta de quem opera | onde está a resposta |
|---|---|
| o gate está de pé? | painel "Gate de retrieval / respostas" (success/failed) |
| quanto falta para o piso? | "Folga mínima": menor (métrica − piso) |
| a lei mudou e o que isso causou? | `inserted/updated/deleted/restamped` do lc04a; volumes do parse |
| os dados estão consistentes? | 6 health checks (paridade, versão única, vetores da mesma lei) |
| os rótulos ainda valem? | golden contract (roteamento, auto-citações) |
| o modelo está respondendo ou recusando tudo? | `model_says_answerable`, `false_refusals` |
| o comportamento mudou sem a métrica mudar? | drift: flips de veredito, textos reescritos |
| o orçamento de contexto está certo? | tokens/palavra medido × 1,7 estimado |
| quanto custa? | latência p50/máx por frase; duração do passo |

**Dashboard:** `DataLabs · RAG LC 214` (`grafana/dashboards-src/build.py`, função `rag_lc214`). Cada gate grava no
`metrics` do passo, **junto com a métrica, o piso que a julgou** (`floor_<grupo>_<métrica>`; `ceiling_..._false_answers`).
Assim o painel desenha valor × piso sem copiar YAML, e se a baseline mudar, o histórico mostra contra qual piso cada
execução foi julgada. Painéis de métrica só aparecem **depois da primeira execução do DAG com este código** (as
execuções antigas não gravaram os pisos).

## 7. Lições aprendidas

1. **Comece por quando não responder.** O custo do erro define o desenho: false answers com teto 0, false refusals
   medidos como custo.
2. **Gate sem baseline não é gate; baseline sem revisão é um piso baixo disfarçado.** Suba a baseline em commit próprio.
3. **Um experimento, uma variável, um número a bater, critério fixado antes.** Foi o que impediu "melhorar" o
   resultado reinterpretando o critério (2/9 ficou 2/9).
4. **Meça a causa, não só o efeito.** Dois recalls (com e sem a provisão no contexto) separam culpa do retriever da
   culpa do modelo.
5. **Confie no que dá para verificar.** Citação resolvida contra o contexto vale mais que o `answerable` do modelo
   (que ficou em `false` 33 de 33 vezes).
6. **Contagem não vê troca.** Reconciliar totais não acha o art. 517; amarrar duas derivações independentes acha.
7. **Rótulo é dado.** O golden set tem contrato e é revalidado a cada execução contra a versão atual da lei.
8. **Determinismo se verifica, não se supõe.** O digest de texto era a régua errada: o veredito é reprodutível, a
   redação nem sempre. Mudou-se o gate, e o drift virou registro com histórico antes de virar limite.
9. **Estimativa vira medição.** 1,7 token/palavra → 1,83 medido; 20-60 s por frase → 130-200 s. Registre ambos.
10. **O gargalo estava na máquina.** Antes de otimizar o software, `nproc`.
11. **"Skipped" não é "pare".** Passo idempotente que avisa "estou atualizado" deixa o resto da cadeia reagir a mudança de código.
12. **Declare o que não passou.** O projeto entregou 2/9 contra a barra 3/9 e disse isso na ADR. É o que dá credibilidade ao resto.

## 8. Ideias de publicação (LinkedIn)

Cada uma com gancho e o número que a sustenta; não publique nenhuma sem rodar o dashboard e conferir o número.

1. **"Meu RAG tem gate de CI, e ele reprovaria a si mesmo."** Retrieval com piso derivado da baseline; o 2/9 vs 3/9.
2. **"Um modelo de 1B respondeu 'não' 33 vezes em 33."** Por que `answerable` constante não é sinal, e como a
   verificação de citação contornou isso (llama 0/9 → qwen 2/9, sem inventar nada).
3. **"A contagem batia e o parser estava errado."** O caso do art. 517: reconciliação × amarração de duas derivações.
4. **"Tolerância em casos, não em %."** Por que 1 caso = 3,2 p.p. é mais honesto que "−3%".
5. **"Aumentei o contexto? Não: troquei 4 por 8 núcleos."** Sweep de `num_ctx` (6144 mantém o recall) e a hipótese
   refutada de blocos menores; ×1,48 de velocidade com a mesma resposta.
6. **"Determinismo: a hipótese que ainda não provei."** O digest de texto vs digest de veredito, a contaminação por
   execuções concorrentes, e por que o gate ficou tolerante com drift registrado.
7. **"O auditor não fala como a lei."** A lacuna fiscal × formal de 0,16 medida em casos pareados.

## 9. Perguntas para se testar

1. Por que o hash do PDF não pode ser o contrato da fonte? O que detecta, então, uma versão nova?
2. Por que `row_hash` exclui `ordem`? Qual seria o sintoma se a incluísse? (Número: 3.590.)
3. Em que caso o context_recall é maior que o recall@k, e por quê?
4. Um erro com `citation_recall_in_context` baixo é do modelo ou do retriever? E com recall em contexto alto, mas recall total baixo?
5. Por que o gate de respostas permite cair 1 caso, mas **não** permite 1 false answer?
6. O que o piso fiscal de citation recall negativo significa para o gate?
7. Por que o lc06 roda depois do lc05, e não em paralelo?
8. Se `model_says_answerable` ficar em 0,0 numa execução futura, o que você olha primeiro?
9. O que teria de ser verdade para elevar a baseline do lc06?
10. Qual afirmação deste estudo **não** está provada?

## 10. Limites e próximos experimentos (um por vez, contra a baseline)

- **Limites:** subconjunto dev de 11 frases; só 2 perguntas sem resposta calibram a abstenção (nenhum limiar de
  similaridade por isso); leitura humana de 3 respostas; CPU-bound (~250 s por frase).
- **Próximo:** glossário fiscal→legal (retrieval **e** geração, ataca o "não trata especificamente"); linhas
  rotuladas no contexto (`[art. 5, § 2º]`); execução completa (~3,5 h) no servidor lambda; modelo maior ou hospedado
  atrás da mesma interface `LLM` e do mesmo gate; calibrar `LC214_LLM_TOKENS_PER_WORD`; respostas reais gravadas
  do Ollama como fixtures de CI.

## Apêndice: reproduzir

```bash
docker compose --profile ai up -d
# DAG completo (força regerar também as respostas):
docker compose exec -T airflow-scheduler airflow dags trigger dag_rag_lc214 --conf '{"force": true}'
# só retrieval, ~15 s:
docker compose exec -T airflow-scheduler python -m pipelines.rag.lc214.lc05_eval
# experimento de prompt no subconjunto dev:
docker compose exec -T airflow-scheduler python -m pipelines.rag.lc214.lc06_generate --golden --dev --variant answer_default
# re-decidir respostas gravadas sem LLM:
docker compose exec -T airflow-scheduler python -m pipelines.rag.lc214.lc06_generate --rescore dev
```
