-- ============================================================================
-- NF-e (IBS/CBS/IS) — controle de lotes + bronze (landing) + ingestão 100% SQL
-- Idempotente. Aplicado pelo docker-entrypoint (volume novo) e por `make db-bootstrap`.
--
-- Fluxo:  inbox/*.xml --(Airflow: mv para processando/<lote>)--> ctrl.fn_nfe_ingerir_lote()
--         --> landing_nfe.nfe_xml (imutável, só INSERT) --> dbt (silver/gold) --> ctrl.fn_nfe_finalizar_lotes()
--
-- Status do lote:  RECEBIDO -> CARREGADO -> PROCESSADO | ERRO
--   CARREGADO  = "indi_processamento = 1": na bronze, aguardando dbt
--   PROCESSADO = "indi_processamento = 2": silver + gold concluídas
--   A marca é por LOTE (1 UPDATE por lote), nunca por registro: a bronze não sofre UPDATE.
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS landing_nfe;   -- raw landing (bronze) da NF-e
CREATE SCHEMA IF NOT EXISTS dq_nfe;        -- falhas dos testes dbt da NF-e (store_failures = quarentena)

CREATE TABLE IF NOT EXISTS ctrl.nfe_lote (
    lote_id         bigserial   PRIMARY KEY,
    nome_lote       text        NOT NULL UNIQUE,     -- pasta do lote (ts do Airflow)
    diretorio       text        NOT NULL,
    status          text        NOT NULL DEFAULT 'RECEBIDO'
                    CHECK (status IN ('RECEBIDO', 'CARREGADO', 'PROCESSADO', 'ERRO')),
    qtd_arquivos    int,
    qtd_carregados  int,
    qtd_rejeitados  int,
    qtd_reenvios    int,                              -- chaves que já existiam na bronze
    recebido_em     timestamptz NOT NULL DEFAULT now(),
    carregado_em    timestamptz,
    processado_em   timestamptz,
    dag_run_id      text,
    mensagem        text
);
CREATE INDEX IF NOT EXISTS ix_nfe_lote_pendente ON ctrl.nfe_lote (lote_id) WHERE status = 'CARREGADO';

CREATE TABLE IF NOT EXISTS landing_nfe.nfe_xml (
    lote_id         bigint      NOT NULL REFERENCES ctrl.nfe_lote (lote_id),
    arquivo         text        NOT NULL,
    chave_acesso    text,                             -- extraída na ingestão (índice p/ reenvios)
    hash_conteudo   text        NOT NULL,             -- md5: reenvio idêntico x reenvio alterado
    tamanho_bytes   int         NOT NULL,
    conteudo        xml         NOT NULL,
    carregado_em    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (lote_id, arquivo)
);
CREATE INDEX IF NOT EXISTS ix_nfe_xml_chave ON landing_nfe.nfe_xml (chave_acesso);

CREATE TABLE IF NOT EXISTS ctrl.nfe_arquivo_rejeitado (
    id              bigserial   PRIMARY KEY,
    lote_id         bigint      NOT NULL REFERENCES ctrl.nfe_lote (lote_id),
    arquivo         text        NOT NULL,
    motivo          text        NOT NULL,
    sqlstate        text,
    inicio_conteudo text,                             -- primeiros bytes, p/ diagnóstico
    rejeitado_em    timestamptz NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------------------
-- Ingestão: lê a pasta do lote no servidor Postgres (pg_ls_dir/pg_read_file).
-- Requer superuser ou a role pg_read_server_files, e a pasta montada no container.
-- Arquivo malformado não derruba o lote: vai para ctrl.nfe_arquivo_rejeitado.
-- Reexecução segura: lote já CARREGADO/PROCESSADO -> no-op; RECEBIDO/ERRO -> recarrega.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ctrl.fn_nfe_ingerir_lote(p_nome_lote text, p_diretorio text, p_dag_run_id text DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    v_lote   bigint;
    v_status text;
    v_arq    text;
    v_txt    text;
    v_xml    xml;
    v_tot    int := 0;
    v_ok     int := 0;
    v_rej    int := 0;
    v_reenv  int := 0;
BEGIN
    INSERT INTO ctrl.nfe_lote (nome_lote, diretorio, dag_run_id)
    VALUES (p_nome_lote, p_diretorio, p_dag_run_id)
    ON CONFLICT (nome_lote) DO UPDATE SET dag_run_id = EXCLUDED.dag_run_id
    RETURNING lote_id, status INTO v_lote, v_status;

    IF v_status IN ('CARREGADO', 'PROCESSADO') THEN
        RETURN v_lote;                                   -- idempotência
    END IF;
    DELETE FROM landing_nfe.nfe_xml          WHERE lote_id = v_lote;   -- carga parcial anterior
    DELETE FROM ctrl.nfe_arquivo_rejeitado   WHERE lote_id = v_lote;

    FOR v_arq IN SELECT f FROM pg_ls_dir(p_diretorio) AS f WHERE f LIKE '%.xml' ORDER BY f LOOP
        v_tot := v_tot + 1;
        v_txt := NULL;
        BEGIN
            v_txt := pg_read_file(p_diretorio || '/' || v_arq);
            v_xml := XMLPARSE(DOCUMENT v_txt);            -- XML malformado levanta exceção aqui
            INSERT INTO landing_nfe.nfe_xml (lote_id, arquivo, chave_acesso, hash_conteudo, tamanho_bytes, conteudo)
            VALUES (v_lote, v_arq,
                    substr((xpath('//n:infNFe/@Id', v_xml,
                                  ARRAY[ARRAY['n', 'http://www.portalfiscal.inf.br/nfe']]))[1]::text, 4),
                    md5(v_txt), octet_length(v_txt), v_xml);
            v_ok := v_ok + 1;
        EXCEPTION WHEN OTHERS THEN
            INSERT INTO ctrl.nfe_arquivo_rejeitado (lote_id, arquivo, motivo, sqlstate, inicio_conteudo)
            VALUES (v_lote, v_arq, SQLERRM, SQLSTATE, left(v_txt, 300));
            v_rej := v_rej + 1;
        END;
    END LOOP;

    -- reenvios: chave que já existia na bronze (lote anterior ou outro arquivo do mesmo lote)
    SELECT count(*) INTO v_reenv
    FROM landing_nfe.nfe_xml x
    WHERE x.lote_id = v_lote
      AND EXISTS (SELECT 1 FROM landing_nfe.nfe_xml o
                  WHERE o.chave_acesso = x.chave_acesso
                    AND (o.lote_id < x.lote_id OR (o.lote_id = x.lote_id AND o.arquivo < x.arquivo)));

    UPDATE ctrl.nfe_lote
       SET status         = CASE WHEN v_ok = 0 THEN 'PROCESSADO' ELSE 'CARREGADO' END,
           qtd_arquivos   = v_tot,
           qtd_carregados = v_ok,
           qtd_rejeitados = v_rej,
           qtd_reenvios   = v_reenv,
           carregado_em   = now(),
           processado_em  = CASE WHEN v_ok = 0 THEN now() END,
           mensagem       = CASE WHEN v_ok = 0 THEN 'lote sem arquivos validos' END
     WHERE lote_id = v_lote;
    RETURN v_lote;
END $$;

-- Fecha os lotes que o dbt processou (todos os CARREGADO até p_lote_max).
CREATE OR REPLACE FUNCTION ctrl.fn_nfe_finalizar_lotes(p_lote_max bigint, p_status text DEFAULT 'PROCESSADO',
                                                       p_mensagem text DEFAULT NULL)
RETURNS int
LANGUAGE sql AS $$
    WITH u AS (
        UPDATE ctrl.nfe_lote
           SET status = p_status, processado_em = now(), mensagem = coalesce(p_mensagem, mensagem)
         WHERE status = 'CARREGADO' AND lote_id <= p_lote_max
        RETURNING 1)
    SELECT count(*)::int FROM u
$$;

-- Correção fora do fluxo principal: devolve um lote à fila (o dbt reprocessa via merge idempotente).
CREATE OR REPLACE FUNCTION ctrl.fn_nfe_reprocessar_lote(p_lote_id bigint, p_motivo text)
RETURNS void
LANGUAGE sql AS $$
    UPDATE ctrl.nfe_lote
       SET status = 'CARREGADO', processado_em = NULL,
           mensagem = 'reprocessamento: ' || p_motivo
     WHERE lote_id = p_lote_id AND status IN ('PROCESSADO', 'ERRO')
$$;

-- ----------------------------------------------------------------------------
-- Observabilidade (Grafana lê obs.*)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW obs.v_nfe_lote AS
SELECT l.*,
       extract(epoch FROM l.carregado_em  - l.recebido_em)  AS seg_ingestao,
       extract(epoch FROM l.processado_em - l.carregado_em) AS seg_transformacao,
       extract(epoch FROM coalesce(l.processado_em, now()) - l.recebido_em) AS seg_total,
       CASE WHEN l.status = 'CARREGADO' AND l.carregado_em < now() - interval '1 hour' THEN 'atrasado'
            ELSE lower(l.status) END AS saude
FROM ctrl.nfe_lote l;

CREATE OR REPLACE VIEW obs.v_nfe_rejeitados AS
SELECT r.lote_id, l.nome_lote, r.arquivo, r.motivo, r.sqlstate, r.rejeitado_em
FROM ctrl.nfe_arquivo_rejeitado r JOIN ctrl.nfe_lote l USING (lote_id);
