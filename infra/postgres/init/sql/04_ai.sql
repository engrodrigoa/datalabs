-- Vector store for the ANP RAG (multilingual-e5-base -> 768 dims)
CREATE TABLE IF NOT EXISTS ai.rag_context_anp (
    id               bigserial PRIMARY KEY,
    chunk_id         text,
    source_id        text,                  -- lineage: gold.ft_anp_combustiveis.id_fato
    chunk_text       text NOT NULL,
    metadata         jsonb,
    embedding        vector(768),
    embedding_model  text,
    created_at       timestamptz NOT NULL DEFAULT now()
);
-- upgrade path for tables created by the previous version
ALTER TABLE ai.rag_context_anp ADD COLUMN IF NOT EXISTS chunk_id text;
ALTER TABLE ai.rag_context_anp ADD COLUMN IF NOT EXISTS source_id text;
ALTER TABLE ai.rag_context_anp ADD COLUMN IF NOT EXISTS embedding_model text;
ALTER TABLE ai.rag_context_anp ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();

DO $$
BEGIN
    CREATE INDEX IF NOT EXISTS ix_rag_context_anp_hnsw
        ON ai.rag_context_anp USING hnsw (embedding vector_cosine_ops);
EXCEPTION WHEN others THEN
    RAISE NOTICE 'HNSW index not created (%). Recreate ai.rag_context_anp with embedding vector(768).', SQLERRM;
END $$;
CREATE UNIQUE INDEX IF NOT EXISTS ux_rag_context_anp_chunk ON ai.rag_context_anp (chunk_id);

-- RAG evaluation history (written by pipelines/rag/anp/rag04_eval_retrival.py; created here so
-- dashboards and checks work before the first evaluation)
CREATE TABLE IF NOT EXISTS ai.rag_eval_metrics (
    id SERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    executado_em TIMESTAMP NOT NULL DEFAULT now(),
    pergunta TEXT NOT NULL,
    tipo_consulta TEXT NOT NULL,
    top_k INT NOT NULL,
    similaridade_media DOUBLE PRECISION,
    similaridade_min DOUBLE PRECISION,
    similaridade_max DOUBLE PRECISION,
    taxa_redundancia DOUBLE PRECISION,
    cnpjs_distintos_retornados INT,
    total_postos_esperado_municipio INT,
    cobertura_municipio DOUBLE PRECISION,
    precision_at_k DOUBLE PRECISION,
    latencia_retrieval_ms DOUBLE PRECISION,
    observacoes TEXT,
    versao_pipeline TEXT,
    metodo_esperado TEXT,
    metodo_obtido TEXT,
    acerto_roteamento BOOLEAN,
    valor_obtido DOUBLE PRECISION,
    valor_esperado DOUBLE PRECISION,
    acerto_valor BOOLEAN
);

CREATE TABLE IF NOT EXISTS ai.rag_eval_summary (
    run_id UUID PRIMARY KEY,
    executado_em TIMESTAMP NOT NULL DEFAULT now(),
    modelo_embedding TEXT,
    top_k INT,
    total_perguntas INT,
    total_registros_contexto INT,
    municipios_distintos_contexto INT,
    bandeiras_distintas_contexto INT,
    similaridade_media_geral DOUBLE PRECISION,
    taxa_redundancia_media DOUBLE PRECISION,
    latencia_media_ms DOUBLE PRECISION,
    plano_execucao_usa_indice BOOLEAN,
    plano_execucao_detalhe TEXT,
    versao_pipeline TEXT,
    acuracia_roteamento DOUBLE PRECISION,
    acuracia_valor DOUBLE PRECISION,
    registros_gold_go INT,
    paridade_contexto_gold BOOLEAN
);
