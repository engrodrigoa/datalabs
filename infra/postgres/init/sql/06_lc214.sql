-- LC 214/2025 serving tables (ADR 0011): a projection of the CURRENT law version in the lake
-- (silver/lc214/<sha12>/), loaded by pipelines/rag/lc214/lc04_load.py. The lake is the system of record:
-- these tables can be dropped and rebuilt from silver at any time.
--
-- One row per provision / article. `row_hash` is the md5 of the content columns (not of the version stamp), so a
-- new law version updates only the provisions whose text or status changed: the load reports how many provisions
-- an amendment touched. `source_sha256` / `parser_version` say which silver produced the row.

CREATE TABLE IF NOT EXISTS ai.lc214_dispositivo (
    dispositivo_id    text PRIMARY KEY,                 -- lc214-2025:art-7-A:par-1:inc-II:ali-a (the citation key)
    parent_id         text,
    art_id            text NOT NULL,                    -- lc214-2025:art-7-A
    tipo              text NOT NULL CHECK (tipo IN ('art', 'par', 'inc', 'ali')),
    rotulo            text NOT NULL,                    -- label as printed: "Art. 7º-A", "§ 10", "II", "a"
    ordem             integer NOT NULL,                 -- reading order in the law
    art               text NOT NULL,
    par               text,
    inc               text,
    ali               text,
    titulo            text,
    titulo_nome       text,
    capitulo          text,
    capitulo_nome     text,
    secao             text,
    secao_nome        text,
    status            text NOT NULL CHECK (status IN ('vigente', 'revogado', 'vetado')),
    texto             text NOT NULL,
    notas             jsonb NOT NULL DEFAULT '[]',
    alterado_por      jsonb NOT NULL DEFAULT '[]',
    tipo_alteracao    jsonb NOT NULL DEFAULT '[]',
    altera_outra_lei  boolean NOT NULL,
    pagina            integer,
    row_hash          text NOT NULL,
    source_sha256     text NOT NULL,
    parser_version    text NOT NULL,
    loaded_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_lc214_dispositivo_art ON ai.lc214_dispositivo (art_id, ordem);
CREATE INDEX IF NOT EXISTS ix_lc214_dispositivo_status ON ai.lc214_dispositivo (status) WHERE status <> 'vigente';

CREATE TABLE IF NOT EXISTS ai.lc214_artigo (
    art_id            text PRIMARY KEY,
    art               text NOT NULL,
    rotulo            text NOT NULL,
    header            text NOT NULL,                    -- "LC 214/2025 | Título ... | Capítulo ... | Art. 7º"
    texto             text NOT NULL,                    -- the whole article: what an answer shows (small-to-big)
    status            text NOT NULL CHECK (status IN ('vigente', 'revogado', 'vetado')),
    pagina            integer,
    n_dispositivos    integer NOT NULL,
    alterado_por      jsonb NOT NULL DEFAULT '[]',
    altera_outra_lei  boolean NOT NULL,
    row_hash          text NOT NULL,
    source_sha256     text NOT NULL,
    parser_version    text NOT NULL,
    loaded_at         timestamptz NOT NULL DEFAULT now()
);

-- The vector store of the LC 214 (step 4c, ADR 0013): one row per chunk of the current version (chunks.parquet, lc03),
-- synced by set difference on the deterministic chunk_id (ADR 0007), loaded by pipelines/rag/lc214/lc04_embed.py.
-- A chunk_id is md5(chunker version | text): same id = same text = same vector, so a new law version or parser that
-- leaves a chunk's text alone only restamps it (no re-embedding). Searched by EXACT cosine distance: no ANN index at
-- this size (867 chunks), so the retrieval measured in lc05 carries no approximation error.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS ai.lc214_chunk (
    chunk_id          text PRIMARY KEY,                 -- lc214-2025:art-6:c1:<md5[:16]>
    art_id            text NOT NULL,                    -- the article the chunk belongs to (small-to-big)
    parte             integer NOT NULL,                 -- 1..n inside the article
    kind              text NOT NULL CHECK (kind IN ('artigo', 'grupo', 'dispositivo', 'janela')),
    texto             text NOT NULL,                    -- what was embedded (without the "passage: " prefix)
    dispositivo_ids   jsonb NOT NULL,                   -- provisions the chunk carries: what lc05 scores
    n_tokens          integer NOT NULL,                 -- EMBEDDING tokens (e5, limit 512), not LLM tokens
    alterado_por      jsonb NOT NULL DEFAULT '[]',
    embedding         vector(384) NOT NULL,             -- resized by lc04_embed when the model changes
    embedding_model   text NOT NULL,
    source_sha256     text NOT NULL,
    parser_version    text NOT NULL,
    chunker_version   text NOT NULL,
    loaded_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_lc214_chunk_art ON ai.lc214_chunk (art_id, parte);
