-- Control + raw landing tables (moved from dag_setup_lab so there is one source of truth)
CREATE TABLE IF NOT EXISTS ctrl.anp_metadata_mensal (
    cat           varchar(20)  NOT NULL,
    "ref"         varchar(6)   NOT NULL,
    "month"       int4         NOT NULL,
    "year"        int4         NOT NULL,
    file_name     varchar(255) NOT NULL,
    url_source    text         NOT NULL,
    download_date timestamp    DEFAULT CURRENT_TIMESTAMP NULL,
    link_name     varchar(30)  NULL,
    CONSTRAINT pk_control_anp PRIMARY KEY (cat, ref)
);

CREATE TABLE IF NOT EXISTS ctrl.anp_metadata_semanal (
    data_ref     date      NULL,
    data_dag_run timestamp NULL,
    status       varchar   NULL
);

CREATE TABLE IF NOT EXISTS bronze.anp_landing_mensal (
    regiao_sigla text, estado_sigla text, municipio text, revenda text, cnpj_da_revenda text,
    nome_da_rua text, numero_rua text, complemento text, bairro text, cep text, produto text,
    data_da_coleta text, valor_de_venda text, valor_de_compra text, unidade_de_medida text,
    bandeira text, arquivo text, ingestion_timestamp timestamp
);

CREATE TABLE IF NOT EXISTS bronze.anp_landing_semanal (
    regiao_sigla text, estado_sigla text, municipio text, revenda text, cnpj_da_revenda text,
    nome_da_rua text, numero_rua text, complemento text, bairro text, cep text, produto text,
    data_da_coleta text, valor_de_venda text, valor_de_compra text, unidade_de_medida text,
    bandeira text, arquivo text, ingestion_timestamp timestamp
);
CREATE INDEX IF NOT EXISTS ix_anp_semanal_arquivo ON bronze.anp_landing_semanal (arquivo);
CREATE INDEX IF NOT EXISTS ix_anp_mensal_arquivo  ON bronze.anp_landing_mensal (arquivo);
