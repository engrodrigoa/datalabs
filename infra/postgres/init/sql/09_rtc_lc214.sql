-- Bridge between the official cClassTrib table and the LC 214 (ADR 0021): the legal reference of each
-- classification (ref_rtc.fundamentacao_legal) resolved into provisions of the law version served
-- (ai.lc214_dispositivo) by pipelines/rtc/rtc03_lc214.py, with the LC 214 router's parser. Rebuilt whole when the
-- reference, the law or the step's code version changes (rtc_sha, law_sha256, bridge_version).
--
-- Grain: one row per citation of a reference ("Art. 146, § 1º, I e II" -> two rows). A reference with no citation
-- ("LC 214/2025" / "Regra Geral") is one row with outcome 'unparsed'.

CREATE TABLE IF NOT EXISTS ai.rtc_lc214_fundamento (
    fdlg_id          integer     NOT NULL,              -- ref_rtc.fundamentacao_legal.fdlg_id
    referencia       text,                              -- fdlg_texto_curto as published: "Art. 412, I"
    ordem            integer     NOT NULL,              -- citation number inside the reference (1..n)
    citacao          text,                              -- the citation as read: "art. 412, inciso I"
    outcome          text        NOT NULL,              -- domain: constraint below (re-applied on every bootstrap)
    dispositivo_id   text,                              -- -> ai.lc214_dispositivo (resolved only)
    candidatos       jsonb,                             -- ambiguous: every provision the citation matches
    status_lei       text,                              -- vigente | revogado | vetado (of the resolved provision)
    texto_check      text        CHECK (texto_check IN ('match', 'near', 'diverges', 'no_transcription')),
    cobertura_texto  numeric(5,4),                      -- share of the provision's words in the Receita's transcription
    rtc_sha          text        NOT NULL,              -- ctrl.rtc_carga.sha of the reference used
    law_sha256       text        NOT NULL,              -- ai.lc214_dispositivo.source_sha256 of the law used
    bridge_version   text        NOT NULL,              -- rtc03_lc214.BRIDGE_VERSION
    computed_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (fdlg_id, ordem)
);
-- ADR 0021 amendment 1 adds 'resolved_by_text'. A named constraint, dropped and re-created, so a volume created with
-- the first version (its inline, auto-named rtc_lc214_fundamento_outcome_check) is migrated in place.
ALTER TABLE ai.rtc_lc214_fundamento DROP CONSTRAINT IF EXISTS rtc_lc214_fundamento_outcome_check;
ALTER TABLE ai.rtc_lc214_fundamento DROP CONSTRAINT IF EXISTS ck_rtc_lc214_fundamento_outcome;
ALTER TABLE ai.rtc_lc214_fundamento ADD CONSTRAINT ck_rtc_lc214_fundamento_outcome
    CHECK (outcome IN ('resolved', 'resolved_by_text', 'ambiguous', 'not_found', 'foreign', 'unparsed'));
CREATE INDEX IF NOT EXISTS ix_rtc_lc214_fundamento_disp ON ai.rtc_lc214_fundamento (dispositivo_id);

COMMENT ON TABLE ai.rtc_lc214_fundamento IS
    'Official legal reference of each cClassTrib resolved into LC 214 provisions (ADR 0021, rtc03_lc214).';
