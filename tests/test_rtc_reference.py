"""RTC reference pipeline (ADR 0019): Flyway replay rules, source contract, rtc01 version watch, rtc02 helpers."""
import json
import sqlite3
from pathlib import Path

import pytest

from pipelines.rag.lc214.lake import LocalLake
from pipelines.rtc import flyway_replay as fr
from pipelines.rtc import rtc01_fetch as rtc01
from pipelines.rtc import rtc02_load as rtc02

# --------------------------------------------------------------------------- a miniature of the real scripts
BEFORE = "PRAGMA foreign_keys = ON;"
B0001 = """
PRAGMA foreign_keys = OFF;
CREATE TABLE TRIBUTO (TBTO_ID INTEGER PRIMARY KEY, TBTO_SIGLA TEXT NOT NULL);
CREATE TABLE SITUACAO_TRIBUTARIA (SITR_ID INTEGER PRIMARY KEY, SITR_CD TEXT NOT NULL);
CREATE TABLE TRIBUTO_SITUACAO_TRIBUTARIA (TRST_ID INTEGER PRIMARY KEY,
    TRST_SITR_ID INTEGER NOT NULL REFERENCES SITUACAO_TRIBUTARIA(SITR_ID),
    TRST_TBTO_ID INTEGER NOT NULL REFERENCES TRIBUTO(TBTO_ID));
CREATE TABLE CLASSIFICACAO_TRIBUTARIA (CLTR_ID INTEGER PRIMARY KEY, CLTR_CD TEXT NOT NULL,
    CLTR_SITR_ID INTEGER NOT NULL REFERENCES SITUACAO_TRIBUTARIA(SITR_ID),
    CLTR_INICIO_VIGENCIA TEXT NOT NULL, CLTR_FIM_VIGENCIA TEXT);
CREATE TABLE VERSAO_BASE_DADO (VRBD_ID INTEGER PRIMARY KEY, VRBD_VERSAO_BASE_DADO TEXT);
INSERT INTO TRIBUTO VALUES (1, 'IS'), (2, 'CBS');
INSERT INTO SITUACAO_TRIBUTARIA VALUES (1, '000'), (2, '000');
INSERT INTO TRIBUTO_SITUACAO_TRIBUTARIA VALUES (1, 1, 2), (2, 2, 1);
INSERT INTO CLASSIFICACAO_TRIBUTARIA VALUES (1, '000001', 1, '2026-01-01', NULL), (2, '000001', 2, '2027-01-01', NULL);
PRAGMA foreign_keys = ON;
"""
# Needs a transaction: the child row is inserted before its parent, legal only with deferred FK checks (as V0011).
V2_DEFERRED = """
PRAGMA defer_foreign_keys = ON;
INSERT INTO CLASSIFICACAO_TRIBUTARIA VALUES (3, '200038', 3, '2026-01-01', NULL);
INSERT INTO SITUACAO_TRIBUTARIA VALUES (3, '200');
INSERT INTO TRIBUTO_SITUACAO_TRIBUTARIA VALUES (3, 3, 2);
INSERT INTO VERSAO_BASE_DADO VALUES (2, 'V0002');
"""
# Must run OUTSIDE a transaction: a REFERENCES column with a default is only accepted with foreign_keys OFF (as V0044).
V10_FK_PRAGMA = """
PRAGMA foreign_keys = OFF;
ALTER TABLE CLASSIFICACAO_TRIBUTARIA ADD COLUMN CLTR_GRUPO INTEGER NOT NULL DEFAULT 1 REFERENCES TRIBUTO(TBTO_ID);
PRAGMA foreign_keys = ON;
INSERT INTO VERSAO_BASE_DADO VALUES (10, 'V0010');
"""
AFTER = "PRAGMA optimize;"


def scripts():
    return {"beforeMigrate.sql": BEFORE, "B0001__base.sql": B0001, "V0010__fk_pragma.sql": V10_FK_PRAGMA,
            "V0002__deferred.sql": V2_DEFERRED, "afterMigrate.sql": AFTER, "README.sql": "SELECT 1;"}


def test_ordered_scripts_numeric_order_and_callbacks():
    assert fr.ordered_scripts(scripts()) == [
        "beforeMigrate.sql", "B0001__base.sql", "V0002__deferred.sql", "V0010__fk_pragma.sql", "afterMigrate.sql"]


def test_transaction_rule_follows_flyway():
    assert fr.runs_in_transaction("V0002__deferred.sql", V2_DEFERRED)
    assert not fr.runs_in_transaction("V0010__fk_pragma.sql", V10_FK_PRAGMA)
    assert not fr.runs_in_transaction("B0001__base.sql", B0001)
    assert not fr.runs_in_transaction("beforeMigrate.sql", BEFORE)


def test_replay_applies_both_flyway_behaviours(tmp_path):
    db = tmp_path / "rtc.db"
    assert fr.replay(scripts(), db) == 5
    con = sqlite3.connect(db)
    assert con.execute("SELECT COUNT(*) FROM CLASSIFICACAO_TRIBUTARIA").fetchone()[0] == 3
    assert fr.database_version(con) == "V0010"
    assert con.execute("PRAGMA foreign_key_check").fetchall() == []


def test_replay_names_the_failing_script(tmp_path):
    bad = scripts() | {"V0003__broken.sql": "INSERT INTO NOPE VALUES (1);"}
    with pytest.raises(fr.ReplayError) as exc:
        fr.replay(bad, tmp_path / "rtc.db")
    assert exc.value.script == "V0003__broken.sql"


def test_same_code_in_two_tax_groups_is_not_an_overlap(tmp_path):
    db = tmp_path / "rtc.db"
    fr.replay(scripts(), db)
    con = sqlite3.connect(db)
    assert fr.overlapping_vigencias(con) == []          # 000001 for CBS and for IS: different groups
    con.execute("INSERT INTO CLASSIFICACAO_TRIBUTARIA VALUES (4, '000001', 1, '2026-06-01', NULL, 1)")
    assert fr.overlapping_vigencias(con) == [("IBSCBS", "000001", 1, 4)]


def test_contract_lists_every_problem(tmp_path):
    db = tmp_path / "rtc.db"
    fr.replay(scripts(), db)
    problems = fr.contract_violations(sqlite3.connect(db))
    assert "table_missing: NCM_APLICAVEL" in problems
    assert any(p.startswith("too_few_rows: CLASSIFICACAO_TRIBUTARIA 3 <") for p in problems)


def test_release_version_skips_the_parent_version():
    pom = ("<project><parent><artifactId>spring-boot-starter-parent</artifactId><version>3.5.16</version></parent>"
           "<artifactId>calculadora</artifactId><version>1.6.1</version></project>")
    assert fr.release_version(pom) == "1.6.1"


# --------------------------------------------------------------------------- rtc01: the version watch
def fake_fetch(_sha, dest, _repo):
    sql = Path(dest) / "flyway" / "sql"
    (sql / "criacao").mkdir(parents=True)
    (sql / "manutencao").mkdir()
    for name, body in scripts().items():
        folder = "manutencao" if name.startswith("V") else "criacao"
        (sql / folder / name).write_text(body, encoding="utf-8")
    (Path(dest) / "pom.xml").write_text("<project><version>9.9.9</version></project>", encoding="utf-8")
    return Path(dest)


def test_rtc01_rejects_a_release_that_breaks_the_contract(tmp_path):
    lake = LocalLake(str(tmp_path / "lake"))
    with pytest.raises(rtc01.SourceError) as exc:
        rtc01.run(lake, sha_of=lambda *_: "a" * 40, fetch=fake_fetch)
    assert exc.value.event == "source_contract_failed"
    assert not lake.exists(rtc01.k_manifest())          # nothing landed, manifest untouched


def test_rtc01_lands_then_skips_the_same_sha(tmp_path, monkeypatch):
    monkeypatch.setattr(fr, "CONTRACT_MIN_ROWS", {"CLASSIFICACAO_TRIBUTARIA": 1, "VERSAO_BASE_DADO": 1})
    lake = LocalLake(str(tmp_path / "lake"))
    sha = "b" * 40
    assert rtc01.run(lake, sha_of=lambda *_: sha, fetch=fake_fetch) == rtc01.EXIT_OK
    manifest = lake.get_json(rtc01.k_manifest())
    assert manifest["current"]["sha"] == sha and manifest["current"]["release"] == "9.9.9"
    assert manifest["current"]["database_version"] == "V0010"
    assert lake.exists(rtc01.k_db(sha))
    report = lake.get_json(rtc01.k_report(sha))
    assert report["row_counts"]["CLASSIFICACAO_TRIBUTARIA"] == 3
    # same sha again: nothing to do
    assert rtc01.run(lake, sha_of=lambda *_: sha, fetch=fake_fetch) == rtc01.EXIT_SKIPPED
    # a new sha becomes current and keeps the previous one
    assert rtc01.run(lake, sha_of=lambda *_: "c" * 40, fetch=fake_fetch) == rtc01.EXIT_OK
    manifest = lake.get_json(rtc01.k_manifest())
    assert manifest["previous"]["sha"] == sha and len(manifest["versions"]) == 2


def test_rtc01_missing_flyway_dir_is_a_layout_change(tmp_path):
    lake = LocalLake(str(tmp_path / "lake"))

    def empty_fetch(_sha, dest, _repo):
        Path(dest).mkdir(parents=True)
        return Path(dest)

    with pytest.raises(rtc01.SourceError) as exc:
        rtc01.run(lake, sha_of=lambda *_: "d" * 40, fetch=empty_fetch)
    assert exc.value.event == "source_layout_changed"


# --------------------------------------------------------------------------- rtc02: pure helpers
@pytest.mark.parametrize("decl,expected", [("INTEGER", "bigint"), ("TEXT", "text"), ("", "text"),
                                           ("NUMERIC(15,2)", "numeric"), ("REAL", "double precision")])
def test_pg_type(decl, expected):
    assert rtc02.pg_type(decl) == expected


def test_table_csv_keeps_nulls_and_leading_zeros(tmp_path):
    con = sqlite3.connect(tmp_path / "t.db")
    con.execute("CREATE TABLE T (A TEXT, B INTEGER, C TEXT)")
    con.execute("INSERT INTO T VALUES ('000001', 7, NULL), ('', NULL, 'x')")
    cols = [c for c, _ in rtc02.source_columns(con, "T")]
    assert cols == ["a", "b", "c"]
    buf, n = rtc02.table_csv(con, "T", cols)
    assert n == 2
    lines = buf.getvalue().splitlines()
    assert lines[0] == "a,b,c"
    assert lines[1] == "000001,7,"                      # NULL -> unquoted empty (COPY reads NULL)
    assert lines[2] == '"",,x'                          # empty string stays an empty string


def test_create_sql_lowercases_and_quotes():
    assert rtc02.create_sql("NCM_APLICAVEL", [("ncma_id", "bigint"), ("ncma_ncm_cd", "text")]) == \
        'CREATE TABLE ref_rtc."ncm_aplicavel" ("ncma_id" bigint, "ncma_ncm_cd" text)'


def test_export_tables_are_the_contract():
    assert set(fr.EXPORT_TABLES) == set(fr.CONTRACT_MIN_ROWS)
    assert json.dumps(fr.EXPORT_TABLES)                 # serialisable (stored in reports)


def test_rtc01_contract_bump_rebuilds_the_same_sha(tmp_path, monkeypatch):
    """A new CONTRACT_VERSION reloads the current release: same sha, no waiting for a new Receita release."""
    monkeypatch.setattr(fr, "CONTRACT_MIN_ROWS", {"CLASSIFICACAO_TRIBUTARIA": 1, "VERSAO_BASE_DADO": 1})
    lake = LocalLake(str(tmp_path / "lake"))
    sha = "e" * 40
    monkeypatch.setattr(fr, "CONTRACT_VERSION", 1)
    assert rtc01.run(lake, sha_of=lambda *_: sha, fetch=fake_fetch) == rtc01.EXIT_OK
    assert rtc01.run(lake, sha_of=lambda *_: sha, fetch=fake_fetch) == rtc01.EXIT_SKIPPED
    monkeypatch.setattr(fr, "CONTRACT_VERSION", 2)
    assert rtc01.run(lake, sha_of=lambda *_: sha, fetch=fake_fetch) == rtc01.EXIT_OK
    manifest = lake.get_json(rtc01.k_manifest())
    assert manifest["current"]["contract_version"] == 2
    assert manifest["previous"] is None                 # same sha rebuilt: not a new version of the source
    assert len(manifest["versions"]) == 1
    assert lake.get_json(rtc01.k_report(sha))["contract_version"] == 2


def test_manifest_without_contract_version_is_v1_and_rebuilds(tmp_path, monkeypatch):
    """Manifests written before contract versions existed are read as v1 (the lab's state after the first PR)."""
    monkeypatch.setattr(fr, "CONTRACT_MIN_ROWS", {"CLASSIFICACAO_TRIBUTARIA": 1, "VERSAO_BASE_DADO": 1})
    lake = LocalLake(str(tmp_path / "lake"))
    sha = "f" * 40
    lake.put_json(rtc01.k_manifest(), {"current": {"sha": sha, "db": rtc01.k_db(sha)}, "previous": None,
                                       "versions": [{"sha": sha}]})
    assert rtc01.run(lake, sha_of=lambda *_: sha, fetch=fake_fetch) == rtc01.EXIT_OK
