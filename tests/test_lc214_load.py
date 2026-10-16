"""LC 214 serving load: upsert by content hash, orphans deleted, guards before anything is written.

The database tests run inside a transaction that is always rolled back: pointing CHECKS_TEST_DSN at a real lab
database never changes its tables.
"""
import os
from pathlib import Path

import polars as pl
import pytest

from pipelines.rag.lc214 import lc04_load as ld
from pipelines.rag.lc214.lake import LocalLake, k_manifest, k_silver

DSN = os.getenv("CHECKS_TEST_DSN")
needs_db = pytest.mark.skipif(not DSN, reason="set CHECKS_TEST_DSN to run against a real Postgres")
DDL = Path(__file__).resolve().parents[1] / "infra/postgres/init/sql/06_lc214.sql"
SHA, NEW = "a" * 64, "b" * 64
P = "lc214-2025:art-"


def _disp(rows=(("1", "Art. 1º Caput."), ("1:par-1", "§ 1º Um."), ("2", "Art. 2º Dois.")), sha=SHA, pv="2"):
    out = []
    for i, (suffix, texto) in enumerate(rows, 1):
        tipo = "par" if ":par" in suffix else "art"
        out.append({"dispositivo_id": P + suffix, "parent_id": P + suffix.split(":")[0] if tipo == "par" else None,
                    "tipo": tipo, "rotulo": texto.split(" ")[0] + " " + texto.split(" ")[1], "ordem": i,
                    "art": suffix.split(":")[0], "par": "1" if tipo == "par" else None, "inc": None, "ali": None,
                    "titulo": "I", "titulo_nome": "NORMAS", "capitulo": None, "capitulo_nome": None, "secao": None,
                    "secao_nome": None, "status": "vigente", "texto": texto, "notas": [], "alterado_por": [],
                    "tipo_alteracao": [], "altera_outra_lei": False, "pagina": 1, "source_sha256": sha,
                    "parser_version": pv})
    return pl.DataFrame(out, schema_overrides={c: pl.List(pl.Utf8) for c in ("notas", "alterado_por",
                                                                             "tipo_alteracao")})


def _arts(sha=SHA):
    return pl.DataFrame({"art_id": [P + "1", P + "2"], "art": ["1", "2"], "rotulo": ["Art. 1º", "Art. 2º"],
                         "header": ["LC 214/2025 | Art. 1º", "LC 214/2025 | Art. 2º"],
                         "texto": ["Art. 1º Caput. § 1º Um.", "Art. 2º Dois."], "status": ["vigente", "vigente"],
                         "pagina": [1, 1], "n_dispositivos": [2, 1], "alterado_por": [[], []],
                         "altera_outra_lei": [False, False], "source_sha256": [sha, sha]},
                        schema_overrides={"alterado_por": pl.List(pl.Utf8)})


def _lake(tmp_path, disp, arts, sha=SHA, chunks_pv="2"):
    lake = LocalLake(str(tmp_path))
    lake.put_json(k_manifest(), {"current": sha})
    lake.put_parquet(k_silver(sha, "dispositivos.parquet"), disp)
    lake.put_parquet(k_silver(sha, "artigos.parquet"), arts)
    lake.put_json(k_silver(sha, "parse_report.json"), {"source_sha256": sha, "parser_version": "2"})
    lake.put_json(k_silver(sha, "chunks_report.json"), {"source_sha256": sha, "parser_version": chunks_pv})
    return lake


# --------------------------------------------------------------------------- no database
def test_row_hash_tracks_content_not_the_version_stamp():
    a = ld.prepare_dispositivos(_disp(), SHA, "2")
    b = ld.prepare_dispositivos(_disp(sha=NEW), NEW, "3")              # new law version, same text
    c = ld.prepare_dispositivos(_disp(rows=(("1", "Art. 1º Caput alterado."),)), SHA, "2")
    assert a["row_hash"].to_list() == b["row_hash"].to_list()           # unchanged rows are restamped, not rewritten
    assert c["row_hash"][0] != a["row_hash"][0]
    assert a["art_id"].to_list() == [P + "1", P + "1", P + "2"]
    assert a["notas"][0] == "[]"                                         # lists travel as JSON for the jsonb columns
    moved = ld.prepare_dispositivos(_disp().with_columns(pl.col("ordem") + 10, pl.col("pagina") + 1), SHA, "2")
    assert moved["row_hash"].to_list() == a["row_hash"].to_list()     # an insertion upstream is not an amendment


def test_silver_from_two_parser_versions_is_refused(tmp_path):
    lake = _lake(tmp_path, _disp(), _arts(), chunks_pv="1")              # lc02 re-parsed, lc03 not yet
    with pytest.raises(ld.LoadError) as e:
        ld.read_silver(lake)
    assert e.value.event == "silver_inconsistent"


def test_consistent_silver_is_read(tmp_path):
    sha, pv, disp, arts = ld.read_silver(_lake(tmp_path, _disp(), _arts()))
    assert (sha, pv, disp.height, arts.height) == (SHA, "2", 3, 2)


# --------------------------------------------------------------------------- real Postgres, always rolled back
@pytest.fixture
def cur():
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(DSN)
    try:
        c = conn.cursor()
        c.execute("CREATE SCHEMA IF NOT EXISTS ai")
        c.execute(DDL.read_text(encoding="utf-8"))
        c.execute("DELETE FROM ai.lc214_dispositivo; DELETE FROM ai.lc214_artigo")   # inside the transaction only
        yield c
    finally:
        conn.rollback()
        conn.close()


def _sync(cur, disp, sha=SHA, **kw):
    return ld.sync_table(cur, "lc214_dispositivo", ld.prepare_dispositivos(disp, sha, "2"), **kw)


@needs_db
def test_first_load_inserts_and_a_reload_changes_nothing(cur):
    assert _sync(cur, _disp()) == {"rows": 3, "inserted": 3, "updated": 0, "deleted": 0, "restamped": 0}
    cur.execute("DROP TABLE stage_lc214_dispositivo")                   # ON COMMIT DROP: same transaction here
    assert _sync(cur, _disp()) == {"rows": 3, "inserted": 0, "updated": 0, "deleted": 0, "restamped": 0}


@needs_db
def test_an_amendment_is_measured_row_by_row(cur):
    _sync(cur, _disp())
    cur.execute("DROP TABLE stage_lc214_dispositivo")
    amended = _disp(rows=(("1", "Art. 1º Caput alterado."), ("2", "Art. 2º Dois."), ("3", "Art. 3º Novo.")), sha=NEW)
    r = _sync(cur, amended, sha=NEW, max_delete_pct=50)
    # art-1 rewritten, art-3 new, art-1:par-1 left the text; art-2 unchanged (it only moved up and got the new
    # version stamp): restamped, not updated
    assert r == {"rows": 3, "inserted": 1, "updated": 1, "deleted": 1, "restamped": 1}
    cur.execute("SELECT count(*) FROM ai.lc214_dispositivo WHERE source_sha256 <> %s", (NEW,))
    assert cur.fetchone()[0] == 0


@needs_db
def test_a_mass_delete_is_blocked_before_anything_is_written(cur):
    _sync(cur, _disp())
    cur.execute("DROP TABLE stage_lc214_dispositivo")
    with pytest.raises(ld.LoadError) as e:
        _sync(cur, _disp(rows=(("1", "Art. 1º Caput."),)))              # 2 of 3 rows would leave: 67% > 5%
    assert e.value.event == "mass_delete_blocked"
