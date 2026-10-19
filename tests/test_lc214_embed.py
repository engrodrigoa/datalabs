"""LC 214 vector store sync (lc04_embed): embed only new text, restamp without re-embedding, refuse to shrink.

The database tests run against a real Postgres + pgvector (CHECKS_TEST_DSN) inside a transaction that is always
rolled back; `run()` commits through a savepoint proxy, so pointing at a lab database never changes it.
"""
import hashlib
import os
from pathlib import Path

import polars as pl
import pytest

from pipelines.commons.ai.context_sync import plan_sync
from pipelines.rag.lc214 import lc04_embed as em
from pipelines.rag.lc214.lake import LocalLake, k_manifest, k_silver

DSN = os.getenv("CHECKS_TEST_DSN")
needs_db = pytest.mark.skipif(not DSN, reason="set CHECKS_TEST_DSN to run against a real Postgres + pgvector")
DDL = Path(__file__).resolve().parents[1] / "infra/postgres/init/sql/06_lc214.sql"
SHA, NEW = "a" * 64, "b" * 64
MODEL = "fake/e5"
P = "lc214-2025:art-"


def _vec(text: str, dim: int) -> list[float]:
    h = hashlib.sha256(text.encode()).digest()
    return [(h[i % len(h)] - 127.5) / 127.5 for i in range(dim)]


class FakeEncoder:
    """Counts what it embeds: the point of the sync is that this number stays small."""

    def __init__(self, model=MODEL, dim=384):
        self.calls, self.model, self.dim = [], model, dim

    def __call__(self):
        return em.Encoder(self.model, self.dim, self.encode)

    def encode(self, texts):
        self.calls.extend(texts)
        return [_vec(t, self.dim) for t in texts]


def _chunks(texts=("Art. 1º Caput.", "Art. 2º Dois.", "Art. 3º Três."), sha=SHA, ids_suffix=""):
    rows = []
    for n, t in enumerate(texts, 1):
        art = P + str(n)
        rows.append({"chunk_id": f"{art}:c1:{hashlib.md5(t.encode()).hexdigest()[:16]}", "art_id": art, "parte": 1,
                     "kind": "artigo", "texto": t, "dispositivo_ids": [art + ids_suffix], "n_tokens": 10,
                     "token_method": "tokenizer", "alterado_por": [], "source_sha256": sha, "chunker_version": "1"})
    return pl.DataFrame(rows, schema_overrides={"dispositivo_ids": pl.List(pl.Utf8), "alterado_por": pl.List(pl.Utf8)})


def _lake(tmp_path, chunks, sha=SHA, method="tokenizer", parser="2"):
    lake = LocalLake(str(tmp_path))
    lake.put_json(k_manifest(), {"current": sha})
    lake.put_parquet(k_silver(sha, "chunks.parquet"), chunks)
    lake.put_json(k_silver(sha, "chunks_report.json"), {"source_sha256": sha, "token_method": method,
                                                         "max_tokens": 512, "chunks": chunks.height,
                                                         "parser_version": parser})
    return lake


# --------------------------------------------------------------------------- no database
def test_estimated_chunks_are_never_embedded(tmp_path):
    with pytest.raises(em.EmbedError) as exc:
        em.read_chunks(_lake(tmp_path, _chunks(), method="estimate"))
    assert exc.value.event == "chunks_estimated"


def test_chunks_that_disagree_with_their_report_are_refused(tmp_path):
    lake = _lake(tmp_path, _chunks())
    lake.put_parquet(k_silver(SHA, "chunks.parquet"), _chunks().head(2))       # lc03 killed between data and report
    with pytest.raises(em.EmbedError) as exc:
        em.read_chunks(lake)
    assert exc.value.event == "silver_inconsistent"


def test_read_stamps_the_parser_version_of_the_report(tmp_path):
    sha, df = em.read_chunks(_lake(tmp_path, _chunks(), parser="3"))
    assert sha == SHA and df["parser_version"].unique().to_list() == ["3"]


def test_churn_is_allowed_shrink_is_blocked():
    existing = {f"old{i}" for i in range(100)}
    churn = plan_sync({f"new{i}" for i in range(100)}, existing, model=MODEL, stored_models={MODEL})
    em.guard_shrink(churn, len(existing))                                        # every id replaced, none lost
    shrink = plan_sync({f"old{i}" for i in range(90)}, existing, model=MODEL, stored_models={MODEL})
    assert em.shrink_pct(shrink, len(existing)) == 10.0
    with pytest.raises(em.EmbedError) as exc:
        em.guard_shrink(shrink, len(existing))
    assert exc.value.event == "store_shrink"
    em.guard_shrink(shrink, len(existing), allow=True)


def test_embed_writes_pgvector_literals_and_checks_the_dimension():
    frame = em.prepare(_chunks().with_columns(pl.lit("2").alias("parser_version")))
    out = em.embed(frame, FakeEncoder()())
    assert out["embedding"][0].startswith("[") and out["embedding"][0].count(",") == 383
    with pytest.raises(em.EmbedError):
        em.embed(frame, em.Encoder(MODEL, 8, lambda t: [[0.0] * 4 for _ in t]))


# --------------------------------------------------------------------------- real Postgres, always rolled back
@pytest.fixture
def conn():
    psycopg2 = pytest.importorskip("psycopg2")
    c = psycopg2.connect(DSN)
    try:
        cur = c.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS ai")
        cur.execute(DDL.read_text(encoding="utf-8"))
        cur.execute("DELETE FROM ai.lc214_chunk")                     # inside the transaction only
        yield c
    finally:
        c.rollback()
        c.close()


class Savepoint:
    """What run() sees as a connection: `with conn:` becomes a savepoint, close/rollback keep the outer transaction."""

    def __init__(self, c):
        self.c = c

    def cursor(self):
        return self.c.cursor()

    def rollback(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        self.c.cursor().execute("SAVEPOINT run")
        return self

    def __exit__(self, exc_type, *_):
        self.c.cursor().execute("ROLLBACK TO SAVEPOINT run" if exc_type else "RELEASE SAVEPOINT run")
        return False


def _run(conn, lake, enc, **kw):
    return em.run(lake, lambda: Savepoint(conn), encoder_factory=enc, model=enc.model, **kw)


def _rows(conn):
    cur = conn.cursor()
    cur.execute("SELECT chunk_id, source_sha256, parser_version, dispositivo_ids, embedding_model "
                "FROM ai.lc214_chunk ORDER BY chunk_id")
    return cur.fetchall()


@needs_db
def test_first_sync_embeds_everything_and_a_rerun_embeds_nothing(conn, tmp_path):
    lake, enc = _lake(tmp_path, _chunks()), FakeEncoder()
    assert _run(conn, lake, enc) == em.EXIT_OK
    assert len(_rows(conn)) == 3 and len(enc.calls) == 3
    assert _run(conn, lake, enc) == em.EXIT_SKIPPED                     # in sync: 99, the model is not loaded
    assert len(enc.calls) == 3


@needs_db
def test_an_amendment_reembeds_only_the_changed_chunk(conn, tmp_path):
    enc = FakeEncoder()
    _run(conn, _lake(tmp_path / "v1", _chunks()), enc)
    amended = _chunks(texts=("Art. 1º Caput.", "Art. 2º Dois, alterado.", "Art. 3º Três."), sha=NEW)
    assert _run(conn, _lake(tmp_path / "v2", amended, sha=NEW), enc) == em.EXIT_OK
    assert enc.calls[3:] == ["Art. 2º Dois, alterado."]                 # one chunk paid, two kept their vectors
    rows = _rows(conn)
    assert len(rows) == 3 and {r[1] for r in rows} == {NEW}             # the kept chunks were restamped


@needs_db
def test_a_new_parser_with_the_same_text_restamps_without_embedding(conn, tmp_path):
    enc = FakeEncoder()
    _run(conn, _lake(tmp_path / "v1", _chunks()), enc)
    renamed = _chunks(ids_suffix="#2")                                  # provision ids changed, text did not
    assert _run(conn, _lake(tmp_path / "v2", renamed, parser="3"), enc) == em.EXIT_OK
    assert len(enc.calls) == 3
    rows = _rows(conn)
    assert {r[2] for r in rows} == {"3"} and all(r[3][0].endswith("#2") for r in rows)


@needs_db
def test_a_shrinking_store_is_refused_before_the_model_loads(conn, tmp_path):
    enc = FakeEncoder()
    _run(conn, _lake(tmp_path / "v1", _chunks()), enc)
    with pytest.raises(em.EmbedError) as exc:
        _run(conn, _lake(tmp_path / "v2", _chunks().head(1)), enc)      # 2 of 3 chunks would leave
    assert exc.value.event == "store_shrink" and len(enc.calls) == 3
    assert len(_rows(conn)) == 3


@needs_db
def test_a_model_change_rebuilds_and_resizes_the_column(conn, tmp_path):
    lake = _lake(tmp_path, _chunks())
    _run(conn, lake, FakeEncoder())
    other = FakeEncoder(model="fake/other", dim=8)
    assert _run(conn, lake, other) == em.EXIT_OK
    assert len(other.calls) == 3 and {r[4] for r in _rows(conn)} == {"fake/other"}
    cur = conn.cursor()
    cur.execute("SELECT atttypmod FROM pg_attribute WHERE attrelid = 'ai.lc214_chunk'::regclass "
                "AND attname = 'embedding'")
    assert cur.fetchone()[0] == 8
