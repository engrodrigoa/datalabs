"""LC 214 retrieval: route first, exact vector search, one block per article under the LLM budget (option C)."""
import os
from pathlib import Path

import pytest

from pipelines.rag.lc214 import retriever as rt
from pipelines.rag.lc214.router import Index

P = "lc214-2025:art-"
WORDS = " ".join(f"palavra{i}" for i in range(200))          # ~340 LLM tokens


def _row(pid, texto, status="vigente", notas=()):
    parent = ":".join(pid.split(":")[:-1]) or None
    tipo = pid.split(":")[-1].split("-")[0]
    return {"dispositivo_id": pid, "parent_id": parent if pid.count(":") > 1 else None,
            "tipo": "art" if pid.count(":") == 1 else tipo, "rotulo": texto.split(" ")[0], "status": status,
            "texto": texto, "notas": list(notas)}


LAW = {
    P + "1": [_row(P + "1", "Art. 1º Remessa entre estabelecimentos."), _row(P + "1:par-1", "§ 1º Filial.")],
    P + "2": [_row(P + "2", "Art. 2º Caput do artigo grande.")]
    + [_row(f"{P}2:par-{n}", f"§ {n}º {WORDS} fim{n}") for n in range(1, 7)]
    + [_row(P + "2:par-7", "§ 7º (Revogado).", status="revogado", notas=["Revogado pela LC 227/2026"])],
    P + "3": [_row(P + "3", "Art. 3º Outro assunto pequeno.")],
    P + "9": [_row(P + "9", "Art. 9º Ficam alteradas as tabelas:"),
              _row(P + "9:inc-I", "I - " + " ".join(f"t{i}" for i in range(3000)))],
}


def _hit(art, n, ids, kind="grupo", texto="texto"):
    return rt.Hit(0, f"{P}{art}:c{n}:x", P + str(art), kind, texto, tuple(ids), 0.0)


# the global ranking the fake "embedding" produces, best first
RANKING = [
    _hit(1, 1, [P + "1", P + "1:par-1"], kind="artigo"),
    _hit(2, 5, [P + "2:par-5"]),
    _hit(9, 2, [P + "9:inc-I"], kind="janela", texto="LC 214/2025 | Art. 9º\nArt. 9º prefixo\nt1500 t1501 t1502"),
    _hit(2, 2, [P + "2:par-2"]),
    _hit(3, 1, [P + "3"], kind="artigo"),
    _hit(2, 1, [P + "2", P + "2:par-1"]),
    _hit(2, 3, [P + "2:par-3"]),
    _hit(2, 4, [P + "2:par-4"]),
    _hit(2, 6, [P + "2:par-6"]),
    _hit(9, 1, [P + "9:inc-I"], kind="janela", texto="LC 214/2025 | Art. 9º\nArt. 9º prefixo\nt0 t1 t2"),
]


class FakeStore:
    def __init__(self):
        self.searches = []

    def search(self, qvec, k, art_id=None):
        self.searches.append(art_id)
        hits = [h for h in RANKING if art_id is None or h.art_id == art_id][:k]
        return [rt.Hit(n, h.chunk_id, h.art_id, h.kind, h.texto, h.dispositivo_ids, n / 10)
                for n, h in enumerate(hits, 1)]

    def provisions(self, art_id):
        return LAW[art_id]

    def header(self, art_id):
        return f"LC 214/2025 | Art. {art_id.split('-')[-1]}"

    def ids(self):
        return [r["dispositivo_id"] for rows in LAW.values() for r in rows]


class Encode:
    def __init__(self):
        self.calls = []

    def __call__(self, q):
        self.calls.append(q)
        return [0.0]


def _retrieve(q, k=5):
    enc = Encode()
    return rt.retrieve(q, FakeStore(), enc, k=k), enc


# --------------------------------------------------------------------------- pure helpers
def test_budget_is_what_is_left_of_num_ctx_with_a_margin():
    assert rt.context_budget("") == int((8192 - 1024 - 500) * 0.8)
    assert rt.context_budget("uma pergunta de seis palavras aqui") < rt.context_budget("")


@pytest.mark.parametrize("pid,label", [
    (P + "5", "caput"), (P + "5:par-9", "§ 9º"), (P + "5:par-10", "§ 10"), (P + "7-A:par-5-A:inc-II", "§ 5º-A, II"),
    (P + "57:par-unico:inc-IV:ali-a", "parágrafo único, IV, a)"), (P + "6:inc-II#2", "II"),
])
def test_relative_labels_read_like_citations(pid, label):
    assert rt.rel_label(pid) == label


# --------------------------------------------------------------------------- routes without retrieval
def test_a_citation_is_answered_by_lookup_without_embedding_the_question():
    r, enc = _retrieve("o que diz o art. 1º?")
    assert r.plan.route == "sql" and not r.hits and not r.blocks and not enc.calls


def test_an_ambiguous_citation_asks_instead_of_searching():
    r, enc = _retrieve("o que diz o art. 77?")
    assert r.plan.route == "clarify" and not enc.calls


# --------------------------------------------------------------------------- vector route
def test_hits_become_one_block_per_article_in_rank_order():
    r, _ = _retrieve("remessa para filial tem incidência?")
    assert r.plan.route == "vector"
    assert [h.rank for h in r.hits] == [1, 2, 3, 4, 5]
    assert [b.art_id for b in r.blocks] == [P + "1", P + "2", P + "9", P + "3"]
    assert r.blocks[1].ranks == [2, 4]                         # two hits of art. 2: one block


def test_a_small_article_is_shown_whole():
    r, _ = _retrieve("remessa para filial tem incidência?")
    b = r.blocks[0]
    assert b.mode == "whole" and b.dispositivo_ids == [P + "1", P + "1:par-1"]


def test_a_big_article_is_shown_in_layers_under_its_ceiling(monkeypatch):
    monkeypatch.setattr(rt, "ANCHOR_MAX_TOKENS", 1500)        # the layering under test, not the production default
    r, _ = _retrieve("remessa para filial tem incidência?")
    b = r.blocks[1]
    assert b.mode == "layered" and b.tokens <= 1500
    # caput, then the article's chunks by similarity (§5, §2, §1, §3) while they fit, shown in reading order
    assert b.dispositivo_ids == [P + "2", P + "2:par-1", P + "2:par-2", P + "2:par-3", P + "2:par-5"]
    assert "[não incluídos neste contexto: § 4º; § 6º]" in b.text and b.omitted == 2
    assert "§ 7º" not in b.text.split("[não incluídos")[1]    # a revoked provision is not "left out"


def test_a_higher_ceiling_shows_the_same_article_whole(monkeypatch):
    monkeypatch.setattr(rt, "ANCHOR_MAX_TOKENS", 2600)
    r, _ = _retrieve("remessa para filial tem incidência?")
    b = r.blocks[1]
    assert b.art_id == P + "2" and b.mode == "whole" and b.omitted == 0
    assert [h.rank for h in r.hits] == [1, 2, 3, 4, 5]        # the ceiling acts after the search: same top k


def test_a_window_of_a_huge_provision_is_shown_not_the_provision():
    r, _ = _retrieve("remessa para filial tem incidência?")
    b = r.blocks[2]
    assert b.mode == "layered" and "[trecho] t1500 t1501 t1502 [trecho] t0 t1 t2" in b.text   # both windows, by rank
    assert "t2999" not in b.text


def test_the_context_never_exceeds_the_budget_and_reports_what_it_dropped():
    store = FakeStore()
    plan = rt.route("remessa para filial tem incidência?", Index(store.ids()))
    blocks, dropped = rt.assemble(store, plan, store.search([0.0], 5), [0.0], budget=150)
    assert sum(b.tokens for b in blocks) <= 150
    assert dropped                                              # hits that did not fit are named, not lost


# --------------------------------------------------------------------------- combined route
def test_the_cited_provision_comes_first_even_when_the_vectors_rank_it_last():
    r, _ = _retrieve("o art. 2º, § 6º se aplica a remessa entre filiais?")
    assert r.plan.route == "combined" and r.plan.anchors == [P + "2:par-6"]
    first = r.blocks[0]
    assert first.art_id == P + "2" and first.reason == "anchor" and P + "2:par-6" in first.dispositivo_ids
    assert first.dispositivo_ids.index(P + "2:par-6") > first.dispositivo_ids.index(P + "2")   # reading order


def test_an_anchor_is_kept_even_without_budget():
    store = FakeStore()
    plan = rt.route("o art. 2º, § 6º se aplica a remessa entre filiais?", Index(store.ids()))
    blocks, dropped = rt.assemble(store, plan, store.search([0.0], 5), [0.0], budget=50)
    assert blocks[0].reason == "anchor" and P + "2:par-6" in blocks[0].dispositivo_ids
    assert len(blocks) == 1 and dropped


# --------------------------------------------------------------------------- real Postgres + pgvector
DSN = os.getenv("CHECKS_TEST_DSN")
DDL = Path(__file__).resolve().parents[1] / "infra/postgres/init/sql/06_lc214.sql"


@pytest.fixture
def cur():
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(DSN)
    try:
        c = conn.cursor()
        c.execute("CREATE SCHEMA IF NOT EXISTS ai")
        c.execute(DDL.read_text(encoding="utf-8"))
        c.execute("DELETE FROM ai.lc214_chunk; DELETE FROM ai.lc214_dispositivo; DELETE FROM ai.lc214_artigo")
        c.execute("ALTER TABLE ai.lc214_chunk ALTER COLUMN embedding TYPE vector(3) USING NULL")   # rolled back
        for art, vec in ((1, "[1,0,0]"), (2, "[0.9,0.1,0]"), (3, "[0,0,1]")):
            pid = f"{P}{art}"
            c.execute("INSERT INTO ai.lc214_dispositivo (dispositivo_id, art_id, tipo, rotulo, ordem, art, status, "
                      "texto, altera_outra_lei, row_hash, source_sha256, parser_version) VALUES "
                      "(%s, %s, 'art', %s, %s, %s, 'vigente', %s, false, 'h', 's', '2')",
                      (pid, pid, f"Art. {art}º", art, str(art), f"Art. {art}º Texto {art}."))
            c.execute("INSERT INTO ai.lc214_artigo (art_id, art, rotulo, header, texto, status, n_dispositivos, "
                      "altera_outra_lei, row_hash, source_sha256, parser_version) VALUES "
                      "(%s, %s, %s, %s, %s, 'vigente', 1, false, 'h', 's', '2')",
                      (pid, str(art), f"Art. {art}º", f"LC 214/2025 | Art. {art}º", f"Art. {art}º Texto {art}."))
            c.execute("INSERT INTO ai.lc214_chunk (chunk_id, art_id, parte, kind, texto, dispositivo_ids, n_tokens, "
                      "embedding, embedding_model, source_sha256, parser_version, chunker_version) VALUES "
                      "(%s, %s, 1, 'artigo', %s, %s, 5, %s, 'm', 's', '2', '1')",
                      (f"{pid}:c1:x", pid, f"Art. {art}º Texto {art}.", f'["{pid}"]', vec))
        yield c
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.skipif(not DSN, reason="set CHECKS_TEST_DSN to run against a real Postgres + pgvector")
def test_pgvector_ranks_by_cosine_distance_and_filters_by_article(cur):
    store = rt.PgStore(cur)
    hits = store.search([1, 0, 0], 3)
    assert [h.art_id for h in hits] == [P + "1", P + "2", P + "3"]
    assert hits[0].distance == pytest.approx(0.0, abs=1e-6) and hits[0].dispositivo_ids == (P + "1",)
    assert [h.art_id for h in store.search([1, 0, 0], 3, art_id=P + "3")] == [P + "3"]

    r = rt.retrieve("texto sobre incidência em operações", store, lambda q: [0, 0, 1], k=2)
    assert r.plan.route == "vector" and [b.art_id for b in r.blocks] == [P + "3", P + "1"]
    assert r.context().startswith("[1] LC 214/2025 | Art. 3º\nArt. 3º Texto 3.")
