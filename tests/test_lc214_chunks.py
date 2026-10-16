"""LC 214 chunking: small-to-big, nothing over the token limit, every live provision covered, stable ids."""
import polars as pl

from pipelines.rag.lc214 import lc03_chunks as lc
from pipelines.rag.lc214.lake import LocalLake, k_manifest


def words(text):                      # deterministic token counter: 1 word = 1 token
    return len(text.split())


def _row(did, tipo, texto, parent=None, rotulo=None, status="vigente", ordem=0):
    return {"dispositivo_id": did, "parent_id": parent, "tipo": tipo, "texto": texto,
            "rotulo": rotulo or texto.split(" ")[0], "status": status, "ordem": ordem,
            "art": did.split(":")[1].removeprefix("art-"), "titulo": "I", "titulo_nome": "DAS NORMAS GERAIS",
            "capitulo": None, "capitulo_nome": None, "secao": None, "secao_nome": None, "pagina": 1,
            "alterado_por": [], "altera_outra_lei": False, "source_sha256": "x" * 64, "parser_version": "1"}


A = "lc214-2025:art-3"


def _article(n_par=3, par_words=30):
    rows = [_row(A, "art", "Art. 3º Para fins desta Lei consideram-se:", rotulo="Art. 3º", ordem=1),
            _row(f"{A}:inc-I", "inc", "I - operações com bens;", parent=A, rotulo="I", ordem=2)]
    for p in range(1, n_par + 1):
        rows.append(_row(f"{A}:par-{p}", "par", f"§ {p}º " + "texto " * par_words, parent=A, rotulo=f"§ {p}º",
                         ordem=2 + p))
    return rows


def test_short_article_is_one_chunk_with_its_header():
    chunks = lc.chunk_article(_article(n_par=1, par_words=5), words, max_tokens=500)
    assert len(chunks) == 1 and chunks[0]["kind"] == "artigo"
    assert chunks[0]["text"].startswith("LC 214/2025 | Título I - DAS NORMAS GERAIS | Art. 3º")


def test_long_article_is_split_and_every_piece_fits_and_knows_its_caput():
    rows = _article(n_par=4, par_words=40)
    chunks = lc.chunk_article(rows, words, max_tokens=80)
    assert len(chunks) > 1
    assert all(words(c["text"]) <= 80 for c in chunks)
    assert all("Art. 3º Para fins" in c["text"] for c in chunks)        # caput as context in every piece
    assert {i for c in chunks for i in c["ids"]} == {r["dispositivo_id"] for r in rows}   # nothing lost


def test_single_huge_provision_falls_back_to_overlapping_windows():
    rows = [_row(A, "art", "Art. 3º " + "palavra " * 300, rotulo="Art. 3º", ordem=1)]
    chunks = lc.chunk_article(rows, words, max_tokens=100)
    assert {c["kind"] for c in chunks} == {"janela"}
    assert all(words(c["text"]) <= 100 for c in chunks)


def test_chunk_ids_are_deterministic_and_track_the_text():
    assert lc.chunk_id("a:c1", "texto") == lc.chunk_id("a:c1", "texto")
    assert lc.chunk_id("a:c1", "texto") != lc.chunk_id("a:c1", "texto novo")
    assert lc.chunk_id("a:c1", "texto").startswith("a:c1:")


def test_revoked_and_vetoed_are_not_embedded_but_the_article_is_kept():
    rows = _article(n_par=1, par_words=5) + [
        _row(f"{A}:par-2", "par", "§ 2º", parent=A, rotulo="§ 2º", status="revogado", ordem=9)]
    chunks, artigos, report = lc.build(pl.DataFrame(rows), words, "test", max_tokens=500)
    assert f"{A}:par-2" not in {i for ids in chunks["dispositivo_ids"].to_list() for i in ids}
    assert artigos.height == 1 and report["excluded_not_live"] == {"revogado": 1}
    assert report["live_provisions_covered"] == 1.0 and report["over_limit"] == 0


def test_estimate_is_labelled_as_such():
    count, method = lc.load_counter("estimate")
    assert method == "estimate" and count("um dois tres") > 3


SHA = "x" * 64


def _lake_with_parse(tmp_path, rows):
    lake = LocalLake(str(tmp_path / "lake"))
    lake.put_json(k_manifest(), {"current": SHA})
    lake.put_parquet(lc.outputs(SHA)["dispositivos"], pl.DataFrame(rows))
    return lake


def test_run_skips_when_up_to_date(tmp_path):
    lake = _lake_with_parse(tmp_path, _article())
    assert lc.run(lake, method="estimate") == lc.EXIT_OK
    assert lake.get_parquet(lc.outputs(SHA)["chunks"]).height >= 1
    assert lc.run(lake, method="estimate") == lc.EXIT_SKIPPED


def test_a_reparse_under_the_same_law_version_is_chunked_again(tmp_path):
    """lc02 rewrites the same object when PARSER_VERSION is bumped: lc03 must not skip it as up to date."""
    lake = _lake_with_parse(tmp_path, _article())
    assert lc.run(lake, method="estimate") == lc.EXIT_OK
    lake.put_parquet(lc.outputs(SHA)["dispositivos"], pl.DataFrame(_article()).with_columns(parser_version=pl.lit("2")))
    assert lc.run(lake, method="estimate") == lc.EXIT_OK
    assert lake.get_json(lc.outputs(SHA)["report"])["parser_version"] == "2"
