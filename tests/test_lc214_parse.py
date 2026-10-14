"""LC 214 parser: markers + layout signals, hierarchy, notes as metadata, source anomalies as a contract."""
import json
import os

import pytest

from pipelines.rag.lc214 import lc02_parse as lp

I = " " * 12   # indentation of the first line of a provision in `pdftotext -layout`


def _parse(text):
    return lp.parse_lines(text.split("\n"))


def _ids(text):
    return [d.id for d in _parse(text)]


# --------------------------------------------------------------------------- markers
def test_hierarchy_caput_items_then_paragraphs():
    text = "\n".join([
        f"{I}Art. 3º Para fins desta Lei Complementar, consideram-se:",
        f"{I}I - operações com:",
        f"{I}a) bens;",
        f"{I}b) serviços;",
        f"{I}II - fornecimento.",
        f"{I}§ 1º Equiparam-se:",
        f"{I}I - o primeiro.",
    ])
    assert _ids(text) == [
        "lc214-2025:art-3", "lc214-2025:art-3:inc-I", "lc214-2025:art-3:inc-I:ali-a",
        "lc214-2025:art-3:inc-I:ali-b", "lc214-2025:art-3:inc-II", "lc214-2025:art-3:par-1",
        "lc214-2025:art-3:par-1:inc-I",          # a new § resets the items: this I belongs to § 1º
    ]


def test_ordinal_up_to_nine_cardinal_from_ten():
    """LC 95/1998: § 9º but § 10. - the bug that merged § 12 into § 9 in the prototype."""
    text = "\n".join([f"{I}Art. 10. Texto.", f"{I}§ 9º Nove.", f"{I}§ 10. Dez.", f"{I}Parágrafo único. X."])
    assert _ids(text) == ["lc214-2025:art-10", "lc214-2025:art-10:par-9", "lc214-2025:art-10:par-10",
                          "lc214-2025:art-10:par-unico"]


def test_inserted_article_suffix():
    assert _ids(f"{I}Art. 7º Sete.\n{I}Art. 7º-A. Inserido pela LC 227.") == ["lc214-2025:art-7", "lc214-2025:art-7-A"]


# --------------------------------------------------------------------------- layout signals
def test_wrapped_reference_at_column_zero_is_not_a_paragraph():
    text = "\n".join([
        f"{I}Art. 5º O disposto nos termos do",
        "§ 3º deste artigo aplica-se também a:",      # column 0: a wrapped reference, not § 3º
        f"{I}I - um caso.",
    ])
    disps = _parse(text)
    assert [d.id for d in disps] == ["lc214-2025:art-5", "lc214-2025:art-5:inc-I"]
    assert "§ 3º deste artigo" in " ".join(disps[0].texto)


def test_column_zero_marker_accepted_when_it_is_the_next_in_sequence():
    """Where the PDF lost the indentation (arts. 542-543): next number + previous line closed."""
    text = "\n".join([f"{I}Art. 542. Revogam-se:", f"{I}I - a lei A;", "II - a lei B.", "Art. 543. Fim."])
    stats = lp.Counter()
    assert [d.id for d in lp.parse_lines(text.split("\n"), stats)] == [
        "lc214-2025:art-542", "lc214-2025:art-542:inc-I", "lc214-2025:art-542:inc-II", "lc214-2025:art-543"]
    assert stats["col0_accepted"] == 2


def test_heading_sets_context_and_wrapped_name_does_not_leak_into_previous_article():
    text = "\n".join([
        f"{I}Art. 1º Primeiro.",
        "                               LIVRO I",
        "DO IMPOSTO SOBRE BENS E SERVIÇOS (IBS) E DA CONTRIBUIÇÃO SOCIAL SOBRE",
        "                        BENS E SERVIÇOS (CBS)",
        "                                     TÍTULO I",
        "                         DAS NORMAS GERAIS",
        f"{I}Art. 2º Segundo.",
    ])
    d1, d2 = _parse(text)
    assert d1.texto == [f"{I}Art. 1º Primeiro.".strip()]
    assert d2.ctx["LIVRO_nome"].endswith("BENS E SERVIÇOS (CBS)")
    assert d2.ctx["TÍTULO"] == "I" and d2.ctx["TÍTULO_nome"] == "DAS NORMAS GERAIS"


def test_quoted_text_of_another_law_is_content_not_structure():
    text = "\n".join([
        f"{I}Art. 496. A Lei nº 5.172 passa a vigorar com as seguintes alterações:",
        f'{I}     "Art. 9º ...............',
        f"{I}     IV - cobrar impostos sobre:",           # an item of the CTN, not of the LC 214
        f"{I}Art. 497. Outro artigo.",
    ])
    disps = _parse(text)
    assert [d.id for d in disps] == ["lc214-2025:art-496", "lc214-2025:art-497"]
    assert disps[0].in_quote and "IV - cobrar" in " ".join(disps[0].texto)


# --------------------------------------------------------------------------- notes and status
def test_notes_become_metadata_and_leave_the_text():
    d = _parse(f"{I}Art. 3º Caput.\n{I}§ 3º Incluem-se a locação. (Parágrafo acrescido pela Lei\n"
               "Complementar nº 227, de 13/1/2026)")[1]
    f = lp.finalize(d)
    assert f["texto"] == "§ 3º Incluem-se a locação."
    assert f["alterado_por"] == ["LC 227/2026"] and f["tipo_alteracao"] == ["acrescido"]
    assert f["status"] == "vigente"


@pytest.mark.parametrize("line,status", [
    ("§ 8º (Revogado pela Lei Complementar nº 227, de 13/1/2026)", "revogado"),
    ("III - (VETADO);", "vetado"),
    ("§ 5º (VETADO na Lei Complementar nº 227, de 13/1/2026)", "vetado"),   # found on the real text
])
def test_revoked_and_vetoed(line, status):
    d = _parse(f"{I}Art. 9º Caput:\n{I}I - um;\n{I}II - dois;\n{I}{line}")[-1]
    assert lp.finalize(d)["status"] == status


# --------------------------------------------------------------------------- source anomalies (contract)
REPEATED = "\n".join([f"{I}Art. 444. Caput.", f"{I}§ 4º Caso:", f"{I}I - um;", f"{I}II - dois; e", f"{I}II - tres."])


def test_label_printed_twice_keeps_the_label_and_gets_a_suffix():
    disps = _parse(REPEATED)
    assert disps[-1].id == "lc214-2025:art-444:par-4:inc-II#2" and disps[-1].rotulo == "II"
    known = {"repeated_labels": [{"id": "lc214-2025:art-444:par-4:inc-II#2"}]}
    assert lp.check_repeated_labels(disps, known) == [{"type": "repeated_label", "id": disps[-1].id}]


def test_unknown_repeated_label_fails():
    with pytest.raises(lp.ParseError) as e:
        lp.check_repeated_labels(_parse(REPEATED), {"repeated_labels": []})
    assert e.value.event == "anomaly_unknown"


DUP_ART = "\n".join([
    f"{I}Art. 260. Redutor de R$ 600,00, por mês. (“Caput” do artigo com redação dada pela Lei Complementar nº 227,",
    "de 13/1/2026)",
    f"{I}Parágrafo único. Atualizado pelo IPCA.",
    f"{I}Art. 260. Redutor de R$ 600,00.",
    f"{I}Parágrafo único. Atualizado pelo IPCA.",
])


def test_duplicate_article_keeps_the_annotated_version():
    kept, report = lp.resolve_duplicate_articles(_parse(DUP_ART), {"duplicate_articles": [
        {"art": "260", "rule": "annotated_wins"}]})
    assert [d.id for d in kept] == ["lc214-2025:art-260", "lc214-2025:art-260:par-unico"]
    assert "por mês" in lp.finalize(kept[0])["texto"]
    assert report[0]["dropped_occurrences"] == [2]


def test_unknown_duplicate_article_fails():
    with pytest.raises(lp.ParseError) as e:
        lp.resolve_duplicate_articles(_parse(DUP_ART), {})
    assert e.value.event == "anomaly_unknown"


# --------------------------------------------------------------------------- invariants and the step
def test_invariants_catch_a_gap():
    disps = _parse(f"{I}Art. 1º Um.\n{I}I - a;\n{I}III - c.")
    inv = lp.invariants(disps, body_words=6)
    assert not inv["I3_contiguous_sequences"] and not inv["passed"]


def test_body_stops_at_signatures():
    body = lp.body_of(f"preâmbulo\n{I}Art. 1º Um.\n{I}Art. 2º Dois.\n\n   Brasília, 16 de janeiro de 2025.\nFULANO")
    assert body.strip().endswith("Art. 2º Dois.")


def test_run_writes_table_and_skips_when_up_to_date(tmp_path):
    lc = tmp_path / "lc214"
    (lc / "text").mkdir(parents=True)
    (lc / "text" / "lc214_abc.txt").write_text(f"{I}Art. 1º Um:\n{I}I - a;\n{I}II - b.\n{I}Art. 2º Dois.\f",
                                               encoding="utf-8")
    (lc / "manifest.json").write_text(json.dumps({"current": "abc" * 4, "versions": [
        {"sha256": "abc" * 4, "text": "text/lc214_abc.txt"}]}), encoding="utf-8")
    reg = tmp_path / "known.yml"
    reg.write_text("{}", encoding="utf-8")
    assert lp.run(str(lc), str(reg)) == lp.EXIT_OK
    parquet, report = lp.outputs(str(lc), "abc" * 4)
    assert os.path.exists(parquet) and json.load(open(report))["invariants"]["passed"]
    assert lp.run(str(lc), str(reg)) == lp.EXIT_SKIPPED
