"""LC 214 router: citations in both orders, lists, other laws, ambiguity, and the structured SQL answer."""
import pytest

from pipelines.rag.lc214.router import Index, parse_citations, render_structure, route, self_references

P = "lc214-2025:"
IDS = [P + i for i in (
    "art-6", "art-6:inc-I", "art-6:inc-II", "art-6:par-2", "art-6:par-2:inc-I", "art-6:par-2:inc-II",
    "art-7-A", "art-12", "art-12:par-1", "art-12:par-1:inc-III", "art-12:par-2", "art-12:par-2:inc-III",
    "art-13", "art-13:par-unico", "art-14", "art-14:inc-I", "art-14:par-2", "art-26:par-5-A", "art-47", "art-48",
    "art-57", "art-57:inc-I", "art-57:inc-I:ali-a", "art-57:par-3", "art-57:par-3:inc-IV",
    "art-57:par-3:inc-IV:ali-a", "art-57:par-4", "art-444:par-4:inc-II", "art-444:par-4:inc-II#2")]
INDEX = Index(IDS)


def _route(q):
    p = route(q, INDEX)
    return p.route, p.anchors


@pytest.mark.parametrize("question,anchor", [
    ("art. 12, § 2º, III", "art-12:par-2:inc-III"),                       # chat order
    ("art 12 §2 III", "art-12:par-2:inc-III"),
    ("art. 12, par. 2, inc. 3", "art-12:par-2:inc-III"),                  # arabic inciso -> roman
    ("inciso III do § 2º do art. 12", "art-12:par-2:inc-III"),            # legal order
    ("LC 214 art 57 §3 IV a", "art-57:par-3:inc-IV:ali-a"),
    ('alínea "a" do inciso IV do § 3º do art. 57', "art-57:par-3:inc-IV:ali-a"),
    ("o que diz o art. 7º-A?", "art-7-A"),
    ("art 7-A", "art-7-A"),
    ("parágrafo único do art. 13", "art-13:par-unico"),
    ("o § 5º-A do art. 26", "art-26:par-5-A"),
    ("inciso II do caput do art. 6º", "art-6:inc-II"),
    ("art. 6º, II", "art-6:inc-II"),                                      # drafting convention: inciso of the caput
])
def test_a_citation_resolves_to_one_provision(question, anchor):
    assert _route(question) == ("sql", [P + anchor])


def test_lists_share_the_article_in_both_orders():
    assert _route("inciso I do caput e o § 2º do art. 14") == ("sql", [P + "art-14:inc-I", P + "art-14:par-2"])
    assert _route("os arts. 47 e 48 tratam de crédito?") == ("combined", [P + "art-47", P + "art-48"])
    assert [c.label() for c in parse_citations("as alíneas do inciso I do art. 350")] == ["art. 350, inciso I"]


@pytest.mark.parametrize("question", [
    "Quais requisitos do art. 14 do Código Tributário Nacional?",
    "art. 243, § 1º, da Lei nº 6.404",
    "nos termos do art. 159, incisos I e II, da Constituição Federal",
    "art. 155, II ou do imposto previsto no art. 156, III, ambos da Constituição Federal;",
    "os arts. 31-A a 31-E da Lei nº 4.591",
])
def test_another_law_is_never_an_anchor(question):
    p = route(question, INDEX)
    assert p.route == "vector" and not p.anchors and p.foreign


def test_the_lc_214_named_explicitly_is_still_ours():
    assert _route("o art. 6º da LC 214 trata de brindes?") == ("combined", [P + "art-6"])


def test_ambiguous_and_missing_citations_ask_instead_of_guessing():
    p = route("o que diz a alínea a do art. 57?", INDEX)
    assert p.route == "clarify" and p.intent == "ambiguous"
    assert sorted(p.options["art. 57, alínea a"]) == [P + "art-57:inc-I:ali-a", P + "art-57:par-3:inc-IV:ali-a"]
    p = route("inciso III do art. 12", INDEX)                              # no inciso III in the caput: 2 §§
    assert p.route == "clarify" and len(p.options["art. 12, inciso III"]) == 2
    p = route("art. 444, § 4º, II", INDEX)                                 # printed twice in the source
    assert p.route == "clarify" and len(p.options["art. 444, § 4, inciso II"]) == 2
    p = route("o que diz o art. 600?", INDEX)
    assert (p.route, p.intent, p.missing) == ("clarify", "not_found", ["art. 600"])


@pytest.mark.parametrize("question,route_,intent", [
    ("O que diz o art. 13?", "sql", "text"),
    ("Me mostra o art. 12, § 2º, inciso III.", "sql", "text"),
    ("art. 13", "sql", "text"),                                            # nothing but the citation
    ("o art. 57, § 4º ainda vale?", "sql", "status"),
    ("O art. 6º ainda está em vigor?", "sql", "status"),
    ("Qual era a redação do § 4º do art. 57 antes de ser revogado?", "sql", "history"),
    ("O art. 6º se aplica à transferência entre filiais?", "combined", "question"),
    ("vendi pra filial, incide IBS?", "vector", "open"),
])
def test_intent_decides_between_lookup_and_retrieval(question, route_, intent):
    p = route(question, INDEX)
    assert (p.route, p.intent) == (route_, intent)


def test_the_structure_is_indented_by_level_and_shows_revocations():
    rows = [
        {"dispositivo_id": P + "art-57", "tipo": "art", "rotulo": "Art. 57", "status": "vigente",
         "texto": "Art. 57. Consideram-se de uso pessoal:", "notas": []},
        {"dispositivo_id": P + "art-57:inc-I", "tipo": "inc", "rotulo": "I", "status": "vigente",
         "texto": "I - os seguintes:", "notas": []},
        {"dispositivo_id": P + "art-57:inc-I:ali-a", "tipo": "ali", "rotulo": "a", "status": "vigente",
         "texto": "a) joias;", "notas": []},
        {"dispositivo_id": P + "art-57:par-4", "tipo": "par", "rotulo": "§ 4º", "status": "revogado",
         "texto": "§ 4º", "notas": ["Revogado pela Lei Complementar nº 227, de 13/1/2026"]},
    ]
    assert render_structure(rows).splitlines() == [
        "Art. 57. Consideram-se de uso pessoal:",
        "  I - os seguintes:",
        "    a) joias;",
        "  § 4º (Revogado pela Lei Complementar nº 227, de 13/1/2026)",       # never an empty label
    ]


def test_a_provision_longer_than_a_page_is_paged_not_cut():
    long = {"dispositivo_id": P + "art-517", "tipo": "art", "rotulo": "Art. 517", "status": "vigente",
            "texto": " ".join(f"w{i}" for i in range(25)), "notas": []}
    lines = render_structure([long], page_words=10).splitlines()
    assert [ln.split("]")[0] for ln in lines] == ["[parte 1 de 3", "[parte 2 de 3", "[parte 3 de 3"]
    assert lines[-1].endswith("w24")


def test_the_law_citing_itself_is_a_test_corpus():
    rows = [
        {"dispositivo_id": P + "art-1", "tipo": "art", "altera_outra_lei": False,
         "texto": "Art. 1º Nos termos do inciso II do caput do art. 6º e do art. 900 desta Lei."},
        {"dispositivo_id": P + "art-2", "tipo": "art", "altera_outra_lei": False,
         "texto": "Art. 2º Observado o art. 14 do Código Tributário Nacional."},
        {"dispositivo_id": P + "art-3", "tipo": "art", "altera_outra_lei": True,               # quoted other law
         "texto": "Art. 3º A Lei nº 123 passa a vigorar: art. 999."},
        {"dispositivo_id": P + "art-540", "tipo": "art", "altera_outra_lei": False,
         "texto": "Art. 540. Ficam revogados: o art. 777."},
    ]
    r = self_references(rows, INDEX)
    # its own label is not a citation; art. 900 is a miss we must see; the CTN and the revoking article are out
    assert (r["resolved"], r["not_found"], r["foreign"], r["own"]) == (1, 1, 1, 2)
    assert r["misses"] == [{"in": P + "art-1", "citation": "art. 900", "outcome": "not_found"}]
    assert r["revoking_articles"] == ["art-540"]
