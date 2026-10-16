"""LC 214 golden set: the labels are a data contract against the current version of the law."""
import copy

import polars as pl
import yaml

from pipelines.rag.lc214 import golden as g
from pipelines.rag.lc214.lake import LocalLake, k_manifest, k_silver

SHA = "abcdef012345" + "0" * 52
P = "lc214-2025:art-"


def _dispositivos():
    rows = [("1", "vigente", [], "Art. 1º Um, nos termos do § 1º deste artigo."), ("1:par-1", "vigente", [], "§ 1º Um."),
            ("2", "vigente", ["LC 227/2026"], "Art. 2º Dois, observado o § 1º do art. 1º."),
            ("3:par-4", "revogado", ["LC 227/2026"], "§ 4º"), ("4", "vigente", [], "Art. 4º Quatro."),
            ("5", "vigente", [], "Art. 5º Cinco.")]
    return pl.DataFrame({"dispositivo_id": [P + r[0] for r in rows], "status": [r[1] for r in rows],
                         "alterado_por": [r[2] for r in rows],
                         "tipo": ["par" if ":par" in r[0] else "art" for r in rows], "texto": [r[3] for r in rows],
                         "altera_outra_lei": [False] * len(rows)},
                        schema_overrides={"alterado_por": pl.List(pl.Utf8)})


def _chunks():
    # art-1 and its § in one chunk, art-2, art-4 and art-5 each in their own; the revoked § is in none
    return pl.DataFrame({"chunk_id": ["c1", "c2", "c4", "c5"],
                         "dispositivo_ids": [[P + "1", P + "1:par-1"], [P + "2"], [P + "4"], [P + "5"]]})


def _case(cid="content-01", ctype="content", route="vector", behavior="answer", expected=(P + "1",),
          q="pergunta do fiscal sem citação?", **kw):
    return {"id": cid, "type": ctype, "route": route, "behavior": behavior,
            "questions": {"fiscal": q}, "expected": list(expected), "mode": "all", **kw}


def _golden(*cases, k=5):
    return {"version": 1, "law_version": SHA[:12], "k": k, "cases": list(cases)}


def _rules(report):
    return [(v["case"], v["rule"]) for v in report["violations"]]


def test_the_committed_golden_set_is_structurally_valid():
    golden = g.load_golden()
    assert g.validate_schema(golden) == []
    assert len(golden["cases"]) >= 30


def test_a_valid_set_passes_and_reports_its_shape():
    report = g.check(_golden(_case(expected=(P + "1", P + "1:par-1")),
                             _case("status-01", "status", "sql", "report_status", (P + "3:par-4",),
                                   q="O § 4º do art. 3 ainda está vigente?", expected_status="revogado")),
                     _dispositivos(), _chunks(), SHA)
    assert report["passed"], report["violations"]
    assert report["by_route"] == {"vector": 1, "sql": 1}
    assert report["routing"] == {"phrasings": 2, "ok": 2, "accuracy": 1.0}
    assert report["self_refs"]["own"] == 1 and report["self_refs"]["rate"] == 1.0      # "§ 1º do art. 1º"
    assert report["per_case"]["content-01"]["chunks_needed"] == 1      # both ids live in the same chunk


def test_a_label_that_does_not_exist_fails():
    report = g.check(_golden(_case(expected=(P + "1:par-9",))), _dispositivos(), _chunks(), SHA)
    assert _rules(report) == [("content-01", "G3_exists")]


def test_a_revoked_provision_labelled_as_live_fails():
    report = g.check(_golden(_case(expected=(P + "3:par-4",))), _dispositivos(), _chunks(), SHA)
    assert ("content-01", "G4_status") in _rules(report)


def test_an_amended_case_needs_an_amendment_note():
    ok = g.check(_golden(_case("amended-01", "amended", expected=(P + "2",))), _dispositivos(), _chunks(), SHA)
    bad = g.check(_golden(_case("amended-01", "amended", expected=(P + "4",))), _dispositivos(), _chunks(), SHA)
    assert ok["passed"] and _rules(bad) == [("amended-01", "G5_amended")]


def test_a_vector_case_must_be_retrievable_within_k_chunks():
    report = g.check(_golden(_case(expected=(P + "1", P + "2", P + "4")), k=2), _dispositivos(), _chunks(), SHA)
    assert _rules(report) == [("content-01", "G6_retrievable")]           # 3 chunks needed, k=2


def test_a_provision_split_into_windows_needs_one_slot_not_one_per_window():
    # art. 517 (23k characters) becomes word windows that all carry the same id: any window retrieves it
    windows = pl.DataFrame({"chunk_id": ["w1", "w2", "w3", "c1"],
                            "dispositivo_ids": [[P + "5"], [P + "5"], [P + "5"], [P + "1", P + "1:par-1"]]})
    index = g.chunk_index(windows)
    assert index[P + "5"] == {"w1", "w2", "w3"}
    assert g.chunks_needed([P + "5"], index) == 1
    assert g.chunks_needed([P + "5", P + "1", P + "1:par-1"], index) == 2
    report = g.check(_golden(_case(expected=(P + "5", P + "1")), k=2), _dispositivos(), windows, SHA)
    assert report["passed"] and report["per_case"]["content-01"]["chunks_needed"] == 2


def test_the_router_decision_is_part_of_the_contract():
    wrong_route = _case("citation-01", "citation", "sql", expected=(P + "2",), q="incide IBS na importação?")
    wrong_anchor = _case("citation-02", "citation", "sql", expected=(P + "2",), q="O que diz o art. 4º?")
    combined = _case("combined-01", "combined", "combined", expected=(P + "1:par-1",), anchors=[P + "1"],
                     q="O art. 1º se aplica às exportações de serviços?")
    ambiguous = _case("ambiguous-01", "ambiguous", "clarify", "ask_path", expected=(P + "1:par-1",),
                      q="o que diz o § 1º do art. 1º?")                       # resolves: not ambiguous at all
    rules = _rules(g.check(_golden(wrong_route, wrong_anchor, combined, ambiguous), _dispositivos(), _chunks(), SHA))
    assert ("citation-01", "R1_route") in rules and ("citation-02", "R2_anchors") in rules
    assert ("ambiguous-01", "R1_route") in rules and not [r for r in rules if r[0] == "combined-01"]


def test_labels_from_another_law_version_fail_the_whole_set():
    report = g.check(_golden(_case()), _dispositivos(), _chunks(), "f" * 64)
    assert ("*", "G2_version") in _rules(report)


def test_a_known_issue_is_reported_not_fatal_and_strict_once_fixed():
    broken = _case(expected=(P + "3:par-4",), known_issue="parser marks it revoked")
    report = g.check(_golden(broken), _dispositivos(), _chunks(), SHA)
    assert report["passed"] and report["known_issues"]

    fixed = _case(expected=(P + "1",), known_issue="was broken")
    report = g.check(_golden(fixed), _dispositivos(), _chunks(), SHA)
    assert _rules(report) == [("content-01", "known_issue_resolved")]


def test_schema_rejects_inconsistent_cases():
    no_answer_with_ids = _case("no_answer-01", "no_answer", "vector", "abstain")
    wrong_route = _case("citation-01", "citation", "vector")
    external_without_source = _case("external-01", "external_ref", behavior="cite_external", scope="now")
    rules = {(v["case"], v["rule"]) for v in g.validate_schema(
        _golden(no_answer_with_ids, wrong_route, external_without_source, copy.deepcopy(wrong_route)))}
    assert {("no_answer-01", "G1_expected"), ("citation-01", "G1_route"), ("citation-01", "G1_duplicate_id"),
            ("external-01", "G1_external_source")} <= rules


def test_run_reads_the_current_version_from_the_lake(tmp_path):
    lake = LocalLake(str(tmp_path))
    lake.put_json(k_manifest(), {"current": SHA, "versions": []})
    lake.put_parquet(k_silver(SHA, "dispositivos.parquet"), _dispositivos())
    lake.put_parquet(k_silver(SHA, "chunks.parquet"), _chunks())
    path = tmp_path / "golden.yml"
    path.write_text(yaml.safe_dump(_golden(_case())), encoding="utf-8")
    assert g.run(lake, path) == g.EXIT_OK
    path.write_text(yaml.safe_dump(_golden(_case(expected=(P + "9",)))), encoding="utf-8")
    assert g.run(lake, path) == g.EXIT_FAILED
