"""LC 214 retrieval evaluation (lc05): metrics per phrasing, a gate against a committed baseline."""
import os
from pathlib import Path

import pytest
import yaml

from pipelines.rag.lc214 import lc05_eval as ev
from pipelines.rag.lc214 import retriever as rt
from pipelines.rag.lc214.router import Plan

P = "lc214-2025:art-"


def _hit(rank, art, ids, d=0.1):
    return rt.Hit(rank, f"{P}{art}:c1:x", P + str(art), "grupo", "t", tuple(ids), d)


def _retrieval(hits, blocks_ids=(), route="vector"):
    r = rt.Retrieval("q", Plan(route, "open"), hits=hits)
    r.blocks = [rt.Block(art_of, "whole", "hit", [], list(ids), "t", 10)
                for art_of, ids in blocks_ids]
    return r


def _case(cid="content-01", expected=(P + "6:inc-II",), mode="all", route="vector", type_="content", scope="now"):
    return {"id": cid, "type": type_, "route": route, "mode": mode, "scope": scope, "expected": list(expected)}


# --------------------------------------------------------------------------- scoring one phrasing
def test_the_first_chunk_carrying_an_expected_provision_sets_the_rank():
    r = _retrieval([_hit(1, 72, [P + "72"]), _hit(2, 6, [P + "6", P + "6:inc-II"]), _hit(3, 6, [P + "6:inc-I"])])
    o = ev.score(_case(), "fiscal", r, k=5)
    assert o.first_rank == 2 and o.rr == 0.5 and o.solved and o.recall == 1.0
    assert o.top == [P + "72", P + "6", P + "6"] and o.top1_distance == 0.1


def test_mode_all_needs_every_provision_mode_any_needs_one():
    r = _retrieval([_hit(1, 4, [P + "4"])])
    both = (P + "4", P + "5")
    assert not ev.score(_case(expected=both), "fiscal", r, 5).solved
    assert ev.score(_case(expected=both, mode="any"), "fiscal", r, 5).solved
    assert ev.score(_case(expected=both), "fiscal", r, 5).recall == 0.5


def test_a_hit_beyond_k_does_not_count():
    r = _retrieval([_hit(n, 70 + n, [f"{P}{70 + n}"]) for n in range(1, 6)] + [_hit(6, 6, [P + "6:inc-II"])])
    o = ev.score(_case(), "fiscal", r, k=5)
    assert o.first_rank is None and o.rr == 0.0 and not o.found


def test_context_recall_sees_what_the_blocks_show_not_only_the_hits():
    # the hit is the caput chunk; the block is the whole article, so the inciso reaches the LLM anyway (small-to-big)
    r = _retrieval([_hit(1, 6, [P + "6"])], blocks_ids=[(P + "6", [P + "6", P + "6:inc-I", P + "6:inc-II"])])
    o = ev.score(_case(), "fiscal", r, k=5)
    assert not o.solved and o.context_recall == 1.0


# --------------------------------------------------------------------------- aggregation
def _outcome(case, phrasing, solved, route="vector", scope="now", top1=0.1, ctx=None, type_="content"):
    exp = [P + "1"]
    o = ev.Outcome(case, type_, route, route, phrasing, scope, "all", exp if type_ != "no_answer" else [],
                   first_rank=1 if solved else None, found=exp if solved else [], top1_distance=top1)
    o.in_context = exp if (solved if ctx is None else ctx) else []
    return o


def test_groups_measure_vector_cases_and_count_combined_only_in_the_context():
    outs = [_outcome("a", "fiscal", True), _outcome("b", "fiscal", False, ctx=True),
            _outcome("c", "fiscal", True, route="combined"), _outcome("d", "fiscal", False, scope="annexes")]
    m = ev.group_metrics(outs, "fiscal")
    assert m["n"] == 2 and m["n_context"] == 3
    assert m["success_at_k"] == 0.5 and m["mrr"] == 0.5 and m["context_recall"] == 1.0


def test_the_vocabulary_gap_compares_the_same_cases():
    outs = [_outcome("a", "fiscal", False), _outcome("a", "formal", True),
            _outcome("b", "fiscal", True), _outcome("b", "formal", True),
            _outcome("c", "fiscal", False)]                        # no formal phrasing: out of the pair
    assert ev.paired_gap(outs) == {"cases": 2, "fiscal_success_at_k": 0.5, "formal_success_at_k": 1.0, "gap": 0.5}


def test_abstention_is_separable_only_when_unanswerable_questions_sit_farther():
    solved = [_outcome("a", "fiscal", True, top1=0.10), _outcome("b", "fiscal", True, top1=0.12)]
    far = ev.abstention(solved + [_outcome("n", "fiscal", False, top1=0.20, type_="no_answer")])
    near = ev.abstention(solved + [_outcome("n", "fiscal", False, top1=0.11, type_="no_answer")])
    assert far["separable"] and not near["separable"]
    assert far["solved"] == {"n": 2, "min": 0.10, "median": 0.11, "max": 0.12}


# --------------------------------------------------------------------------- the gate
METRICS = {"fiscal": {"n": 20, "n_context": 22, "success_at_k": 0.5, "hit_at_k": 0.6, "recall_at_k": 0.55,
                      "mrr": 0.4, "context_recall": 0.7},
           "formal": {"n": 10, "n_context": 10, "success_at_k": 0.7, "hit_at_k": 0.8, "recall_at_k": 0.75,
                      "mrr": 0.6, "context_recall": 0.9}}


def _baseline(metrics=METRICS):
    return {"eval_version": ev.EVAL_VERSION, "metrics": metrics}


def test_no_baseline_is_not_a_pass():
    with pytest.raises(ev.EvalError) as exc:
        ev.gate(METRICS, None)
    assert exc.value.event == "eval_baseline_missing"


def test_a_baseline_of_another_metric_definition_is_refused():
    with pytest.raises(ev.EvalError) as exc:
        ev.gate(METRICS, {"eval_version": "0", "metrics": METRICS})
    assert exc.value.event == "eval_baseline_incompatible"


def test_one_case_of_tolerance_two_cases_fail():
    one = {**METRICS, "fiscal": {**METRICS["fiscal"], "success_at_k": 0.45}}       # 1 of 20 cases lost
    two = {**METRICS, "fiscal": {**METRICS["fiscal"], "success_at_k": 0.40}}       # 2 of 20
    assert ev.gate(one, _baseline()) == ([], [])
    regressions, _ = ev.gate(two, _baseline())
    assert [(r["group"], r["metric"], r["threshold"]) for r in regressions] == [("fiscal", "success_at_k", 0.45)]


def test_an_improvement_is_reported_never_failed():
    better = {**METRICS, "formal": {**METRICS["formal"], "mrr": 0.9}}
    regressions, improvements = ev.gate(better, _baseline())
    assert not regressions and [(i["group"], i["metric"]) for i in improvements] == [("formal", "mrr")]


def test_only_the_gated_metrics_fail_the_run():
    worse_hit = {**METRICS, "fiscal": {**METRICS["fiscal"], "hit_at_k": 0.1}}
    assert ev.gate(worse_hit, _baseline()) == ([], [])


def test_the_baseline_file_round_trips(tmp_path):
    rep = {"eval_version": ev.EVAL_VERSION, "measured_at": "2026-10-18T00:00:00Z", "fingerprint": {"k": 5},
           "metrics": METRICS, "paired": {"cases": 0}}
    path = tmp_path / "eval_baseline.yml"
    path.write_text(ev.baseline_yaml(rep), encoding="utf-8")
    assert ev.load_baseline(path)["metrics"] == METRICS
    assert ev.gate(METRICS, ev.load_baseline(path)) == ([], [])
    assert ev.load_baseline(tmp_path / "missing.yml") is None


# --------------------------------------------------------------------------- real Postgres + pgvector, end to end
DSN = os.getenv("CHECKS_TEST_DSN")
DDL = Path(__file__).resolve().parents[1] / "infra/postgres/init/sql/06_lc214.sql"
LAW = "c" * 64
TEXTS = {1: "Art. 1º Transferência entre estabelecimentos do mesmo contribuinte.",
         2: "Art. 2º Importação de bens materiais.", 3: "Art. 3º Serviços financeiros."}
VECS = {1: "[1,0,0]", 2: "[0,1,0]", 3: "[0,0,1]"}


def _golden(tmp_path):
    g = {"version": 2, "law_version": LAW[:12], "k": 2, "cases": [
        {"id": "content-01", "type": "content", "route": "vector", "behavior": "answer", "mode": "all",
         "questions": {"fiscal": "mandei da matriz pra filial", "formal": "transferência entre estabelecimentos"},
         "expected": [P + "1"]},
        {"id": "content-02", "type": "content", "route": "vector", "behavior": "answer", "mode": "all",
         "questions": {"fiscal": "trouxe mercadoria de fora"}, "expected": [P + "2"]},
        {"id": "no_answer-01", "type": "no_answer", "route": "vector", "behavior": "abstain",
         "questions": {"fiscal": "multa da EFD ICMS"}, "expected": []},
    ]}
    path = tmp_path / "golden_set.yml"
    path.write_text(yaml.safe_dump(g, allow_unicode=True), encoding="utf-8")
    return path


def encode(q):
    """A fake embedding with a vocabulary gap: the auditor's 'matriz/filial' lands on art. 2."""
    if "estabelecimentos" in q:
        return [1, 0, 0]
    if "matriz" in q or "fora" in q:
        return [0.1, 1, 0]
    return [0, 0.2, 1]


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
        for art, text in TEXTS.items():
            pid = f"{P}{art}"
            c.execute("INSERT INTO ai.lc214_dispositivo (dispositivo_id, art_id, tipo, rotulo, ordem, art, status, "
                      "texto, altera_outra_lei, row_hash, source_sha256, parser_version) VALUES "
                      "(%s, %s, 'art', %s, %s, %s, 'vigente', %s, false, 'h', %s, '2')",
                      (pid, pid, f"Art. {art}º", art, str(art), text, LAW))
            c.execute("INSERT INTO ai.lc214_artigo (art_id, art, rotulo, header, texto, status, n_dispositivos, "
                      "altera_outra_lei, row_hash, source_sha256, parser_version) VALUES "
                      "(%s, %s, %s, %s, %s, 'vigente', 1, false, 'h', %s, '2')",
                      (pid, str(art), f"Art. {art}º", f"LC 214/2025 | Art. {art}º", text, LAW))
            c.execute("INSERT INTO ai.lc214_chunk (chunk_id, art_id, parte, kind, texto, dispositivo_ids, n_tokens, "
                      "embedding, embedding_model, source_sha256, parser_version, chunker_version) VALUES "
                      "(%s, %s, 1, 'artigo', %s, %s, 5, %s, 'fake/e5', %s, '2', '1')",
                      (f"{pid}:c1:x", pid, text, f'["{pid}"]', VECS[art], LAW))
        yield c
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.skipif(not DSN, reason="set CHECKS_TEST_DSN to run against a real Postgres + pgvector")
def test_first_measurement_becomes_the_gate_and_a_regression_fails_it(cur, tmp_path):
    golden, baseline = _golden(tmp_path), tmp_path / "eval_baseline.yml"
    with pytest.raises(ev.EvalError) as exc:                                    # no baseline yet: not a pass
        ev.run(cur, encode, golden_path=golden, baseline_path=baseline)
    assert exc.value.event == "eval_baseline_missing"
    assert ev.run(cur, encode, golden_path=golden, baseline_path=baseline, write_baseline=True) == ev.EXIT_OK

    b = ev.load_baseline(baseline)
    assert b["fingerprint"]["law_version"] == LAW[:12] and b["fingerprint"]["embedding_model"] == "fake/e5"
    # formal finds art. 1 at rank 1; the fiscal phrasing of the same case misses it (rank 2 within k=2: hit, MRR 0.5)
    assert b["metrics"]["formal"] == {"n": 1, "n_context": 1, "success_at_k": 1.0, "hit_at_k": 1.0,
                                      "recall_at_k": 1.0, "mrr": 1.0, "context_recall": 1.0}
    assert b["metrics"]["fiscal"]["n"] == 2 and b["metrics"]["fiscal"]["mrr"] == 0.75
    assert b["paired"] == {"cases": 1, "fiscal_success_at_k": 1.0, "formal_success_at_k": 1.0, "gap": 0.0}
    assert ev.run(cur, encode, golden_path=golden, baseline_path=baseline) == ev.EXIT_OK

    # a broken re-embedding pushes art. 1 out of the top k. Strict gate: with n = 1, one case of tolerance would set
    # the threshold at 0 and nothing could fail (the tolerance only makes sense with tens of cases)
    cur.execute(f"UPDATE ai.lc214_chunk SET embedding = '[-1,0,0]' WHERE art_id = '{P}1'")
    assert ev.run(cur, encode, golden_path=golden, baseline_path=baseline, tolerance_cases=0) == ev.EXIT_FAILED


@pytest.mark.skipif(not DSN, reason="set CHECKS_TEST_DSN to run against a real Postgres + pgvector")
def test_labels_of_another_law_version_are_refused(cur, tmp_path):
    golden = _golden(tmp_path)
    g = yaml.safe_load(golden.read_text(encoding="utf-8"))
    g["law_version"] = "d" * 12
    golden.write_text(yaml.safe_dump(g), encoding="utf-8")
    with pytest.raises(ev.EvalError) as exc:
        ev.run(cur, encode, golden_path=golden, baseline_path=tmp_path / "b.yml")
    assert exc.value.event == "eval_labels_stale"
