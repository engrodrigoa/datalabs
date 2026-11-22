"""Cited explanation per cClassTrib (rtc04_explain, ADR 0022): context from the bridge, barriers, checks, increments.

The LLM is a fake returning Ollama-shaped bodies: CI tests the code around the model (prompt, fingerprint, citation
checks, planning, gate), never the model itself. The model is measured in the lab (dag_rtc_explain).
"""
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from pipelines.rag.lc214 import lc06_generate as gen
from pipelines.rtc import rtc04_explain as ex

P = "lc214-2025:art-"


def _row(pid, texto, status="vigente"):
    parent = ":".join(pid.split(":")[:-1]) if pid.count(":") > 1 else None
    return {"dispositivo_id": pid, "parent_id": parent, "tipo": "art" if pid.count(":") == 1 else "par",
            "rotulo": texto.split(" ")[0], "status": status, "texto": texto, "notas": []}


LAW = {
    P + "138": [_row(P + "138", "Art. 138. Ficam reduzidas em 60% (sessenta por cento) as alíquotas do IBS e da CBS "
                                "incidentes sobre o fornecimento dos insumos agropecuários do Anexo IX."),
                _row(P + "138:par-2", "§ 2º O fornecimento dos insumos de que trata o caput fica diferido.")],
    P + "6": [_row(P + "6", "Art. 6º O IBS e a CBS não incidem sobre:"),
              _row(P + "6:inc-II", "II - a transferência entre estabelecimentos do mesmo contribuinte;")],
}


class FakeStore:
    def provisions(self, art_id):
        return LAW[art_id]

    def header(self, art_id):
        return f"LC 214/2025 | Art. {art_id.split('-')[-1]}"

    def ids(self):
        return [r["dispositivo_id"] for rows in LAW.values() for r in rows]


def body(answer, citations, answerable=True, done="stop", raw=None):
    return {"response": raw if raw is not None else json.dumps(
                {"answerable": answerable, "answer": answer, "citations": citations}, ensure_ascii=False),
            "done_reason": done, "prompt_eval_count": 420, "eval_count": 80}


GROUNDED = body("Os insumos do Anexo IX têm as alíquotas reduzidas em 60%, conforme o art. 138.",
                [{"artigo": "138", "dispositivo": "caput"}])
NOT_LEVIED = body("A transferência entre estabelecimentos do mesmo contribuinte não sofre incidência.",
                  [{"artigo": "6", "dispositivo": "inciso II"}])


class FakeLLM:
    model = "qwen2.5:3b"

    def __init__(self, replies=None, digest="357c53fb659c"):
        self.replies, self._digest, self.calls = {"410002": NOT_LEVIED, **(replies or {})}, digest, []
        self.model_digest = digest

    def digest(self):
        return self._digest

    def generate(self, system, prompt, schema, options):
        self.calls.append({"system": system, "prompt": prompt, "options": options})
        code = prompt.rsplit("classificação ", 1)[-1].split(" ")[0]
        return self.replies.get(code, GROUNDED)


def classe(code="200038", anchors=(P + "138",), cltr_id=1, grupo="IBSCBS", **kw):
    c = ex.Classe(cltr_id, grupo, code, "Fornecimento dos insumos agropecuários e aquícolas (Anexo IX)",
                  cst="200", descricao_cst="Alíquota reduzida", tratamento="Alíquota reduzida em 60%",
                  tipo_aliquota="Padrão", fundamentacao_legal="Art. 138", dt_inicio_vigencia=date(2026, 1, 1),
                  reducoes={"CBS": Decimal("60.0000"), "IBSUF": Decimal("60.0000"), "IBSMun": Decimal("60.0000")},
                  ponte=[{"outcome": "resolved", "dispositivo_id": a} for a in anchors])
    for k, v in kw.items():
        setattr(c, k, v)
    return c


def _explain(c=None, reply=None):
    c = c or classe()
    llm = FakeLLM({c.cclasstrib: reply} if reply else None)
    task = ex.prepare(c, FakeStore(), llm.model, llm.digest(), ex.prompt_version())
    return ex.explain(task, llm, gen.Index(FakeStore().ids())), llm


# --------------------------------------------------------------------------- context and prompt
def test_the_facts_are_the_tables_own_words_and_a_zero_reduction_is_not_stated():
    text = ex.facts(classe(reducoes={"CBS": Decimal("100"), "IBSUF": Decimal("12.5"), "IBSMun": Decimal("0")}))
    assert "Redução da alíquota: CBS 100%; IBS estadual 12,5%" in text
    assert "IBS municipal" not in text                     # 0 = no reduction (not rate zero): left out
    assert "Tratamento: Alíquota reduzida em 60%" in text and "Vigência na tabela: desde 2026-01-01" in text
    assert "Anexo da LC 214/2025: XIV" in ex.facts(classe(anexo_lc214="14")) and ex.roman("I-A") == "I-A"


def test_the_context_is_the_bridge_articles_and_the_law_comes_before_the_table():
    c = classe(anchors=(P + "138:par-2", P + "6:inc-II"))
    task = ex.prepare(c, FakeStore(), "m", "d", "pv")
    assert [b.art_id for b in task.blocks] == [P + "138", P + "6"]       # one block per article, citation order
    assert task.prompt.index("Ficam reduzidas") < task.prompt.index("Dados da tabela") < task.prompt.index("200038")
    assert task.group == f"{P}138:par-2|{P}6:inc-II"


def test_only_linked_bridge_rows_are_anchors_and_a_partial_reference_is_flagged():
    c = classe(anchors=())
    c.ponte = [{"outcome": "resolved", "dispositivo_id": P + "6:inc-II"},
               {"outcome": "resolved_by_text", "dispositivo_id": P + "138"},
               {"outcome": "ambiguous", "dispositivo_id": None}, {"outcome": "resolved", "dispositivo_id": P + "138"}]
    assert c.anchors == [P + "6:inc-II", P + "138"] and c.partial


# --------------------------------------------------------------------------- barriers and checks
def test_a_grounded_explanation_is_shown_with_the_law_as_source():
    e, llm = _explain()
    assert (e.outcome, e.cita_ponte, e.numeros_sem_fonte, e.fontes) == ("explained", True, [], ["art. 138"])
    assert e.dispositivos_citados == [P + "138"] and e.model == "qwen2.5:3b" and e.prompt_eval_count == 420
    assert llm.calls[0]["options"]["temperature"] == 0 and llm.calls[0]["options"]["num_ctx"] == gen.rt.NUM_CTX


def test_citing_the_caput_for_an_anchored_paragraph_is_valid_but_does_not_cite_the_bridge():
    e, _ = _explain(classe(code="515001", anchors=(P + "138:par-2",)), GROUNDED)
    assert e.outcome == "explained" and e.dispositivos_citados == [P + "138"] and e.cita_ponte is False


def test_a_part_of_the_anchor_cites_the_bridge():
    assert ex.cites_bridge([P + "6:inc-II"], [P + "6"]) and not ex.cites_bridge([P + "6"], [P + "6:inc-II"])


def test_a_percentage_neither_the_table_nor_the_law_states_is_flagged():
    reply = body("A redução é de 60% e, no Anexo IX, de 40 por cento.", [{"artigo": "138", "dispositivo": ""}])
    e, _ = _explain(reply=reply)
    assert e.outcome == "explained" and e.numeros_sem_fonte == ["40%"]
    assert ex.ungrounded_numbers("redução de 60,00%", "60% (sessenta por cento)") == []


def test_without_a_valid_citation_the_explanation_is_refused_and_the_models_text_kept():
    reply = body("Reduz em 60% conforme o art. 999.", [{"artigo": "999", "dispositivo": ""}])
    e, _ = _explain(reply=reply)
    assert (e.outcome, e.recusa, e.explicacao) == ("refused", "no_valid_citation", ex.REFUSAL)
    assert e.resposta_modelo.startswith("Reduz") and e.violacoes == 1 and e.fontes == []


@pytest.mark.parametrize("reply, outcome", [(body("", [], raw='{"answer": "cut'), "invalid_json"),
                                            (body("", [], raw='{"answer": "cut', done="length"), "truncated")])
def test_a_reply_off_the_schema_is_refused_and_named(reply, outcome):
    e, _ = _explain(reply=reply)
    assert (e.outcome, e.recusa, e.explicacao) == (outcome, outcome, ex.REFUSAL)


def test_no_linked_provision_gets_a_fixed_text_without_calling_the_llm():
    c = classe(code="000001", anchors=(), fundamentacao_legal="LC 214/2025", tratamento="Tributação integral")
    c.ponte = [{"outcome": "unparsed", "dispositivo_id": None}]
    e, llm = _explain(c)
    assert e.outcome == "no_anchor" and not llm.calls and e.model is None
    assert "LC 214/2025" in e.explicacao and "Tributação integral" in e.explicacao


def test_each_call_records_its_usage_with_the_outcome(monkeypatch):
    seen = []
    monkeypatch.setattr(ex, "record_llm_call", seen.append)
    _explain()
    assert seen[0].app == "rtc_explain" and seen[0].tags["outcome"] == "explained"
    assert seen[0].tags["cclasstrib"] == "200038" and seen[0].prompt_tokens == 420


# --------------------------------------------------------------------------- fingerprint and planning
def test_the_fingerprint_moves_with_the_model_the_facts_or_the_law_and_not_otherwise():
    def fp(c=None, digest="d1", store=None):
        return ex.prepare(c or classe(), store or FakeStore(), "m", digest, "pv").fingerprint
    assert fp() == fp()
    assert fp(digest="d2") != fp()                                         # a re-pulled tag is another model
    assert fp(classe(tratamento="Alíquota reduzida em 30%")) != fp()
    no_anchor = classe(anchors=())
    assert fp(no_anchor, "d1") == fp(no_anchor, "d2")                      # no LLM: the model does not matter


def test_the_plan_skips_current_rows_and_puts_golden_cases_first_then_groups_by_anchors():
    store = FakeStore()
    tasks = [ex.prepare(c, store, "m", "d", "pv") for c in (
        classe("200038", cltr_id=1), classe("410002", (P + "6:inc-II",), cltr_id=2),
        classe("515001", (P + "138",), cltr_id=3), classe("200099", (P + "6:inc-II",), cltr_id=4))]
    existing = {1: tasks[0].fingerprint}
    order = [t.classe.cltr_id for t in ex.plan(tasks, existing, golden_ids={4})]
    assert order == [4, 3, 2]                     # 1 is current; golden 4 first; then art. 138 before art. 6
    assert [t.classe.cltr_id for t in ex.plan(tasks, existing, set(), force=True)] == [1, 3, 4, 2]


# --------------------------------------------------------------------------- golden set, metrics, gate
def test_the_committed_golden_set_holds_its_schema():
    golden = ex.load_golden()
    expected = {p for c in golden["cases"] for p in c.get("expected") or []}
    assert len(golden["cases"]) == 10 and ex.golden_violations(golden, expected) == []
    assert ex.golden_violations(golden, expected - {P + "308"}) == [
        "explain-02: lc214-2025:art-308 is not a provision of the law served"]


def test_a_bad_golden_case_is_named():
    golden = {"cases": [{"id": "x1", "cclasstrib": "1", "outcome": "no_anchor", "expected": [P + "6"]},
                        {"id": "explain-01", "cclasstrib": "1", "outcome": "maybe"}]}
    assert ex.golden_violations(golden, {P + "6"}) == [
        "x1: id must be unique and look like explain-NN", "x1: a no_anchor case expects no provision",
        "explain-01: outcome must be explained or no_anchor"]


def _rows():
    return {
        1: {"outcome": "explained", "dispositivos_citados": [P + "138"], "explicacao": "reduz 60%", "sha": "s1",
            "cita_ponte": True, "numeros_sem_fonte": [], "violacoes": 0, "pendente": False},
        2: {"outcome": "explained", "dispositivos_citados": [P + "6"], "explicacao": "não incide", "sha": "s2",
            "cita_ponte": False, "numeros_sem_fonte": ["40%"], "violacoes": 1, "pendente": False},
        3: {"outcome": "no_anchor", "dispositivos_citados": [], "explicacao": "fixo", "sha": "s3", "pendente": False},
    }


GOLDEN = {"cases": [
    {"id": "explain-01", "cclasstrib": "200038", "expected": [P + "138"], "mentions": [60],
     "review": {"verdict": "ok", "sha": "s1"}},
    {"id": "explain-02", "cclasstrib": "410002", "expected": [P + "6:inc-II"], "review": {"verdict": "ok", "sha": "old"}},
    {"id": "explain-03", "cclasstrib": "000001", "outcome": "no_anchor"}]}
TARGETS = {"explain-01": 1, "explain-02": 2, "explain-03": 3}


def test_golden_eval_checks_outcome_citation_numbers_and_flags_a_stale_review():
    g = ex.golden_eval(GOLDEN, TARGETS, _rows())
    assert [(x["outcome_ok"], x["cites_expected"], x["mentions_ok"], x["review"]) for x in g] == [
        (True, True, True, "ok"), (True, False, True, "stale"), (True, None, None, "pending")]


def test_metrics_count_the_current_rows():
    m = ex.metrics(list(_rows().values()), ex.golden_eval(GOLDEN, TARGETS, _rows()))
    assert (m["explained_pct"], m["cites_bridge_pct"]) == ({"value": 100.0, "n": 2}, {"value": 50.0, "n": 2})
    assert (m["numbers_ungrounded"], m["violations"], m["golden_cites_expected_pct"]["value"]) == (1, 1, 50.0)


def test_the_gate_passes_without_a_baseline_and_fails_a_drop_beyond_one_case():
    m = ex.metrics(list(_rows().values()), ex.golden_eval(GOLDEN, TARGETS, _rows()))
    assert ex.gate(m, None)[0] == [] and "no baseline" in ex.gate(m, None)[1][0]
    base = {"metrics": {"cites_bridge_pct": {"value": 100.0, "n": 2}, "golden_cites_expected_pct": {"value": 100.0},
                        "numbers_ungrounded": 0}}
    failures, _ = ex.gate(m, base)
    assert [f["metric"] for f in failures] == ["numbers_ungrounded"]       # 50 = 100 - 1 case of 2: tolerated
    failures, _ = ex.gate(m, base, tolerance_cases=0)
    assert [f["metric"] for f in failures] == ["golden_cites_expected_pct", "cites_bridge_pct", "numbers_ungrounded"]


def test_the_gate_waits_for_the_golden_cases_and_the_batch():
    rows = _rows()
    rows[2]["pendente"] = True
    m = ex.metrics(list(rows.values()), ex.golden_eval(GOLDEN, TARGETS, rows))
    failures, notes = ex.gate(m, {"metrics": {"numbers_ungrounded": 0, "cites_bridge_pct": {"value": 100.0}}}, 0)
    assert failures == [] and len(notes) == 2                              # both parts deferred


def test_the_baseline_round_trips(tmp_path):
    m = ex.metrics(list(_rows().values()), ex.golden_eval(GOLDEN, TARGETS, _rows()))
    path = tmp_path / "b.yml"
    path.write_text(ex.baseline_yaml(m, {"measured_at": "t", "model": {"model": "m"}, "rtc_sha": "r",
                                         "law_version": "l"}), encoding="utf-8")
    base = ex.load_baseline(path)
    assert base["metrics"]["cites_bridge_pct"] == {"value": 50.0, "n": 2} and ex.gate(m, base, 0)[0] == []


# --------------------------------------------------------------------------- the run (fake database)
class FakeDB:
    """Just enough of psycopg2 for run(): answers by the table each query reads, keeps the explanation rows."""

    def __init__(self, classes, law_sha="L" * 64, rtc_sha="R" * 40, bridge_ok=True, silver_sha=None):
        self.classes, self.law_sha, self.rtc_sha = classes, law_sha, rtc_sha
        self.bridge = {(rtc_sha, law_sha, ex.BRIDGE_VERSION)} if bridge_ok else {(rtc_sha, "old", "1")}
        self.silver_sha, self.rows, self.commits = silver_sha or rtc_sha, {}, 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass


class FakeCursor:
    def __init__(self, db):
        self.db, self.result, self.description, self.rowcount = db, [], None, 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def _set(self, cols, rows):
        self.description, self.result = [(c,) for c in cols], rows

    def execute(self, sql, params=None):
        q, db = " ".join(sql.split()), self.db
        if "FROM ctrl.rtc_carga" in q:
            self.result = [(db.rtc_sha,)]
        elif "min(source_sha256)" in q:
            self.result = [(db.law_sha,)]
        elif "DISTINCT rtc_sha, law_sha256, bridge_version" in q:
            self.result = list(db.bridge)
        elif q == "SELECT dispositivo_id FROM ai.lc214_dispositivo":
            self.result = [(p,) for p in FakeStore().ids()]
        elif "FROM ai.lc214_dispositivo WHERE art_id" in q:
            rows = LAW[params[0]]
            self._set(list(rows[0]), [tuple(r.values()) for r in rows])
        elif "FROM ai.lc214_artigo" in q:
            self.result = [(FakeStore().header(params[0]),)]
        elif "FROM silver.rtc_cclasstrib" in q:
            cols = ["cltr_id", "grupo_tributo", "cclasstrib", "descricao", "cst", "descricao_cst", "tipo_aliquota",
                    "tratamento", "anexo_lc214", "fundamentacao_legal", "dt_inicio_vigencia", "dt_fim_vigencia",
                    "rtc_sha"]
            self._set(cols, [tuple(getattr(c, k) for k in cols[:-1]) + (db.silver_sha,) for c in db.classes])
        elif "FROM silver.rtc_percentual_reducao" in q:
            self.result = [(c.cltr_id, t, v) for c in db.classes for t, v in c.reducoes.items()]
        elif "JOIN ref_rtc.fundamentacao_classificacao" in q:
            self.result = [(c.cltr_id, p["outcome"], p["dispositivo_id"]) for c in db.classes for p in c.ponte]
        elif q.startswith(f"SELECT cltr_id, row_fingerprint FROM {ex.TABLE}"):
            self.result = [(k, r["row_fingerprint"]) for k, r in db.rows.items()]
        elif q.startswith(f"DELETE FROM {ex.TABLE}"):
            gone = [k for k in db.rows if k not in params[0]]
            for k in gone:
                del db.rows[k]
            self.rowcount = len(gone)
        elif q.startswith(f"UPDATE {ex.TABLE}"):
            todo, rtc, law, bv, keep = params
            for k, r in db.rows.items():
                if k in keep:
                    r.update(pendente=k in todo, rtc_sha=rtc, law_sha256=law, bridge_version=bv)
        elif q.startswith(f"INSERT INTO {ex.TABLE}"):
            cols = q.split("(", 1)[1].split(")", 1)[0].split(", ")
            row = dict(zip(cols, params))
            if "DO NOTHING" in q and row["cltr_id"] in db.rows:
                return
            db.rows[row["cltr_id"]] = {**db.rows.get(row["cltr_id"], {}), **row}
        elif q.startswith("SELECT cltr_id, grupo_tributo, cclasstrib, outcome"):
            cols = [*ex.ROW_COLS, "pendente"]
            self._set(cols, [tuple(r.get(c) for c in cols) for r in db.rows.values()])
        else:
            raise AssertionError(f"unexpected query: {q[:90]}")

    def fetchall(self):
        return self.result

    def fetchone(self):
        return self.result[0] if self.result else None


def _classes():
    unparsed = classe("000001", (), cltr_id=9, fundamentacao_legal="LC 214/2025")
    unparsed.ponte = [{"outcome": "unparsed", "dispositivo_id": None}]
    return [classe("200038", cltr_id=1), classe("410002", (P + "6:inc-II",), cltr_id=2), unparsed]


@pytest.fixture
def golden_file(tmp_path):
    path = tmp_path / "golden.yml"
    path.write_text(yaml.safe_dump({"cases": [{"id": "explain-01", "cclasstrib": "410002",
                                               "expected": [P + "6:inc-II"]}]}), encoding="utf-8")
    return path


def test_a_run_explains_everything_then_skips_until_the_model_changes(golden_file, tmp_path):
    db, llm = FakeDB(_classes()), FakeLLM()
    kw = {"golden_path": golden_file, "baseline_path": tmp_path / "none.yml"}
    assert ex.run(db, llm, **kw) == ex.EXIT_OK
    assert {k: r["outcome"] for k, r in db.rows.items()} == {1: "explained", 2: "explained", 9: "no_anchor"}
    assert llm.calls[0]["prompt"].rstrip("?").endswith("410002 tem esse tratamento")       # the golden case first
    assert db.commits >= 4                                                   # one per row, after the plan
    assert ex.run(db, FakeLLM(), **kw) == ex.EXIT_SKIPPED                     # same fingerprints: nothing to do
    again = FakeLLM(digest="new-digest")
    assert ex.run(db, again, **kw) == ex.EXIT_OK and len(again.calls) == 2    # no_anchor kept: no model in it


def test_the_time_budget_leaves_rows_pending_for_the_next_run(golden_file, tmp_path):
    db, ticks = FakeDB(_classes()), iter(range(0, 10_000, 60))
    kw = {"golden_path": golden_file, "baseline_path": tmp_path / "none.yml"}
    assert ex.run(db, FakeLLM(), max_minutes=1.5, clock=lambda: next(ticks), **kw) == ex.EXIT_OK
    done = {k: r.get("outcome") for k, r in db.rows.items()}
    assert done == {2: "explained", 9: None, 1: None}                       # the golden case got in; placeholders
    assert all(db.rows[k]["pendente"] for k in (1, 9)) and not db.rows[2]["pendente"]
    assert ex.run(db, FakeLLM(), **kw) == ex.EXIT_OK
    assert {k: r["outcome"] for k, r in db.rows.items()} == {1: "explained", 2: "explained", 9: "no_anchor"}


@pytest.mark.parametrize("db", [FakeDB(_classes(), law_sha=None), FakeDB(_classes(), bridge_ok=False),
                                FakeDB(_classes(), silver_sha="older")], ids=["no_law", "stale_bridge", "stale_silver"])
def test_it_waits_for_the_law_the_bridge_and_dbt(db, golden_file):
    assert ex.run(db, FakeLLM(), golden_path=golden_file) == ex.EXIT_SKIPPED and not db.rows


def test_no_ollama_is_a_skip_not_a_failure(golden_file):
    class Down(FakeLLM):
        def digest(self):
            raise gen.LLMUnavailable("lc214_gen_llm_unavailable", "down")
    db = FakeDB(_classes())
    assert ex.run(db, Down(), golden_path=golden_file) == ex.EXIT_SKIPPED and not db.rows


def test_a_stale_golden_label_stops_the_run(tmp_path):
    path = tmp_path / "golden.yml"
    path.write_text(yaml.safe_dump({"cases": [{"id": "explain-01", "cclasstrib": "1", "expected": [P + "999"]}]}),
                    encoding="utf-8")
    with pytest.raises(ex.ExplainError, match="art-999"):
        ex.run(FakeDB(_classes()), FakeLLM(), golden_path=path)


def test_the_gate_fails_the_run_below_the_baseline(golden_file, tmp_path):
    base = tmp_path / "base.yml"
    base.write_text(yaml.safe_dump({"metrics": {"numbers_ungrounded": 0, "golden_cites_expected_pct": {"value": 100.0}}}))
    reply = body("Não incide (art. 6º, II) e a alíquota cai 15%.", [{"artigo": "6", "dispositivo": "inciso II"}])
    db = FakeDB(_classes())
    assert ex.run(db, FakeLLM({"410002": reply}), do_gate=True, golden_path=golden_file,
                  baseline_path=base) == ex.EXIT_FAILED
    assert db.rows[2]["numeros_sem_fonte"] == ["15%"]


def test_the_dag_task_and_the_docs_name_the_same_entry_points():
    dag = (Path(__file__).resolve().parents[1] / "airflow" / "dags" / "dag_rtc_explain.py").read_text()
    assert "rtc/rtc04_explain.py" in dag and "--gate" in dag
