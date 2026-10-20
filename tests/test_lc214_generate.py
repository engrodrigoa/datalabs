"""LC 214 answer generation (lc06): three barriers to "não consta", citations checked by the router's parser.

The LLM is a fake that returns recorded Ollama bodies (tests/fixtures/lc214_ollama_replies.json): CI tests the code
around the model (prompt, options, JSON, citation check, refusal rules, metrics), never the model itself. The model is
measured in the lab, by the DAG task generate_answers.
"""
import json
from pathlib import Path

import pytest

from pipelines.rag.lc214 import lc06_generate as gen
from pipelines.rag.lc214 import retriever as rt
from pipelines.rag.lc214.lake import LocalLake, k_manifest, k_silver
from pipelines.rag.lc214.router import Index

P = "lc214-2025:art-"
REPLIES = json.loads((Path(__file__).parent / "fixtures" / "lc214_ollama_replies.json").read_text(encoding="utf-8"))


def _row(pid, texto, status="vigente"):
    parent = ":".join(pid.split(":")[:-1]) if pid.count(":") > 1 else None
    return {"dispositivo_id": pid, "parent_id": parent, "tipo": "art" if pid.count(":") == 1 else "par",
            "rotulo": texto.split(" ")[0], "status": status, "texto": texto, "notas": []}


LAW = {
    P + "1": [_row(P + "1", "Art. 1º Remessa entre estabelecimentos do mesmo contribuinte."),
              _row(P + "1:par-1", "§ 1º Inclui a filial.")],
    P + "2": [_row(P + "2", "Art. 2º Multa por atraso.")],
    P + "3": [_row(P + "3", "Art. 3º Outro assunto.")],
}


class FakeStore:
    """The vector search always ranks art. 1 first; art. 3 exists in the law but never reaches the context."""

    def __init__(self, ranking=(P + "1",)):
        self.ranking = list(ranking)

    def search(self, qvec, k, art_id=None):
        arts = [a for a in self.ranking if art_id is None or a == art_id][:k]
        return [rt.Hit(n, f"{a}:c1:x", a, "artigo", "t", tuple(r["dispositivo_id"] for r in LAW[a]), n / 10)
                for n, a in enumerate(arts, 1)]

    def provisions(self, art_id):
        return LAW[art_id]

    def header(self, art_id):
        return f"LC 214/2025 | Art. {art_id.split('-')[-1]}º"

    def ids(self):
        return [r["dispositivo_id"] for rows in LAW.values() for r in rows]


class FakeLLM:
    model = "llama3.2:1b"

    def __init__(self, reply="grounded", by_question=None):
        self.reply, self.by_question, self.calls = reply, by_question or {}, []

    def digest(self):
        return "a80c4f17acd5"

    def generate(self, system, prompt, schema, options):
        self.calls.append({"system": system, "prompt": prompt, "schema": schema, "options": options})
        question = prompt.rsplit("Pergunta: ", 1)[-1].split("\n")[0]
        return REPLIES[self.by_question.get(question, self.reply)]


def encode(q):
    return [0.0]


def _answer(question="remessa para filial incide?", reply="grounded", store=None):
    llm = FakeLLM(reply)
    return gen.answer(question, store or FakeStore(), encode, llm), llm


# --------------------------------------------------------------------------- reading the reply
def test_a_reply_that_holds_the_schema_is_read():
    r = gen.parse_reply(REPLIES["grounded"]["response"])
    assert r["answerable"] is True and r["citations"][1] == {"artigo": "1", "dispositivo": "§ 1º"}


@pytest.mark.parametrize("raw", ["", "não é json", "[]", '{"answerable": "sim", "answer": "x", "citations": []}',
                                 '{"answerable": true, "answer": "x", "citations": [{"dispositivo": "§ 1º"}]}',
                                 REPLIES["cut_by_num_predict"]["response"]])
def test_a_reply_off_the_schema_is_not_read(raw):
    assert gen.parse_reply(raw) is None


@pytest.mark.parametrize("artigo,dispositivo,text", [
    ("5", "§ 2º, inciso III", "art. 5, § 2º, inciso III"), ("Art. 5º", "", "Art. 5º"), ("7-A", "caput", "art. 7-A"),
])
def test_a_citation_becomes_text_the_router_reads(artigo, dispositivo, text):
    assert gen.citation_text(artigo, dispositivo) == text


def test_each_citation_is_resolved_and_checked_against_what_the_context_showed():
    index, shown = Index(FakeStore().ids()), {P + "1", P + "1:par-1"}

    def status(artigo, dispositivo=""):
        return gen.check_citation({"artigo": artigo, "dispositivo": dispositivo}, index, shown)["status"]

    assert status("1", "§ 1º") == "valid"
    assert status("3") == "outside_context"               # exists in the law, the model did not read it here
    assert status("99") == "not_found" and status("1", "§ 9º") == "not_found"
    assert status("14 do Código Tributário Nacional") == "foreign"


# --------------------------------------------------------------------------- one question
def test_a_grounded_answer_is_shown_with_its_citations():
    a, llm = _answer()
    assert a.answerable and a.refusal is None and a.blocks == [P + "1"]
    assert a.valid_ids == [P + "1", P + "1:par-1"] and not a.violations
    assert a.public()["citations"] == ["art. 1", "art. 1, § 1º"]
    assert a.prompt_eval_count == 410 and a.tokens_per_word == round(410 / a.prompt_words, 3)


def test_the_call_is_repeatable_and_does_not_cut_the_context():
    _, llm = _answer()
    opts = llm.calls[0]["options"]
    assert opts["temperature"] == 0 and opts["seed"] == gen.SEED
    assert opts["num_ctx"] == rt.NUM_CTX and opts["num_predict"] == rt.ANSWER_TOKENS   # never Ollama's 2048 default
    assert llm.calls[0]["schema"] == gen.SCHEMA and "Art. 1º Remessa" in llm.calls[0]["prompt"]


def test_an_invented_citation_is_dropped_and_counted_the_answer_stays():
    a, _ = _answer(reply="one_invented")
    assert a.answerable and a.valid_ids == [P + "1:par-1"]
    assert [v["status"] for v in a.violations] == ["not_found"]
    assert a.public()["citations"] == ["art. 1, § 1º"]


def test_without_a_valid_citation_the_answer_is_refused():                    # barrier 3
    a, _ = _answer(reply="outside_only")
    assert not a.answerable and a.refusal == "no_valid_citation" and a.answer == gen.REFUSAL
    assert a.public() == {"answerable": False, "answer": gen.REFUSAL, "citations": []}


def test_the_model_can_refuse():                                              # barrier 2
    a, _ = _answer(reply="model_refuses")
    assert not a.answerable and a.refusal == "model" and a.reply_answerable is False


def test_a_reply_cut_by_num_predict_is_refused_and_named():
    a, _ = _answer(reply="cut_by_num_predict")
    assert not a.answerable and a.refusal == "truncated" and a.json_valid is False


def test_a_missing_citation_asks_without_calling_the_llm():                  # barrier 1
    a, llm = _answer("o art. 600 se aplica à remessa?")
    assert a.route == "clarify" and a.refusal == "clarify" and "art. 600" in a.answer and not llm.calls


def test_no_context_block_refuses_without_calling_the_llm():                 # barrier 1
    a, llm = _answer(store=FakeStore(ranking=()))
    assert a.refusal == "no_context" and not llm.calls


def test_a_citation_alone_is_answered_by_lookup_without_the_llm():
    a, llm = _answer("o que diz o art. 1º?")
    assert a.route == "sql" and a.answerable and not llm.calls
    assert "§ 1º Inclui a filial." in a.answer and a.public()["citations"] == ["art. 1"]


def test_another_law_in_the_question_is_flagged_in_the_prompt():
    _, llm = _answer("a imunidade do art. 14 do Código Tributário Nacional vale para remessa?")
    assert "menciona outra norma" in llm.calls[0]["prompt"]


# --------------------------------------------------------------------------- the golden set
GOLDEN = {"version": 2, "law_version": "0cf36f4433ce", "k": 5, "cases": [
    {"id": "content-01", "type": "content", "route": "vector", "behavior": "answer", "mode": "all",
     "questions": {"fiscal": "vendi para a filial, incide?", "formal": "remessa entre estabelecimentos?"},
     "expected": [P + "1:par-1"]},
    {"id": "no_answer-01", "type": "no_answer", "route": "vector", "behavior": "abstain",
     "questions": {"fiscal": "multa da EFD?"}, "expected": []},
    {"id": "citation-01", "type": "citation", "route": "sql", "behavior": "answer",
     "questions": {"fiscal": "o que diz o art. 1º?"}, "expected": [P + "1"]},
    {"id": "external-09", "type": "external_ref", "route": "vector", "behavior": "cite_external", "scope": "annexes",
     "questions": {"fiscal": "tabela do anexo?"}, "expected": [P + "2"]},
]}


def _golden_run(by_question=None):
    llm = FakeLLM(by_question=by_question or {"remessa entre estabelecimentos?": "one_invented",
                                              "multa da EFD?": "model_refuses"})
    return gen.run_golden(GOLDEN, FakeStore(), encode, llm, progress=lambda _: None), llm


def test_the_golden_run_covers_vector_and_combined_now_only():
    outcomes, llm = _golden_run()
    assert [(o.case, o.phrasing) for o in outcomes] == [("content-01", "fiscal"), ("content-01", "formal"),
                                                         ("no_answer-01", "fiscal")]
    assert len(llm.calls) == 3                                          # sql and out-of-scope cases are not generated


def test_metrics_separate_refusals_citations_and_the_retrievers_share():
    outcomes, _ = _golden_run()
    fiscal, formal = gen.group_metrics(outcomes, "fiscal"), gen.group_metrics(outcomes, "formal")
    assert fiscal["n"] == 2 and fiscal["n_abstain"] == 1 and fiscal["refusal_accuracy"] == 1.0
    assert fiscal["citation_recall"] == 1.0 and fiscal["citation_recall_in_context"] == 1.0
    assert formal["citation_validity"] == 0.5                           # 1 of 2 citations was invented
    assert formal["citation_recall"] == 1.0 and fiscal["json_valid"] == 1.0
    assert fiscal["refusals"] == {"model": 1}


def test_a_wrong_refusal_and_a_wrong_answer_are_both_counted():
    outcomes, _ = _golden_run({"vendi para a filial, incide?": "model_refuses", "multa da EFD?": "grounded"})
    m = gen.group_metrics(outcomes, "fiscal")
    assert m["refusal_accuracy"] == 0.0 and m["false_refusals"] == 1 and m["false_answers"] == 1


def test_the_same_inputs_give_the_same_digest():
    assert gen.answers_digest(_golden_run()[0]) == gen.answers_digest(_golden_run()[0])
    other = _golden_run({"vendi para a filial, incide?": "model_refuses"})[0]
    assert gen.answers_digest(other) != gen.answers_digest(_golden_run()[0])


def test_calibration_ignores_ratios_only_a_cached_prefix_explains():
    outcomes, _ = _golden_run()
    outcomes[0].answer["prompt_eval_count"] = 5                          # a cache hit: far fewer tokens than words
    c = gen.calibration(outcomes)
    assert c["calls"] == 3 and c["cache_suspect"] == 1 and c["measured_median"] is not None


# --------------------------------------------------------------------------- the step: report and exit 99
@pytest.fixture
def step_env(tmp_path, monkeypatch):
    lake = LocalLake(str(tmp_path / "lake"))
    lake.put_json(k_manifest(), {"current": "0cf36f4433ce" + "0" * 52})
    golden_path = tmp_path / "golden_set.yml"
    golden_path.write_text(json.dumps(GOLDEN), encoding="utf-8")       # JSON is YAML
    monkeypatch.setattr(gen.rt, "PgStore", lambda cur: FakeStore())
    fp = {"law_version": "0cf36f4433ce", "llm": {"model": "llama3.2:1b", "digest": "a80c4f17acd5", "prompt_version": "p"}}
    monkeypatch.setattr(gen, "fingerprint", lambda cur, llm, path, k, spec=None: (fp, "fp0000000001"))
    return lake, golden_path


def test_the_report_is_written_once_per_fingerprint(step_env):
    lake, golden_path = step_env
    llm = FakeLLM()
    assert gen.run(None, encode, llm, lake, golden_path) == gen.EXIT_OK
    key = k_silver("0cf36f4433ce" + "0" * 52, "answers/fp0000000001.json")
    rep = lake.get_json(key)
    assert rep["fingerprint_key"] == "fp0000000001" and len(rep["outcomes"]) == 3 and rep["answers_digest"]
    calls = len(llm.calls)
    assert gen.run(None, encode, llm, lake, golden_path) == gen.EXIT_SKIPPED and len(llm.calls) == calls
    assert gen.run(None, encode, llm, lake, golden_path, force=True) == gen.EXIT_OK


SHA = "0cf36f4433ce" + "0" * 52


def _dev_reports(lake):
    root = Path(lake._path(k_silver(SHA, "answers/dev")))
    return sorted(p.name for p in root.glob("*.json")) if root.exists() else []


def test_a_subset_keeps_every_run_apart_and_never_skips(step_env, monkeypatch):
    lake, golden_path = step_env
    real_datetime, ticks = gen.datetime, iter(range(100))

    class Clock(real_datetime):                         # every call is a minute later: two runs, two names
        @classmethod
        def now(cls, tz=None):
            return real_datetime(2026, 10, 20, 20, next(ticks), tzinfo=tz)

    monkeypatch.setattr(gen, "datetime", Clock)
    for _ in range(2):
        assert gen.run(None, encode, FakeLLM(), lake, golden_path, cases={"no_answer-01"}) == gen.EXIT_OK
    assert not lake.exists(k_silver(SHA, "answers/fp0000000001.json"))          # the full run's report is untouched
    names = _dev_reports(lake)
    assert len(names) == 2 and all(n.startswith("fp0000000001-cases-") for n in names)


def test_stale_labels_stop_the_run(step_env, monkeypatch):
    lake, golden_path = step_env
    monkeypatch.setattr(gen, "fingerprint", lambda cur, llm, path, k, spec=None: ({"law_version": "ffffffffffff"}, "k"))
    with pytest.raises(gen.GenError) as exc:
        gen.run(None, encode, FakeLLM(), lake, golden_path)
    assert exc.value.event == "lc214_gen_labels_stale"


# --------------------------------------------------------------------------- the Ollama client
class FakeRequests:
    class ConnectionError(Exception):
        pass

    class Timeout(Exception):
        pass

    def __init__(self, tags=None, down=False):
        self.tags, self.down, self.sent = tags or [], down, []

    def request(self, method, url, timeout, **kw):
        self.sent.append((method, url, kw))
        if self.down:
            raise self.ConnectionError("refused")
        body = {"models": self.tags} if url.endswith("/api/tags") else REPLIES["grounded"]
        return type("R", (), {"raise_for_status": lambda self: None, "json": lambda self: body})()


def _client(**kw):
    c = gen.OllamaClient(url="http://ollama:11434/api/generate", model="llama3.2:1b")
    c.requests = FakeRequests(**kw)
    return c


def test_the_model_digest_is_part_of_what_is_generated():
    c = _client(tags=[{"name": "llama3.2:1b", "digest": "baf6a787fdffd633537aa2eb51cfd54cb93ff08e28040095462bb63daf552878"}])
    assert c.digest() == "baf6a787fdff"


def test_a_missing_model_fails_and_a_missing_ollama_is_named():
    with pytest.raises(gen.GenError) as exc:
        _client(tags=[{"name": "qwen2.5:3b", "digest": "x"}]).digest()
    assert exc.value.event == "lc214_gen_model_missing"
    with pytest.raises(gen.LLMUnavailable):
        _client(down=True).digest()


def test_the_request_asks_for_json_schema_output():
    c = _client()
    c.generate("sys", "prompt", gen.SCHEMA, gen.llm_options())
    method, url, kw = c.requests.sent[0]
    assert url == "http://ollama:11434/api/generate" and kw["json"]["format"] == gen.SCHEMA
    assert kw["json"]["stream"] is False and kw["json"]["system"] == "sys"


def test_the_prompt_version_changes_with_the_prompt(monkeypatch):
    before = gen.prompt_version()
    monkeypatch.setattr(gen, "SYSTEM", gen.SYSTEM + " ")
    assert gen.prompt_version() != before


# --------------------------------------------------------------------------- patch 1.1: timings, the model's text, dev
def test_ollamas_split_of_the_latency_is_kept():
    a, _ = _answer()                                    # grounded: prompt 6.2 s, answer 2.8 s
    assert (a.prompt_eval_s, a.eval_s) == (6.2, 2.8) and a.prompt_eval_rate == round(410 / 6.2, 1)


def test_a_refusal_keeps_what_the_model_said_without_showing_it():
    a, _ = _answer(reply="model_refuses")
    assert a.reply_answer.startswith("O contexto não trata") and a.public()["answer"] == gen.REFUSAL


def test_the_dev_subset_is_what_the_golden_set_marks(step_env):
    lake, golden_path = step_env
    marked = {**GOLDEN, "cases": [{**c, "dev": True} if c["id"] == "no_answer-01" else c for c in GOLDEN["cases"]]}
    assert gen.dev_cases(marked) == {"no_answer-01"}
    golden_path.write_text(json.dumps(marked), encoding="utf-8")
    llm = FakeLLM()
    assert gen.run(None, encode, llm, lake, golden_path, dev=True) == gen.EXIT_OK and len(llm.calls) == 1
    assert not lake.exists(k_silver(SHA, "answers/fp0000000001.json"))
    [name] = _dev_reports(lake)
    assert name.startswith("fp0000000001-dev-")


def test_dev_without_marked_cases_says_so(step_env):
    lake, golden_path = step_env
    with pytest.raises(gen.GenError) as exc:
        gen.run(None, encode, FakeLLM(), lake, golden_path, dev=True)
    assert exc.value.event == "lc214_gen_no_dev_cases"


def test_the_committed_golden_set_has_a_dev_subset_of_every_behavior():
    from pipelines.rag.lc214.golden import load_golden

    golden = load_golden()
    dev = [c for c in golden["cases"] if c["id"] in gen.dev_cases(golden)]
    assert {c["behavior"] for c in dev} == {"answer", "abstain", "cite_external"}
    assert {c["route"] for c in dev} == {"vector", "combined"}
    assert 8 <= sum(len(c["questions"]) for c in dev) <= 14


# --------------------------------------------------------------------------- patch 1.2: prompt variants
def test_variants_change_one_thing_and_compose_in_any_order():
    base, last, default = (gen.prompt_spec(v) for v in ("baseline", "verdict_last", "answer_default"))
    assert list(base.schema["properties"]) == ["answerable", "answer", "citations"]
    assert list(last.schema["properties"]) == ["answer", "citations", "answerable"] and last.system == base.system
    assert default.schema == base.schema and default.system != base.system
    both = gen.prompt_spec("answer_default+verdict_last")
    assert both.name == "verdict_last+answer_default" == gen.prompt_spec("verdict_last+answer_default").name
    assert both.system == default.system and both.schema == last.schema


def test_the_schema_order_is_part_of_the_prompt_version():
    # with sort_keys the reordered schema hashed the same: verdict_last would have been skipped as already generated
    versions = {gen.prompt_version(gen.prompt_spec(v)) for v in ("baseline", "verdict_last", "answer_default",
                                                                  "verdict_last+answer_default")}
    assert len(versions) == 4


def test_an_unknown_variant_is_refused():
    with pytest.raises(gen.GenError) as exc:
        gen.prompt_spec("verdict_first")
    assert exc.value.event == "lc214_gen_unknown_variant"


def test_the_variant_reaches_the_model():
    llm = FakeLLM()
    gen.answer("remessa para filial incide?", FakeStore(), encode, llm, spec=gen.prompt_spec("verdict_last"))
    assert list(llm.calls[0]["schema"]["properties"])[-1] == "answerable"


def test_the_raw_reply_is_kept():
    a, _ = _answer(reply="model_refuses")
    assert json.loads(a.raw_response)["answerable"] is False


def test_the_progress_line_shows_the_models_words_and_the_dropped_citations():
    a, _ = _answer(reply="one_invented")
    o = gen.outcome(GOLDEN["cases"][0], "fiscal", a)
    assert "bad: art. 99 (not_found)" in gen.progress_line(1, 1, o, a)
    a, _ = _answer(reply="model_refuses")
    o = gen.outcome(GOLDEN["cases"][1], "fiscal", a)
    assert 'model: "O contexto não trata de multa por atraso de EFD."' in gen.progress_line(1, 1, o, a)
