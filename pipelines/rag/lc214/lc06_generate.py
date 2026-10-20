"""LC 214 answer generation: the retrieved context -> a cited JSON answer from the local LLM (ADR 0015).

Step 6 of the legislation RAG. The question goes through the same path as lc05 (router -> retriever, ADR 0012/0013);
this step adds the G of RAG and three barriers that decide when the answer is "não consta":

    question -> router -> retriever -> [1] pre-LLM -> Ollama (JSON Schema) -> [2] model -> [3] citations -> answer
                                         clarify or no block   answerable=false   no citation left in the context

    [1] deterministic: an ambiguous/missing citation asks (clarify); a route with no context block refuses. The LLM
        is not called: there is nothing it could ground an answer on.
    [2] the model: the JSON carries `answerable`; the prompt asks for false when the context does not support it.
    [3] deterministic: every citation the model returns goes through the router's own citation parser
        (parse_citations + Index.resolve, the code that resolves 99.6% of the law's self-citations) and is checked
        against the provisions the context SHOWED (Block.dispositivo_ids):
            valid            the provision was in the context
            outside_context  it exists in the law, but the model did not read it here (memory, not grounding)
            not_found        no such provision (invented or unparseable)
            foreign          another law (the model was told to cite only the LC 214): neither valid nor a violation
        Bad citations are dropped and counted as violations; the answer is refused only when no valid citation is
        left: an answer that cannot point at the text it rests on is not shown.

Routes without the LLM
    sql      the cited provision as structure (router.render_structure), like the router CLI
    clarify  the router's question back (barrier 1)

Determinism and the exit 99
    temperature 0 + a fixed seed + num_ctx explicit (Ollama's default 2048 would silently cut the context). The same
    inputs give the same answers, so the golden run is skipped (exit 99) when a report for the same fingerprint is
    in the lake: law + chunks + embedding model (lc05's fingerprint), golden set, LLM model AND its digest (a re-pulled
    tag is another model), prompt version, retriever settings. `answers_digest` in the report hashes every answer:
    two runs with --force must print the same digest.

Calibrating the token estimate (ADR 0013 left it open)
    The retriever budgets LLM tokens as words x 1.7. Ollama returns prompt_eval_count: the report gives the measured
    ratio per call and its median. Caveat: Ollama reuses the KV cache of a shared prefix (the system prompt), so a
    call after the first may count fewer tokens; a ratio below 1.0 token/word is impossible for Portuguese and is
    flagged `cache_suspect` instead of being used.

Not gated here (patch 2 of ADR 0015 adds the gate against a committed baseline, like lc05): this step measures
and writes the report (silver/lc214/<sha12>/answers/<fingerprint>.json).

Run in the lab (needs Postgres, the embedding model and the Ollama of profile `ai`):
    python -m pipelines.rag.lc214.lc06_generate "o art. 6º se aplica a transferência entre filiais?"
    python -m pipelines.rag.lc214.lc06_generate --golden                  the golden set (DAG task generate_answers)
    python -m pipelines.rag.lc214.lc06_generate --golden --case content-03 --case no_answer-01   a subset, no report
Exit codes: 0 generated | 99 report up to date, or no Ollama (optional layer) | 1 failure.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Protocol

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.logger import get_logger, log_event  # noqa: E402
from pipelines.observability import current_step  # noqa: E402
from pipelines.rag.lc214 import retriever as rt  # noqa: E402
from pipelines.rag.lc214.golden import GOLDEN_PATH, load_golden  # noqa: E402
from pipelines.rag.lc214.router import Index, parse_citations, render_structure  # noqa: E402

logger = get_logger("lc06_generate")

GEN_VERSION = "1"                    # bump when the report's definitions change
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:1b")
SEED = int(os.getenv("LC214_GEN_SEED", "42"))
TIMEOUT_S = int(os.getenv("LC214_GEN_TIMEOUT_S", "600"))       # per call: the first one loads the model on CPU
GROUPS = ("fiscal", "formal")
EXIT_OK, EXIT_FAILED, EXIT_SKIPPED = 0, 1, 99

REFUSAL = "Não há informações suficientes na LC 214/2025 para responder a esta pergunta."

SYSTEM = """Você responde perguntas sobre a Lei Complementar nº 214/2025 (IBS, CBS e Imposto Seletivo).
Use SOMENTE os trechos da lei que estão no contexto. Não use conhecimento de fora do contexto.

Regras:
1. Se o contexto não permitir responder, devolva "answerable": false e explique em uma frase o que falta.
2. Cada afirmação da resposta precisa de um dispositivo do contexto que a sustente. Liste-os em "citations":
   "artigo" é o número do artigo (ex.: "5", "7-A") e "dispositivo" é a parte dentro dele
   ("caput", "§ 1º", "parágrafo único", "§ 2º, inciso III", "inciso II, alínea a").
3. Cite apenas dispositivos da LC 214/2025 que aparecem no contexto.
4. Se a pergunta ou o contexto remeter a outra lei (Código Tributário Nacional, Constituição, outra lei
   complementar), diga que a regra está nessa outra norma e cite o dispositivo da LC 214 que faz a remissão.
   Não invente o conteúdo da outra norma.
5. Responda em português, de forma objetiva, em no máximo 5 frases."""

PROMPT = "Contexto (trechos da LC 214/2025):\n{context}\n\nPergunta: {question}"
FOREIGN_NOTE = "\n\nA pergunta menciona outra norma: {foreign}. A regra dela não está no contexto."

SCHEMA = {
    "type": "object",
    "properties": {
        "answerable": {"type": "boolean"},
        "answer": {"type": "string"},
        "citations": {"type": "array", "items": {
            "type": "object",
            "properties": {"artigo": {"type": "string"}, "dispositivo": {"type": "string"}},
            "required": ["artigo", "dispositivo"]}},
    },
    "required": ["answerable", "answer", "citations"],
}


def llm_options() -> dict:
    return {"temperature": 0, "seed": SEED, "num_ctx": rt.NUM_CTX, "num_predict": rt.ANSWER_TOKENS}


def prompt_version() -> str:
    """Anything that changes what the model is asked changes this hash (and with it the fingerprint)."""
    spec = json.dumps({"system": SYSTEM, "prompt": PROMPT, "foreign": FOREIGN_NOTE, "schema": SCHEMA,
                       "refusal": REFUSAL}, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(spec.encode()).hexdigest()[:12]


class GenError(Exception):
    def __init__(self, event: str, message: str, **fields):
        super().__init__(message)
        self.event, self.fields = event, fields


class LLMUnavailable(GenError):
    pass


# --------------------------------------------------------------------------- the LLM
class LLM(Protocol):
    model: str

    def digest(self) -> str: ...
    def generate(self, system: str, prompt: str, schema: dict, options: dict) -> dict: ...


class OllamaClient:
    """Ollama's /api/generate with structured outputs (`format` = JSON Schema, Ollama >= 0.5; the stack pins 0.6.5)."""

    def __init__(self, url: str = OLLAMA_URL, model: str = OLLAMA_MODEL, timeout: int = TIMEOUT_S):
        import requests

        self.requests, self.url, self.model, self.timeout = requests, url, model, timeout
        self.base = url.split("/api/")[0]

    def _call(self, method: str, path_or_url: str, **kw):
        url = path_or_url if path_or_url.startswith("http") else self.base + path_or_url
        try:
            resp = self.requests.request(method, url, timeout=kw.pop("timeout", 10), **kw)
        except (self.requests.ConnectionError, self.requests.Timeout) as exc:
            cause = type(exc).__name__
            raise LLMUnavailable("lc214_gen_llm_unavailable", f"Ollama not reachable at {self.base} ({cause}): "
                                 "start the profile `ai` (make up-ai)") from None
        resp.raise_for_status()
        return resp.json()

    def digest(self) -> str:
        names = {self.model, self.model if ":" in self.model else f"{self.model}:latest"}
        for m in self._call("GET", "/api/tags").get("models") or []:
            if m.get("name") in names or m.get("model") in names:
                return str(m.get("digest", ""))[:12]
        raise GenError("lc214_gen_model_missing", f"Ollama has no model {self.model}: pull it with "
                       "`docker compose --profile ai up ollama-pull` (OLLAMA_MODEL)")

    def generate(self, system, prompt, schema, options):
        return self._call("POST", self.url, timeout=self.timeout, json={
            "model": self.model, "system": system, "prompt": prompt, "format": schema, "options": options,
            "stream": False, "keep_alive": "10m"})


# --------------------------------------------------------------------------- reading the reply
def parse_reply(raw: str) -> dict | None:
    """The model's JSON, or None when it does not hold the schema (a reply cut by num_predict, for instance)."""
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict) or not isinstance(data.get("answerable"), bool) \
            or not isinstance(data.get("answer"), str) or not isinstance(data.get("citations"), list):
        return None
    cites = []
    for c in data["citations"]:
        if not isinstance(c, dict) or not isinstance(c.get("artigo"), str):
            return None
        disp = c.get("dispositivo")
        cites.append({"artigo": c["artigo"].strip(), "dispositivo": disp.strip() if isinstance(disp, str) else ""})
    return {"answerable": data["answerable"], "answer": data["answer"].strip(), "citations": cites}


def citation_text(artigo: str, dispositivo: str) -> str:
    """{"artigo": "5", "dispositivo": "§ 2º, inciso III"} -> "art. 5, § 2º, inciso III" (chat order, which the
    router reads). The model sometimes repeats the prefix ("Art. 5º"); "caput" adds nothing to the path."""
    art = artigo.strip()
    art = art if art.lower().startswith("art") else f"art. {art}"
    disp = dispositivo.strip()
    return art if not disp or disp.lower() == "caput" else f"{art}, {disp}"


def check_citation(cite: dict, index: Index, shown: set[str]) -> dict:
    text = citation_text(cite["artigo"], cite["dispositivo"])
    parsed = parse_citations(text)
    out = {**cite, "text": text, "ids": []}
    if parsed and all(c.foreign for c in parsed):
        return out | {"status": "foreign"}
    candidates = sorted({pid for c in parsed if not c.foreign for pid in index.resolve(c)})
    in_context = [p for p in candidates if p in shown]
    if in_context:
        return out | {"status": "valid", "ids": in_context}
    return out | {"status": "outside_context" if candidates else "not_found", "ids": candidates}


# --------------------------------------------------------------------------- one question
@dataclass
class Answer:
    question: str
    route: str
    intent: str
    answerable: bool = False                 # what is shown: False -> the refusal text
    answer: str = REFUSAL
    refusal: str | None = None               # clarify | no_context | model | no_valid_citation | invalid_json | truncated
    llm_called: bool = False
    citations: list[dict] = field(default_factory=list)      # every citation the model returned, with its status
    foreign: list[str] = field(default_factory=list)
    blocks: list[str] = field(default_factory=list)          # art_id of each context block, in order
    context_ids: list[str] = field(default_factory=list)     # what the context showed (what may be cited)
    context_tokens: int = 0                                  # the retriever's estimate
    reply_answerable: bool | None = None                     # the model's own flag (barrier 2)
    json_valid: bool | None = None
    done_reason: str | None = None
    prompt_words: int = 0
    prompt_tokens_estimate: int = 0
    prompt_eval_count: int | None = None
    eval_count: int | None = None
    latency_s: float | None = None

    @property
    def valid_ids(self) -> list[str]:
        return sorted({p for c in self.citations if c["status"] == "valid" for p in c["ids"]})

    @property
    def violations(self) -> list[dict]:
        return [c for c in self.citations if c["status"] in ("outside_context", "not_found")]

    @property
    def tokens_per_word(self) -> float | None:
        return round(self.prompt_eval_count / self.prompt_words, 3) if self.prompt_eval_count and self.prompt_words \
            else None

    def public(self) -> dict:
        """What a user would receive."""
        return {"answerable": self.answerable, "answer": self.answer,
                "citations": [c["text"] for c in self.citations if c["status"] == "valid"] if self.answerable else []}


def build_prompt(question: str, r: rt.Retrieval) -> str:
    text = PROMPT.format(context=r.context(), question=question.strip())
    if r.plan.foreign:
        text += FOREIGN_NOTE.format(foreign="; ".join(r.plan.foreign))
    return text


def full_label(pid: str) -> str:
    """lc214-2025:art-5:par-2:inc-III -> 'art. 5, § 2º, III'."""
    art, rel = pid.split(":")[1].removeprefix("art-"), rt.rel_label(pid)
    return f"art. {art}" if rel == "caput" else f"art. {art}, {rel}"


def _structure(store: rt.Store, anchors: list[str]) -> str:
    parts = []
    for a in anchors:
        rows = [row for row in store.provisions(rt.art_of(a))
                if row["dispositivo_id"] == a or row["dispositivo_id"].startswith(a + ":")]
        parts.append(render_structure(rows))
    return "\n\n".join(parts)


def answer(question: str, store: rt.Store, encode: Callable[[str], list[float]], llm: LLM,
           index: Index | None = None, k: int = rt.TOP_K) -> Answer:
    index = index or Index(store.ids())
    r = rt.retrieve(question, store, encode, index=index, k=k)
    a = Answer(question, r.plan.route, r.plan.intent, foreign=list(r.plan.foreign),
               blocks=[b.art_id for b in r.blocks], context_ids=sorted({p for b in r.blocks for p in b.dispositivo_ids}),
               context_tokens=r.tokens)

    if r.plan.route == "sql":                                  # a citation: the text by lookup, no LLM
        a.answerable, a.answer = True, _structure(store, r.plan.anchors)
        a.citations = [{"artigo": "", "dispositivo": "", "text": full_label(p), "ids": [p], "status": "valid"}
                       for p in r.plan.anchors]
        return a
    if r.plan.route == "clarify":                              # barrier 1: ask, never guess
        a.refusal, a.answer = "clarify", r.plan.reason()
        return a
    if not r.blocks:                                           # barrier 1: nothing to ground an answer on
        a.refusal = "no_context"
        return a

    prompt = build_prompt(question, r)
    a.prompt_words = len((SYSTEM + "\n" + prompt).split())
    a.prompt_tokens_estimate = rt.llm_tokens(SYSTEM + "\n" + prompt)
    t0 = time.monotonic()
    resp = llm.generate(SYSTEM, prompt, SCHEMA, llm_options())
    a.latency_s = round(time.monotonic() - t0, 2)
    a.llm_called = True
    a.done_reason = resp.get("done_reason")
    a.prompt_eval_count, a.eval_count = resp.get("prompt_eval_count"), resp.get("eval_count")

    reply = parse_reply(resp.get("response", ""))
    a.json_valid = reply is not None
    if reply is None:
        a.refusal = "truncated" if a.done_reason == "length" else "invalid_json"
        return a
    a.reply_answerable = reply["answerable"]
    shown = set(a.context_ids)
    a.citations = [check_citation(c, index, shown) for c in reply["citations"]]
    if not reply["answerable"]:                                # barrier 2: the model says the context is not enough
        a.refusal = "model"
        return a
    if not a.valid_ids:                                        # barrier 3: nothing it says points at the context
        a.refusal = "no_valid_citation"
        return a
    a.answerable, a.answer = True, reply["answer"]
    return a


# --------------------------------------------------------------------------- the golden set
@dataclass
class Outcome:
    case: str
    type: str
    behavior: str
    phrasing: str
    expected: list[str]
    should_refuse: bool
    in_context: list[str]            # expected provisions the context showed: what the model could have cited
    cited_expected: list[str]        # expected provisions the answer cited (same branch: an article or its parts)
    answer: dict

    @property
    def refused(self) -> bool:
        return not self.answer["answerable"]

    @property
    def refusal_correct(self) -> bool:
        return self.refused == self.should_refuse


def same_branch(a: str, b: str) -> bool:
    """art-4 and art-4:par-1 are one branch: citing the article for a paragraph (or the reverse) points the reader at
    the right text. Lenient on purpose for a first measurement; the gate (patch 2) may tighten it."""
    return a == b or a.startswith(b + ":") or b.startswith(a + ":")


def generated(case: dict) -> bool:
    return case.get("route") in ("vector", "combined") and case.get("scope", "now") == "now"


def outcome(case: dict, phrasing: str, a: Answer) -> Outcome:
    expected = list(case.get("expected") or [])
    shown, cited = set(a.context_ids), a.valid_ids if a.answerable else []
    return Outcome(case["id"], case["type"], case.get("behavior", "answer"), phrasing, expected,
                   case.get("behavior") == "abstain", [p for p in expected if p in shown],
                   [p for p in expected if any(same_branch(p, c) for c in cited)],
                   asdict(a) | {"valid_ids": a.valid_ids, "violations": len(a.violations)})


def run_golden(golden: dict, store: rt.Store, encode, llm: LLM, cases: set[str] | None = None,
               progress: Callable[[str], None] = print) -> list[Outcome]:
    k = golden.get("k", 5)
    index = Index(store.ids())
    todo = [(c, p, q) for c in golden.get("cases") or [] if generated(c) and (not cases or c["id"] in cases)
            for p, q in (c.get("questions") or {}).items()]
    out = []
    for n, (case, phrasing, question) in enumerate(todo, 1):
        a = answer(str(question), store, encode, llm, index=index, k=k)
        o = outcome(case, phrasing, a)
        out.append(o)
        progress(f"  [{n}/{len(todo)}] {case['id']}/{phrasing}: "
                 + ("ANSWER" if a.answerable else f"REFUSE ({a.refusal})")
                 + f" cites {len(a.valid_ids)} valid / {len(a.violations)} bad, expected {len(o.cited_expected)}/"
                   f"{len(o.expected)} (context {len(o.in_context)}) {a.latency_s or 0:.1f}s")
    return out


# --------------------------------------------------------------------------- aggregation
def _mean(values) -> float | None:
    values = list(values)
    return round(sum(values) / len(values), 4) if values else None


def group_metrics(outcomes: list[Outcome], phrasing: str) -> dict:
    """What the gate (patch 2) will read. Two recalls separate the two causes of a missing citation:
    citation_recall counts every answerable case; citation_recall_in_context only the ones whose expected provision
    reached the context (a miss there is the model's, not the retriever's)."""
    group = [o for o in outcomes if o.phrasing == phrasing]
    answerable = [o for o in group if not o.should_refuse and o.expected]
    in_ctx = [o for o in answerable if o.in_context]
    cites = [c for o in group for c in o.answer["citations"] if o.answer["llm_called"]]
    llm = [o for o in group if o.answer["llm_called"]]
    lat = sorted(o.answer["latency_s"] for o in llm if o.answer["latency_s"] is not None)
    return {
        "n": len(group), "n_answerable": len(answerable), "n_abstain": sum(o.should_refuse for o in group),
        "refusal_accuracy": _mean(o.refusal_correct for o in group),
        "false_refusals": sum(o.refused and not o.should_refuse for o in group),
        "false_answers": sum(not o.refused and o.should_refuse for o in group),
        "citation_validity": _mean(c["status"] == "valid" for c in cites if c["status"] != "foreign"),
        "citation_recall": _mean(bool(o.cited_expected) for o in answerable),
        "citation_recall_in_context": _mean(bool(o.cited_expected) for o in in_ctx),
        "json_valid": _mean(o.answer["json_valid"] for o in llm),
        "refusals": dict(Counter(o.answer["refusal"] for o in group if o.answer["refusal"])),
        "latency_p50_s": round(statistics.median(lat), 2) if lat else None,
        "latency_max_s": lat[-1] if lat else None,
    }


def calibration(outcomes: list[Outcome]) -> dict:
    """Measured LLM tokens per word vs the retriever's estimate (rt.LLM_TOKENS_PER_WORD)."""
    ratios = [o.answer["prompt_eval_count"] / o.answer["prompt_words"] for o in outcomes
              if o.answer["prompt_eval_count"] and o.answer["prompt_words"]]
    usable = [r for r in ratios if r >= 1.0]
    return {"estimate_tokens_per_word": rt.LLM_TOKENS_PER_WORD, "calls": len(ratios),
            "cache_suspect": len(ratios) - len(usable),
            "measured_median": round(statistics.median(usable), 3) if usable else None,
            "measured_min": round(min(usable), 3) if usable else None,
            "measured_max": round(max(usable), 3) if usable else None,
            "max_prompt_eval_count": max((o.answer["prompt_eval_count"] or 0 for o in outcomes), default=0),
            "num_ctx": rt.NUM_CTX}


def answers_digest(outcomes: list[Outcome]) -> str:
    """One hash over every answer (text, flag, citations): two runs of the same fingerprint must print the same."""
    body = [[o.case, o.phrasing, o.answer["answerable"], o.answer["answer"], o.answer["citations"]] for o in outcomes]
    return hashlib.md5(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:12]


def report(outcomes: list[Outcome], fp: dict, key: str) -> dict:
    return {
        "gen_version": GEN_VERSION, "fingerprint_key": key,
        "measured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fingerprint": fp, "answers_digest": answers_digest(outcomes),
        "metrics": {g: group_metrics(outcomes, g) for g in GROUPS},
        "calibration": calibration(outcomes),
        "outcomes": [asdict(o) | {"refused": o.refused, "refusal_correct": o.refusal_correct} for o in outcomes],
    }


def summary_line(rep: dict) -> str:
    def fmt(v):
        return "-" if v is None else f"{v:.3f}"

    parts = [f"{g} (n={m['n']}): refusal_acc {fmt(m['refusal_accuracy'])} cit_validity {fmt(m['citation_validity'])}"
             f" cit_recall {fmt(m['citation_recall'])} (in context {fmt(m['citation_recall_in_context'])})"
             f" json {fmt(m['json_valid'])} p50 {m['latency_p50_s']}s"
             for g, m in rep["metrics"].items()]
    c = rep["calibration"]
    parts.append(f"tokens/word {c['measured_median']} measured vs {c['estimate_tokens_per_word']} estimated"
                 + (f" ({c['cache_suspect']} cache-suspect)" if c["cache_suspect"] else ""))
    return " | ".join(parts) + f" | digest {rep['answers_digest']}"


# --------------------------------------------------------------------------- what is being generated
def fingerprint(cur, llm: LLM, golden_path: Path, k: int) -> tuple[dict, str]:
    from pipelines.rag.lc214.lc05_eval import EvalError, fingerprint as store_fingerprint

    try:
        fp = store_fingerprint(cur, golden_path, k)
    except EvalError as exc:
        raise GenError(exc.event, str(exc)) from None
    fp["llm"] = {"model": llm.model, "digest": llm.digest(), "options": llm_options(), "prompt_version": prompt_version()}
    fp["gen_version"] = GEN_VERSION
    key = hashlib.md5(json.dumps(fp, sort_keys=True).encode()).hexdigest()[:12]
    return fp, key


# --------------------------------------------------------------------------- the step
def run(cur, encode, llm: LLM, lake=None, golden_path: Path = GOLDEN_PATH, cases: set[str] | None = None,
        force: bool = False) -> int:
    step = current_step()
    golden = load_golden(golden_path)
    k = golden.get("k", 5)
    fp, key = fingerprint(cur, llm, golden_path, k)
    if golden.get("law_version") != fp["law_version"]:
        raise GenError("lc214_gen_labels_stale", f"golden set labelled against {golden.get('law_version')}, vectors "
                       f"are of {fp['law_version']}: the golden contract (G2) must pass first")

    report_key = None
    if lake is not None and not cases:
        from pipelines.rag.lc214.lake import k_manifest, k_silver
        report_key = k_silver(lake.get_json(k_manifest())["current"], f"answers/{key}.json")
        if not force and lake.exists(report_key):
            log_event(logger, "lc214_gen_up_to_date", f"answers for fingerprint {key} already in {lake.url(report_key)}"
                      " (same law, chunks, golden set, model digest and prompt): nothing to generate",
                      fingerprint=key)
            return EXIT_SKIPPED

    log_event(logger, "lc214_gen_started", f"model {llm.model}@{fp['llm']['digest']} prompt "
              f"{fp['llm']['prompt_version']} fingerprint {key}", fingerprint=key, model=llm.model)
    t0 = time.monotonic()
    outcomes = run_golden(golden, rt.PgStore(cur), encode, llm, cases)
    rep = report(outcomes, fp, key)
    line = summary_line(rep)
    log_event(logger, "lc214_gen_measured", line, fingerprint=key, digest=rep["answers_digest"],
              duration_s=round(time.monotonic() - t0, 1),
              **{f"{g}_{m}": rep["metrics"][g][m] for g in GROUPS
                 for m in ("refusal_accuracy", "citation_validity", "citation_recall", "citation_recall_in_context")})
    for g in GROUPS:
        step.set(**{f"{g}_{m}": v for m, v in rep["metrics"][g].items() if isinstance(v, (int, float))})
    step.set(tokens_per_word=rep["calibration"]["measured_median"])
    step.add(rows_in=len(outcomes), rows_out=sum(not o.refused for o in outcomes))

    if report_key:
        lake.put_json(report_key, rep)
        log_event(logger, "lc214_gen_report_written", lake.url(report_key), fingerprint=key)
    return EXIT_OK


def _connect():
    from pipelines.commons.dw_client import get_pg_connection

    conn = get_pg_connection()
    conn.autocommit = True        # read-only for up to an hour: no idle-in-transaction session against lc04's upsert
    return conn


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("question", nargs="?", help="one question (prints the JSON answer); omit with --golden")
    ap.add_argument("--golden", action="store_true", help="answer every generated phrasing of the golden set")
    ap.add_argument("--case", action="append", default=[], help="with --golden: only this case (repeatable, no report)")
    ap.add_argument("--force", action="store_true", help="generate even when the report for this fingerprint exists")
    ap.add_argument("--context", action="store_true", help="single question: also print the context sent")
    ap.add_argument("--lake", default=None, help="'s3' (default, LC214_LAKE), file:///dir, or 'none' (no report)")
    ap.add_argument("--golden-path", default=str(GOLDEN_PATH))
    args = ap.parse_args(argv)
    if bool(args.question) == args.golden:
        ap.error("give a question or --golden")
    try:
        from pipelines.commons.ai.embeddings import encode_query, load_model
        from pipelines.rag.lc214.lake import LakeError, lake_from_env

        llm = OllamaClient()
        llm.digest()                  # fail (or skip) on a missing Ollama/model before loading the embedding model
        model = load_model()
        conn = _connect()
        try:
            with conn.cursor() as cur:
                encode = (lambda q: encode_query(model, q))
                if args.golden:
                    lake = None if args.lake == "none" else lake_from_env(args.lake)
                    return run(cur, encode, llm, lake, Path(args.golden_path), set(args.case), args.force)
                a = answer(args.question, rt.PgStore(cur), encode, llm)
                if args.context:
                    print(rt.retrieve(args.question, rt.PgStore(cur), encode).context() + "\n")
        finally:
            conn.close()
        print(json.dumps(a.public(), ensure_ascii=False, indent=2))
        print(f"route={a.route} refusal={a.refusal} blocks={a.blocks} citations="
              f"{[(c['text'], c['status']) for c in a.citations]} latency={a.latency_s}s "
              f"prompt_eval_count={a.prompt_eval_count} (estimate {a.prompt_tokens_estimate}) "
              f"done_reason={a.done_reason}", file=sys.stderr)
        return EXIT_OK
    except LLMUnavailable as exc:
        log_event(logger, exc.event, str(exc), level=30)
        return EXIT_SKIPPED if args.golden else EXIT_FAILED      # optional layer: no Ollama, no generation
    except GenError as exc:
        log_event(logger, exc.event, str(exc), level=40, **exc.fields)
        return EXIT_FAILED
    except LakeError as exc:
        log_event(logger, exc.event, str(exc), level=40)
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
