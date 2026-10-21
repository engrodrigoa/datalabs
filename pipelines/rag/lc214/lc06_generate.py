"""LC 214 answer generation: the retrieved context -> a cited JSON answer from the local LLM (ADR 0015).

Step 6 of the legislation RAG. The question goes through the same path as lc05 (router -> retriever, ADR 0012/0013);
this step adds the G of RAG and three barriers that decide when the answer is "não consta":

    question -> router -> retriever -> [1] pre-LLM -> Ollama (JSON Schema) -> [2] model -> [3] citations -> answer
                                         clarify or no block   answerable=false   no citation left in the context

    [1] deterministic: an ambiguous/missing citation asks (clarify); a route with no context block refuses. The LLM
        is not called: there is nothing it could ground an answer on.
    [2] the model: the JSON carries `answerable`; the prompt asks for false when the context does not support it.
        Obeyed only under the decision rule `model`; under `citations` it is recorded and barrier 3 decides alone
        (the 1B said false in 33 of 33 dev replies: a constant is not a signal, ADR 0015 decision 12).
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

Reports: answers/<fingerprint>.json for the full run (skipped when present); answers/dev/<fingerprint>-<subset>-<time>.json
for a subset (--dev / --case: never skipped, every run kept, to compare variants and check the digest).

Not gated here (patch 2 of ADR 0015 adds the gate against a committed baseline, like lc05): this step measures
and writes the report (silver/lc214/<sha12>/answers/<fingerprint>.json).

Run in the lab (needs Postgres, the embedding model and the Ollama of profile `ai`):
    python -m pipelines.rag.lc214.lc06_generate "o art. 6º se aplica a transferência entre filiais?"
    python -m pipelines.rag.lc214.lc06_generate --golden                  the golden set (DAG task generate_answers)
    python -m pipelines.rag.lc214.lc06_generate --golden --case content-03 --case no_answer-01   a subset (answers/dev/)
    python -m pipelines.rag.lc214.lc06_generate --golden --dev            the dev subset (prompt experiments)
    python -m pipelines.rag.lc214.lc06_generate --golden --dev --variant verdict_last     one experiment
Prompt variants (LC214_GEN_VARIANT / --variant, composed with '+'): see VARIANTS. Each is part of the fingerprint.
Decision rules (LC214_GEN_DECISION / --decision): see DECISIONS. NOT part of the fingerprint: every report scores
both, and recorded replies are re-decided without the LLM:
    python -m pipelines.rag.lc214.lc06_generate --rescore dev [--detail citations]
    python -m pipelines.rag.lc214.lc06_generate --rescore s3://silver/lc214/<sha12>/answers/dev/<report>.json
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

GEN_VERSION = "3"                    # bump when the report's definitions change (3: metrics under both decisions)
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:1b")
SEED = int(os.getenv("LC214_GEN_SEED", "42"))
TIMEOUT_S = int(os.getenv("LC214_GEN_TIMEOUT_S", "600"))       # per call: the first one loads the model on CPU
VARIANT = os.getenv("LC214_GEN_VARIANT", "baseline")
DECISION = os.getenv("LC214_GEN_DECISION", "model")
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

# answer_default: the same rules, but answering is the default and refusing the exception, stated last. Small models
# give a refusal rule stated first more weight than the rest of the prompt (hypothesis H2, ADR 0015).
SYSTEM_ANSWER_DEFAULT = """Você explica a Lei Complementar nº 214/2025 (IBS, CBS e Imposto Seletivo) a um auditor fiscal,
usando os trechos da lei que estão no contexto.

Regras:
1. Responda à pergunta com base nos trechos do contexto. Se algum trecho trata do assunto, responda, mesmo que a
   resposta seja parcial, e diga o que o contexto não cobre.
2. Cada afirmação da resposta precisa de um dispositivo do contexto que a sustente. Liste-os em "citations":
   "artigo" é o número do artigo (ex.: "5", "7-A") e "dispositivo" é a parte dentro dele
   ("caput", "§ 1º", "parágrafo único", "§ 2º, inciso III", "inciso II, alínea a").
3. Cite apenas dispositivos da LC 214/2025 que aparecem no contexto. Não use conhecimento de fora do contexto.
4. Se a pergunta ou o contexto remeter a outra lei (Código Tributário Nacional, Constituição, outra lei
   complementar), diga que a regra está nessa outra norma e cite o dispositivo da LC 214 que faz a remissão.
   Não invente o conteúdo da outra norma.
5. Devolva "answerable": false somente se nenhum trecho do contexto tratar do assunto da pergunta; nesse caso,
   explique em uma frase o que falta.
6. Responda em português, de forma objetiva, em no máximo 5 frases."""

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


# Named prompt variants: each one changes ONE thing, and they compose with "+" ("verdict_last+answer_default").
# The variant is part of the fingerprint, so every experiment is its own report.
VARIANTS = {
    "baseline": "the first prompt: refusal rule first, the verdict (answerable) first in the JSON",
    "verdict_last": "the same prompt; the JSON asks for answer and citations BEFORE answerable (H1: a model that "
                    "writes the verdict first commits to it before reading its own answer)",
    "answer_default": "answering is the default, refusing the exception, stated last (H2)",
}


# Who decides "não consta" once the model has replied (barriers 2 and 3). Not part of the generation fingerprint: it
# changes nothing the model wrote, only what is shown. Every report scores BOTH, so switching costs no LLM time.
DECISIONS = {
    "model": "barrier 2 trusts the model's `answerable`, then barrier 3 needs a valid citation",
    "citations": "the model's `answerable` is recorded, not obeyed: a reply with a valid citation is shown (a small "
                 "model's self-assessment is not calibrated; a citation can be checked)",
}
FINAL_REFUSALS = ("clarify", "no_context", "invalid_json", "truncated")     # decided before the reply is read


def decide(pre_refusal: str | None, reply_answerable: bool | None, has_valid: bool, rule: str) -> tuple[bool, str | None]:
    """(answerable, refusal). The single place barriers 2 and 3 are applied: live and in --rescore."""
    if rule not in DECISIONS:
        raise GenError("lc214_gen_unknown_decision", f"unknown decision {rule!r}: use one of {sorted(DECISIONS)}")
    if pre_refusal in FINAL_REFUSALS:
        return False, pre_refusal
    if rule == "model" and not reply_answerable:
        return False, "model"
    if not has_valid:
        return False, "no_valid_citation"
    return True, None


@dataclass(frozen=True)
class PromptSpec:
    name: str
    system: str
    schema: dict


def prompt_spec(variant: str | None = None) -> PromptSpec:
    names = [n.strip() for n in (variant or VARIANT).split("+") if n.strip()]
    unknown = [n for n in names if n not in VARIANTS]
    if unknown or not names:
        raise GenError("lc214_gen_unknown_variant", f"unknown prompt variant {unknown or variant!r}: use one of "
                       f"{sorted(VARIANTS)}, composed with '+'")
    mods = [n for n in VARIANTS if n in names and n != "baseline"]          # canonical order: a+b == b+a
    system, schema = SYSTEM, SCHEMA
    if "answer_default" in mods:
        system = SYSTEM_ANSWER_DEFAULT
    if "verdict_last" in mods:
        order = ["answer", "citations", "answerable"]
        schema = {**SCHEMA, "properties": {k: SCHEMA["properties"][k] for k in order}, "required": order}
    return PromptSpec("+".join(mods) or "baseline", system, schema)


def llm_options() -> dict:
    return {"temperature": 0, "seed": SEED, "num_ctx": rt.NUM_CTX, "num_predict": rt.ANSWER_TOKENS}


def prompt_version(spec: PromptSpec | None = None) -> str:
    """Anything that changes what the model is asked changes this hash (and with it the fingerprint). No sort_keys:
    the ORDER of the schema's properties is the order the model writes them in (Ollama turns the schema into a
    grammar), so it is part of the prompt."""
    spec = spec or prompt_spec()
    body = json.dumps([spec.system, PROMPT, FOREIGN_NOTE, spec.schema, REFUSAL], ensure_ascii=False)
    return hashlib.md5(body.encode()).hexdigest()[:12]


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
    reply_answer: str | None = None                          # the model's own text, kept when it is not shown:
                                                             # a refusal says what the model found missing
    json_valid: bool | None = None
    done_reason: str | None = None
    prompt_words: int = 0
    prompt_tokens_estimate: int = 0
    prompt_eval_count: int | None = None
    eval_count: int | None = None
    latency_s: float | None = None
    raw_response: str | None = None                          # the model's reply as received (fixtures for CI, audit)
    load_s: float | None = None                              # Ollama's split of the latency (ns -> s): where the
    prompt_eval_s: float | None = None                       # time goes; on CPU the prompt dominates
    eval_s: float | None = None

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

    @property
    def prompt_eval_rate(self) -> float | None:
        return round(self.prompt_eval_count / self.prompt_eval_s, 1) if self.prompt_eval_count and self.prompt_eval_s \
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


def _seconds(ns) -> float | None:
    return round(ns / 1e9, 2) if isinstance(ns, (int, float)) else None


def answer(question: str, store: rt.Store, encode: Callable[[str], list[float]], llm: LLM,
           index: Index | None = None, k: int = rt.TOP_K, spec: PromptSpec | None = None,
           decision: str | None = None) -> Answer:
    spec, decision = spec or prompt_spec(), decision or DECISION
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
    a.prompt_words = len((spec.system + "\n" + prompt).split())
    a.prompt_tokens_estimate = rt.llm_tokens(spec.system + "\n" + prompt)
    t0 = time.monotonic()
    resp = llm.generate(spec.system, prompt, spec.schema, llm_options())
    a.latency_s = round(time.monotonic() - t0, 2)
    a.llm_called = True
    a.done_reason = resp.get("done_reason")
    a.prompt_eval_count, a.eval_count = resp.get("prompt_eval_count"), resp.get("eval_count")
    a.load_s, a.prompt_eval_s, a.eval_s = (_seconds(resp.get(f)) for f in ("load_duration", "prompt_eval_duration",
                                                                           "eval_duration"))

    a.raw_response = resp.get("response", "")
    reply = parse_reply(a.raw_response)
    a.json_valid = reply is not None
    if reply is None:
        a.refusal = "truncated" if a.done_reason == "length" else "invalid_json"
        return a
    a.reply_answerable, a.reply_answer = reply["answerable"], reply["answer"]
    shown = set(a.context_ids)
    a.citations = [check_citation(c, index, shown) for c in reply["citations"]]
    a.answerable, a.refusal = decide(None, reply["answerable"], bool(a.valid_ids), decision)   # barriers 2 and 3
    if a.answerable:
        a.answer = reply["answer"]
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


def dev_cases(golden: dict) -> set[str]:
    """The cases marked `dev: true`: a fixed subset (~11 phrasings, a few minutes) to iterate a prompt experiment on;
    the full run (~80 min on CPU) confirms only the variant that wins."""
    return {c["id"] for c in golden.get("cases") or [] if c.get("dev") and generated(c)}


def outcome(case: dict, phrasing: str, a: Answer) -> Outcome:
    expected = list(case.get("expected") or [])
    shown, cited = set(a.context_ids), a.valid_ids if a.answerable else []
    return Outcome(case["id"], case["type"], case.get("behavior", "answer"), phrasing, expected,
                   case.get("behavior") == "abstain", [p for p in expected if p in shown],
                   [p for p in expected if any(same_branch(p, c) for c in cited)],
                   asdict(a) | {"valid_ids": a.valid_ids, "violations": len(a.violations),
                                "prompt_eval_rate": a.prompt_eval_rate})


def progress_line(n: int, total: int, o: Outcome, a: Answer) -> str:
    """One line per phrasing, with what is needed to read a failure without opening the report: the model's own
    words when it refused and the citations that were dropped."""
    line = (f"  [{n}/{total}] {o.case}/{o.phrasing}: " + ("ANSWER" if a.answerable else f"REFUSE ({a.refusal})")
            + f" cites {len(a.valid_ids)} valid / {len(a.violations)} bad, expected {len(o.cited_expected)}/"
              f"{len(o.expected)} (context {len(o.in_context)}) {a.latency_s or 0:.1f}s")
    if a.violations:
        line += " | bad: " + "; ".join(f"{_short(c['text'], 40)} ({c['status']})" for c in a.violations)
    if not a.answerable and a.reply_answer:
        line += f' | model: "{_short(a.reply_answer, 100)}"'
    return line


def _short(text: str, n: int) -> str:
    text = " ".join(text.split())
    return text if len(text) <= n else text[:n - 1] + "…"


def run_golden(golden: dict, store: rt.Store, encode, llm: LLM, cases: set[str] | None = None,
               progress: Callable[[str], None] = print, spec: PromptSpec | None = None,
               decision: str | None = None) -> list[Outcome]:
    k = golden.get("k", 5)
    index = Index(store.ids())
    todo = [(c, p, q) for c in golden.get("cases") or [] if generated(c) and (not cases or c["id"] in cases)
            for p, q in (c.get("questions") or {}).items()]
    out = []
    for n, (case, phrasing, question) in enumerate(todo, 1):
        a = answer(str(question), store, encode, llm, index=index, k=k, spec=spec, decision=decision)
        o = outcome(case, phrasing, a)
        out.append(o)
        progress(progress_line(n, len(todo), o, a))
    return out


# --------------------------------------------------------------------------- aggregation
def _mean(values) -> float | None:
    values = list(values)
    return round(sum(values) / len(values), 4) if values else None


def rescore(o: Outcome, rule: str) -> Outcome:
    """The same reply under another decision rule: answerable, refusal, shown text and cited_expected recomputed from
    what the report recorded (the pre-decision refusal, the model's flag, the citations' statuses)."""
    a = dict(o.answer)
    if not a.get("llm_called"):                                # sql / clarify / no_context: no reply to re-decide
        return o
    pre = a.get("refusal") if a.get("refusal") in FINAL_REFUSALS else None
    valid = sorted({p for c in a.get("citations") or [] if c["status"] == "valid" for p in c["ids"]})
    a["answerable"], a["refusal"] = decide(pre, a.get("reply_answerable"), bool(valid), rule)
    a["answer"] = a.get("reply_answer") if a["answerable"] else REFUSAL
    cited = valid if a["answerable"] else []
    return Outcome(o.case, o.type, o.behavior, o.phrasing, o.expected, o.should_refuse, o.in_context,
                   [p for p in o.expected if any(same_branch(p, c) for c in cited)], a)


def outcome_from_dict(d: dict) -> Outcome:
    names = Outcome.__dataclass_fields__
    return Outcome(**{k: v for k, v in d.items() if k in names})


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
        # how often the model itself says it can answer: a value stuck at 0 or 1 is not a signal (ADR 0015, dev run 2)
        "model_says_answerable": _mean(bool(o.answer.get("reply_answerable")) for o in llm if o.answer["json_valid"]),
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
            "num_ctx": rt.NUM_CTX,
            "prompt_eval_rate_median": _median(o.answer["prompt_eval_rate"] for o in outcomes),
            "eval_rate_median": _median(o.answer["eval_count"] / o.answer["eval_s"] for o in outcomes
                                        if o.answer["eval_count"] and o.answer["eval_s"]),
            "prompt_share_of_latency": _median(o.answer["prompt_eval_s"] / o.answer["latency_s"] for o in outcomes
                                               if o.answer["prompt_eval_s"] and o.answer["latency_s"])}


def _median(values) -> float | None:
    values = [v for v in values if v is not None]
    return round(statistics.median(values), 2) if values else None


def answers_digest(outcomes: list[Outcome]) -> str:
    """One hash over every answer (text, flag, citations): two runs of the same fingerprint must print the same."""
    body = [[o.case, o.phrasing, o.answer["answerable"], o.answer["answer"], o.answer["citations"]] for o in outcomes]
    return hashlib.md5(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:12]


def metrics_by_decision(outcomes: list[Outcome]) -> dict:
    return {rule: {g: group_metrics([rescore(o, rule) for o in outcomes], g) for g in GROUPS} for rule in DECISIONS}


def report(outcomes: list[Outcome], fp: dict, key: str, subset: list[str] | None = None,
           decision: str | None = None) -> dict:
    decision = decision or DECISION
    by_decision = metrics_by_decision(outcomes)
    return {
        "gen_version": GEN_VERSION, "fingerprint_key": key,
        "variant": (fp.get("llm") or {}).get("variant"), "subset": subset, "decision": decision,
        "measured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fingerprint": fp, "answers_digest": answers_digest(outcomes),
        "metrics": by_decision[decision],
        "metrics_by_decision": by_decision,
        "calibration": calibration(outcomes),
        "outcomes": [asdict(o) | {"refused": o.refused, "refusal_correct": o.refusal_correct} for o in outcomes],
    }


def summary_line(rep: dict) -> str:
    def fmt(v):
        return "-" if v is None else f"{v:.3f}"

    parts = [f"{g} (n={m['n']}): refusal_acc {fmt(m['refusal_accuracy'])} cit_validity {fmt(m['citation_validity'])}"
             f" cit_recall {fmt(m['citation_recall'])} (in context {fmt(m['citation_recall_in_context'])})"
             f" json {fmt(m['json_valid'])} p50 " + ("-" if m['latency_p50_s'] is None else f"{m['latency_p50_s']}s")
             for g, m in rep["metrics"].items()]
    c = rep["calibration"]
    parts.append(f"tokens/word {c['measured_median']} measured vs {c['estimate_tokens_per_word']} estimated"
                 + (f" ({c['cache_suspect']} cache-suspect)" if c["cache_suspect"] else "")
                 + f"; prompt {c['prompt_eval_rate_median']} tok/s, answer {c['eval_rate_median']} tok/s")
    other = [f"{rule}: " + ", ".join(f"{g} refusal_acc {fmt(m[g]['refusal_accuracy'])} cit_recall "
                                     f"{fmt(m[g]['citation_recall'])}" for g in m)
             for rule, m in (rep.get("metrics_by_decision") or {}).items() if rule != rep.get("decision")]
    says = "/".join(fmt(m.get("model_says_answerable")) for m in rep["metrics"].values())
    return (f"[{rep.get('variant') or 'baseline'}, decision {rep.get('decision')}] " + " | ".join(parts)
            + f" | model says answerable {says}" + (f" | under {'; '.join(other)}" if other else "")
            + f" | digest {rep['answers_digest']}")


# --------------------------------------------------------------------------- what is being generated
def fingerprint(cur, llm: LLM, golden_path: Path, k: int, spec: PromptSpec | None = None) -> tuple[dict, str]:
    from pipelines.rag.lc214.lc05_eval import EvalError, fingerprint as store_fingerprint

    try:
        fp = store_fingerprint(cur, golden_path, k)
    except EvalError as exc:
        raise GenError(exc.event, str(exc)) from None
    spec = spec or prompt_spec()
    fp["llm"] = {"model": llm.model, "digest": llm.digest(), "options": llm_options(), "variant": spec.name,
                 "prompt_version": prompt_version(spec)}
    fp["gen_version"] = GEN_VERSION
    key = hashlib.md5(json.dumps(fp, sort_keys=True).encode()).hexdigest()[:12]
    return fp, key


# --------------------------------------------------------------------------- the step
def run(cur, encode, llm: LLM, lake=None, golden_path: Path = GOLDEN_PATH, cases: set[str] | None = None,
        force: bool = False, dev: bool = False, variant: str | None = None, decision: str | None = None) -> int:
    step = current_step()
    spec, decision = prompt_spec(variant), decision or DECISION
    decide(None, True, True, decision)                         # an unknown rule fails before an hour of generation
    golden = load_golden(golden_path)
    extra = set(cases or ())
    if dev:
        cases = (cases or set()) | dev_cases(golden)
        if not cases:
            raise GenError("lc214_gen_no_dev_cases", "no case is marked `dev: true` in the golden set")
    k = golden.get("k", 5)
    fp, key = fingerprint(cur, llm, golden_path, k, spec)
    if golden.get("law_version") != fp["law_version"]:
        raise GenError("lc214_gen_labels_stale", f"golden set labelled against {golden.get('law_version')}, vectors "
                       f"are of {fp['law_version']}: the golden contract (G2) must pass first")

    # The full run: one report per fingerprint, and a re-run is a no-op (exit 99). A subset (--dev, --case) is an
    # experiment: it never skips and keeps every run (answers/dev/...-<time>.json), so variants can be compared and
    # two runs of the same variant must show the same digest.
    report_key = None
    if lake is not None:
        from pipelines.rag.lc214.lake import k_manifest, k_silver
        root = k_silver(lake.get_json(k_manifest())["current"], "answers")
        if cases:
            tag = "dev" if dev and not extra else "cases-" + hashlib.md5(",".join(sorted(cases)).encode()).hexdigest()[:8]
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            report_key = f"{root}/dev/{key}-{tag}-{stamp}.json"
        else:
            report_key = f"{root}/{key}.json"
        if not cases and not force and lake.exists(report_key):
            log_event(logger, "lc214_gen_up_to_date", f"answers for fingerprint {key} already in {lake.url(report_key)}"
                      " (same law, chunks, golden set, model digest and prompt): nothing to generate",
                      fingerprint=key)
            return EXIT_SKIPPED

    log_event(logger, "lc214_gen_started", f"model {llm.model}@{fp['llm']['digest']} variant {spec.name} prompt "
              f"{fp['llm']['prompt_version']} decision {decision} fingerprint {key}"
              + (f" subset {len(cases)} cases" if cases else ""),
              fingerprint=key, model=llm.model, variant=spec.name)
    t0 = time.monotonic()
    outcomes = run_golden(golden, rt.PgStore(cur), encode, llm, cases, spec=spec, decision=decision)
    rep = report(outcomes, fp, key, sorted(cases) if cases else None, decision)
    line = summary_line(rep)
    log_event(logger, "lc214_gen_measured", line, fingerprint=key, digest=rep["answers_digest"], variant=spec.name,
              duration_s=round(time.monotonic() - t0, 1),
              **{f"{g}_{m}": rep["metrics"][g][m] for g in GROUPS
                 for m in ("refusal_accuracy", "citation_validity", "citation_recall", "citation_recall_in_context")})
    for g in GROUPS:
        step.set(**{f"{g}_{m}": v for m, v in rep["metrics"][g].items() if isinstance(v, (int, float))})
    step.set(tokens_per_word=rep["calibration"]["measured_median"])
    step.label(variant=spec.name, decision=decision)
    step.add(rows_in=len(outcomes), rows_out=sum(not o.refused for o in outcomes))

    if report_key:
        lake.put_json(report_key, rep)
        log_event(logger, "lc214_gen_report_written", lake.url(report_key), fingerprint=key)
    return EXIT_OK


# --------------------------------------------------------------------------- re-deciding recorded replies
def rescore_table(rep: dict) -> list[str]:
    """A report's replies under every decision rule: what would have been shown, without calling the LLM again."""
    outcomes = [outcome_from_dict(d) for d in rep["outcomes"]]

    def fmt(v):
        return "-" if v is None else f"{v:.3f}"

    head = (f"{rep.get('variant') or 'baseline'} | fingerprint {rep.get('fingerprint_key')} | "
            f"{len(outcomes)} phrasings | measured {rep.get('measured_at')} | digest {rep.get('answers_digest')}")
    lines = [head]
    for rule in DECISIONS:
        for g in GROUPS:
            m = group_metrics([rescore(o, rule) for o in outcomes], g)
            if not m["n"]:
                continue
            lines.append(f"  {rule:<9} {g:<6} n={m['n']:<2} refusal_acc {fmt(m['refusal_accuracy'])} "
                         f"(false refusals {m['false_refusals']}, false answers {m['false_answers']}) "
                         f"cit_validity {fmt(m['citation_validity'])} cit_recall {fmt(m['citation_recall'])} "
                         f"(in context {fmt(m['citation_recall_in_context'])}) model says answerable "
                         f"{fmt(m['model_says_answerable'])}")
    return lines


def rescore_detail(rep: dict, rule: str) -> list[str]:
    out = []
    for d in rep["outcomes"]:
        o = rescore(outcome_from_dict(d), rule)
        a = o.answer
        verdict = "ANSWER" if a["answerable"] else f"REFUSE ({a['refusal']})"
        ok = "ok " if o.refusal_correct else "BAD"
        valid = sorted({p for c in a.get("citations") or [] if c["status"] == "valid" for p in c["ids"]})
        out.append(f"  {ok} {o.case}/{o.phrasing}: {verdict} expected {_ids(o.expected)} cited {_ids(o.cited_expected)}"
                   f" valid {_ids(valid)} (in context {len(o.in_context)}/{len(o.expected)}) | "
                   f'model ({a.get("reply_answerable")}): "{_short(a.get("reply_answer") or "", 160)}"')
    return out


def _ids(ids: list[str]) -> str:
    return "[" + ", ".join(full_label(p) for p in ids) + "]" if ids else "[]"


def report_keys(lake, refs: list[str]) -> list[str]:
    """A ref is a lake key, its s3:// / file:// URL, or `dev` (every dev report of the current law version)."""
    from pipelines.rag.lc214.lake import k_manifest, k_silver

    keys = []
    for ref in refs:
        if ref == "dev":
            prefix = k_silver(lake.get_json(k_manifest())["current"], "answers/dev")
            keys += [k for k in lake.keys(prefix) if k.endswith(".json")]
        elif ref.startswith("s3://"):
            keys.append(ref.removeprefix("s3://"))
        elif ref.startswith("file://") and hasattr(lake, "root"):
            keys.append("/".join(os.path.relpath(ref.removeprefix("file://"), lake.root).split(os.sep)))
        else:
            keys.append(ref)
    return keys


def run_rescore(lake, refs: list[str], detail: str | None = None, out: Callable[[str], None] = print) -> int:
    keys = report_keys(lake, refs)
    if not keys:
        raise GenError("lc214_gen_rescore_empty", f"no report found for {refs}")
    for key in keys:
        rep = lake.get_json(key)
        if not rep or "outcomes" not in rep:
            raise GenError("lc214_gen_rescore_not_a_report", f"{key} is not an lc06 report")
        out("\n".join(rescore_table(rep)))
        if detail:
            out("\n".join(rescore_detail(rep, detail)))
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
    ap.add_argument("--case", action="append", default=[], help="with --golden: only this case (repeatable, report "
                    "in answers/dev/)")
    ap.add_argument("--dev", action="store_true", help="with --golden: only the cases marked dev: true (report in "
                    "answers/dev/, never skipped)")
    ap.add_argument("--variant", default=None, help=f"prompt variant (LC214_GEN_VARIANT, default {VARIANT}): "
                    f"{', '.join(VARIANTS)}, composed with '+'")
    ap.add_argument("--decision", default=None, help=f"who decides 'não consta' after the reply (LC214_GEN_DECISION, "
                    f"default {DECISION}): {', '.join(DECISIONS)}")
    ap.add_argument("--rescore", nargs="+", metavar="REPORT", help="re-decide recorded replies under every decision "
                    "rule, without the LLM: lake keys, s3:// URLs, or 'dev' (every dev report of the current law)")
    ap.add_argument("--detail", default=None, choices=sorted(DECISIONS), help="with --rescore: one line per phrasing "
                    "under this rule")
    ap.add_argument("--force", action="store_true", help="generate even when the report for this fingerprint exists")
    ap.add_argument("--context", action="store_true", help="single question: also print the context sent")
    ap.add_argument("--lake", default=None, help="'s3' (default, LC214_LAKE), file:///dir, or 'none' (no report)")
    ap.add_argument("--golden-path", default=str(GOLDEN_PATH))
    args = ap.parse_args(argv)
    if args.rescore:                                           # read-only: no Postgres, no models, no Ollama
        from pipelines.rag.lc214.lake import LakeError, lake_from_env
        try:
            return run_rescore(lake_from_env(None if args.lake in (None, "none") else args.lake), args.rescore,
                               args.detail)
        except (GenError, LakeError) as exc:
            log_event(logger, exc.event, str(exc), level=40)
            return EXIT_FAILED
    if bool(args.question) == args.golden:
        ap.error("give a question or --golden (or --rescore)")
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
                    return run(cur, encode, llm, lake, Path(args.golden_path), set(args.case), args.force, args.dev,
                               args.variant, args.decision)
                a = answer(args.question, rt.PgStore(cur), encode, llm, spec=prompt_spec(args.variant),
                           decision=args.decision)
                if args.context:
                    print(rt.retrieve(args.question, rt.PgStore(cur), encode).context() + "\n")
        finally:
            conn.close()
        print(json.dumps(a.public(), ensure_ascii=False, indent=2))
        print(f"variant={prompt_spec(args.variant).name} decision={args.decision or DECISION} route={a.route} refusal={a.refusal} blocks={a.blocks} citations="
              f"{[(c['text'], c['status']) for c in a.citations]}\n"
              f"latency={a.latency_s}s (load {a.load_s}s, prompt {a.prompt_eval_s}s at {a.prompt_eval_rate} tok/s, "
              f"answer {a.eval_s}s) prompt_eval_count={a.prompt_eval_count} (estimate {a.prompt_tokens_estimate}) "
              f"done_reason={a.done_reason}" + (f"\nmodel said: {a.reply_answer}" if a.reply_answer and not a.answerable
                                                else ""), file=sys.stderr)
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
