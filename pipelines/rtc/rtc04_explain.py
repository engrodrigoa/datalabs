"""Why does a cClassTrib have its treatment? A cited explanation per official classification (ADR 0022).

Step 4 of the RTC reference pipeline. The bridge (rtc03, ADR 0021) already says WHICH provisions of the LC 214 ground
each classification; this step asks the local LLM to explain the treatment from exactly those provisions, plus the
facts of the Receita's table, and to cite the law. No vector search: the anchor is deterministic, so the hard part of
the LC 214 RAG (the auditor's words against the law's, ADR 0015) is not in the way.

    silver.rtc_cclasstrib (+ reductions) ---+
    ai.rtc_lc214_fundamento (the bridge) ---+--> provisions -> article blocks (retriever.build_block, no hits)
                                                 -> prompt: law first, table facts last -> qwen2.5:3b (JSON Schema)
                                                 -> barriers (lc06) -> ai.rtc_cclasstrib_explicacao

Barriers (the same code as lc06): the model's `answerable`, then every citation, from the JSON field and from the
text, resolved by the router's parser and checked against the provisions the context SHOWED. An explanation that
cites nothing it was shown is not shown ("refused"). A classification with no linked provision ("Regra Geral",
another law only) gets a fixed text, without the LLM (`no_anchor`).

Checks per row, deterministic, stored with it:
    cita_ponte         a valid citation falls on a provision of the bridge (or inside it): the explanation rests on
                       the text the Receita points at, not on a neighbour in the same article
    numeros_sem_fonte  percentages the explanation states that are neither in the table's facts nor in the law shown
                       (an invented rate is the costliest error for an auditor)

Incremental and resumable (like the vector sync, ADR 0007): each row has a fingerprint of everything that decides
its text (prompt with context and facts, prompt version, model and its digest, EXPLAIN_VERSION). A run generates
only the rows whose fingerprint changed, golden cases first, then grouped by provisions (the law comes first in the
prompt: consecutive calls share a prefix Ollama can reuse). Each row commits on its own, so a run that stops
(time budget, a crash) is continued by the next one; rows waiting for it are `pendente`.

Gate (--gate): the golden cases (explain_golden.yml, generated first) and the batch metrics against
explain_baseline.yml, within RTC_EXPLAIN_TOLERANCE_CASES; percentages without a source may never grow. Without a
baseline the gate warns and passes: the first full run in the lab measures, `--write-baseline` records it.

Run in the lab (Postgres, the law served, the Ollama of profile `ai`):
    python -m pipelines.rtc.rtc04_explain                     the batch (DAG task explain_cclasstrib: --gate)
    python -m pipelines.rtc.rtc04_explain 200038              explain one cClassTrib now, print it (nothing written)
    python -m pipelines.rtc.rtc04_explain --review            the golden cases as stored, for a human verdict
    python -m pipelines.rtc.rtc04_explain --write-baseline    record the current metrics as the gate's baseline
Exit codes: 0 generated | 99 nothing to do, no Ollama, law or bridge not ready | 1 failure (incl. the gate).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable

import yaml

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.ai.llm_usage import from_ollama, record_llm_call  # noqa: E402
from pipelines.commons.logger import get_logger, log_event  # noqa: E402
from pipelines.observability import current_step  # noqa: E402
from pipelines.rag.lc214 import lc06_generate as gen  # noqa: E402
from pipelines.rag.lc214 import retriever as rt  # noqa: E402
from pipelines.rag.lc214.router import Index  # noqa: E402
from pipelines.rtc.rtc03_lc214 import BRIDGE_VERSION, LINKED  # noqa: E402

logger = get_logger("rtc04_explain")

EXIT_OK, EXIT_FAILED, EXIT_SKIPPED = 0, 1, 99
# Bump when what a row means changes (prompt rules, the checks): every row is regenerated on the next runs.
EXPLAIN_VERSION = "1"
TABLE = "ai.rtc_cclasstrib_explicacao"
USAGE_APP = "rtc_explain"                     # ai.llm_call.app (ADR 0018)
MAX_MINUTES = float(os.getenv("RTC_EXPLAIN_MAX_MINUTES", "80"))     # no new call starts after this; the rest waits
TOLERANCE_CASES = float(os.getenv("RTC_EXPLAIN_TOLERANCE_CASES", "1"))
GOLDEN_PATH = Path(__file__).with_name("explain_golden.yml")
BASELINE_PATH = Path(__file__).with_name("explain_baseline.yml")
OUTCOMES = ("explained", "refused", "no_anchor", "invalid_json", "truncated")

SYSTEM = """Você explica a um auditor fiscal por que uma classificação tributária (cClassTrib) da tabela oficial da
Receita Federal tem o tratamento que tem, com base na Lei Complementar nº 214/2025 (IBS, CBS e Imposto Seletivo).

Regras:
1. Use somente o contexto: os trechos da LC 214/2025 que a Receita indica como fundamento da classificação e os dados
   da tabela. Não use conhecimento de fora do contexto.
2. Diga o que a classificação abrange e qual é o tratamento (redução de alíquota, alíquota zero, isenção, suspensão,
   diferimento, não incidência, Imposto Seletivo etc.), ligando cada afirmação ao dispositivo da lei que a sustenta.
3. Liste em "citations" os dispositivos da LC 214/2025 que sustentam a explicação: "artigo" é o número do artigo
   (ex.: "138", "7-A") e "dispositivo" é a parte dentro dele ("caput", "§ 1º", "inciso II", "§ 2º, inciso III").
4. Os dados da tabela (descrição, tratamento, percentuais) são fatos da Receita: use-os como estão. Não calcule nem
   invente percentuais ou alíquotas.
5. Devolva "answerable": false somente se nenhum trecho do contexto sustentar o tratamento da tabela; nesse caso,
   diga em uma frase o que falta.
6. Responda em português, de forma objetiva, em no máximo 5 frases."""

# The law first and the table last: classifications grounded in the same provisions share the whole prefix
# (system + context), which Ollama can keep in its cache between consecutive calls.
PROMPT = ("Contexto (trechos da LC 214/2025 que a Receita indica como fundamento):\n{context}\n\n"
          "Dados da tabela oficial da Receita (cClassTrib):\n{facts}\n\n"
          "Pergunta: por que a classificação {cclasstrib} tem esse tratamento?")
REFUSAL = ("Não foi possível explicar esta classificação com os trechos da LC 214/2025 que a Receita indica como "
           "fundamento.")
NO_ANCHOR = ("A tabela da Receita não liga esta classificação a um dispositivo da LC 214/2025 (fundamentação "
             "publicada: {ref}). Sem o trecho da lei, a explicação não é gerada. Tratamento na tabela: {tratamento}.")
GRUPO = {"IBSCBS": "IBS e CBS", "IS": "Imposto Seletivo"}
TRIBUTO = {"CBS": "CBS", "IBSUF": "IBS estadual", "IBSMun": "IBS municipal"}


class ExplainError(Exception):
    def __init__(self, event: str, message: str):
        super().__init__(message)
        self.event = event


def prompt_version() -> str:
    """Anything that changes what the model is asked, besides the row's own context and facts."""
    body = json.dumps([SYSTEM, PROMPT, gen.SCHEMA, REFUSAL, gen.llm_options()], ensure_ascii=False)
    return hashlib.md5(body.encode()).hexdigest()[:12]


# --------------------------------------------------------------------------- the classification
@dataclass
class Classe:
    """One row of silver.rtc_cclasstrib, with its official reductions and its bridge rows."""
    cltr_id: int
    grupo_tributo: str
    cclasstrib: str
    descricao: str
    cst: str | None = None
    descricao_cst: str | None = None
    tipo_aliquota: str | None = None
    tratamento: str | None = None
    anexo_lc214: str | None = None
    fundamentacao_legal: str | None = None
    dt_inicio_vigencia: object = None
    dt_fim_vigencia: object = None
    reducoes: dict[str, Decimal] = field(default_factory=dict)     # tributo -> % (the latest rule of each tax)
    ponte: list[dict] = field(default_factory=list)                # [{outcome, dispositivo_id}] from rtc03

    @property
    def anchors(self) -> list[str]:
        """The provisions the bridge linked, in citation order, each once."""
        out = []
        for p in self.ponte:
            if p["outcome"] in LINKED and p["dispositivo_id"] not in out:
                out.append(p["dispositivo_id"])
        return out

    @property
    def partial(self) -> bool:
        """Some citations of the reference are linked and others are not (ambiguous, not found)."""
        return bool(self.anchors) and any(p["outcome"] in ("ambiguous", "not_found") for p in self.ponte)


def pct(value) -> str:
    """60.0000 -> '60%', 12.5 -> '12,5%' (the Portuguese decimal comma, as the law writes it)."""
    d = Decimal(str(value)).normalize()
    text = format(d, "f")
    return text.replace(".", ",") + "%"


def roman(value) -> str:
    """'9' -> 'IX': the law names its annexes in roman numerals (the table stores the number)."""
    text = str(value).strip()
    if not text.isdigit() or not 0 < int(text) < 40:
        return text
    n, out = int(text), ""
    for v, r in ((10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I")):
        while n >= v:
            out, n = out + r, n - v
    return out


def facts(c: Classe) -> str:
    """The table's facts, one per line, deterministic (part of the fingerprint: no 'today' in here)."""
    lines = [f"cClassTrib {c.cclasstrib}: {(c.descricao or '').strip()}",
             f"Tributos: {GRUPO.get(c.grupo_tributo, c.grupo_tributo)}"]
    if c.cst:
        lines.append(f"CST {c.cst}: {(c.descricao_cst or '').strip()}".rstrip(": "))
    if c.tratamento:
        lines.append(f"Tratamento: {c.tratamento.strip()}")
    if c.tipo_aliquota:
        lines.append(f"Tipo de alíquota: {c.tipo_aliquota.strip()}")
    order = [*TRIBUTO, *sorted(set(c.reducoes) - set(TRIBUTO))]           # CBS, IBS estadual, IBS municipal
    cuts = [f"{TRIBUTO.get(t, t)} {pct(c.reducoes[t])}" for t in order
            if t in c.reducoes and c.reducoes[t] and Decimal(str(c.reducoes[t])) > 0]
    if cuts:                       # 0 in the table means "no reduction", not "rate zero": not stated at all
        lines.append("Redução da alíquota: " + "; ".join(cuts))
    if c.anexo_lc214:
        lines.append(f"Anexo da LC 214/2025: {roman(c.anexo_lc214)}")
    if c.fundamentacao_legal:
        lines.append(f"Fundamentação legal publicada pela Receita: {c.fundamentacao_legal.strip()}")
    if c.dt_inicio_vigencia:
        end = f" até {c.dt_fim_vigencia}" if c.dt_fim_vigencia else ""
        lines.append(f"Vigência na tabela: desde {c.dt_inicio_vigencia}{end}")
    return "\n".join(lines)


def build_context(store: rt.Store, anchors: list[str]) -> list[rt.Block]:
    """One block per article of the anchors (whole when it fits, else caput + the cited subtree), under the budget.
    No vector hits: what is shown is decided by the bridge alone."""
    by_art: dict[str, list[str]] = {}
    for a in anchors:
        by_art.setdefault(rt.art_of(a), []).append(a)
    blocks, left = [], rt.context_budget()
    for art_id, ps in by_art.items():
        limit = max(min(rt.ANCHOR_MAX_TOKENS, left), rt.MIN_BLOCK_TOKENS)      # a cited provision is never dropped
        block = rt.build_block(store, art_id, ps, [], limit, [])
        blocks.append(block)
        left -= block.tokens
    return blocks


def context_text(blocks: list[rt.Block]) -> str:
    return "\n\n".join(f"[{n}] {b.text}" for n, b in enumerate(blocks, 1))


def build_prompt(c: Classe, blocks: list[rt.Block]) -> str:
    return PROMPT.format(context=context_text(blocks), facts=facts(c), cclasstrib=c.cclasstrib)


def row_fingerprint(*parts) -> str:
    return hashlib.md5(json.dumps([EXPLAIN_VERSION, *parts], ensure_ascii=False, default=str).encode()).hexdigest()


# --------------------------------------------------------------------------- deterministic checks
_PCT = re.compile(r"(\d{1,3}(?:[.,]\d+)?)\s*(?:%|por\s+cento)", re.IGNORECASE)


def percents(text: str | None) -> set[Decimal]:
    out = set()
    for m in _PCT.finditer(text or ""):
        try:
            out.add(Decimal(m.group(1).replace(",", ".")).normalize())
        except InvalidOperation:
            continue
    return out


def ungrounded_numbers(answer: str, *sources: str) -> list[str]:
    """Percentages in the explanation that no source states (table facts, law shown)."""
    known = set().union(*(percents(s) for s in sources)) if sources else set()
    return sorted(pct(p) for p in percents(answer) - known)


def in_branch(cited: str, anchor: str) -> bool:
    """A citation of the anchor itself or of a part of it (inciso II of the cited § 1º). The article's caput for an
    anchored paragraph does NOT count: it points the reader at the wrong text."""
    return cited == anchor or cited.startswith(anchor + ":")


def cites_bridge(valid_ids: list[str], anchors: list[str]) -> bool:
    return any(in_branch(v, a) for v in valid_ids for a in anchors)


# --------------------------------------------------------------------------- one classification
@dataclass
class Explanation:
    cltr_id: int
    grupo_tributo: str
    cclasstrib: str
    outcome: str
    explicacao: str
    recusa: str | None = None                 # model | no_valid_citation | invalid_json | truncated
    resposta_modelo: str | None = None        # the model's own text when it is not shown (what it found missing)
    fontes: list[str] = field(default_factory=list)                # "art. 138, § 2º": the law cited as source
    dispositivos_ponte: list[str] = field(default_factory=list)
    dispositivos_contexto: list[str] = field(default_factory=list)
    dispositivos_citados: list[str] = field(default_factory=list)
    citacoes: list[dict] = field(default_factory=list)
    cita_ponte: bool | None = None
    numeros_sem_fonte: list[str] = field(default_factory=list)
    violacoes: int = 0
    ancora_parcial: bool = False
    context_tokens: int = 0
    prompt_eval_count: int | None = None
    eval_count: int | None = None
    latency_s: float | None = None
    row_fingerprint: str = ""
    model: str | None = None
    model_digest: str | None = None
    prompt_version: str | None = None

    def sha(self) -> str:
        """What a reviewer read: the shown text and its sources (a review is stale once this changes)."""
        return hashlib.md5(json.dumps([self.explicacao, self.fontes], ensure_ascii=False).encode()).hexdigest()[:12]


@dataclass
class Task:
    """A classification ready to explain: its prompt (or fixed text) and the fingerprint of it."""
    classe: Classe
    blocks: list[rt.Block]
    prompt: str | None
    fingerprint: str
    group: str                                # the anchors, as a sort key: same context -> consecutive calls


def prepare(c: Classe, store: rt.Store, model: str, digest: str, pv: str) -> Task:
    anchors = c.anchors
    if not anchors:
        text = no_anchor_text(c)
        return Task(c, [], None, row_fingerprint("no_anchor", text), "")
    blocks = build_context(store, anchors)
    prompt = build_prompt(c, blocks)
    return Task(c, blocks, prompt, row_fingerprint(pv, model, digest, SYSTEM, prompt), "|".join(anchors))


def no_anchor_text(c: Classe) -> str:
    ref = (c.fundamentacao_legal or "sem fundamentação").strip()
    return NO_ANCHOR.format(ref=ref, tratamento=(c.tratamento or "não informado").strip())


def explain(task: Task, llm: gen.LLM, index: Index, decision: str | None = None, pv: str | None = None) -> Explanation:
    c, decision = task.classe, decision or gen.DECISION
    e = Explanation(c.cltr_id, c.grupo_tributo, c.cclasstrib, "no_anchor", "", row_fingerprint=task.fingerprint,
                    dispositivos_ponte=c.anchors, ancora_parcial=c.partial)
    if task.prompt is None:                                    # barrier 1: nothing to ground an explanation on
        e.explicacao = no_anchor_text(c)
        return e

    e.dispositivos_contexto = sorted({p for b in task.blocks for p in b.dispositivo_ids})
    e.context_tokens = sum(b.tokens for b in task.blocks)
    e.model, e.model_digest, e.prompt_version = llm.model, getattr(llm, "model_digest", None), pv or prompt_version()
    options = gen.llm_options()
    t0 = time.monotonic()
    resp = llm.generate(SYSTEM, task.prompt, gen.SCHEMA, options)
    e.latency_s = round(time.monotonic() - t0, 2)
    usage = from_ollama(resp, app=USAGE_APP, model=llm.model, prompt=task.prompt, system=SYSTEM,
                        latency_s=e.latency_s, model_digest=e.model_digest, route="anchor",
                        num_ctx=options.get("num_ctx"),
                        tags={"cclasstrib": c.cclasstrib, "grupo": c.grupo_tributo, "anchors": len(c.anchors)})
    try:
        return _read_reply(e, task, resp, index, decision)
    finally:
        usage.tags.update({"outcome": e.outcome} | ({"recusa": e.recusa} if e.recusa else {}))
        record_llm_call(usage)


def _read_reply(e: Explanation, task: Task, resp: dict, index: Index, decision: str) -> Explanation:
    e.prompt_eval_count, e.eval_count = resp.get("prompt_eval_count"), resp.get("eval_count")
    reply = gen.parse_reply(resp.get("response", ""))
    if reply is None:
        e.outcome = "truncated" if resp.get("done_reason") == "length" else "invalid_json"
        e.recusa, e.explicacao = e.outcome, REFUSAL
        return e
    shown = set(e.dispositivos_contexto)
    a = gen.Answer(f"cClassTrib {e.cclasstrib}", "anchor", "explain", context_ids=sorted(shown))
    a.citations = [gen.check_citation(cit, index, shown) for cit in reply["citations"]]
    a.text_citations, a.cites_text = gen.text_citations(reply["answer"], index, shown), decision == "model_text"
    a.answerable, a.refusal = gen.decide(None, reply["answerable"], bool(a.valid_ids), decision)
    e.citacoes, e.violacoes = a.counted, len(a.violations)
    e.dispositivos_citados = a.valid_ids
    if not a.answerable:
        e.outcome, e.recusa, e.explicacao, e.resposta_modelo = "refused", a.refusal, REFUSAL, reply["answer"]
        return e
    e.outcome, e.explicacao = "explained", reply["answer"]
    e.fontes = [gen.full_label(p) for p in a.valid_ids]
    e.cita_ponte = cites_bridge(a.valid_ids, e.dispositivos_ponte)
    e.numeros_sem_fonte = ungrounded_numbers(reply["answer"], facts(task.classe), context_text(task.blocks))
    return e


# --------------------------------------------------------------------------- planning
def plan(tasks: list[Task], existing: dict[int, str], golden_ids: set[int], force: bool = False) -> list[Task]:
    """The tasks whose fingerprint is not stored yet: golden cases first (the gate can judge a partial run), then
    grouped by anchors (shared prompt prefix), then by code."""
    todo = [t for t in tasks if force or existing.get(t.classe.cltr_id) != t.fingerprint]
    return sorted(todo, key=lambda t: (t.classe.cltr_id not in golden_ids, t.group, t.classe.cclasstrib,
                                       t.classe.grupo_tributo, t.classe.cltr_id))


# --------------------------------------------------------------------------- golden set and metrics
def load_golden(path: Path = GOLDEN_PATH) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def golden_violations(golden: dict, law_ids: set[str]) -> list[str]:
    """Labels are data: an id that left the law (an amendment, a renumbering) makes the case stale."""
    out = []
    seen = set()
    for case in golden.get("cases") or []:
        cid = case.get("id", "?")
        if not re.fullmatch(r"explain-\d{2}", str(cid)) or cid in seen:
            out.append(f"{cid}: id must be unique and look like explain-NN")
        seen.add(cid)
        if case.get("outcome", "explained") not in ("explained", "no_anchor"):
            out.append(f"{cid}: outcome must be explained or no_anchor")
        for pid in case.get("expected") or []:
            if pid not in law_ids:
                out.append(f"{cid}: {pid} is not a provision of the law served")
        if case.get("outcome") == "no_anchor" and case.get("expected"):
            out.append(f"{cid}: a no_anchor case expects no provision")
    return out


def golden_targets(golden: dict, classes: list[Classe]) -> dict[str, int]:
    """case id -> cltr_id: the code in its tax group, the latest start of validity."""
    best: dict[tuple, Classe] = {}
    for c in classes:
        key = (c.grupo_tributo, c.cclasstrib)
        if key not in best or str(c.dt_inicio_vigencia or "") > str(best[key].dt_inicio_vigencia or ""):
            best[key] = c
    out = {}
    for case in golden.get("cases") or []:
        c = best.get((case.get("grupo", "IBSCBS"), str(case["cclasstrib"])))
        if c:
            out[case["id"]] = c.cltr_id
    return out


def golden_eval(golden: dict, targets: dict[str, int], rows: dict[int, dict]) -> list[dict]:
    """Per case: outcome as expected, cites an expected provision (or a part of it), states the expected numbers;
    plus the human review status against the text as it is now."""
    out = []
    for case in golden.get("cases") or []:
        cid, row = case["id"], rows.get(targets.get(case["id"], -1))
        want = case.get("outcome", "explained")
        res = {"case": cid, "cclasstrib": str(case["cclasstrib"]), "grupo": case.get("grupo", "IBSCBS"),
               "present": row is not None and not row.get("pendente")}
        if not res["present"]:
            out.append(res)
            continue
        cited = row.get("dispositivos_citados") or []
        expected = case.get("expected") or []
        stated = percents(row.get("explicacao"))
        res |= {"outcome": row["outcome"], "outcome_ok": row["outcome"] == want,
                "cites_expected": None if want != "explained" else
                any(in_branch(v, a) for v in cited for a in expected),
                "mentions_ok": None if want != "explained" else
                all(Decimal(str(n)).normalize() in stated for n in case.get("mentions") or [])}
        review = case.get("review") or {}
        verdict = review.get("verdict", "pending")
        res["review"] = "stale" if verdict != "pending" and review.get("sha") != row.get("sha") else verdict
        out.append(res)
    return out


def _rate(num: int, den: int) -> dict:
    return {"value": round(100 * num / den, 2) if den else None, "n": den}


def metrics(rows: list[dict], golden: list[dict]) -> dict:
    current = [r for r in rows if not r.get("pendente")]
    anchored = [r for r in current if r["outcome"] != "no_anchor"]
    explained = [r for r in current if r["outcome"] == "explained"]
    g = [x for x in golden if x["present"]]
    g_exp = [x for x in g if x.get("cites_expected") is not None]
    return {
        "rows": len(rows), "pending": len(rows) - len(current),
        "outcomes": dict(Counter(r["outcome"] for r in current)),
        "explained_pct": _rate(len(explained), len(anchored)),
        "cites_bridge_pct": _rate(sum(1 for r in explained if r.get("cita_ponte")), len(explained)),
        "numbers_ungrounded": sum(1 for r in explained if r.get("numeros_sem_fonte")),
        "violations": sum(r.get("violacoes") or 0 for r in current),
        "golden_present": len(g), "golden_total": len(golden),
        "golden_outcome_pct": _rate(sum(1 for x in g if x["outcome_ok"]), len(g)),
        "golden_cites_expected_pct": _rate(sum(1 for x in g_exp if x["cites_expected"]), len(g_exp)),
        "golden_mentions_pct": _rate(sum(1 for x in g_exp if x["mentions_ok"]), len(g_exp)),
        "review": dict(Counter(x["review"] for x in g)),
    }


GATED_BATCH = ("explained_pct", "cites_bridge_pct")
GATED_GOLDEN = ("golden_outcome_pct", "golden_cites_expected_pct", "golden_mentions_pct")


def gate(m: dict, base: dict | None, tolerance_cases: float = TOLERANCE_CASES) -> tuple[list[dict], list[str]]:
    """(failures, notes). Each rate may drop at most `tolerance_cases` of its own denominator below the baseline;
    percentages without a source may not grow at all. The golden part is judged once every golden case is current;
    the batch part once nothing is pending."""
    if not base:
        return [], ["no baseline (explain_baseline.yml): measured, not gated; record it with --write-baseline"]
    failures, notes = [], []
    golden_ready = m["golden_present"] == m["golden_total"]
    batch_ready = m["pending"] == 0
    names = (GATED_GOLDEN if golden_ready else ()) + (GATED_BATCH if batch_ready else ())
    if not golden_ready:
        notes.append(f"golden gate deferred: {m['golden_present']}/{m['golden_total']} cases current")
    if not batch_ready:
        notes.append(f"batch gate deferred: {m['pending']} rows pending")
    for name in names:
        cur, ref = m[name], (base.get("metrics") or {}).get(name) or {}
        if cur["value"] is None or ref.get("value") is None:
            continue
        floor = ref["value"] - 100 * tolerance_cases / max(cur["n"] or 1, 1)
        if cur["value"] < floor:
            failures.append({"metric": name, "value": cur["value"], "floor": round(floor, 2), "baseline": ref["value"]})
    ceiling = (base.get("metrics") or {}).get("numbers_ungrounded")
    if batch_ready and ceiling is not None and m["numbers_ungrounded"] > ceiling:
        failures.append({"metric": "numbers_ungrounded", "value": m["numbers_ungrounded"], "ceiling": ceiling})
    return failures, notes


def baseline_yaml(m: dict, ctx: dict) -> str:
    head = ("# rtc04_explain baseline (ADR 0022): the thresholds of the explain gate, from a measured run.\n"
            "# Written by `rtc04_explain --write-baseline`. Review it like a label change: a lower baseline accepts\n"
            "# a worse explainer. Rates may drop at most RTC_EXPLAIN_TOLERANCE_CASES of their n; numbers_ungrounded\n"
            "# may not grow.\n")
    body = {"explain_version": EXPLAIN_VERSION, "measured_at": ctx["measured_at"], "model": ctx["model"],
            "rtc_sha": ctx["rtc_sha"], "law_version": ctx["law_version"],
            "metrics": {k: m[k] for k in (*GATED_BATCH, *GATED_GOLDEN, "numbers_ungrounded")}}
    return head + yaml.safe_dump(body, sort_keys=False, allow_unicode=True)


def load_baseline(path: Path = BASELINE_PATH) -> dict | None:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# --------------------------------------------------------------------------- database
def versions(cur) -> tuple[str | None, str | None, set[tuple]]:
    """(rtc sha in use, law sha256 served, the (rtc sha, law sha, bridge version) the bridge was built from)."""
    cur.execute("SELECT sha FROM ctrl.rtc_carga ORDER BY loaded_at DESC, carga_id DESC LIMIT 1")
    rtc = cur.fetchone()
    cur.execute("SELECT min(source_sha256) FROM ai.lc214_dispositivo")
    law = cur.fetchone()
    cur.execute("SELECT DISTINCT rtc_sha, law_sha256, bridge_version FROM ai.rtc_lc214_fundamento")
    return (rtc[0] if rtc else None), (law[0] if law else None), set(cur.fetchall())


def read_classes(cur) -> tuple[list[Classe], set]:
    """Every official classification (silver, built by dbt_rtc) with its reductions and bridge rows; plus the
    rtc sha(s) silver was built from."""
    cur.execute("""SELECT cltr_id, grupo_tributo, cclasstrib, descricao, cst, descricao_cst, tipo_aliquota,
                          tratamento, anexo_lc214, fundamentacao_legal, dt_inicio_vigencia, dt_fim_vigencia, rtc_sha
                   FROM silver.rtc_cclasstrib ORDER BY cltr_id""")
    cols = [d[0] for d in cur.description]
    raw = [dict(zip(cols, r)) for r in cur.fetchall()]
    shas = {r.pop("rtc_sha") for r in raw}
    by_id = {r["cltr_id"]: Classe(**{k: (str(v) if k == "anexo_lc214" and v is not None else v)
                                     for k, v in r.items()}) for r in raw}
    cur.execute("""SELECT DISTINCT ON (cltr_id, tributo) cltr_id, tributo, p_reducao
                   FROM silver.rtc_percentual_reducao ORDER BY cltr_id, tributo, dt_inicio_vigencia DESC""")
    for cid, tributo, value in cur.fetchall():
        if cid in by_id and value is not None:
            by_id[cid].reducoes[tributo] = Decimal(str(value))
    cur.execute("""SELECT fc.fdcl_cltr_id, f.outcome, f.dispositivo_id
                   FROM ai.rtc_lc214_fundamento f
                   JOIN ref_rtc.fundamentacao_classificacao fc ON fc.fdcl_fdlg_id = f.fdlg_id
                   ORDER BY fc.fdcl_cltr_id, f.fdlg_id, f.ordem""")
    for cid, outcome, pid in cur.fetchall():
        if cid in by_id:
            by_id[cid].ponte.append({"outcome": outcome, "dispositivo_id": pid})
    return list(by_id.values()), shas


def read_existing(cur) -> dict[int, str]:
    cur.execute(f"SELECT cltr_id, row_fingerprint FROM {TABLE}")
    return dict(cur.fetchall())


ROW_COLS = ("cltr_id", "grupo_tributo", "cclasstrib", "outcome", "recusa", "explicacao", "resposta_modelo", "fontes",
            "dispositivos_ponte", "dispositivos_contexto", "dispositivos_citados", "citacoes", "cita_ponte",
            "numeros_sem_fonte", "violacoes", "ancora_parcial", "context_tokens", "prompt_eval_count", "eval_count",
            "latency_s", "row_fingerprint", "model", "model_digest", "prompt_version")


def read_rows(cur) -> dict[int, dict]:
    cur.execute(f"SELECT {', '.join(ROW_COLS)}, pendente FROM {TABLE}")
    cols = [d[0] for d in cur.description]
    out = {}
    for r in cur.fetchall():
        row = dict(zip(cols, r))
        row["sha"] = hashlib.md5(json.dumps([row["explicacao"], list(row["fontes"] or [])],
                                            ensure_ascii=False).encode()).hexdigest()[:12]
        out[row["cltr_id"]] = row
    return out


def prepare_table(cur, keep: list[int], todo: list[Task], rtc_sha: str, law_sha: str) -> int:
    """Delete classifications that left the table, give each new one a placeholder row (pendente, no outcome yet),
    flag the rows to regenerate (their old text stays readable until replaced), restamp the lineage of the rest
    (same fingerprint = same prompt: only the version labels moved). Returns the rows deleted."""
    cur.execute(f"DELETE FROM {TABLE} WHERE NOT (cltr_id = ANY(%s::int[]))", (keep,))
    deleted = cur.rowcount
    for t in todo:
        cur.execute(f"INSERT INTO {TABLE} (cltr_id, grupo_tributo, cclasstrib, pendente, row_fingerprint, "
                    f"explain_version, rtc_sha, law_sha256, bridge_version) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    f"ON CONFLICT (cltr_id) DO NOTHING", (t.classe.cltr_id, t.classe.grupo_tributo, t.classe.cclasstrib,
                                                         True, "", EXPLAIN_VERSION, rtc_sha, law_sha, BRIDGE_VERSION))
    cur.execute(f"UPDATE {TABLE} SET pendente = (cltr_id = ANY(%s::int[])), rtc_sha = %s, law_sha256 = %s, "
                f"bridge_version = %s WHERE cltr_id = ANY(%s::int[])",
                ([t.classe.cltr_id for t in todo], rtc_sha, law_sha, BRIDGE_VERSION, keep))
    return deleted


def upsert(cur, e: Explanation, rtc_sha: str, law_sha: str) -> None:
    values = {k: getattr(e, k) for k in ROW_COLS}
    values["citacoes"] = json.dumps(e.citacoes, ensure_ascii=False, default=str)
    cols = [*ROW_COLS, "pendente", "explain_version", "rtc_sha", "law_sha256", "bridge_version", "generated_at"]
    params = [values[k] for k in ROW_COLS] + [False, EXPLAIN_VERSION, rtc_sha, law_sha, BRIDGE_VERSION,
                                              datetime.now(timezone.utc)]
    marks = ", ".join("%s::jsonb" if k == "citacoes" else "%s" for k in cols)
    updates = ", ".join(f"{k} = EXCLUDED.{k}" for k in cols if k != "cltr_id")
    cur.execute(f"INSERT INTO {TABLE} ({', '.join(cols)}) VALUES ({marks}) "
                f"ON CONFLICT (cltr_id) DO UPDATE SET {updates}", params)


# --------------------------------------------------------------------------- the run
def _ready(cur) -> tuple[str, str] | None:
    rtc_sha, law_sha, bridge = versions(cur)
    if not rtc_sha:
        raise ExplainError("reference_not_loaded", "ctrl.rtc_carga is empty: run dag_rtc_ref first")
    if not law_sha:
        log_event(logger, "law_not_loaded", "ai.lc214_dispositivo is empty (AI profile): nothing to explain from")
        return None
    if bridge != {(rtc_sha, law_sha, BRIDGE_VERSION)}:
        log_event(logger, "bridge_not_current", "ai.rtc_lc214_fundamento was not built from the reference and law "
                  "in use (link_lc214 must run first)", bridge=sorted(map(str, bridge)))
        return None
    return rtc_sha, law_sha


def _tasks(cur, llm: gen.LLM, store: rt.Store, rtc_sha: str) -> list[Task] | None:
    classes, shas = read_classes(cur)
    if shas - {rtc_sha}:
        log_event(logger, "silver_not_current", f"silver.rtc_cclasstrib was built from {sorted(shas)}, the reference "
                  f"in use is {rtc_sha[:12]}: dbt_rtc must run first")
        return None
    digest, pv = llm.digest(), prompt_version()
    return [prepare(c, store, llm.model, digest, pv) for c in classes]


def run(conn, llm: gen.LLM, *, force: bool = False, do_gate: bool = False, max_minutes: float = MAX_MINUTES,
        golden_path: Path = GOLDEN_PATH, baseline_path: Path = BASELINE_PATH,
        clock: Callable[[], float] = time.monotonic) -> int:
    step, started = current_step(), clock()
    golden = load_golden(golden_path)
    with conn.cursor() as cur:
        ready = _ready(cur)
        if not ready:
            return EXIT_SKIPPED
        rtc_sha, law_sha = ready
        store = rt.PgStore(cur)
        law_ids = set(store.ids())
        stale = golden_violations(golden, law_ids)
        if stale:
            raise ExplainError("explain_golden_stale", "explain_golden.yml: " + "; ".join(stale))
        try:
            tasks = _tasks(cur, llm, store, rtc_sha)
        except gen.LLMUnavailable as exc:                        # an optional layer: profile ai not up
            log_event(logger, "llm_unavailable", str(exc))
            return EXIT_SKIPPED
        if tasks is None:
            return EXIT_SKIPPED
        targets = golden_targets(golden, [t.classe for t in tasks])
        todo = plan(tasks, read_existing(cur), set(targets.values()), force)
        deleted = prepare_table(cur, [t.classe.cltr_id for t in tasks], todo, rtc_sha, law_sha)
        conn.commit()
        step.label(source_sha=rtc_sha[:12], law_version=law_sha[:12], model=llm.model,
                   prompt_version=prompt_version())
        if not todo:
            log_event(logger, "explanations_up_to_date", f"{len(tasks)} classifications already explained "
                      f"(reference {rtc_sha[:12]} x law {law_sha[:12]}, {llm.model})", deleted=deleted)
            return EXIT_SKIPPED

        index, done, pv = Index(law_ids), Counter(), prompt_version()
        log_event(logger, "explain_planned", f"{len(todo)} of {len(tasks)} classifications to explain "
                  f"(budget {max_minutes:g} min)", todo=len(todo), total=len(tasks), deleted=deleted)
        for n, task in enumerate(todo, 1):
            if (clock() - started) / 60 >= max_minutes:
                log_event(logger, "explain_budget_reached", f"{max_minutes:g} min reached: {len(todo) - n + 1} "
                          "classifications wait for the next run", left=len(todo) - n + 1)
                break
            try:
                e = explain(task, llm, index, pv=pv)
            except gen.LLMUnavailable as exc:
                conn.commit()
                raise ExplainError("llm_lost", f"{exc} (after {n - 1} explanations; the next run continues)") from None
            upsert(cur, e, rtc_sha, law_sha)
            conn.commit()
            done[e.outcome] += 1
            log_event(logger, "cclasstrib_explained", f"[{n}/{len(todo)}] {e.cclasstrib} {e.grupo_tributo}: "
                      f"{e.outcome}{' (' + e.recusa + ')' if e.recusa else ''}"
                      f"{'' if e.cita_ponte in (None, True) else ', does not cite the bridge'}"
                      f"{', numbers without source ' + str(e.numeros_sem_fonte) if e.numeros_sem_fonte else ''}",
                      cclasstrib=e.cclasstrib, grupo=e.grupo_tributo, outcome=e.outcome, latency_s=e.latency_s,
                      prompt_eval_count=e.prompt_eval_count)

        rows = read_rows(cur)
    g = golden_eval(golden, targets, rows)
    m = metrics(list(rows.values()), g)
    step.add(rows_in=len(tasks), rows_out=sum(done.values()), **{f"generated_{k}": v for k, v in done.items()})
    step.set(pending=m["pending"], numbers_ungrounded=m["numbers_ungrounded"], violations=m["violations"],
             **{k: m[k]["value"] for k in (*GATED_BATCH, *GATED_GOLDEN) if m[k]["value"] is not None})
    log_event(logger, "explain_finished", summary_line(m, done), **{k: v for k, v in m.items()},
              generated=dict(done))
    for x in g:
        if x["present"] and (not x["outcome_ok"] or x.get("cites_expected") is False or x.get("mentions_ok") is False):
            log_event(logger, "explain_golden_miss", f"{x['case']} {x['cclasstrib']}: {x}", level=30, **x)
    loaded = EXIT_OK if done else EXIT_SKIPPED              # a budget of 0 generates nothing: never exit 0 for that
    if not do_gate:
        return loaded
    failures, notes = gate(m, load_baseline(baseline_path))
    for note in notes:
        log_event(logger, "explain_gate_note", note)
    if failures:
        log_event(logger, "explain_gate_failed", f"below the baseline: {failures}", level=40, failures=failures)
        return EXIT_FAILED
    return loaded


def summary_line(m: dict, done: Counter) -> str:
    def v(name):
        x = m[name]["value"]
        return "–" if x is None else f"{x:g}%"
    return (f"generated {sum(done.values())} ({dict(done)}); {m['rows']} rows, {m['pending']} pending; explained "
            f"{v('explained_pct')} of the anchored, cite the bridge {v('cites_bridge_pct')}, "
            f"{m['numbers_ungrounded']} with numbers without source; golden {m['golden_present']}/{m['golden_total']}"
            f" current: outcome {v('golden_outcome_pct')}, expected provision {v('golden_cites_expected_pct')}, "
            f"numbers {v('golden_mentions_pct')}; review {m['review']}")


# --------------------------------------------------------------------------- CLI
def _print_one(e: Explanation, out=print) -> None:
    out(f"cClassTrib {e.cclasstrib} ({GRUPO.get(e.grupo_tributo, e.grupo_tributo)}): {e.outcome}"
        f"{' / ' + e.recusa if e.recusa else ''}")
    out(e.explicacao)
    if e.fontes:
        out("Fonte: LC 214/2025, " + "; ".join(e.fontes))
    if e.resposta_modelo:
        out(f"(modelo: {e.resposta_modelo})")
    out(f"ponte: {', '.join(e.dispositivos_ponte) or '–'} | cita a ponte: {e.cita_ponte} | números sem fonte: "
        f"{e.numeros_sem_fonte or '–'} | {e.latency_s} s, {e.prompt_eval_count} tokens de prompt")


def cli_one(conn, llm: gen.LLM, code: str, grupo: str | None, out=print) -> int:
    """Explain one cClassTrib now (every validity and tax group of the code, unless --grupo), write nothing."""
    with conn.cursor() as cur:
        store = rt.PgStore(cur)
        classes = [c for c in read_classes(cur)[0] if c.cclasstrib == code and (not grupo or c.grupo_tributo == grupo)]
        if not classes:
            out(f"cClassTrib {code} not found in silver.rtc_cclasstrib")
            return EXIT_FAILED
        index, digest, pv = Index(store.ids()), llm.digest(), prompt_version()
        for c in classes:
            _print_one(explain(prepare(c, store, llm.model, digest, pv), llm, index, pv=pv), out)
            out("")
    return EXIT_OK


def cli_review(conn, golden_path: Path = GOLDEN_PATH, out=print) -> int:
    """The golden cases as stored, with the sha a review must quote (explain_golden.yml: review.sha)."""
    golden = load_golden(golden_path)
    with conn.cursor() as cur:
        rows = read_rows(cur)
        classes = read_classes(cur)[0]
    targets = golden_targets(golden, classes)
    for case, res in zip(golden.get("cases") or [], golden_eval(golden, targets, rows)):
        row = rows.get(targets.get(case["id"], -1))
        out(f"== {case['id']}  cClassTrib {case['cclasstrib']} ({case.get('grupo', 'IBSCBS')})  review: "
            f"{res.get('review', 'not generated')}")
        if row:
            out(f"sha {row['sha']} | {row['outcome']} | cites expected: {res.get('cites_expected')} | "
                f"numbers: {res.get('mentions_ok')}")
            out(row["explicacao"])
            if row["fontes"]:
                out("Fonte: LC 214/2025, " + "; ".join(row["fontes"]))
        out("")
    return EXIT_OK


def cli_write_baseline(conn, llm: gen.LLM, golden_path: Path = GOLDEN_PATH,
                       baseline_path: Path = BASELINE_PATH, out=print) -> int:
    golden = load_golden(golden_path)
    with conn.cursor() as cur:
        rtc_sha, law_sha, _ = versions(cur)
        rows = read_rows(cur)
        classes = read_classes(cur)[0]
    m = metrics(list(rows.values()), golden_eval(golden, golden_targets(golden, classes), rows))
    if m["pending"] or m["golden_present"] < m["golden_total"]:
        out(f"not written: {m['pending']} rows pending, golden {m['golden_present']}/{m['golden_total']} "
            "(finish the batch first)")
        return EXIT_FAILED
    ctx = {"measured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "model": {"model": llm.model, "digest": llm.digest(), "prompt_version": prompt_version()},
           "rtc_sha": (rtc_sha or "")[:12], "law_version": (law_sha or "")[:12]}
    baseline_path.write_text(baseline_yaml(m, ctx), encoding="utf-8")
    out(f"written {baseline_path}: {summary_line(m, Counter())}")
    return EXIT_OK


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("cclasstrib", nargs="?", help="explain this code now and print it (nothing is written)")
    ap.add_argument("--grupo", choices=sorted(GRUPO), help="with a code: only this tax group")
    ap.add_argument("--force", action="store_true", help="regenerate every row (CPU: hours)")
    ap.add_argument("--gate", action="store_true", help="fail when the metrics drop below explain_baseline.yml")
    ap.add_argument("--max-minutes", type=float, default=MAX_MINUTES, help="no new call after this (default %(default)s)")
    ap.add_argument("--review", action="store_true", help="print the golden cases as stored")
    ap.add_argument("--write-baseline", action="store_true", help="record the current metrics as the baseline")
    args = ap.parse_args(argv)
    from pipelines.commons.dw_client import get_pg_connection

    conn = get_pg_connection()
    try:
        if args.review:
            return cli_review(conn)
        llm = gen.OllamaClient()
        if args.write_baseline:
            return cli_write_baseline(conn, llm)
        if args.cclasstrib:
            return cli_one(conn, llm, args.cclasstrib, args.grupo)
        return run(conn, llm, force=args.force, do_gate=args.gate, max_minutes=args.max_minutes)
    except (ExplainError, gen.GenError) as exc:
        log_event(logger, exc.event, str(exc), level=40)
        return EXIT_FAILED
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
