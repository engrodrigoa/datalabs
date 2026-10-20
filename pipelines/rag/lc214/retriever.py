"""LC 214 retrieval (step 4c, ADR 0013): vector search over ai.lc214_chunk + context assembly with a token budget.

    question -> router (ADR 0012)
                  sql / clarify      -> no retrieval (answered by lookup / by asking)
                  vector / combined  -> top-k chunks by EXACT cosine distance (no ANN index: 867 chunks)
                                        -> context blocks, one per article, under the LLM budget

Small-to-big with a ceiling ("option C")
    A chunk is what is searched; the ARTICLE is what is cited. Each article in the context is shown whole when it
    fits LC214_ANCHOR_MAX_TOKENS (2,000 LLM tokens ~ 1,170 words: chosen by lc05, ADR 0013), else in
    layers: its caput, the cited provision (combined route), its chunks ranked by similarity to the question while
    they fit, and the labels of what was left out, so the answer can say "see also § 4º".

Order and budget
    Anchors first (the auditor cited them: always in the context), then the hits in rank order, one block per
    article (two hits of art. 5 are one block). Greedy: a block takes min(ceiling, what is left of the budget);
    hits that no longer fit are reported in `dropped`, never silently lost.

Two tokenizers, two rulers
    `n_tokens` of a chunk counts EMBEDDING tokens (e5, limit 512, lc03). The budget here counts LLM tokens
    (llama3.2:1b, num_ctx 6144), which this step does not have: they are ESTIMATED as words x
    LC214_LLM_TOKENS_PER_WORD (1.7, conservative for legal Portuguese). The generation step measures the real ratio
    with Ollama's prompt_eval_count and corrects the default; the margin (LC214_BUDGET_MARGIN) absorbs the error.

    budget = (num_ctx - answer reserve - fixed prompt - question) x (1 - margin)
           = (6144 - 1024 - 500 - ~30) x 0.8 ~ 3,670 tokens of context
    6144, not 8192 (ADR 0015): the same context_recall on the golden set (no context needed more than ~3,640
    tokens), and on CPU the generation time grows with the prompt.

CLI (lab): python -m pipelines.rag.lc214.retriever "o art. 6º se aplica a transferência entre filiais?" [--context]
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from dataclasses import dataclass, field
from typing import Callable, Protocol

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.rag.lc214.router import Index, Plan, route  # noqa: E402

TOP_K = int(os.getenv("LC214_TOP_K", "5"))
NUM_CTX = int(os.getenv("LC214_NUM_CTX", "6144"))
ANSWER_TOKENS = int(os.getenv("LC214_ANSWER_TOKENS", "1024"))
PROMPT_TOKENS = int(os.getenv("LC214_PROMPT_TOKENS", "500"))
BUDGET_MARGIN = float(os.getenv("LC214_BUDGET_MARGIN", "0.2"))
ANCHOR_MAX_TOKENS = int(os.getenv("LC214_ANCHOR_MAX_TOKENS", "2000"))
LLM_TOKENS_PER_WORD = float(os.getenv("LC214_LLM_TOKENS_PER_WORD", "1.7"))
MIN_BLOCK_TOKENS = 120              # below this an article block would be its header and little else
ARTICLE_CHUNKS = 64                 # chunks ranked inside one article (art. 517 has 13 windows; most have 1)
OMITTED_LABELS_MAX = 30


# --------------------------------------------------------------------------- tokens and budget
def llm_tokens(text: str, per_word: float = LLM_TOKENS_PER_WORD) -> int:
    return math.ceil(len(text.split()) * per_word)


def context_budget(question: str = "") -> int:
    return int((NUM_CTX - ANSWER_TOKENS - PROMPT_TOKENS - llm_tokens(question)) * (1 - BUDGET_MARGIN))


# --------------------------------------------------------------------------- data access
@dataclass(frozen=True)
class Hit:
    rank: int
    chunk_id: str
    art_id: str
    kind: str
    texto: str
    dispositivo_ids: tuple[str, ...]
    distance: float


class Store(Protocol):
    def search(self, qvec: list[float], k: int, art_id: str | None = None) -> list[Hit]: ...
    def provisions(self, art_id: str) -> list[dict]: ...
    def header(self, art_id: str) -> str: ...
    def ids(self) -> list[str]: ...


def vector_literal(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.7g}" for x in v) + "]"


class PgStore:
    """ai.lc214_chunk (vectors) + ai.lc214_dispositivo / ai.lc214_artigo (what is shown). Values are bind
    parameters: the question never reaches the SQL text."""

    def __init__(self, cur):
        self.cur = cur

    def search(self, qvec, k, art_id=None):
        where = "WHERE art_id = %(art)s" if art_id else ""
        self.cur.execute(
            "SELECT chunk_id, art_id, kind, texto, dispositivo_ids, embedding <=> %(q)s::vector AS distance "
            f"FROM ai.lc214_chunk {where} ORDER BY distance, chunk_id LIMIT %(k)s",
            {"q": vector_literal(qvec), "k": k, "art": art_id})
        return [Hit(n, r[0], r[1], r[2], r[3], tuple(r[4]), float(r[5])) for n, r in enumerate(self.cur.fetchall(), 1)]

    def provisions(self, art_id):
        self.cur.execute("SELECT dispositivo_id, parent_id, tipo, rotulo, status, texto, notas "
                         "FROM ai.lc214_dispositivo WHERE art_id = %s ORDER BY ordem", (art_id,))
        cols = [d[0] for d in self.cur.description]
        return [dict(zip(cols, r)) for r in self.cur.fetchall()]

    def header(self, art_id):
        self.cur.execute("SELECT header FROM ai.lc214_artigo WHERE art_id = %s", (art_id,))
        row = self.cur.fetchone()
        return row[0] if row else art_id

    def ids(self):
        self.cur.execute("SELECT dispositivo_id FROM ai.lc214_dispositivo")
        return [r[0] for r in self.cur.fetchall()]


# --------------------------------------------------------------------------- rendering one article
def art_of(pid: str) -> str:
    return ":".join(pid.split(":")[:2])


def _ordinal(value: str) -> str:
    """'1' -> '1º', '10' -> '10', '5-A' -> '5º-A' (the law writes ordinals up to the 9th)."""
    num, _, suffix = value.partition("-")
    out = f"{num}º" if num.isdigit() and int(num) < 10 else num
    return out + (f"-{suffix}" if suffix else "")


def rel_label(pid: str) -> str:
    """lc214-2025:art-5:par-2:inc-III -> '§ 2º, III': the provision as the article's reader would cite it."""
    out = []
    for part in pid.split(":")[2:]:
        kind, _, value = part.partition("-")
        value = value.split("#")[0]
        if kind == "par":
            out.append("parágrafo único" if value == "unico" else f"§ {_ordinal(value)}")
        elif kind == "inc":
            out.append(value)
        elif kind == "ali":
            out.append(f"{value})")
    return ", ".join(out) or "caput"


def _cut(text: str, max_tokens: int) -> str:
    words = text.split()
    keep = max(1, int(max_tokens / LLM_TOKENS_PER_WORD))
    return text if len(words) <= keep else " ".join(words[:keep]) + f" [...] (trecho: {keep} de {len(words)} palavras)"


def render(rows: list[dict], included: set[str], overrides: dict[str, str] | None = None,
           row_cap: int | None = None) -> str:
    """The included provisions in reading order, indented by depth; revoked/vetoed show their note (like the SQL
    answer of the router); `overrides` replaces a provision's text by the window that was retrieved."""
    overrides = overrides or {}
    out = []
    for r in rows:
        pid = r["dispositivo_id"]
        if pid not in included:
            continue
        pad = "  " * (pid.count(":") - 1)
        if r["status"] != "vigente":
            note = next((n for n in r.get("notas") or [] if n.lower().startswith(("revogad", "vetad"))), r["status"])
            out.append(f"{pad}{r['rotulo']} ({note})")
            continue
        text = overrides.get(pid, r["texto"])
        if row_cap and llm_tokens(text) > row_cap:
            text = _cut(text, row_cap)
        out.append(pad + text)
    return "\n".join(out)


@dataclass
class Unit:
    """Something to add to an article block: an anchor's subtree or a retrieved chunk."""
    ids: set[str]
    overrides: dict[str, str] = field(default_factory=dict)
    mandatory: bool = False


def _units_for(rows: list[dict], anchors: list[str], ranked: list[Hit]) -> list[Unit]:
    ids = [r["dispositivo_id"] for r in rows]
    units = []
    for a in anchors:
        if a == art_of(a):                       # the whole article was cited: its caput is the anchor, the rest ranks
            units.append(Unit({ids[0]}, mandatory=True))
        else:
            units.append(Unit({p for p in ids if p == a or p.startswith(a + ":")}, mandatory=True))
    for h in ranked:
        if h.kind == "janela":                   # a window of one huge provision: show the window, not the provision
            units.append(Unit(set(h.dispositivo_ids), {h.dispositivo_ids[0]: "[trecho] " + h.texto.split("\n")[-1]}))
        else:
            units.append(Unit(set(h.dispositivo_ids)))
    return units


@dataclass
class Block:
    art_id: str
    mode: str                     # whole | layered
    reason: str                   # anchor | hit
    ranks: list[int]              # ranks of the global top-k that landed in this article
    dispositivo_ids: list[str]    # what the block shows: the ids an answer may cite
    text: str
    tokens: int
    omitted: int = 0              # live provisions of the article left out (layered)
    truncated: bool = False       # something had to be cut to fit (a mandatory anchor bigger than its limit)


def build_block(store: Store, art_id: str, anchors: list[str], ranked: list[Hit], limit: int, ranks: list[int]
                ) -> Block:
    """Option C for one article: whole if it fits `limit`, else caput + anchors + ranked chunks + omitted labels."""
    rows, header = store.provisions(art_id), store.header(art_id)
    reason = "anchor" if anchors else "hit"
    every = [r["dispositivo_id"] for r in rows]
    whole = header + "\n" + render(rows, set(every))
    if llm_tokens(whole) <= limit:
        return Block(art_id, "whole", reason, ranks, every, whole, llm_tokens(whole))

    by_id = {r["dispositivo_id"]: r for r in rows}

    def with_ancestors(ids: set[str]) -> set[str]:
        out = set(ids)
        for pid in ids:
            parent = by_id.get(pid, {}).get("parent_id")
            while parent and parent not in out and parent in by_id:
                out.add(parent)
                parent = by_id[parent].get("parent_id")
        return out

    row_cap = max(limit // 2, MIN_BLOCK_TOKENS)
    included, overrides, truncated = {every[0]}, {}, False

    def text_of(inc: set[str], ov: dict[str, str]) -> str:
        left = [p for p in every if p not in inc and by_id[p]["status"] == "vigente"
                and (by_id[p].get("parent_id") in inc or by_id[p].get("parent_id") is None)]
        tail = ""
        if left:
            labels = [rel_label(p) for p in left[:OMITTED_LABELS_MAX]]
            more = f" e mais {len(left) - OMITTED_LABELS_MAX}" if len(left) > OMITTED_LABELS_MAX else ""
            tail = f"\n[não incluídos neste contexto: {'; '.join(labels)}{more}]"
        return header + "\n" + render(rows, inc, ov, row_cap) + tail

    for unit in _units_for(rows, anchors, ranked):
        if unit.ids <= included and not unit.overrides:
            continue
        ov = dict(overrides)
        for pid, window in unit.overrides.items():
            if pid in included and pid not in overrides:
                continue                                      # the whole provision is already there
            if window not in ov.get(pid, ""):
                ov[pid] = f"{ov[pid]} {window}" if pid in ov else window   # two windows of one provision add up
        if ov == overrides and unit.ids <= included:
            continue
        cand = with_ancestors(included | unit.ids)
        if llm_tokens(text_of(cand, ov)) <= limit:
            included, overrides = cand, ov
        elif unit.mandatory:
            included, overrides, truncated = cand, ov, True

    text = text_of(included, overrides)
    if llm_tokens(text) > limit:                              # only a mandatory anchor can get here
        text, truncated = _cut(text, limit), True
    omitted = sum(1 for p in every if p not in included and by_id[p]["status"] == "vigente")
    return Block(art_id, "layered", reason, ranks, [p for p in every if p in included], text, llm_tokens(text),
                 omitted, truncated)


# --------------------------------------------------------------------------- retrieval
@dataclass
class Retrieval:
    question: str
    plan: Plan
    hits: list[Hit] = field(default_factory=list)            # the global top-k: what lc05 scores
    blocks: list[Block] = field(default_factory=list)        # what the LLM reads
    budget: int = 0
    dropped: list[str] = field(default_factory=list)         # articles of hits that did not fit the budget

    @property
    def tokens(self) -> int:
        return sum(b.tokens for b in self.blocks)

    def context(self) -> str:
        return "\n\n".join(f"[{n}] {b.text}" for n, b in enumerate(self.blocks, 1))


def assemble(store: Store, plan: Plan, hits: list[Hit], qvec: list[float], budget: int) -> tuple[list[Block], list[str]]:
    """Anchors first, then hits by rank, one block per article, greedy under the budget."""
    order: dict[str, dict] = {}
    for a in plan.anchors:
        order.setdefault(art_of(a), {"anchors": [], "ranks": []})["anchors"].append(a)
    for h in hits:
        order.setdefault(h.art_id, {"anchors": [], "ranks": []})["ranks"].append(h.rank)

    blocks, dropped, left = [], [], budget
    for art_id, spec in order.items():
        limit = min(ANCHOR_MAX_TOKENS, left)
        if spec["anchors"]:
            limit = max(limit, MIN_BLOCK_TOKENS)              # the cited provision is never dropped
        elif limit < MIN_BLOCK_TOKENS:
            dropped.append(art_id)
            continue
        ranked = store.search(qvec, ARTICLE_CHUNKS, art_id=art_id)
        block = build_block(store, art_id, spec["anchors"], ranked, limit, spec["ranks"])
        blocks.append(block)
        left -= block.tokens
    return blocks, dropped


def retrieve(question: str, store: Store, encode: Callable[[str], list[float]], index: Index | None = None,
             k: int = TOP_K) -> Retrieval:
    plan = route(question, index or Index(store.ids()))
    result = Retrieval(question, plan, budget=context_budget(question))
    if plan.route not in ("vector", "combined"):
        return result
    qvec = encode(question)
    result.hits = store.search(qvec, k)
    result.blocks, result.dropped = assemble(store, plan, result.hits, qvec, result.budget)
    return result


# --------------------------------------------------------------------------- CLI (lab use)
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Retrieve the LC 214 context for a question (router + vector search)")
    ap.add_argument("question")
    ap.add_argument("--k", type=int, default=TOP_K)
    ap.add_argument("--context", action="store_true", help="print the assembled context")
    args = ap.parse_args(argv)

    from pipelines.commons.ai.embeddings import encode_query, load_model
    from pipelines.commons.dw_client import get_pg_connection

    model = load_model()
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            r = retrieve(args.question, PgStore(cur), lambda q: encode_query(model, q), k=args.k)
    finally:
        conn.close()

    print(f"route={r.plan.route} intent={r.plan.intent} anchors={r.plan.anchors}\n{r.plan.reason()}")
    for h in r.hits:
        print(f"  #{h.rank} d={h.distance:.4f} {h.kind:<11} {h.chunk_id}  {', '.join(h.dispositivo_ids[:4])}"
              + (" ..." if len(h.dispositivo_ids) > 4 else ""))
    for n, b in enumerate(r.blocks, 1):
        print(f"  [{n}] {b.art_id} {b.mode} ({b.reason}, hits {b.ranks}) {b.tokens} tokens, "
              f"{len(b.dispositivo_ids)} dispositivos" + (f", {b.omitted} omitidos" if b.omitted else "")
              + (" TRUNCATED" if b.truncated else ""))
    print(f"context {r.tokens}/{r.budget} tokens (estimate, {LLM_TOKENS_PER_WORD}/word)"
          + (f"; dropped: {', '.join(r.dropped)}" if r.dropped else ""))
    if args.context:
        print("\n" + r.context())
    return 0


if __name__ == "__main__":
    sys.exit(main())
