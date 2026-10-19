"""LC 214 retrieval evaluation: the golden set through the real retriever, gated against a committed baseline (ADR 0014).

Step 5 of the legislation RAG. Every phrasing of every measured case (scope `now`, routes vector and combined) goes
through `retriever.retrieve()` - router, exact vector search, context assembly - exactly as a user question would.

What is measured, per phrasing group (fiscal = how the auditor asks, formal = the wording of the law)
    on the top-k chunks (route vector only: a combined case gets its provision from the SQL anchor, not the vectors)
        success@k   the case is solved: every expected provision (mode all) / one of them (mode any) in the top k
        hit@k       at least one expected provision in the top k
        recall@k    share of the expected provisions in the top k
        MRR         1 / rank of the first chunk that carries an expected provision (0 when none in the top k)
    on the assembled context (routes vector and combined: what the LLM will read)
        context_recall  share of the expected provisions in the blocks (anchors + articles of the hits, whole or in
                        layers): small-to-big shows up here as context_recall > recall@k
    reported, not gated
        the fiscal x formal gap on the cases that have both phrasings (same cases, two vocabularies)
        top-1 distance of solved, missed and no_answer questions: the data for an abstention threshold (later)
        cases out of scope (annexes, future_project)

The gate: thresholds from the first measurement
    eval_baseline.yml (committed, reviewed in a PR like golden_set.yml) holds the metrics of a reference run.
    A run fails when a gated metric drops below baseline - LC214_EVAL_TOLERANCE_CASES / n. The search is exact and
    the model fixed, so the same inputs give the same scores; the tolerance (default 1 case) absorbs a near tie
    flipping on another CPU. An improvement never fails: it is reported, and raising the baseline is a deliberate
    commit (--write-baseline). No baseline yet: the run fails and says how to create it (a gate without thresholds
    is not a gate).

Run in the lab (needs the model and Postgres):
    python -m pipelines.rag.lc214.lc05_eval                    measure + gate
    python -m pipelines.rag.lc214.lc05_eval --write-baseline   measure + write eval_baseline.yml (review, commit)
    python -m pipelines.rag.lc214.lc05_eval --misses           also print every missed phrasing
Exit codes: 0 gate holds | 1 regression, missing baseline or stale labels.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import statistics
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import yaml

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.logger import get_logger, log_event  # noqa: E402
from pipelines.observability import current_step  # noqa: E402
from pipelines.rag.lc214 import retriever as rt  # noqa: E402
from pipelines.rag.lc214.golden import GOLDEN_PATH, load_golden  # noqa: E402
from pipelines.rag.lc214.router import Index  # noqa: E402

logger = get_logger("lc05_eval")

EVAL_VERSION = "1"                  # bump when a metric's definition changes (the old baseline is not comparable)
BASELINE_PATH = Path(__file__).with_name("eval_baseline.yml")
TOLERANCE_CASES = float(os.getenv("LC214_EVAL_TOLERANCE_CASES", "1"))
GROUPS = ("fiscal", "formal")
GATED = ("success_at_k", "mrr", "context_recall")
RETRIEVAL = ("success_at_k", "hit_at_k", "recall_at_k", "mrr")
EXIT_OK, EXIT_FAILED = 0, 1


class EvalError(Exception):
    def __init__(self, event: str, message: str, **fields):
        super().__init__(message)
        self.event, self.fields = event, fields


# --------------------------------------------------------------------------- one phrasing
@dataclass
class Outcome:
    case: str
    type: str
    route: str                      # the case's route (golden set)
    routed: str                     # the router's decision for this phrasing
    phrasing: str
    scope: str
    mode: str | None
    expected: list[str]
    first_rank: int | None = None   # rank of the first chunk carrying an expected provision
    found: list[str] = field(default_factory=list)          # expected provisions in the top k
    in_context: list[str] = field(default_factory=list)     # expected provisions in the assembled blocks
    top: list[str] = field(default_factory=list)            # art_id of each top-k hit, in rank order
    top1_distance: float | None = None
    context_tokens: int = 0
    dropped: int = 0

    @property
    def solved(self) -> bool:
        if not self.expected:
            return False
        return set(self.found) == set(self.expected) if self.mode != "any" else bool(self.found)

    @property
    def recall(self) -> float:
        return len(self.found) / len(self.expected) if self.expected else 0.0

    @property
    def rr(self) -> float:
        return 1.0 / self.first_rank if self.first_rank else 0.0

    @property
    def context_recall(self) -> float:
        return len(self.in_context) / len(self.expected) if self.expected else 0.0


def score(case: dict, phrasing: str, r: rt.Retrieval, k: int) -> Outcome:
    """Compare one retrieval with the case's labels. A provision counts as retrieved when a top-k chunk carries it
    (its `dispositivo_ids`): a window of art. 517 retrieves the provision it was cut from."""
    expected = list(case.get("expected") or [])
    o = Outcome(case["id"], case["type"], case["route"], r.plan.route, phrasing, case.get("scope", "now"),
                case.get("mode"), expected, top=[h.art_id for h in r.hits[:k]],
                top1_distance=round(r.hits[0].distance, 6) if r.hits else None,
                context_tokens=r.tokens, dropped=len(r.dropped))
    want = set(expected)
    for h in r.hits[:k]:
        carried = want & set(h.dispositivo_ids)
        if carried and o.first_rank is None:
            o.first_rank = h.rank
        o.found += sorted(carried - set(o.found))
    shown = {pid for b in r.blocks for pid in b.dispositivo_ids}
    o.in_context = sorted(want & shown)
    o.found = [p for p in expected if p in o.found]
    return o


# --------------------------------------------------------------------------- the run over the golden set
def measured(case: dict) -> bool:
    return case.get("route") in ("vector", "combined") and bool(case.get("expected"))


def evaluate(golden: dict, store: rt.Store, encode: Callable[[str], list[float]]) -> list[Outcome]:
    k = golden.get("k", 5)
    index = Index(store.ids())
    out = []
    for case in golden.get("cases") or []:
        if not (measured(case) or case.get("type") == "no_answer"):
            continue
        for phrasing, question in (case.get("questions") or {}).items():
            out.append(score(case, phrasing, rt.retrieve(str(question), store, encode, index=index, k=k), k))
    return out


def _mean(values) -> float | None:
    values = list(values)
    return round(sum(values) / len(values), 4) if values else None


def group_metrics(outcomes: list[Outcome], phrasing: str) -> dict:
    """The metrics of one phrasing group, over the cases in scope `now`."""
    now = [o for o in outcomes if o.phrasing == phrasing and o.scope == "now" and o.expected]
    vec = [o for o in now if o.route == "vector"]
    return {
        "n": len(vec), "n_context": len(now),
        "success_at_k": _mean(o.solved for o in vec), "hit_at_k": _mean(bool(o.found) for o in vec),
        "recall_at_k": _mean(o.recall for o in vec), "mrr": _mean(o.rr for o in vec),
        "context_recall": _mean(o.context_recall for o in now),
    }


def paired_gap(outcomes: list[Outcome]) -> dict:
    """success@k of the two vocabularies on the SAME cases (only the cases with both phrasings): the gap is the cost
    of asking like an auditor instead of like the law."""
    by = {(o.case, o.phrasing): o for o in outcomes if o.scope == "now" and o.route == "vector" and o.expected}
    pairs = sorted({c for c, p in by if p == "formal" and (c, "fiscal") in by})
    fiscal, formal = (_mean(by[(c, p)].solved for c in pairs) for p in GROUPS)
    return {"cases": len(pairs), "fiscal_success_at_k": fiscal, "formal_success_at_k": formal,
            "gap": round(formal - fiscal, 4) if pairs else None}


def _spread(values) -> dict | None:
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    return {"n": len(values), "min": values[0], "median": round(statistics.median(values), 6), "max": values[-1]}


def abstention(outcomes: list[Outcome]) -> dict:
    """Top-1 cosine distance (lower = closer) of solved, missed and unanswerable questions. An abstention threshold
    works only if the unanswerable ones sit above the solved ones; this is the data to choose it, not the choice."""
    answerable = [o for o in outcomes if o.route == "vector" and o.expected and o.scope == "now"]
    solved = _spread(o.top1_distance for o in answerable if o.solved)
    missed = _spread(o.top1_distance for o in answerable if not o.solved)
    no_answer = _spread(o.top1_distance for o in outcomes if o.type == "no_answer")
    separable = bool(solved and no_answer and no_answer["min"] > solved["max"])
    return {"solved": solved, "missed": missed, "no_answer": no_answer, "separable": separable}


def report(outcomes: list[Outcome], fingerprint: dict) -> dict:
    out_of_scope = [o for o in outcomes if o.scope != "now" and o.expected]
    return {
        "eval_version": EVAL_VERSION,
        "measured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fingerprint": fingerprint,
        "metrics": {g: group_metrics(outcomes, g) for g in GROUPS},
        "paired": paired_gap(outcomes),
        "abstention": abstention(outcomes),
        "out_of_scope": {"phrasings": len(out_of_scope), "success_at_k": _mean(o.solved for o in out_of_scope)},
        "misrouted": [f"{o.case}/{o.phrasing}: {o.routed}" for o in outcomes
                      if o.routed != o.route and o.type != "no_answer"],
        "outcomes": [asdict(o) | {"solved": o.solved} for o in outcomes],
    }


# --------------------------------------------------------------------------- the gate
def gate(metrics: dict, baseline: dict | None, tolerance_cases: float = TOLERANCE_CASES) -> tuple[list[dict], list[dict]]:
    """(regressions, improvements). Threshold = baseline - tolerance_cases / n, per group and gated metric."""
    if not baseline or not baseline.get("metrics"):
        raise EvalError("eval_baseline_missing", f"no baseline in {BASELINE_PATH.name}: run `python -m "
                        "pipelines.rag.lc214.lc05_eval --write-baseline`, review the numbers and commit the file")
    if str(baseline.get("eval_version")) != EVAL_VERSION:
        raise EvalError("eval_baseline_incompatible", f"baseline measured with eval v{baseline.get('eval_version')}, "
                        f"this is v{EVAL_VERSION}: the metrics changed meaning, write a new baseline")
    regressions, improvements = [], []
    for g in GROUPS:
        now, ref = metrics.get(g) or {}, baseline["metrics"].get(g) or {}
        for m in GATED:
            if now.get(m) is None or ref.get(m) is None:
                continue
            n = now["n_context"] if m == "context_recall" else now["n"]
            slack = tolerance_cases / n if n else 0.0
            item = {"group": g, "metric": m, "value": now[m], "baseline": ref[m], "threshold": round(ref[m] - slack, 4)}
            if now[m] < ref[m] - slack - 1e-9:
                regressions.append(item)
            elif now[m] > ref[m] + slack + 1e-9:
                improvements.append(item)
    return regressions, improvements


def load_baseline(path: Path = BASELINE_PATH) -> dict | None:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or None


def baseline_yaml(rep: dict) -> str:
    body = {k: rep[k] for k in ("eval_version", "measured_at", "fingerprint", "metrics", "paired")}
    head = ("# LC 214 retrieval baseline (ADR 0014): the thresholds of the lc05 gate, taken from a measured run.\n"
            "# Written by `python -m pipelines.rag.lc214.lc05_eval --write-baseline`. Review it like a label change:\n"
            "# a lower baseline accepts a worse retriever. The gate fails a run below baseline - "
            "LC214_EVAL_TOLERANCE_CASES / n.\n")
    return head + yaml.safe_dump(body, sort_keys=False, allow_unicode=True)


# --------------------------------------------------------------------------- what is being evaluated
def fingerprint(cur, golden_path: Path, k: int) -> dict:
    cur.execute("SELECT DISTINCT source_sha256, parser_version, chunker_version, embedding_model FROM ai.lc214_chunk")
    rows = cur.fetchall()
    if len(rows) != 1:
        raise EvalError("eval_store_mixed" if rows else "eval_store_empty",
                        f"ai.lc214_chunk holds {len(rows)} versions/models: run embed_chunks first")
    sha, parser, chunker, model = rows[0]
    return {"law_version": sha[:12], "parser_version": parser, "chunker_version": chunker, "embedding_model": model,
            "golden_sha": hashlib.md5(Path(golden_path).read_bytes()).hexdigest()[:12], "k": k,
            "retriever": {"anchor_max_tokens": rt.ANCHOR_MAX_TOKENS, "llm_tokens_per_word": rt.LLM_TOKENS_PER_WORD,
                          "num_ctx": rt.NUM_CTX, "budget_margin": rt.BUDGET_MARGIN}}


def summary_line(rep: dict) -> str:
    def fmt(v):
        return "-" if v is None else f"{v:.3f}"

    parts = []
    for g in GROUPS:
        m = rep["metrics"][g]
        parts.append(f"{g} (n={m['n']}/{m['n_context']}): success@k {fmt(m['success_at_k'])} hit@k {fmt(m['hit_at_k'])}"
                     f" recall@k {fmt(m['recall_at_k'])} MRR {fmt(m['mrr'])} context {fmt(m['context_recall'])}")
    p = rep["paired"]
    parts.append(f"paired {p['cases']} cases: fiscal {fmt(p['fiscal_success_at_k'])} vs formal "
                 f"{fmt(p['formal_success_at_k'])}")
    return " | ".join(parts)


# --------------------------------------------------------------------------- the step
def run(cur, encode, lake=None, golden_path: Path = GOLDEN_PATH, baseline_path: Path = BASELINE_PATH,
        write_baseline: bool = False, misses: bool = False, tolerance_cases: float = TOLERANCE_CASES) -> int:
    step = current_step()
    golden = load_golden(golden_path)
    k = golden.get("k", 5)
    fp = fingerprint(cur, golden_path, k)
    if golden.get("law_version") != fp["law_version"]:
        raise EvalError("eval_labels_stale", f"golden set labelled against {golden.get('law_version')}, vectors are "
                        f"of {fp['law_version']}: the golden contract (G2) must pass before an evaluation")

    outcomes = evaluate(golden, rt.PgStore(cur), encode)
    rep = report(outcomes, fp)
    line = summary_line(rep)
    log_event(logger, "lc214_eval_measured", line, law_version=fp["law_version"], model=fp["embedding_model"],
              **{f"{g}_{m}": rep["metrics"][g][m] for g in GROUPS for m in (*RETRIEVAL, "context_recall")})
    for g in GROUPS:
        step.set(**{f"{g}_{m}": rep["metrics"][g][m] for m in (*RETRIEVAL, "context_recall")
                    if rep["metrics"][g][m] is not None})
    ab = rep["abstention"]
    log_event(logger, "lc214_eval_abstention", "top-1 distance " + "; ".join(
        f"{name} {s['min']:.4f}..{s['median']:.4f}..{s['max']:.4f} (n={s['n']})" if s else f"{name} -"
        for name, s in ((n, ab[n]) for n in ("solved", "missed", "no_answer")))
        + f"; separable={ab['separable']}", separable=ab["separable"])
    step.set(paired_gap=rep["paired"]["gap"], abstention_separable=ab["separable"])
    step.add(rows_in=len(outcomes))
    if misses:
        for o in outcomes:
            if o.expected and not o.solved:
                print(f"  MISS {o.case}/{o.phrasing} [{o.scope}] expected {o.expected} found {o.found} "
                      f"first_rank={o.first_rank} context {len(o.in_context)}/{len(o.expected)} top {o.top}")

    if lake is not None:
        from pipelines.rag.lc214.lake import k_manifest, k_silver
        sha = lake.get_json(k_manifest())["current"]
        key = k_silver(sha, f"eval/{rep['measured_at'].replace(':', '').replace('-', '')}.json")
        lake.put_json(key, rep)
        log_event(logger, "lc214_eval_report_written", lake.url(key))

    if write_baseline:
        text = baseline_yaml(rep)
        try:
            Path(baseline_path).write_text(text, encoding="utf-8")
            log_event(logger, "lc214_eval_baseline_written", f"{baseline_path}: review the numbers and commit it")
        except OSError as exc:
            print(text)
            raise EvalError("eval_baseline_not_written", f"{baseline_path}: {exc}. The YAML is printed above: save it "
                            "there from the host") from None
        return EXIT_OK

    regressions, improvements = gate(rep["metrics"], load_baseline(baseline_path), tolerance_cases)
    for i in improvements:
        log_event(logger, "lc214_eval_improved", f"{i['group']} {i['metric']} {i['value']:.3f} > baseline "
                  f"{i['baseline']:.3f}: raise the baseline with --write-baseline", **i)
    if regressions:
        for r in regressions:
            log_event(logger, "lc214_eval_regression", f"{r['group']} {r['metric']} {r['value']:.3f} below threshold "
                      f"{r['threshold']:.3f} (baseline {r['baseline']:.3f})", level=40, **r)
        log_event(logger, "lc214_eval_gate_failed", f"{len(regressions)} regression(s): {line}", level=40)
        return EXIT_FAILED
    log_event(logger, "lc214_eval_gate_passed", line, improvements=len(improvements))
    return EXIT_OK


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--write-baseline", action="store_true", help=f"write {BASELINE_PATH.name} from this run")
    ap.add_argument("--misses", action="store_true", help="print every phrasing that was not solved")
    ap.add_argument("--lake", default=None, help="'s3' (default, LC214_LAKE), file:///dir, or 'none' (no report)")
    ap.add_argument("--golden", default=str(GOLDEN_PATH))
    args = ap.parse_args(argv)
    try:
        from pipelines.commons.ai.embeddings import encode_query, load_model
        from pipelines.commons.dw_client import get_pg_connection
        from pipelines.rag.lc214.lake import LakeError, lake_from_env

        lake = None if args.lake == "none" else lake_from_env(args.lake)
        model = load_model()
        conn = get_pg_connection()
        try:
            with conn.cursor() as cur:
                return run(cur, lambda q: encode_query(model, q), lake, Path(args.golden),
                           write_baseline=args.write_baseline, misses=args.misses)
        finally:
            conn.rollback()                         # read-only: nothing to commit
            conn.close()
    except EvalError as exc:
        log_event(logger, exc.event, str(exc), level=40, **exc.fields)
        return EXIT_FAILED
    except LakeError as exc:
        log_event(logger, exc.event, str(exc), level=40)
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
