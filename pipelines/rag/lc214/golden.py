"""LC 214 golden set: the data contract between the labelled questions and the current version of the law.

The golden set (`golden_set.yml`) labels each question with the provision ids that answer it. Those labels are
data about a specific version of the law: an amendment can renumber, revoke or rewrite a labelled provision, and
a label written by hand (or by an LLM) can simply be wrong. A wrong label is worse than no label, because the
evaluation then punishes the right answer. So the labels are checked like any other data contract, before the
evaluation uses them (lc05) and whenever the law changes:

    G1 schema        known fields and values, unique case ids, type <-> route <-> behavior consistent
    G2 version       the set was labelled against the current law version (manifest `current`)
    G3 exists        every expected id is a row of dispositivos.parquet
    G4 status        expected ids are `vigente`, or the `expected_status` of a status case
    G5 amended       an `amended` case points at provisions that carry an amendment note
    G6 retrievable   a vector case's ids land in chunks, and a mode=all case needs at most k chunks

and the router (router.py, ADR 0012), which is deterministic, so any miss is a bug, not a score:

    R1 route         every phrasing of a case takes the case's route (sql | combined | vector | clarify)
    R2 anchors       sql: the anchors are the expected ids; combined: the case's `anchors`; clarify: the options
                     are the expected ids (ambiguous) or the citation is reported missing (not_found)
    R3 self-refs     the citations the law makes to itself resolve (>= LC214_MIN_SELF_REF): a parser regression
                     shows up on 1,335 real citations, not only on the handful in the set

A case with `known_issue` documents a failure we know about: its violations are reported, not fatal. Strict: when
it stops failing, the contract fails until the marker is removed (the same rule as an xfail(strict=True)).

Run against the lake: `python -m pipelines.rag.lc214.golden` (LC214_LAKE / --lake, like the lc steps).
Exit codes: 0 contract holds | 1 violations (or the inputs are missing).
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import polars as pl
import yaml

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.logger import get_logger, log_event  # noqa: E402
from pipelines.observability import current_step  # noqa: E402
from pipelines.rag.lc214.lake import Lake, LakeError, k_manifest, k_silver, lake_from_env  # noqa: E402
from pipelines.rag.lc214.router import Index, route, self_references  # noqa: E402

logger = get_logger("lc214_golden")

GOLDEN_PATH = Path(__file__).with_name("golden_set.yml")
EXIT_OK, EXIT_FAILED = 0, 1
MIN_SELF_REF = float(os.getenv("LC214_MIN_SELF_REF", "0.99"))

# type -> (route the router must take, allowed behaviors). The route is the router's decision; what happens after
# it (abstain when nothing is similar enough) is the behavior: a no_answer question still goes to the vectors.
TYPES = {
    "content": ("vector", {"answer"}),
    "amended": ("vector", {"answer"}),
    "anomaly": ("vector", {"answer"}),
    "external_ref": ("vector", {"cite_external"}),
    "no_answer": ("vector", {"abstain"}),
    "citation": ("sql", {"answer"}),
    "status": ("sql", {"report_status"}),
    "out_of_scope": ("sql", {"abstain"}),
    "combined": ("combined", {"answer"}),
    "ambiguous": ("clarify", {"ask_path"}),
    "not_found": ("clarify", {"report_missing"}),
}
EMPTY_EXPECTED = {"no_answer", "not_found"}
STATUSES = {"revogado", "vetado"}
MODES = {"all", "any"}
SCOPES = {"now", "annexes", "future_project"}
CASE_ID = re.compile(r"^[a-z_]+-\d{2}$")
PROVISION_ID = re.compile(r"^lc214-2025:art-\d+(-[A-Z])?(:(par-(\d+(-[A-Z])?|unico)|inc-[IVXLC]+(-[A-Z])?|ali-[a-z])"
                          r"(#\d+)?)*$")


def load_golden(path: Path | str = GOLDEN_PATH) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# --------------------------------------------------------------------------- G1: schema (no data needed)
def validate_schema(golden: dict) -> list[dict]:
    """Structural violations of the set itself. Runs in CI, without the lake."""
    out = []

    def bad(case, rule, detail):
        out.append({"case": case, "rule": rule, "detail": detail})

    if not re.fullmatch(r"[0-9a-f]{12}", str(golden.get("law_version", ""))):
        bad("*", "G1_law_version", "law_version must be the sha12 of the labelled version")
    if not isinstance(golden.get("k"), int) or golden["k"] < 1:
        bad("*", "G1_k", "k must be a positive integer")
    cases = golden.get("cases") or []
    if not cases:
        bad("*", "G1_cases", "no cases")
    for cid, n in Counter(c.get("id") for c in cases).items():
        if n > 1:
            bad(cid, "G1_duplicate_id", f"case id used {n} times")

    for c in cases:
        cid, ctype = c.get("id", "?"), c.get("type")
        if not CASE_ID.fullmatch(str(cid)):
            bad(cid, "G1_case_id", "case id must look like 'content-01'")
        if ctype not in TYPES:
            bad(cid, "G1_type", f"unknown type {ctype!r}")
            continue
        route, behaviors = TYPES[ctype]
        if c.get("route") != route:
            bad(cid, "G1_route", f"type {ctype} goes by route {route!r}, not {c.get('route')!r}")
        if c.get("behavior") not in behaviors:
            bad(cid, "G1_behavior", f"type {ctype} expects behavior in {sorted(behaviors)}")
        if "dev" in c and not isinstance(c["dev"], bool):
            bad(cid, "G1_dev", "dev must be true or false")
        questions = c.get("questions") or {}
        if not str(questions.get("fiscal", "")).strip():
            bad(cid, "G1_question", "the fiscal phrasing is required (it is the primary user)")
        if set(questions) - {"fiscal", "formal"}:
            bad(cid, "G1_question", f"unknown phrasings {sorted(set(questions) - {'fiscal', 'formal'})}")
        expected = c.get("expected")
        if not isinstance(expected, list):
            bad(cid, "G1_expected", "expected must be a list (empty for no_answer)")
            continue
        for pid in expected:
            if not PROVISION_ID.fullmatch(str(pid)):
                bad(cid, "G1_expected_id", f"{pid!r} is not a provision id")
        if ctype in EMPTY_EXPECTED:
            if expected:
                bad(cid, "G1_expected", f"a {ctype} case has nothing to retrieve")
        else:
            if not expected:
                bad(cid, "G1_expected", "expected ids are required")
            if route != "clarify" and c.get("mode") not in MODES:
                bad(cid, "G1_mode", f"mode must be one of {sorted(MODES)}")
        anchors = c.get("anchors")
        if ctype == "combined":
            if not anchors or not all(PROVISION_ID.fullmatch(str(a)) for a in anchors):
                bad(cid, "G1_anchors", "a combined case lists the cited provisions in `anchors`")
        elif anchors is not None:
            bad(cid, "G1_anchors", "only combined cases have `anchors` (sql anchors are the expected ids)")
        if ctype in ("status", "out_of_scope") and c.get("expected_status") not in STATUSES:
            bad(cid, "G1_expected_status", f"expected_status must be one of {sorted(STATUSES)}")
        if ctype not in ("status", "out_of_scope") and "expected_status" in c:
            bad(cid, "G1_expected_status", "only status and out_of_scope cases expect a non-vigente status")
        if ctype == "external_ref":
            if not str(c.get("external_source", "")).strip():
                bad(cid, "G1_external_source", "name the law or table the answer depends on")
            if c.get("scope") not in SCOPES:
                bad(cid, "G1_scope", f"scope must be one of {sorted(SCOPES)}")
        elif c.get("scope", "now") != "now":
            bad(cid, "G1_scope", "only external_ref cases can be out of the current scope")
    return out


# --------------------------------------------------------------------------- G2-G6: against the data
def chunk_index(chunks: pl.DataFrame) -> dict[str, set[str]]:
    """provision id -> the chunks that carry it. Usually one; a provision too long for one chunk (art. 517 quotes
    23k characters of the LC 123) is split into word windows, and retrieving any of them retrieves it."""
    index = defaultdict(set)
    for cid, ids in chunks.select("chunk_id", "dispositivo_ids").iter_rows():
        for pid in ids:
            index[pid].add(cid)
    return index


def chunks_needed(expected: list[str], index: dict[str, set[str]]) -> int:
    """Fewest top-k slots that retrieve every expected provision: provisions sharing a chunk count once."""
    groups: list[set[str]] = []
    for pid in expected:
        found = set(index.get(pid, ()))
        if not found:
            continue
        overlapping = [g for g in groups if g & found]
        for g in overlapping:
            groups.remove(g)
            found |= g
        groups.append(found)
    return len(groups)


def routing_violations(case: dict, index: Index) -> list[dict]:
    """R1/R2 for every phrasing of a case: the router is deterministic, so the expectation is exact."""
    out, ctype = [], case.get("type")
    if ctype not in TYPES:
        return out
    want_route = TYPES[ctype][0]
    expected = sorted(case.get("expected") or [])
    for name, question in (case.get("questions") or {}).items():
        plan = route(str(question), index)

        def bad(rule, detail, _name=name):
            out.append({"case": case.get("id"), "rule": rule, "detail": f"{_name}: {detail}"})

        if plan.route != want_route:
            bad("R1_route", f"routed to {plan.route} ({plan.reason()}), expected {want_route}")
            continue
        if want_route == "sql" and sorted(plan.anchors) != expected:
            bad("R2_anchors", f"anchors {plan.anchors}, expected {expected}")
        elif want_route == "combined" and sorted(plan.anchors) != sorted(case.get("anchors") or []):
            bad("R2_anchors", f"anchors {plan.anchors}, expected {case.get('anchors')}")
        elif ctype == "ambiguous" and sorted(i for ids in plan.options.values() for i in ids) != expected:
            bad("R2_options", f"options {plan.options}, expected {expected}")
        elif ctype == "not_found" and not plan.missing:
            bad("R2_missing", f"expected a missing citation, got {plan.intent}")
    return out


def check(golden: dict, dispositivos: pl.DataFrame, chunks: pl.DataFrame, current_sha: str) -> dict:
    """The full contract. Returns a report; `passed` is False on any violation outside a known_issue case."""
    k = golden.get("k", 5)
    violations = validate_schema(golden)
    if golden.get("law_version") != current_sha[:12]:
        violations.append({"case": "*", "rule": "G2_version", "detail": f"labelled against {golden.get('law_version')}"
                           f", current law is {current_sha[:12]}: review every label before evaluating"})

    rows = {r["dispositivo_id"]: r for r in dispositivos.select("dispositivo_id", "status", "alterado_por")
            .iter_rows(named=True)}
    chunk_of = chunk_index(chunks)
    index = Index(dispositivos["dispositivo_id"].to_list())

    per_case, known, resolved = {}, [], []
    for c in golden.get("cases") or []:
        cid, ctype = c.get("id"), c.get("type")
        found = []

        def bad(rule, detail, _found=found, _cid=cid):
            _found.append({"case": _cid, "rule": rule, "detail": detail})

        expected = [p for p in (c.get("expected") or []) if isinstance(p, str)]
        anchors = [p for p in (c.get("anchors") or []) if isinstance(p, str)]
        want_status = c.get("expected_status", "vigente")
        for pid in dict.fromkeys(expected + anchors):
            row = rows.get(pid)
            if row is None:
                bad("G3_exists", f"{pid} is not in dispositivos")
                continue
            if row["status"] != want_status:
                bad("G4_status", f"{pid} is {row['status']}, expected {want_status}")
            if ctype == "amended" and not row["alterado_por"]:
                bad("G5_amended", f"{pid} has no amendment note")

        needed = chunks_needed(expected, chunk_of)
        if TYPES.get(ctype, ("",))[0] in ("vector", "combined"):
            for pid in expected:
                if pid in rows and pid not in chunk_of:
                    bad("G6_retrievable", f"{pid} is in no chunk: a vector search can never return it")
            if c.get("mode") == "all" and needed > k:
                bad("G6_retrievable", f"needs {needed} chunks, more than k={k}: coverage@{k} cannot reach 1")
        routed = routing_violations(c, index)
        found.extend(routed)
        per_case[cid] = {"type": ctype, "route": c.get("route"), "scope": c.get("scope", "now"),
                         "expected": len(expected), "chunks_needed": needed,
                         "phrasings": len(c.get("questions") or {}), "routed_ok": len(c.get("questions") or {})
                         - len({v["detail"].split(":")[0] for v in routed})}

        if c.get("known_issue"):
            if found:
                known.extend(found)
            else:
                resolved.append(cid)
                violations.append({"case": cid, "rule": "known_issue_resolved",
                                   "detail": "the case passes now: remove `known_issue`"})
        else:
            violations.extend(found)

    refs = self_references(dispositivos.select("dispositivo_id", "tipo", "texto", "altera_outra_lei").to_dicts(),
                           index)
    if refs["rate"] < MIN_SELF_REF:
        violations.append({"case": "*", "rule": "R3_self_refs", "detail": f"the router resolves {refs['rate']:.2%} "
                           f"of the law's {refs['own']} citations of itself, below {MIN_SELF_REF:.0%}: "
                           f"first misses {[m['citation'] + ' in ' + m['in'] for m in refs['misses'][:5]]}"})
    phrasings = sum(v["phrasings"] for v in per_case.values())
    routed_ok = sum(v["routed_ok"] for v in per_case.values())
    vector = [v["chunks_needed"] for v in per_case.values() if v["route"] in ("vector", "combined") and v["expected"]]
    return {
        "passed": not violations,
        "law_version": golden.get("law_version"),
        "current_version": current_sha[:12],
        "k": k,
        "cases": len(per_case),
        "by_type": dict(Counter(v["type"] for v in per_case.values())),
        "by_route": dict(Counter(v["route"] for v in per_case.values())),
        "by_scope": dict(Counter(v["scope"] for v in per_case.values())),
        "chunks_needed": {"max": max(vector, default=0), "multi_chunk_cases": sum(n > 1 for n in vector)},
        "routing": {"phrasings": phrasings, "ok": routed_ok, "accuracy": round(routed_ok / phrasings, 4) if phrasings
                    else 1.0},
        "self_refs": {k: v for k, v in refs.items() if k != "misses"} | {"misses": refs["misses"][:20]},
        "violations": violations,
        "known_issues": known,
        "known_issues_resolved": resolved,
        "per_case": per_case,
    }


# --------------------------------------------------------------------------- CLI: read the current version from the lake
def run(lake: Lake, path: Path | str = GOLDEN_PATH) -> int:
    sha = lake.get_json(k_manifest())["current"]
    dispositivos = lake.get_parquet(k_silver(sha, "dispositivos.parquet"))
    chunks = lake.get_parquet(k_silver(sha, "chunks.parquet"))
    report = check(load_golden(path), dispositivos, chunks, sha)
    current_step().add(rows_in=report["cases"], routing_accuracy=report["routing"]["accuracy"],
                       self_ref_rate=report["self_refs"]["rate"])

    for v in report["known_issues"]:
        log_event(logger, "golden_known_issue", f"{v['case']} {v['rule']}: {v['detail']}", case=v["case"])
    for v in report["violations"]:
        log_event(logger, "golden_contract_violation", f"{v['case']} {v['rule']}: {v['detail']}", level=40,
                  case=v["case"], rule=v["rule"])
    r, sr = report["routing"], report["self_refs"]
    summary = (f"{report['cases']} cases {report['by_type']}, routes {report['by_route']}, "
               f"routing {r['ok']}/{r['phrasings']} phrasings, self-references {sr['resolved']}/{sr['own']} "
               f"({sr['rate']:.2%}), chunks needed max={report['chunks_needed']['max']}, "
               f"{len(report['violations'])} violation(s), {len(report['known_issues'])} known issue(s)")
    if report["passed"]:
        log_event(logger, "golden_contract_passed", summary, cases=report["cases"], law_version=sha[:12],
                  known_issues=len(report["known_issues"]), routing_accuracy=r["accuracy"],
                  self_ref_rate=sr["rate"], self_ref_misses=sr["own"] - sr["resolved"])
        return EXIT_OK
    log_event(logger, "golden_contract_failed", summary, level=40, cases=report["cases"], law_version=sha[:12],
              violations=len(report["violations"]))
    return EXIT_FAILED


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lake", default=None, help="'s3' (default, LC214_LAKE) or file:///dir")
    ap.add_argument("--golden", default=str(GOLDEN_PATH))
    args = ap.parse_args(argv)
    try:
        return run(lake_from_env(args.lake), args.golden)
    except LakeError as exc:
        log_event(logger, exc.event, str(exc), level=40)
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
