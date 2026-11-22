"""Official legal basis of each cClassTrib (ref_rtc) -> provisions of the LC 214 (ai.lc214_dispositivo).

Step 3 of the RTC reference pipeline (ADR 0021): the bridge between the NF-e side (every item carries a
cClassTrib) and the law side (the LC 214 RAG). The Receita's table gives each classification a short legal
reference (FUNDAMENTACAO_LEGAL.FDLG_TEXTO_CURTO: "Art. 412, I", "Art. 128, I, II e XIII") and a transcription of
the cited text (FDLG_TEXTO). This step reads the reference with the LC 214 router's own parser (no LLM: the same
deterministic code the golden contract checks) and resolves it against the provisions of the law version served:

    reference "Art. 146, § 1º, I e II"
        -> citations  art. 146, § 1, inciso I  |  art. 146, § 1, inciso II
        -> provisions lc214-2025:art-146:par-1:inc-I  |  lc214-2025:art-146:par-1:inc-II   (outcome resolved)

One row per (reference, citation) in ai.rtc_lc214_fundamento, with:
    outcome       resolved | ambiguous (the law prints several candidates) | not_found | foreign (another law) |
                  unparsed (no citation in the reference, e.g. "LC 214/2025" with "Regra Geral")
    status_lei    vigente | revogado | vetado: a cClassTrib grounded in a revoked provision is a signal
    texto_check   the transcription against the law served, per resolved provision:
                  match (the provision's text is in the transcription, word by word) | near (>= 90% of its words) |
                  diverges | no_transcription ("Regra Geral", empty)
                  A divergence is a fact about two sources, not a verdict: the table can be behind an amendment,
                  or carry a transcription slip; the row says which provision and how much of it was found.

Skip rule (exit 99): the table already holds the bridge of the same (rtc sha, law sha256, BRIDGE_VERSION), or the
law is not served yet (dag_rag_lc214 needs the AI profile): `law_not_loaded`, the checks are `skipped`.
Load: DELETE + INSERT in one transaction (~250 rows): readers see the old bridge or the new one.

Exit codes: 0 rebuilt | 99 unchanged or law not loaded | 1 failed.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import unicodedata
from collections import Counter
from dataclasses import dataclass

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.logger import get_logger, log_event  # noqa: E402
from pipelines.observability import current_step  # noqa: E402
from pipelines.rag.lc214.router import Index, parse_citations  # noqa: E402

logger = get_logger("rtc03_lc214")

EXIT_OK, EXIT_SKIPPED, EXIT_FAILED = 0, 99, 1
# Bump when the rows this step writes change meaning (parser rule, text check): the next run rebuilds the bridge
# even if neither source changed. The router's own changes are covered too: it is this step's parser.
BRIDGE_VERSION = "1"
NEAR = 0.90                 # share of the provision's words found in the transcription to call it "near"
MIN_TRANSCRIPTION_WORDS = 5
TABLE = "ai.rtc_lc214_fundamento"


# --------------------------------------------------------------------------- pure logic (unit-tested)
def words(text: str | None) -> list[str]:
    """Accent-free lower-case words. The transcriptions carry real newlines, literal '\\n' and long dashes; the law
    carries its own punctuation: only the words are compared."""
    text = (text or "").replace("\\n", " ")
    text = unicodedata.normalize("NFKD", text.lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.findall(r"[a-z0-9]+", text)


def _contains(seq: list[str], sub: list[str]) -> bool:
    n = len(sub)
    return n > 0 and any(seq[i:i + n] == sub for i in range(len(seq) - n + 1))


def text_check(provision_text: str, transcription: str | None) -> tuple[str, float | None]:
    """(match | near | diverges | no_transcription, share of the provision's words found in the transcription)."""
    t = words(transcription)
    if len(t) < MIN_TRANSCRIPTION_WORDS:
        return "no_transcription", None
    p = words(provision_text)
    if not p:
        return "no_transcription", None
    if _contains(t, p):
        return "match", 1.0
    pool = Counter(t)
    found = sum(min(n, pool[w]) for w, n in Counter(p).items())
    share = round(found / len(p), 4)
    return ("near" if share >= NEAR else "diverges"), share


@dataclass
class BridgeRow:
    fdlg_id: int
    referencia: str | None
    ordem: int
    citacao: str | None
    outcome: str
    dispositivo_id: str | None = None
    candidatos: list[str] | None = None
    status_lei: str | None = None
    texto_check: str | None = None
    cobertura_texto: float | None = None


def build_bridge(fundamentos: list[dict], law: dict[str, dict]) -> list[BridgeRow]:
    """fundamentos: [{fdlg_id, fdlg_texto_curto, fdlg_texto}]; law: dispositivo_id -> {status, texto}.
    One row per citation of each reference; a reference with no citation is one `unparsed` row."""
    index = Index(law)
    out = []
    for f in fundamentos:
        ref = (f.get("fdlg_texto_curto") or "").strip() or None
        cites = parse_citations(ref) if ref else []
        if not cites:
            out.append(BridgeRow(f["fdlg_id"], ref, 1, None, "unparsed"))
            continue
        for n, c in enumerate(cites, 1):
            row = BridgeRow(f["fdlg_id"], ref, n, c.label(), "foreign" if c.foreign else "")
            if not c.foreign:
                hits = index.resolve(c)
                if len(hits) == 1:
                    prov = law[hits[0]]
                    row.outcome, row.dispositivo_id, row.status_lei = "resolved", hits[0], prov["status"]
                    row.texto_check, row.cobertura_texto = text_check(prov["texto"], f.get("fdlg_texto"))
                elif hits:
                    row.outcome, row.candidatos = "ambiguous", hits
                else:
                    row.outcome = "not_found"
            out.append(row)
    return out


def summarize(rows: list[BridgeRow]) -> dict:
    """Counts for the step's metrics and the log line (the health checks read the table itself)."""
    outcome = Counter(r.outcome for r in rows)
    check = Counter(r.texto_check for r in rows if r.outcome == "resolved")
    own = outcome["resolved"] + outcome["ambiguous"] + outcome["not_found"]
    return {
        "references": len({r.fdlg_id for r in rows}),
        "citations": len(rows),
        **{f"outcome_{k}": outcome[k] for k in ("resolved", "ambiguous", "not_found", "foreign", "unparsed")},
        "resolved_pct": round(100 * outcome["resolved"] / own, 2) if own else None,
        "cites_inactive": sum(1 for r in rows if r.outcome == "resolved" and r.status_lei != "vigente"),
        **{f"text_{k}": check[k] for k in ("match", "near", "diverges", "no_transcription")},
    }


# --------------------------------------------------------------------------- database
def versions(cur) -> tuple[str | None, str | None]:
    """(rtc sha of the reference in use, sha256 of the law served)."""
    cur.execute("SELECT sha FROM ctrl.rtc_carga ORDER BY loaded_at DESC, carga_id DESC LIMIT 1")
    rtc = cur.fetchone()
    cur.execute("SELECT min(source_sha256), count(DISTINCT source_sha256) FROM ai.lc214_dispositivo")
    law_sha, n = cur.fetchone()
    if n and n > 1:
        raise BridgeError("law_mixed_versions", f"ai.lc214_dispositivo holds {n} law versions (check "
                          "lc214_serving_single_version): the bridge would resolve against a mix")
    return (rtc[0] if rtc else None), law_sha


def current_fingerprint(cur) -> set[tuple]:
    cur.execute(f"SELECT DISTINCT rtc_sha, law_sha256, bridge_version FROM {TABLE}")
    return set(cur.fetchall())


def read_inputs(cur) -> tuple[list[dict], dict[str, dict]]:
    cur.execute("SELECT fdlg_id, fdlg_texto_curto, fdlg_texto FROM ref_rtc.fundamentacao_legal ORDER BY fdlg_id")
    cols = [d[0] for d in cur.description]
    fundamentos = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.execute("SELECT dispositivo_id, status, texto FROM ai.lc214_dispositivo")
    law = {pid: {"status": status, "texto": texto} for pid, status, texto in cur.fetchall()}
    return fundamentos, law


def write(cur, rows: list[BridgeRow], rtc_sha: str, law_sha: str) -> None:
    cur.execute(f"DELETE FROM {TABLE}")
    cur.executemany(
        f"""INSERT INTO {TABLE} (fdlg_id, referencia, ordem, citacao, outcome, dispositivo_id, candidatos,
                                status_lei, texto_check, cobertura_texto, rtc_sha, law_sha256, bridge_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)""",
        [(r.fdlg_id, r.referencia, r.ordem, r.citacao, r.outcome, r.dispositivo_id,
          json.dumps(r.candidatos) if r.candidatos else None, r.status_lei, r.texto_check, r.cobertura_texto,
          rtc_sha, law_sha, BRIDGE_VERSION) for r in rows])


class BridgeError(Exception):
    def __init__(self, event: str, message: str):
        super().__init__(message)
        self.event = event


def run(conn, *, force: bool = False) -> int:
    step = current_step()
    with conn.cursor() as cur:
        rtc_sha, law_sha = versions(cur)
        if not rtc_sha:
            raise BridgeError("reference_not_loaded", "ctrl.rtc_carga is empty: run rtc01/rtc02 first")
        if not law_sha:
            log_event(logger, "law_not_loaded", "ai.lc214_dispositivo is empty (dag_rag_lc214 runs on the AI "
                      "profile): nothing to link the references to")
            return EXIT_SKIPPED
        step.label(source_sha=rtc_sha[:12], law_version=law_sha[:12], bridge_version=BRIDGE_VERSION)
        if current_fingerprint(cur) == {(rtc_sha, law_sha, BRIDGE_VERSION)} and not force:
            log_event(logger, "bridge_up_to_date", f"reference {rtc_sha[:12]} x law {law_sha[:12]} already linked")
            return EXIT_SKIPPED

        fundamentos, law = read_inputs(cur)
        rows = build_bridge(fundamentos, law)
        try:
            write(cur, rows, rtc_sha, law_sha)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    s = summarize(rows)
    step.add(rows_in=len(fundamentos), rows_out=len(rows),
             **{k: v for k, v in s.items() if isinstance(v, (int, float)) and k not in ("references", "citations")})
    log_event(logger, "bridge_built",
              f"{s['references']} references -> {s['citations']} citations: {s['outcome_resolved']} resolved "
              f"({s['resolved_pct']}%), {s['outcome_ambiguous']} ambiguous, {s['outcome_not_found']} not found; "
              f"{s['cites_inactive']} cite an inactive provision; transcription {s['text_match']} match / "
              f"{s['text_near']} near / {s['text_diverges']} diverge",
              rtc_sha=rtc_sha, law_sha256=law_sha, **s)
    for r in rows:
        if r.outcome in ("ambiguous", "not_found"):
            log_event(logger, f"reference_{r.outcome}", f"{r.referencia!r}: {r.citacao} {r.outcome}",
                      level=30, fdlg_id=r.fdlg_id, candidates=r.candidatos)
    return EXIT_OK


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--force", action="store_true", help="rebuild the bridge even if nothing changed")
    args = ap.parse_args(argv)
    from pipelines.commons.dw_client import get_pg_connection

    conn = get_pg_connection()
    try:
        return run(conn, force=args.force)
    except BridgeError as exc:
        log_event(logger, exc.event, str(exc), level=40)
        return EXIT_FAILED
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
