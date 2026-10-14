"""LC 214/2025 layout text (lc01) -> one row per provision ("dispositivo") with its hierarchy, status and notes.

Step 2 of the legislation RAG. Input: the "current" version in DATASOURCE_DIR/lc214/manifest.json.
Output: parsed/lc214_<sha12>_dispositivos.parquet + parsed/lc214_<sha12>_parse_report.json.

How a PDF becomes structure
    A PDF has no structure, only text at positions. Articles, paragraphs and items are drafting conventions,
    so the parser reads two kinds of signal, line by line (a state machine):
      * markers:  Art. 1º | Art. 10. | Art. 7º-A.   § 1º | § 10. | Parágrafo único.   I - | LXIX -   a)
                  (ordinal up to the ninth, cardinal from the tenth on: LC 95/1998, art. 10)
      * layout:   `pdftotext -layout` keeps the first line of a provision indented and wraps the rest at column 0
    A marker at column 0 is a wrapped reference ("nos termos do\\n§ 3º deste artigo"), unless it is the next
    expected number of its sequence AND the previous line closed a sentence (; : .) - where the PDF lost the
    indentation (arts. 542-543).
    Headings (LIVRO, TÍTULO, CAPÍTULO, Seção, Subseção) set the context of what follows, they are not provisions.
    Text of ANOTHER law quoted inside an amending article ("Art. 9º ....") is content of that article, not
    structure of the LC 214.

Ids
    lc214-2025:art-7-A:par-1:inc-II:ali-a   built from the printed labels, so a citation maps to one row.
    A label printed twice under the same parent keeps the printed label and gets a suffix: inc-II#2.

Data contract (the run fails, exit 1, instead of publishing a wrong table)
    I1 articles 1..N with no gap                    I3 contiguous sequences under each parent (I, II, III...)
    I2 unique ids                                   I4 >= LC214_MIN_COVERAGE of the body words land in a provision
    Source defects are allowed only when listed in known_anomalies.yml (Art. 260 printed twice, inc. II twice).

Exit codes: 0 parsed | 99 this version was already parsed by this parser version | 1 failed.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field

import polars as pl
import yaml

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.env_loader import DATASOURCE_DIR  # noqa: E402
from pipelines.commons.logger import get_logger, log_event  # noqa: E402
from pipelines.observability import current_step  # noqa: E402

logger = get_logger("lc02_parse")

PARSER_VERSION = "1"            # bump when the output changes on purpose (forces a re-parse)
LAW_ID = "lc214-2025"
LC214_DIR = os.getenv("LC214_DIR", os.path.join(DATASOURCE_DIR, "lc214"))
MIN_COVERAGE = float(os.getenv("LC214_MIN_COVERAGE", "0.98"))
ANOMALIES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "known_anomalies.yml")
EXIT_OK, EXIT_SKIPPED, EXIT_FAILED = 0, 99, 1

MIN_INDENT = 8
RX = {
    "art": re.compile(r"Art\. (\d+)[º°]?(?:-([A-Z]+))?[º°]?\.?\s"),
    "par": re.compile(r"(?:§ (\d+)[º°]?(?:-([A-Z]+))?\.?|(Parágrafo único)\.?)\s"),
    "inc": re.compile(r"([IVXLC]+)(?:-([A-Z]+))?\s[-–]\s"),
    "ali": re.compile(r"([a-z])(?:-([A-Z]+))?\)\s"),
}
TYPES = ("art", "par", "inc", "ali")
HEAD = re.compile(r"(LIVRO|T[ÍI]TULO|CAP[ÍI]TULO|Se[çc][ãa]o|Subse[çc][ãa]o) ([IVXLC]+(?:-[A-Z])?)\s*$")
LEVEL = {"LIVRO": 0, "TÍTULO": 1, "TITULO": 1, "CAPÍTULO": 2, "CAPITULO": 2, "Seção": 3, "Secao": 3,
         "Subseção": 4, "Subsecao": 4}
CANON = {"TITULO": "TÍTULO", "CAPITULO": "CAPÍTULO", "Secao": "Seção", "Subsecao": "Subseção"}
QUOTE_OPEN = re.compile(r'\s*["“]')
BODY_END = re.compile(r"^\s*(?:Brasília, \d+ de|ANEXO I\s*$)", re.M)   # signatures, then the annexes
# notes added by the consolidation (Câmara), not law text: "(Parágrafo acrescido pela Lei Complementar nº 227...)"
NOTE = re.compile(r"\(([^()]*?(?:acrescid[oa]|reda[çc][ãa]o dada|Revogad[oa]|VETAD[OA]|Vide |Inclu[íi]d[oa])"
                  r"[^()]*?)\)", re.S)
AMENDED_BY = re.compile(r"Lei Complementar n[ºo] (\d+), de \d{1,2}/\d{1,2}/(\d{4})")
ROMAN = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100}


class ParseError(Exception):
    def __init__(self, event: str, message: str, **fields):
        super().__init__(message)
        self.event, self.fields = event, fields


@dataclass
class Disp:
    tipo: str
    path: dict                 # {"art": "7-A", "par": "1", "inc": "II", "ali": "a"}
    ctx: dict                  # {"LIVRO": "I", "LIVRO_nome": "...", "TÍTULO": "I", ...}
    occ: int                   # occurrence of its article (2 = the article was printed twice)
    linha: int
    pagina: int
    rotulo: str                # label as printed: "Art. 7º-A", "§ 10", "Parágrafo único", "II", "a"
    in_quote: bool = False
    texto: list = field(default_factory=list)

    @property
    def id(self) -> str:
        parts = [LAW_ID, f"art-{self.path['art']}"]
        parts += [f"{k}-{self.path[k]}" for k in ("par", "inc", "ali") if k in self.path]
        return ":".join(parts)


# --------------------------------------------------------------------------- small pure helpers
def roman(r: str) -> int:
    n = 0
    for a, b in zip(r, r[1:] + " "):
        n += -ROMAN[a] if b != " " and ROMAN[a] < ROMAN[b] else ROMAN[a]
    return n


def norm_key(tipo: str, m: re.Match) -> str:
    if tipo == "par" and m.group(3):
        return "unico"
    return f"{m.group(1)}-{m.group(2)}" if m.group(2) else m.group(1)


def seq_val(tipo: str, key: str) -> int:
    base = key.split("#")[0].split("-")[0]
    if tipo == "inc":
        return roman(base)
    if tipo == "ali":
        return ord(base) - 96
    return 1 if base == "unico" else int(base)


def is_inserted(key: str) -> bool:
    """7-A, II-A: inserted by a later law between two numbers, outside the 1..n sequence."""
    return "-" in key.split("#")[0]


def parent_of(tipo: str, path: dict) -> str:
    keep = {"art": (), "par": ("art",), "inc": ("art", "par"), "ali": ("art", "par", "inc")}[tipo]
    return ":".join(f"{k}={path[k]}" for k in keep if k in path)


def label_of(tipo: str, m: re.Match) -> str:
    raw = m.group(0).strip()
    return raw.rstrip(".").rstrip(" -–").rstrip(")").strip() if tipo != "ali" else m.group(1)


def body_of(text: str) -> str:
    """From the first article to the signatures: drops the preamble and the annexes."""
    start = re.search(r"^\s*Art\. 1[º°]", text, re.M)
    if not start:
        raise ParseError("body_not_found", "no 'Art. 1º' in the text")
    end = BODY_END.search(text, start.start())
    return text[: end.start()] if end else text


# --------------------------------------------------------------------------- the state machine
def parse_lines(lines: list[str], stats: Counter | None = None) -> list[Disp]:
    stats = stats if stats is not None else Counter()
    disps: list[Disp] = []
    cur: Disp | None = None
    path: dict = {}
    ctx: dict = {}
    pending_head: str | None = None
    in_quote = False
    last: dict = {}                     # last sequence value per (tipo, parent)
    seen_labels: Counter = Counter()    # (art occurrence, parent, tipo, key) -> times printed
    art_occ: Counter = Counter()
    prev = ""
    page = 1

    for i, raw in enumerate(lines, 1):
        page += raw.count("\f")
        raw = raw.replace("\f", "")
        s = raw.strip()
        if not s:
            continue
        ind = len(raw) - len(raw.lstrip(" "))
        closed = prev.rstrip().endswith((";", ":", "."))
        prev = s

        h = HEAD.match(s)
        if h and not in_quote:
            kind = CANON.get(h.group(1), h.group(1))
            ctx = {k: v for k, v in ctx.items() if LEVEL.get(k.replace("_nome", ""), 99) < LEVEL[kind]}
            ctx[kind] = h.group(2)
            pending_head = kind
            continue
        if pending_head:   # heading names can wrap over several lines: everything until the next marker
            if not any(r.match(s) for r in RX.values()):
                ctx[pending_head + "_nome"] = (ctx.get(pending_head + "_nome", "") + " " + s).strip()
                continue
            pending_head = None

        m_art = RX["art"].match(s)
        real_art = bool(m_art) and (ind >= MIN_INDENT or (closed and not m_art.group(2)
                                                           and int(m_art.group(1)) == last.get(("art", ""), 0) + 1))
        if in_quote and real_art:
            in_quote = False                              # a real LC 214 article closes the quoted block
        if not in_quote and cur is not None and cur.tipo == "art" and ind >= MIN_INDENT and QUOTE_OPEN.match(raw):
            in_quote = True                               # another law's text starts inside this article
            stats["quoted_blocks"] += 1
        if in_quote:
            cur.texto.append(s)
            cur.in_quote = True
            continue

        hit = None
        for tipo in TYPES:
            m = RX[tipo].match(s)
            if not m:
                continue
            if ind < MIN_INDENT:
                key = norm_key(tipo, m)
                if not (closed and not is_inserted(key)
                        and seq_val(tipo, key) == last.get((tipo, parent_of(tipo, path)), 0) + 1):
                    break                                 # column 0 without sequence evidence = continuation
                stats["col0_accepted"] += 1
            hit = (tipo, m)
            break

        if not hit:
            if cur is not None:
                cur.texto.append(s)                       # wrapped line of the current provision
            continue

        tipo, m = hit
        key = norm_key(tipo, m)
        if tipo == "art":
            art_occ[key] += 1
            path = {"art": key}
        else:
            keep = {"par": ("art",), "inc": ("art", "par"), "ali": ("art", "par", "inc")}[tipo]
            path = {k: v for k, v in path.items() if k in keep}
            label_key = (path["art"], art_occ[path["art"]], parent_of(tipo, path), tipo, key)
            seen_labels[label_key] += 1
            if seen_labels[label_key] > 1:                # printed twice: keep the label, make the id unique
                key = f"{key}#{seen_labels[label_key]}"
            path[tipo] = key
        cur = Disp(tipo, dict(path), dict(ctx), art_occ[path["art"]], i, page, label_of(tipo, m), texto=[s])
        disps.append(cur)
        if not is_inserted(key):
            last[(tipo, parent_of(tipo, path))] = seq_val(tipo, key)
    return disps


# --------------------------------------------------------------------------- notes, status, anomalies
def finalize(d: Disp) -> dict:
    full = re.sub(r"\s+", " ", " ".join(d.texto)).strip()
    notes = [re.sub(r"\s+", " ", n).strip() for n in NOTE.findall(full)]
    clean = re.sub(r"\s+", " ", NOTE.sub(" ", full)).strip()
    # "(VETADO)" and "(VETADO na Lei Complementar nº 227, ...)": a veto of the amending law counts too
    status = ("vetado" if re.search(r"\(VETAD[OA]\b", full)
              else "revogado" if any(n.lower().startswith("revogad") for n in notes) else "vigente")
    amended = sorted({f"LC {n}/{y}" for note in notes for n, y in AMENDED_BY.findall(note)})
    kinds = sorted({k for note in notes for k, rx in (("redacao_dada", r"reda[çc][ãa]o dada"), ("acrescido",
                    r"acrescid"), ("revogado", r"revogad"), ("vetado", r"vetad"), ("vide", r"^vide"))
                    if re.search(rx, note, re.I)})
    return {"texto": clean, "texto_original": full, "notas": notes, "status": status,
            "alterado_por": amended, "tipo_alteracao": kinds}


def resolve_duplicate_articles(disps: list[Disp], known: dict) -> tuple[list[Disp], list[dict]]:
    """An article printed twice: keep the version the registry rule picks, drop the other one."""
    occs = defaultdict(set)
    for d in disps:
        occs[d.path["art"]].add(d.occ)
    rules = {str(e["art"]): e for e in known.get("duplicate_articles", [])}
    dropped, report = set(), []
    for art, o in sorted(occs.items()):
        if len(o) == 1:
            continue
        entry = rules.get(art)
        if not entry:
            raise ParseError("anomaly_unknown", f"Art. {art} printed {len(o)} times and not in known_anomalies.yml",
                             art=art)
        if entry["rule"] != "annotated_wins":
            raise ParseError("anomaly_rule_unknown", f"rule {entry['rule']!r} for Art. {art}")
        caputs = {d.occ: finalize(d) for d in disps if d.tipo == "art" and d.path["art"] == art}
        annotated = [occ for occ, f in caputs.items() if "redacao_dada" in f["tipo_alteracao"]]
        if len(annotated) != 1:
            raise ParseError("anomaly_rule_undecided", f"Art. {art}: {len(annotated)} annotated versions, rule "
                             "annotated_wins needs exactly one", art=art)
        dropped |= {(art, occ) for occ in o if occ != annotated[0]}
        report.append({"type": "duplicate_article", "art": art, "kept_occurrence": annotated[0],
                       "dropped_occurrences": sorted(o - {annotated[0]}), "verified": entry.get("verified", False)})
    return [d for d in disps if (d.path["art"], d.occ) not in dropped], report


def check_repeated_labels(disps: list[Disp], known: dict) -> list[dict]:
    found = {d.id for d in disps if "#" in d.id}
    allowed = {e["id"] for e in known.get("repeated_labels", [])}
    unknown = sorted(found - allowed)
    if unknown:
        raise ParseError("anomaly_unknown", f"labels printed twice, not in known_anomalies.yml: {unknown[:5]}",
                         ids=unknown)
    for gone in sorted(allowed - found):
        log_event(logger, "anomaly_resolved", f"{gone} is no longer in the source: remove it from the registry",
                  id=gone)
    return [{"type": "repeated_label", "id": i} for i in sorted(found)]


# --------------------------------------------------------------------------- the data contract
def invariants(disps: list[Disp], body_words: int) -> dict:
    arts = sorted({seq_val("art", d.path["art"]) for d in disps if d.tipo == "art"})
    ids = Counter(d.id for d in disps)
    seqs = defaultdict(list)
    for d in disps:
        key = d.path[d.tipo]
        if d.tipo != "art" and not is_inserted(key) and "#" not in key:
            seqs[(d.tipo, parent_of(d.tipo, d.path))].append(seq_val(d.tipo, key))
    gaps = [{"tipo": t, "parent": p, "seq": v[:10]} for (t, p), v in seqs.items() if v != list(range(1, len(v) + 1))]
    words = sum(len(" ".join(d.texto).split()) for d in disps)
    result = {
        "I1_articles_contiguous": arts == list(range(1, (arts[-1] if arts else 0) + 1)),
        "I2_unique_ids": all(c == 1 for c in ids.values()),
        "I3_contiguous_sequences": not gaps,
        "I4_coverage": round(words / body_words, 4) if body_words else 0.0,
        "last_article": arts[-1] if arts else 0,
        "duplicated_ids": [k for k, c in ids.items() if c > 1][:10],
        "sequence_gaps": gaps[:10],
    }
    result["passed"] = (result["I1_articles_contiguous"] and result["I2_unique_ids"]
                        and result["I3_contiguous_sequences"] and result["I4_coverage"] >= MIN_COVERAGE)
    return result


def to_frame(disps: list[Disp], sha: str) -> pl.DataFrame:
    rows = []
    for ordem, d in enumerate(disps, 1):
        f = finalize(d)
        pid = None
        if d.tipo != "art":
            up = {k: v for k, v in d.path.items() if k != d.tipo}
            pid = ":".join([LAW_ID, f"art-{up['art']}"] + [f"{k}-{up[k]}" for k in ("par", "inc") if k in up])
        rows.append({
            "dispositivo_id": d.id, "parent_id": pid, "tipo": d.tipo, "rotulo": d.rotulo, "ordem": ordem,
            "art": d.path["art"], "par": d.path.get("par"), "inc": d.path.get("inc"), "ali": d.path.get("ali"),
            "livro": d.ctx.get("LIVRO"), "livro_nome": d.ctx.get("LIVRO_nome"),
            "titulo": d.ctx.get("TÍTULO"), "titulo_nome": d.ctx.get("TÍTULO_nome"),
            "capitulo": d.ctx.get("CAPÍTULO"), "capitulo_nome": d.ctx.get("CAPÍTULO_nome"),
            "secao": d.ctx.get("Seção"), "secao_nome": d.ctx.get("Seção_nome"),
            "subsecao": d.ctx.get("Subseção"), "subsecao_nome": d.ctx.get("Subseção_nome"),
            "altera_outra_lei": d.in_quote, "pagina": d.pagina, "linha": d.linha,
            "source_sha256": sha, "parser_version": PARSER_VERSION, **f,
        })
    return pl.DataFrame(rows, infer_schema_length=None)


# --------------------------------------------------------------------------- the step
def outputs(lc_dir: str, sha: str) -> tuple[str, str]:
    base = os.path.join(lc_dir, "parsed", f"lc214_{sha[:12]}")
    return base + "_dispositivos.parquet", base + "_parse_report.json"


def run(lc_dir: str = LC214_DIR, anomalies_file: str = ANOMALIES_FILE, force: bool = False) -> int:
    step = current_step()
    manifest_path = os.path.join(lc_dir, "manifest.json")
    if not os.path.exists(manifest_path):
        raise ParseError("source_missing", f"{manifest_path} not found: run lc01_extract first")
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)
    sha = manifest["current"]
    version = next(v for v in manifest["versions"] if v["sha256"] == sha)
    parquet, report_path = outputs(lc_dir, sha)
    if not force and os.path.exists(parquet) and os.path.exists(report_path):
        with open(report_path, encoding="utf-8") as f:
            if json.load(f).get("parser_version") == PARSER_VERSION:
                log_event(logger, "parse_up_to_date", f"version {sha[:12]} already parsed (parser v{PARSER_VERSION})",
                          sha256=sha)
                return EXIT_SKIPPED

    with open(os.path.join(lc_dir, version["text"]), encoding="utf-8") as f:
        body = body_of(f.read())
    with open(anomalies_file, encoding="utf-8") as f:
        known = yaml.safe_load(f) or {}

    stats = Counter()
    disps = parse_lines(body.split("\n"), stats)
    disps, dup_report = resolve_duplicate_articles(disps, known)
    label_report = check_repeated_labels(disps, known)
    first = re.search(r"^\s*Art\. 1[º°]", body, re.M).start()
    inv = invariants(disps, len(body[first:].split()))

    df = to_frame(disps, sha)
    report = {
        "source_sha256": sha, "parser_version": PARSER_VERSION, "dispositivos": df.height,
        "by_type": dict(Counter(df["tipo"].to_list())), "by_status": dict(Counter(df["status"].to_list())),
        "amended_by_lc227": int(df["alterado_por"].list.contains("LC 227/2026").sum()),
        "articles_quoting_other_laws": int(df.filter(pl.col("tipo") == "art")["altera_outra_lei"].sum()),
        "col0_accepted": stats["col0_accepted"], "invariants": inv, "anomalies": dup_report + label_report,
    }
    if not inv["passed"]:
        raise ParseError("parse_contract_failed", f"invariants failed: {json.dumps(inv, ensure_ascii=False)}",
                         invariants=inv)

    os.makedirs(os.path.dirname(parquet), exist_ok=True)
    df.write_parquet(parquet + ".tmp")
    os.replace(parquet + ".tmp", parquet)
    with open(report_path + ".tmp", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    os.replace(report_path + ".tmp", report_path)

    log_event(logger, "parse_done", f"{df.height} dispositivos from version {sha[:12]} "
              f"({report['by_type']}), coverage {inv['I4_coverage']:.1%}, {len(report['anomalies'])} known anomalies",
              sha256=sha, rows=df.height, coverage=inv["I4_coverage"], anomalies=len(report["anomalies"]))
    step.add(rows_out=df.height)
    return EXIT_OK


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dir", default=LC214_DIR)
    ap.add_argument("--force", action="store_true", help="re-parse even if this version was already parsed")
    args = ap.parse_args(argv)
    try:
        return run(args.dir, force=args.force)
    except ParseError as exc:
        log_event(logger, exc.event, str(exc), level=40, **exc.fields)
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
