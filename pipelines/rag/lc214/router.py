"""LC 214 question router: a citation is resolved by SQL, an open question goes to vectors, both when both (ADR 0012).

    "o que diz o art. 12, § 2º, III?"          -> sql       the cited provision, literal, no LLM
    "o art. 57, § 4º ainda vale?"              -> sql       status by lookup: "revogado pela LC 227/2026"
    "o art. 6º se aplica a transferência...?"  -> combined  the cited article is a mandatory anchor + vector search
    "vendi pra filial, incide IBS?"            -> vector    no citation
    "o que diz a alínea a do art. 57?"         -> clarify   3 alíneas "a" in art. 57: ask for the full path
    "o que diz o art. 600?"                    -> clarify   no such provision: say so, never guess

Why not vectors for everything: an embedding retrieves similar text, not identifiers (ADR 0005). "art. 6º" in a
question does not bring art. 6º back, and the auditor who cited it expects it in the answer.

A citation is read in both orders: the chat order (big to small: "art. 12, § 2º, III, a") and the legal order
(small to big: "alínea a do inciso III do § 2º do art. 12", how the law cites itself: 895 times in its own text).
A citation of ANOTHER law ("art. 14 do Código Tributário Nacional", "art. 243 da Lei nº 6.404") is not an anchor:
it is reported as `foreign`, the question goes to vectors, and the answer must say the rule lives elsewhere.

The router needs only the set of provision ids (no database): the same code runs in the golden contract (DAG), in
the unit tests and in the CLI. The CLI renders the SQL answer from Postgres:
    python -m pipelines.rag.lc214.router "o que diz o art. 57?" [--db]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import unicodedata
from collections import Counter
from dataclasses import dataclass, field

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

LAW_ID = "lc214-2025"
LEVELS = ("par", "inc", "ali")              # below the article, in the law's hierarchy
ROMAN = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100}
ANSWER_PAGE_WORDS = int(os.getenv("LC214_ANSWER_PAGE_WORDS", "400"))

# --------------------------------------------------------------------------- lexicon (on accent-free lower text)
_ORD = r"(?:\s*(?:o|º|°)(?![a-z]))?"
ART = re.compile(r"\bart(?:igo)?s?\.?\s*(\d{1,3})" + _ORD + r"(?:\s*-\s*([a-z])\b)?")
PAR = re.compile(r"(?:§\s*|\bpar(?:agrafo)?\.?\s+(?=\d))(\d{1,2})" + _ORD + r"(?:\s*-\s*([a-z])\b)?"
                 + r"|\bpar(?:agrafo)?\.?\s*unico\b|\bp\.?\s?u\.?(?=\s|$|,)")
INC = re.compile(r"\binc(?:isos?)?\.?\s*([ivxlc]+|\d{1,2})\b(?:\s*-\s*([a-z])\b)?")
ART_MORE = re.compile(r"^\s*(?:,|e|a)\s*(\d{1,3})" + _ORD + r"(?:\s*-\s*([a-z])\b)?")      # "arts. 47 e 48", "31-A a 31-E"
ALI = re.compile(r"\bal(?:ineas?\s+|\.\s*)\"?([a-z])\"?(?![a-z])")      # "alíneas do": the s is not a letter
CAPUT = re.compile(r"\bcaput\b")
# chat order only, right after a § or an inciso of the same citation: "art. 12, II", "§ 2º, III, a"
BARE_ROMAN = re.compile(r"^[\s,]*([IVXLC]+)\b(?![a-zà-ú])")                 # on the ORIGINAL case
BARE_LETTER = re.compile(r"^[\s,]*\"?([a-z])\"?(?=\s*(?:[,.;:?!)]|$))")
LINK = re.compile(r"^[\s,]*(?:(?:d[oa]s?|e|no|na|,)\s+)*$")                     # what may sit between parts
LIST = re.compile(r"^[\s,]*(?:e\s+)?(?:(?:o|a|os|as|no|na|nos|nas|do|da|dos|das|ao)\s+)?$")   # "..., o inciso", "e a alínea"
FOREIGN = re.compile(r"^[\s,]*(?:(?:e|a)\s+[ivxlc\d]+[\s,]*)*(?:d[aeo]s?\s+)?(?:lei\b|codigo|ctn\b|constituicao|cf\b|cf/88|emenda|decreto|adct\b|"
                     r"ato das disposicoes|"
                     r"resolucao|lc\s*n?[ºo]?\s*(?!214\b)\d|lei complementar n?[ºo.]?\s*(?!214\b)\d)")

# "..., todos da Constituição Federal" / "ambos da Lei nº ...": names the other law of the whole list
FOREIGN_LATER = re.compile(r"\b(?:todos|todas|ambos|ambas)\s+(?=d[ao]s?\s)"
                           + FOREIGN.pattern[FOREIGN.pattern.index("(?:d[aeo]s?"):])
# a period ends the sentence unless it closes an abbreviation of a citation ("art.", "inc.", "nº.")
SENTENCE_END = re.compile(r";|(?<!\bart)(?<!\barts)(?<!\binc)(?<!\bal)(?<!\bpar)(?<!\bn)(?<!\bno)\.(?:\s|$)|$")

INTENT_HISTORY = re.compile(r"redacao (?:anterior|original|antiga|revogada)|antes d[ae] (?:ser )?(?:revog|alter)|"
                            r"versao anterior|\bera\b.*\bredacao|como era\b")
INTENT_STATUS = re.compile(r"\bvigen|\bem vigor\b|ainda vale|ainda (?:esta|e) valid|foi revogad|esta revogad|"
                           r"\brevogad|\bvetad")
INTENT_TEXT = re.compile(r"o que (?:diz|dispoe|estabelece|preve|determina|fala)|\btexto\b|\bteor\b|mostr|"
                         r"transcrev|\bleia\b|\bcit[ae]\b|redacao d[oa]")
STOP = set("o a os as de do da dos das e em no na nos nas que qual quais um uma me por para com se ao aos esse essa "
           "isso este esta lc 214 lei complementar dispositivo artigo".split())


def _plain(text: str) -> str:
    text = unicodedata.normalize("NFKD", text.lower())
    return "".join(ch for ch in text if not unicodedata.combining(ch) or ch in "º°").replace("°", "º")


def _roman(token: str) -> str:
    """'3' -> 'III', 'iii' -> 'III' (the ids use the printed roman numerals)."""
    if token.isdigit():
        n, out = int(token), ""
        for v, s in ((100, "C"), (90, "XC"), (50, "L"), (40, "XL"), (10, "X"), (9, "IX"), (5, "V"), (4, "IV"),
                     (1, "I")):
            while n >= v:
                out, n = out + s, n - v
        return out
    return token.upper()


# --------------------------------------------------------------------------- citations
@dataclass
class Citation:
    art: str
    par: str | None = None
    inc: str | None = None
    ali: str | None = None
    caput: bool = False
    span: tuple[int, int] = (0, 0)
    foreign: str | None = None          # "codigo tributario nacional", "lei nº 6.404"... when it is another law

    def path(self) -> dict:
        return {k: getattr(self, k) for k in LEVELS if getattr(self, k)}

    def label(self) -> str:
        parts = [f"art. {self.art}"]
        if self.par:
            parts.append("parágrafo único" if self.par == "unico" else f"§ {self.par}")
        if self.inc:
            parts.append(f"inciso {self.inc}")
        if self.ali:
            parts.append(f"alínea {self.ali}")
        return ", ".join(parts)


def _components(plain: str, original: str) -> list[tuple[int, int, str, str]]:
    """Every citation part in the question: (start, end, kind, value), in text order."""
    found = []
    for m in ART.finditer(plain):
        found.append((m.start(), m.end(), "art", m.group(1) + (f"-{m.group(2).upper()}" if m.group(2) else "")))
        end = m.end()
        while more := ART_MORE.match(plain[end:]):                     # "arts. 47 e 48", "31-A a 31-E": one article each
            found.append((end + more.start(1), end + more.end(), "art",
                          more.group(1) + (f"-{more.group(2).upper()}" if more.group(2) else "")))
            end += more.end()
    for m in PAR.finditer(plain):
        value = (m.group(1) + (f"-{m.group(2).upper()}" if m.group(2) else "")) if m.group(1) else "unico"
        found.append((m.start(), m.end(), "par", value))
    for m in INC.finditer(plain):
        found.append((m.start(), m.end(), "inc", _roman(m.group(1)) + (f"-{m.group(2).upper()}" if m.group(2) else "")))
    for m in ALI.finditer(plain):
        found.append((m.start(), m.end(), "ali", m.group(1)))
    for m in CAPUT.finditer(plain):
        found.append((m.start(), m.end(), "caput", ""))
    found.sort()
    # chat order: a bare roman after an article or a §, a bare letter after an inciso ("art. 57, § 3º, IV, a")
    extra = []
    for _, end, kind, _ in found:
        if kind in ("art", "par") and (m := BARE_ROMAN.match(original[end:])):
            extra.append((end + m.start(1), end + m.end(1), "inc", m.group(1)))
            end, kind = end + m.end(1), "inc"
        if kind == "inc" and (m := BARE_LETTER.match(plain[end:])):
            extra.append((end + m.start(1), end + m.end(1), "ali", m.group(1)))
    return sorted(set(found + extra))


def parse_citations(question: str) -> list[Citation]:
    """Citations in the question, in both orders, lists included.

    Parts that sit next to each other form a segment; "e" splits segments ("inciso I do caput e do § 2º do art. 14"
    is two citations of art. 14). A segment without an article takes it from the segment it is listed with: the
    NEXT one in the legal order ("alínea b do inciso I e o inciso IV do caput do art. 195"), the PREVIOUS one in
    the chat order ("art. 47 e 48"). It also takes the levels above its own top level (an inciso listed with
    "inciso IV do caput" is of the caput too). A segment of another law ("... da Constituição Federal") passes that
    to the segments listed with it, so a list of constitutional articles is not read as LC 214 citations."""
    plain = _plain(question)
    original = unicodedata.normalize("NFC", question)
    if len(original) != len(plain):                      # keep offsets aligned for the bare-roman lookups
        original = plain.upper()

    segs: list[dict] = []                                # {kinds, start, end, linked_to_next}
    for c in _components(plain, original):
        start, end, kind, value = c
        if segs:
            gap = plain[segs[-1]["end"]:start]
            same_seg = LINK.match(gap) and not re.search(r"\be\b", gap) and (kind not in segs[-1]["kinds"]
                                                                            or kind == "caput")
            if same_seg:
                segs[-1]["kinds"][kind], segs[-1]["end"] = value, end
                continue
            segs[-1]["linked"] = bool(LINK.match(gap) or LIST.match(gap))
        segs.append({"kinds": {kind: value}, "start": start, "end": end, "linked": False})

    for seg in segs:
        tail = FOREIGN.match(plain[seg["end"]:]) if "art" in seg["kinds"] else None
        seg["foreign"] = plain[seg["end"]:seg["end"] + tail.end() + 25].strip(" ,") if tail else None
    for seg in segs:
        if not seg["foreign"]:
            stop = SENTENCE_END.search(plain, seg["end"]).start()
            if later := FOREIGN_LATER.search(plain, seg["end"], stop):
                seg["foreign"] = plain[later.start():later.end() + 25].strip(" ,")
    for i in range(len(segs) - 2, -1, -1):              # a foreign law named at the end of a list covers the list
        if segs[i]["linked"] and segs[i + 1]["foreign"] and not segs[i]["foreign"]:
            segs[i]["foreign"] = segs[i + 1]["foreign"]

    def source(i: int) -> dict | None:
        j = i
        while j < len(segs) - 1 and segs[j]["linked"]:  # legal order: the article comes after
            j += 1
            if "art" in segs[j]["kinds"]:
                return segs[j]
        j = i
        while j > 0 and segs[j - 1]["linked"]:          # chat order: the article came before
            j -= 1
            if "art" in segs[j]["kinds"]:
                return segs[j]
        return None

    out = []
    for i, seg in enumerate(segs):
        kinds = dict(seg["kinds"])
        if "art" not in kinds:
            src = source(i)
            if src is None:
                continue                                 # "o § 4º" alone: no article, nothing to resolve
            top = next((k for k in LEVELS if k in kinds), None)
            for k in LEVELS[:LEVELS.index(top)] if top else ():
                if k in src["kinds"] and not (k == "par" and "caput" in kinds):
                    kinds[k] = src["kinds"][k]
            if "caput" in src["kinds"] and "par" not in kinds:
                kinds["caput"] = ""
            kinds["art"] = src["kinds"]["art"]
            seg["foreign"] = seg["foreign"] or src["foreign"]
        out.append(Citation(art=kinds["art"], par=kinds.get("par"), inc=kinds.get("inc"), ali=kinds.get("ali"),
                            caput="caput" in kinds, span=(seg["start"], seg["end"]), foreign=seg["foreign"]))
    return out


# --------------------------------------------------------------------------- resolution against the provisions
def _split(pid: str) -> tuple[str, list[tuple[str, str]]]:
    parts = pid.split(":")
    return parts[1].removeprefix("art-"), [tuple(p.split("-", 1)) for p in parts[2:]]


class Index:
    """The provision ids of the current law version, grouped by article (built from the table or the parquet)."""

    def __init__(self, ids):
        self.by_art: dict[str, list[tuple[str, list[tuple[str, str]]]]] = {}
        for pid in ids:
            art, path = _split(pid)
            self.by_art.setdefault(art, []).append((pid, path))

    def resolve(self, c: Citation) -> list[str]:
        """Every provision that matches the cited levels, allowing unstated intermediate levels: "inciso III do
        art. 12" matches art-12:par-1:inc-III and art-12:par-2:inc-III. A provision printed twice in the source
        (inc-II#2) is a separate candidate: the citation is ambiguous in the law itself."""
        rows = self.by_art.get(c.art, [])
        want = c.path()
        deepest = next((k for k in reversed(LEVELS) if k in want), None)
        hits = []
        for pid, path in rows:
            kinds = [k for k, _ in path]
            if deepest is None:
                if not path:
                    hits.append(pid)
                continue
            if not kinds or kinds[-1] != deepest:
                continue
            got = {k: v.split("#")[0] for k, v in path}
            if any(got.get(k) != v for k, v in want.items()):
                continue
            if c.caput and "par" in got:                # "inciso III do caput": not an inciso of a §
                continue
            hits.append(pid)
        # Drafting convention: "art. 6º, II" is the inciso of the CAPUT (one of a § is cited "art. 6º, § 2º, II").
        # Only for an inciso cited right under the article, and only when it leaves one candidate. An alínea cited
        # without its inciso ("alínea a do art. 57") is never guessed: every option is shown.
        if len(hits) > 1 and c.inc and not c.par and not c.ali:
            of_caput = [h for h in hits if ":par-" not in h]
            if len(of_caput) == 1:
                return of_caput
        return sorted(hits)


# --------------------------------------------------------------------------- the decision
@dataclass
class Plan:
    route: str                                   # sql | combined | vector | clarify
    intent: str                                  # text | status | history | question | ambiguous | not_found | open
    anchors: list[str] = field(default_factory=list)
    options: dict[str, list[str]] = field(default_factory=dict)     # citation label -> candidate ids (ambiguous)
    missing: list[str] = field(default_factory=list)                # cited provisions that do not exist
    foreign: list[str] = field(default_factory=list)                # citations of other laws

    def reason(self) -> str:
        if self.route == "clarify":
            if self.missing:
                return f"não existe na LC 214: {', '.join(self.missing)}"
            return "; ".join(f"{k} é ambíguo: {len(v)} dispositivos" for k, v in self.options.items())
        return {"sql": f"citação ({self.intent}): resposta por consulta exata",
                "combined": "citação + pergunta: o dispositivo citado é âncora, a busca vetorial completa",
                "vector": "sem citação da LC 214: busca vetorial"}[self.route] + (
            f"; outra lei citada: {', '.join(self.foreign)}" if self.foreign else "")


def _residual_words(plain: str, spans: list[tuple[int, int]]) -> list[str]:
    for s, e in sorted(spans, reverse=True):
        plain = plain[:s] + " " + plain[e:]
    return [w for w in re.findall(r"[a-z]+", plain) if w not in STOP and len(w) > 2]


def route(question: str, index: Index) -> Plan:
    plain = _plain(question)
    cites = parse_citations(question)
    own = [c for c in cites if not c.foreign]
    foreign = [f"{c.label()} ({c.foreign})" for c in cites if c.foreign]
    if not own:
        return Plan("vector", "open", foreign=foreign)

    anchors, options, missing = [], {}, []
    for c in own:
        hits = index.resolve(c)
        if not hits:
            missing.append(c.label())
        elif len(hits) > 1:
            options[c.label()] = hits
        else:
            anchors.append(hits[0])
    if missing or options:
        return Plan("clarify", "not_found" if missing else "ambiguous", anchors, options, missing, foreign)

    if INTENT_HISTORY.search(plain):
        intent = "history"
    elif INTENT_STATUS.search(plain):
        intent = "status"
    elif INTENT_TEXT.search(plain) or len(_residual_words(plain, [c.span for c in own])) <= 1:
        intent = "text"
    else:
        return Plan("combined", "question", anchors, foreign=foreign)
    return Plan("sql", intent, anchors, foreign=foreign)


# --------------------------------------------------------------------------- the law as a test corpus
REVOKING = re.compile(r"^Art\. [\dº°-]+[A-Z]?\.? Ficam revogad")
OWN_LABEL = re.compile(r"^Art\.\s*\d+\s*[º°]?(?:\s*-\s*[A-Z])?\.?")       # "Art. 7º-A." opening its own caput


def self_references(rows: list[dict], index: Index) -> dict:
    """Resolve the citations the law makes to itself ("nos termos do inciso II do § 2º do art. 60"): every one of
    them names a provision that exists, so each unresolved citation is a parser miss we can see. Excluded: text
    quoted from other laws (`altera_outra_lei`) and the revoking articles (540, 542, 543), whose items cite
    provisions of a law named only in the item above ("XXXVI - na Lei nº X: b) o parágrafo único do art. 22")."""
    revoking = {r["dispositivo_id"].split(":")[1] for r in rows if r["tipo"] == "art" and REVOKING.match(r["texto"])}
    stats, misses = Counter(), []
    for r in rows:
        if r["altera_outra_lei"] or r["dispositivo_id"].split(":")[1] in revoking:
            continue
        text = OWN_LABEL.sub("", r["texto"]) if r["tipo"] == "art" else r["texto"]   # a label is not a citation
        for c in parse_citations(text):
            if c.foreign:
                stats["foreign"] += 1
                continue
            hits = index.resolve(c)
            outcome = "resolved" if len(hits) == 1 else "ambiguous" if hits else "not_found"
            stats[outcome] += 1
            if outcome != "resolved":
                misses.append({"in": r["dispositivo_id"], "citation": c.label(), "outcome": outcome})
    own = stats["resolved"] + stats["ambiguous"] + stats["not_found"]
    return {**{k: stats[k] for k in ("resolved", "ambiguous", "not_found", "foreign")}, "own": own,
            "rate": round(stats["resolved"] / own, 4) if own else 1.0, "revoking_articles": sorted(revoking),
            "misses": misses}


# --------------------------------------------------------------------------- the SQL answer (structure)
def render_structure(rows: list[dict], page_words: int = ANSWER_PAGE_WORDS) -> str:
    """The cited provision and everything under it, indented by level; revoked/vetoed show the note, not an empty
    label; a provision longer than one page is paged ("parte 1 de 13"), never cut. Rows in reading order, the
    cited provision first."""
    base = rows[0]["dispositivo_id"].count(":") if rows else 0
    out = []
    for r in rows:
        pad = "  " * (r["dispositivo_id"].count(":") - base)      # depth from the id: an inciso of the caput is one
        if r["status"] != "vigente":                              # level below the article, one of a § two levels
            note = next((n for n in r.get("notas") or [] if n.lower().startswith(("revogad", "vetad"))), r["status"])
            out.append(f"{pad}{r['rotulo']} ({note})")
            continue
        words = r["texto"].split()
        if len(words) <= page_words:
            out.append(pad + r["texto"])
            continue
        pages = [words[i:i + page_words] for i in range(0, len(words), page_words)]
        for n, page in enumerate(pages, 1):
            out.append(f"{pad}[parte {n} de {len(pages)}] {' '.join(page)}")
    return "\n".join(out)


# --------------------------------------------------------------------------- CLI (lab use)
def _db_rows(cur, anchor: str) -> list[dict]:
    cur.execute("SELECT dispositivo_id, tipo, rotulo, status, texto, notas FROM ai.lc214_dispositivo "
                "WHERE dispositivo_id = %s OR dispositivo_id LIKE %s ORDER BY ordem", (anchor, anchor + ":%"))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Route a question about the LC 214 (and answer citations from SQL)")
    ap.add_argument("question")
    ap.add_argument("--db", action="store_true", help="answer sql routes from ai.lc214_dispositivo")
    args = ap.parse_args(argv)

    from pipelines.commons.dw_client import get_pg_connection
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT dispositivo_id FROM ai.lc214_dispositivo")
            plan = route(args.question, Index(r[0] for r in cur.fetchall()))
            print(f"route={plan.route} intent={plan.intent} anchors={plan.anchors}\n{plan.reason()}")
            for label, ids in plan.options.items():
                print(f"  {label}: " + " | ".join(ids))
            if args.db and plan.route in ("sql", "combined"):
                for anchor in plan.anchors:
                    print("\n" + render_structure(_db_rows(cur, anchor)))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
