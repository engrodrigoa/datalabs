"""LC 214/2025 provisions (lc02) -> chunks to embed (small) + articles to answer with (big).

Step 3 of the legislation RAG. Output, next to the parsed table in silver/lc214/<sha12>/:
    chunks.parquet         what gets embedded (lc04)
    artigos.parquet        what the answer shows: the whole article (small-to-big)
    chunks_report.json     written last: the commit point

Why small-to-big
    The article is the unit of citation, so the answer shows the whole article. But the embedding model reads
    at most 512 tokens and truncates the rest SILENTLY (sentence-transformers raises no error): the tail of a
    long article would never reach the vector. So an article that fits becomes one chunk; one that does not is
    split along its own structure - caput + its items, then each § with its items, then single items - and every
    piece carries a header (Título, Capítulo, Art.) and the start of the caput, so it still says what it is about.
    Consecutive pieces are packed while they fit, to avoid a cloud of one-line chunks.

What is NOT embedded
    Revoked and vetoed provisions have no text left, only the label: they live in the dispositivos table and are
    answered by lookup (lc04 router), never by similarity.

Token counts
    With the real tokenizer of the embedding model (AI image), counts are exact and any chunk over the limit
    fails the run. Without it (host venv, CI) counts are an estimate (words x LC214_TOKENS_PER_WORD), recorded
    as `token_method: estimate` in the report: good to develop with, not a measurement.

Exit codes: 0 written | 99 up to date (same law version, parser, chunker and token method) | 1 failed.
"""
from __future__ import annotations

import argparse
import hashlib
import math
import os
import statistics
import sys
from collections import Counter
from dataclasses import dataclass, field

import polars as pl

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.ai.embeddings import MODEL_NAME, PASSAGE_PREFIX  # noqa: E402
from pipelines.commons.logger import get_logger, log_event  # noqa: E402
from pipelines.observability import current_step  # noqa: E402
from pipelines.rag.lc214.lake import Lake, LakeError, k_manifest, k_silver, lake_from_env  # noqa: E402

logger = get_logger("lc03_chunks")

CHUNKER_VERSION = "1"          # bump when the chunk text changes on purpose (new ids, re-embed in lc04)
MAX_TOKENS = int(os.getenv("LC214_MAX_TOKENS", "512"))           # e5 max_seq_length, special tokens included
TOKENS_PER_WORD = float(os.getenv("LC214_TOKENS_PER_WORD", "1.5"))
CAPUT_PREFIX_WORDS = 40
WINDOW_OVERLAP_WORDS = 20
EXIT_OK, EXIT_SKIPPED, EXIT_FAILED = 0, 99, 1


class ChunkError(Exception):
    def __init__(self, event: str, message: str, **fields):
        super().__init__(message)
        self.event, self.fields = event, fields


# --------------------------------------------------------------------------- token counting
def estimate_tokens(text: str) -> int:
    return math.ceil(len(text.split()) * TOKENS_PER_WORD) + 4      # + prefix and special tokens


def load_counter(method: str = os.getenv("LC214_TOKENS", "auto")):
    """(count, method). 'auto' tries the model tokenizer and falls back to the estimate, saying so."""
    if method != "estimate":
        try:
            import transformers
            from transformers import AutoTokenizer

            transformers.logging.set_verbosity_error()               # no "sequence longer than 512" noise
            tok = AutoTokenizer.from_pretrained(MODEL_NAME)
            return (lambda t: len(tok(PASSAGE_PREFIX + t, add_special_tokens=True)["input_ids"])), "tokenizer"
        except Exception as exc:
            if method == "tokenizer":
                raise ChunkError("tokenizer_unavailable", f"{MODEL_NAME}: {type(exc).__name__}: {exc}")
            log_event(logger, "token_count_estimated", f"tokenizer unavailable ({type(exc).__name__}): "
                      f"counts are estimates ({TOKENS_PER_WORD} tokens/word)", level=30)
    return estimate_tokens, "estimate"


# --------------------------------------------------------------------------- structure
@dataclass
class Unit:
    """A provision with its descendants: the caput with its items, a § with its items, an item with its alíneas."""
    head: dict
    children: list = field(default_factory=list)

    def rows(self) -> list[dict]:
        out = [self.head]
        for c in self.children:
            out += c.rows()
        return out


def header_of(caput: dict) -> str:
    parts = ["LC 214/2025"]
    for level, label in (("titulo", "Título"), ("capitulo", "Capítulo"), ("secao", "Seção")):
        if caput.get(level):
            name = caput.get(f"{level}_nome") or ""
            parts.append(f"{label} {caput[level]}" + (f" - {name}" if name else ""))
    parts.append(caput["rotulo"])
    return " | ".join(parts)


def truncate_words(text: str, n: int) -> str:
    words = text.split()
    return text if len(words) <= n else " ".join(words[:n]) + " (...)"


def build_units(rows: list[dict]) -> tuple[dict, list[Unit]]:
    """Rows of one article (in order) -> its caput and the top-level units (caput block, then each §)."""
    caput = rows[0]
    by_id = {r["dispositivo_id"]: Unit(r) for r in rows}
    caput_unit = by_id[caput["dispositivo_id"]]
    top = [caput_unit]
    for r in rows[1:]:
        unit = by_id[r["dispositivo_id"]]
        parent = by_id.get(r["parent_id"])
        if r["tipo"] == "par":
            top.append(unit)
        elif parent is not None:
            parent.children.append(unit)
    return caput, top


# --------------------------------------------------------------------------- packing
def chunk_article(rows: list[dict], count, max_tokens: int = MAX_TOKENS) -> list[dict]:
    """One article -> chunks that each fit in max_tokens, covering every provision with text."""
    caput, top = build_units(rows)
    header = header_of(caput)
    full = header + "\n" + "\n".join(r["texto"] for r in rows)
    if count(full) <= max_tokens:
        return [{"kind": "artigo", "text": full, "ids": [r["dispositivo_id"] for r in rows]}]

    chunks: list[dict] = []
    caput_prefix = truncate_words(caput["texto"], CAPUT_PREFIX_WORDS)

    def emit(context: list[str], body_rows: list[dict], kind: str):
        chunks.append({"kind": kind, "text": "\n".join([header, *context, *[r["texto"] for r in body_rows]]),
                       "ids": [r["dispositivo_id"] for r in body_rows]})

    def fits(context, body_rows):
        return count("\n".join([header, *context, *[r["texto"] for r in body_rows]])) <= max_tokens

    def windows(context: list[str], row: dict):
        """Last resort: a single provision bigger than the limit, split into overlapping word windows."""
        words = row["texto"].split()
        size = len(words)
        while size > 1 and not fits(context, [{**row, "texto": " ".join(words[:size])}]):
            size = int(size * 0.8)
        step = max(1, size - WINDOW_OVERLAP_WORDS)
        for start in range(0, len(words), step):
            emit(context, [{**row, "texto": " ".join(words[start:start + size])}], "janela")
            if start + size >= len(words):
                break

    def pack(units: list[Unit], context: list[str], kind: str):
        batch: list[dict] = []
        for u in units:
            rows_u = u.rows()
            if batch and fits(context, batch + rows_u):
                batch += rows_u
                continue
            if batch:
                emit(context, batch, kind)
                batch = []
            if fits(context, rows_u):
                batch = rows_u
            elif u.children:            # too big: its head alone, then its children with the head as context
                head_ctx = context + [truncate_words(u.head["texto"], CAPUT_PREFIX_WORDS)]
                if fits(context, [u.head]):
                    emit(context, [u.head], kind)
                else:
                    windows(context, u.head)
                pack(u.children, head_ctx, "dispositivo")
            else:
                windows(context, u.head)
        if batch:
            emit(context, batch, kind)

    caput_unit, paragraphs = top[0], top[1:]
    pack([caput_unit], [], "grupo")                        # the caput block needs no caput prefix
    pack(paragraphs, [caput_prefix], "grupo")             # the caput text starts with its own label
    return chunks


def chunk_id(source_id: str, text: str) -> str:
    """Same pattern as the ANP store (ADR 0007): lineage + content hash, stable across library upgrades."""
    return f"{source_id}:{hashlib.md5(f'{CHUNKER_VERSION}|{text}'.encode('utf-8')).hexdigest()[:16]}"


# --------------------------------------------------------------------------- the step
def outputs(sha: str) -> dict:
    return {"dispositivos": k_silver(sha, "dispositivos.parquet"), "chunks": k_silver(sha, "chunks.parquet"),
            "artigos": k_silver(sha, "artigos.parquet"), "report": k_silver(sha, "chunks_report.json")}


def build(df: pl.DataFrame, count, method: str, max_tokens: int = MAX_TOKENS) -> tuple[pl.DataFrame, pl.DataFrame, dict]:
    chunk_rows, art_rows, excluded = [], [], Counter()
    for _, art_df in df.sort("ordem").group_by("art", maintain_order=True):
        rows = art_df.to_dicts()
        caput = rows[0]
        live = [r for r in rows if r["status"] == "vigente" and r["texto"].strip() != r["rotulo"]]
        excluded.update(r["status"] for r in rows if r not in live)
        art_rows.append({
            "art_id": caput["dispositivo_id"], "art": caput["art"], "rotulo": caput["rotulo"],
            "header": header_of(caput), "texto": "\n".join(r["texto"] for r in rows),
            "status": caput["status"], "pagina": caput["pagina"], "n_dispositivos": len(rows),
            "alterado_por": sorted({a for r in rows for a in r["alterado_por"]}),
            "altera_outra_lei": caput["altera_outra_lei"], "source_sha256": caput["source_sha256"],
        })
        if not live or live[0]["tipo"] != "art":       # revoked/vetoed caput: answered by lookup, not embedded
            continue
        for n, c in enumerate(chunk_article(live, count, max_tokens), 1):
            chunk_rows.append({
                "chunk_id": chunk_id(f"{caput['dispositivo_id']}:c{n}", c["text"]),
                "art_id": caput["dispositivo_id"], "parte": n, "kind": c["kind"], "texto": c["text"],
                "dispositivo_ids": c["ids"], "n_tokens": count(c["text"]), "token_method": method,
                "titulo": caput["titulo"], "capitulo": caput["capitulo"], "secao": caput["secao"],
                "alterado_por": sorted({a for r in live if r["dispositivo_id"] in c["ids"] for a in r["alterado_por"]}),
                "altera_outra_lei": caput["altera_outra_lei"], "pagina": caput["pagina"],
                "source_sha256": caput["source_sha256"], "chunker_version": CHUNKER_VERSION,
            })
    chunks = pl.DataFrame(chunk_rows, infer_schema_length=None)
    artigos = pl.DataFrame(art_rows, infer_schema_length=None)

    tokens = chunks["n_tokens"].to_list()
    covered = {i for ids in chunks["dispositivo_ids"].to_list() for i in ids}
    live_ids = set(df.filter((pl.col("status") == "vigente") & (pl.col("texto") != pl.col("rotulo"))
                             )["dispositivo_id"].to_list())
    report = {
        "token_method": method, "max_tokens": max_tokens, "chunker_version": CHUNKER_VERSION,
        "artigos": artigos.height, "chunks": chunks.height, "by_kind": dict(Counter(chunks["kind"].to_list())),
        "tokens": {"p50": statistics.median(tokens), "p95": sorted(tokens)[int(0.95 * (len(tokens) - 1))],
                   "max": max(tokens)},
        "over_limit": sum(t > max_tokens for t in tokens),
        "articles_split": int(chunks.group_by("art_id").len().filter(pl.col("len") > 1).height),
        "live_provisions_covered": round(len(covered & live_ids) / len(live_ids), 4) if live_ids else 1.0,
        "excluded_not_live": dict(excluded),
    }
    return chunks, artigos, report


def run(lake: Lake, force: bool = False, method: str = os.getenv("LC214_TOKENS", "auto")) -> int:
    step = current_step()
    sha = lake.get_json(k_manifest())["current"]
    out = outputs(sha)
    if not lake.exists(out["dispositivos"]):
        raise ChunkError("input_missing", f"{lake.url(out['dispositivos'])} not found: run lc02_parse first")

    dispositivos = lake.get_parquet(out["dispositivos"])
    # The input file is keyed by the law version only: a parser bump rewrites it under the same name, so the
    # parser version is part of "same input" (without it, a re-parse would leave stale chunks behind).
    parser_version = dispositivos["parser_version"][0] if "parser_version" in dispositivos.columns else None
    count, used = load_counter(method)
    if not force and lake.exists(out["report"]) and lake.exists(out["chunks"]):
        prev = lake.get_json(out["report"])
        if (prev.get("chunker_version") == CHUNKER_VERSION and prev.get("token_method") == used
                and prev.get("parser_version") == parser_version):
            log_event(logger, "chunks_up_to_date", f"version {sha[:12]} already chunked ({used})", sha256=sha)
            return EXIT_SKIPPED

    chunks, artigos, report = build(dispositivos, count, used)
    report["parser_version"] = parser_version
    if report["over_limit"] and used == "tokenizer":
        raise ChunkError("chunk_over_limit", f"{report['over_limit']} chunk(s) above {MAX_TOKENS} tokens: the model "
                         "would truncate them silently", over_limit=report["over_limit"])
    if report["live_provisions_covered"] < 1.0:
        raise ChunkError("chunk_coverage_failed", f"only {report['live_provisions_covered']:.2%} of the live "
                         "provisions landed in a chunk", coverage=report["live_provisions_covered"])

    lake.put_parquet(out["chunks"], chunks)
    lake.put_parquet(out["artigos"], artigos)
    lake.put_json(out["report"], {"source_sha256": sha, **report})      # last: the commit point

    log_event(logger, "chunks_done", f"{report['chunks']} chunks from {report['artigos']} articles "
              f"({report['by_kind']}), tokens p50={report['tokens']['p50']} p95={report['tokens']['p95']} "
              f"max={report['tokens']['max']} ({used}), {report['articles_split']} articles split",
              sha256=sha, chunks=report["chunks"], token_method=used, tokens_max=report["tokens"]["max"],
              articles_split=report["articles_split"])
    step.add(rows_in=artigos.height, rows_out=chunks.height)
    return EXIT_OK


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--lake", default=None, help="'s3' (default, LC214_LAKE) or file:///dir")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--tokens", choices=["auto", "tokenizer", "estimate"], default=os.getenv("LC214_TOKENS", "auto"))
    args = ap.parse_args(argv)
    try:
        return run(lake_from_env(args.lake), force=args.force, method=args.tokens)
    except ChunkError as exc:
        log_event(logger, exc.event, str(exc), level=40, **exc.fields)
        return EXIT_FAILED
    except LakeError as exc:
        log_event(logger, exc.event, str(exc), level=40)
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
