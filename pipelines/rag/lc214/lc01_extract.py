"""LC 214/2025 (PDF published by the Câmara dos Deputados) -> versioned raw PDF + layout text.

Step 1 of the legislation RAG. It only fetches, checks and extracts: parsing into articles is lc02.

Source contract: structural, not byte-level
    The source is the "norma atualizada" PDF: the Câmara re-publishes it whenever the law is amended
    (LC 227/2026 already did). Pinning a sha256 would break the pipeline on every legitimate change, so the
    contract checks what must stay true across versions:
        * the bytes are a PDF (an HTML error page served with status 200 is rejected)
        * at least LC214_MIN_PAGES pages (a truncated download is rejected)
        * the text has the law's title and its last article (the extraction produced real text)
    The sha256 has another job: it tells *whether the version changed*.
        same sha    -> exit 99 (SKIPPED): nothing to re-parse, nothing downstream runs
        new sha     -> the new version is stored next to the previous ones and becomes "current"
        rejected    -> exit 1; the file goes to rejected/ for inspection and the manifest is untouched

Why `pdftotext -layout` (poppler)
    The parser (lc02) relies on the layout: a new provision starts indented, a wrapped line starts at
    column 0. That signal was measured on this extractor; another library would change it.

Where it goes (lake.py has the full layout)
    landing/lc214/raw/lc214_<sha12>.pdf      every version ever accepted
    bronze/lc214/text/lc214_<sha12>.txt      layout text, one form feed (\\f) per page
    landing/lc214/manifest.json              versions + "current": written LAST, it is the commit point
    landing/lc214/rejected/                  files that failed the contract
    Download and extraction run in a temporary directory that is deleted at the end: nothing stays on local disk.

Exit codes: 0 new version | 99 unchanged | 1 failed (contract, download or extraction).
"""
from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from pipelines.commons.logger import get_logger, log_event  # noqa: E402
from pipelines.observability import current_step  # noqa: E402
from pipelines.rag.lc214.lake import Lake, LakeError, k_manifest, k_raw, k_rejected, k_text, lake_from_env  # noqa: E402

logger = get_logger("lc01_extract")

DEFAULT_URL = ("https://www2.camara.leg.br/legin/fed/leicom/2025/"
               "leicomplementar-214-16-janeiro-2025-796905-normaatualizada-pl.pdf")
SOURCE_URL = os.getenv("LC214_PDF_URL", DEFAULT_URL)
MIN_PAGES = int(os.getenv("LC214_MIN_PAGES", "250"))
MARKERS = ("LEI COMPLEMENTAR Nº 214", "Art. 544")
HTTP_RETRIES = int(os.getenv("LC214_HTTP_RETRIES", "4"))
HTTP_TIMEOUT_S = float(os.getenv("LC214_HTTP_TIMEOUT_S", "60"))

EXIT_OK, EXIT_SKIPPED, EXIT_FAILED = 0, 99, 1


class SourceError(Exception):
    """A failure with a stable event name (queried in Loki) and a human message."""

    def __init__(self, event: str, message: str):
        super().__init__(message)
        self.event = event


# --------------------------------------------------------------------------- pure helpers (unit-tested)
def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def contract_violations(head: bytes, pages: int, text: str, min_pages: int = MIN_PAGES) -> list[str]:
    """Everything that is wrong with this file, not only the first problem (one run, the full diagnosis)."""
    problems = []
    if not head.startswith(b"%PDF"):
        problems.append("not_a_pdf")
    if pages < min_pages:
        problems.append(f"too_few_pages: {pages} < {min_pages}")
    for marker in MARKERS:
        if marker not in text:
            problems.append(f"marker_missing: {marker!r}")
    return problems


def count_pages(text: str) -> int:
    """pdftotext ends every page with a form feed: the same page boundary lc02 sees."""
    return text.count("\f")


def empty_manifest() -> dict:
    return {"current": None, "previous": None, "versions": []}


# --------------------------------------------------------------------------- side effects (injectable)
def extract_layout(pdf_path: str, txt_path: str) -> None:
    if shutil.which("pdftotext") is None:
        raise SourceError("extractor_missing", "pdftotext not found: install poppler-utils (Dockerfile has it)")
    subprocess.run(["pdftotext", "-layout", "-enc", "UTF-8", pdf_path, txt_path], check=True,
                   capture_output=True, timeout=300)


def download(url: str, dest: str, session=None, retries: int = HTTP_RETRIES, sleep=time.sleep) -> int:
    """GET with retries and exponential backoff; streams to dest.part and renames only when complete."""
    if session is None:
        import requests

        session = requests.Session()
    part = dest + ".part"
    last = "no answer"
    for attempt in range(retries):
        try:
            with session.get(url, stream=True, timeout=HTTP_TIMEOUT_S,
                             headers={"User-Agent": "datalabs/lc214 (public data, study project)"}) as r:
                if r.status_code == 200:
                    written = 0
                    with open(part, "wb") as f:
                        for chunk in r.iter_content(1 << 16):
                            f.write(chunk)
                            written += len(chunk)
                    os.replace(part, dest)
                    return written
                last = f"HTTP {r.status_code}"
                if r.status_code in (403, 404, 410):
                    break                       # not transient: retrying will not help
        except Exception as exc:                # network errors are transient by default
            last = f"{type(exc).__name__}: {exc}"
        if attempt < retries - 1:
            sleep(min(2 ** attempt, 30))
    raise SourceError("source_unreachable", f"{url}: {last} after {attempt + 1} attempt(s)")


# --------------------------------------------------------------------------- the step
def run(source: str, lake: Lake, *, fetch=download, extract=extract_layout) -> int:
    """`source` is a URL or a local file (e.g. the PDF you already downloaded by hand)."""
    step = current_step()
    with tempfile.TemporaryDirectory(prefix="lc214_") as work:
        incoming = os.path.join(work, "incoming.pdf")
        if os.path.isfile(source):
            shutil.copyfile(source, incoming)
            origin = os.path.abspath(source)
        else:
            fetch(source, incoming)
            origin = source

        sha = sha256_file(incoming)
        manifest = lake.get_json(k_manifest(), default=empty_manifest())
        current = manifest.get("current")
        if current == sha and lake.exists(k_text(sha)):
            log_event(logger, "source_unchanged", f"LC 214 unchanged (sha {sha[:12]}): nothing to do", sha256=sha)
            step.add(files_total=1, rows_out=0)
            return EXIT_SKIPPED

        def reject(problems: list[str], pages: int = 0) -> int:
            lake.put_file(k_rejected(sha), incoming, "application/pdf")
            log_event(logger, "source_contract_failed", f"LC 214 rejected: {'; '.join(problems)}", level=40,
                      sha256=sha, pages=pages, problems=problems, kept_at=lake.url(k_rejected(sha)))
            return EXIT_FAILED

        # cheap check first: an HTML error page must fail as "not_a_pdf", not as an extractor crash
        with open(incoming, "rb") as f:
            head = f.read(8)
        if not head.startswith(b"%PDF"):
            return reject(["not_a_pdf"])
        txt = os.path.join(work, "incoming.txt")
        try:
            extract(incoming, txt)
        except subprocess.CalledProcessError as exc:
            return reject([f"extraction_failed: pdftotext exit {exc.returncode}"])
        with open(txt, encoding="utf-8") as f:
            text = f.read()
        pages = count_pages(text)

        problems = contract_violations(head, pages, text)
        if problems:
            return reject(problems, pages)

        # data first, manifest last: a run killed in between leaves an orphan object, never a dangling "current"
        lake.put_file(k_raw(sha), incoming, "application/pdf")
        lake.put_file(k_text(sha), txt, "text/plain; charset=utf-8")
        size = os.path.getsize(incoming)

    manifest["versions"] = [v for v in manifest.get("versions", []) if v["sha256"] != sha] + [{
        "sha256": sha, "pages": pages, "bytes": size, "source": origin,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "pdf": k_raw(sha), "text": k_text(sha),
    }]
    manifest["previous"], manifest["current"] = current, sha
    lake.put_json(k_manifest(), manifest)

    event = "source_version_new" if current is None else "source_version_changed"
    log_event(logger, event, f"LC 214 version {sha[:12]} accepted ({pages} pages) -> {lake.url(k_raw(sha))}"
              + (f", replaces {current[:12]}" if current else ""),
              sha256=sha, previous_sha256=current, pages=pages, versions=len(manifest["versions"]))
    step.add(files_total=1, rows_out=pages)
    return EXIT_OK


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--pdf", help="use a local PDF instead of downloading (same contract applies)")
    ap.add_argument("--url", default=SOURCE_URL)
    ap.add_argument("--lake", default=None, help="'s3' (default, LC214_LAKE) or file:///dir")
    args = ap.parse_args(argv)
    try:
        return run(args.pdf or args.url, lake_from_env(args.lake))
    except (SourceError, LakeError) as exc:
        log_event(logger, exc.event, str(exc), level=40)
        return EXIT_FAILED
    except subprocess.CalledProcessError as exc:
        log_event(logger, "extraction_failed", f"pdftotext exit {exc.returncode}: {exc.stderr[:300]!r}", level=40)
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
