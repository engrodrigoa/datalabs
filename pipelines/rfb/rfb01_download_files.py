"""RFB (CNPJ open data) -> local landing zone, from the Receita Federal public share (Nextcloud WebDAV).

The source changed in Oct/2026 and the old "list the folder, download what is listed" stopped working:
  * the share link changed and its root moved up: the months now live under /Dados/Cadastros/CNPJ/<YYYY-MM>
  * folder listing (PROPFIND) answers 200 with an empty text/html body for ANY path, even a missing folder or a
    rejected link: it carries no information at all (re-measured 2026-10-12)
  * direct downloads still work, but the server is flaky under bursts: the same file answered 200, then 404,
    then 200 again within minutes, and probing 37 files in a row came back with a random subset missing

So the download is driven by a CONTRACT instead of a listing: every reference month has the same 37 files
(EXPECTED_FILES). Each file is fetched with retries, backoff and a pause between requests, a 404 only counts
as "missing" after every attempt, and a local file only counts as done if it is a readable ZIP of the expected
size (a truncated ZIP from a killed run used to pass as "already downloaded"). The source check is a 1-byte
ranged GET on the smallest contract file (PROBE_FILE): GET is the only method whose status means something
(206/200 present, 401 link rejected, 404 missing).

Exit codes (observability runner): 0 all files present | 99 the month is not published yet (skipped, no new
data) | 1 anything else, with the cause in a structured event: source_auth_failed (share link rotated ->
update RFB_SHARE_URL), source_dir_not_found (RFB_SHARE_DIR wrong), source_incomplete (files that never came).
"""
from __future__ import annotations

import os
import re
import sys
import time
import zipfile
from urllib.parse import quote
from warnings import filterwarnings

import requests

filterwarnings("ignore")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.env_loader import DATASOURCE_DIR, rfb_reference  # noqa: E402
from pipelines.commons.logger import get_logger, log_event  # noqa: E402
from pipelines.observability import current_step  # noqa: E402

logger = get_logger("rfb_download")

HOST = "https://arquivos.receitafederal.gov.br"
WEBDAV = f"{HOST}/public.php/webdav"
# Public link published by the Receita Federal for the CNPJ open data: not a secret (anyone with the link can
# download). When the Receita re-creates the share, paste the new browser link in RFB_SHARE_URL.
DEFAULT_SHARE_URL = "https://arquivos.receitafederal.gov.br/index.php/s/gn672Ad4CF8N6TK"
DEFAULT_SHARE_DIR = "/Dados/Cadastros/CNPJ"

EXPECTED_FILES = (
    [f"{entity}{i}.zip" for entity in ("Empresas", "Estabelecimentos", "Socios") for i in range(10)]
    + [f"{name}.zip" for name in ("Simples", "Cnaes", "Motivos", "Municipios", "Naturezas", "Paises", "Qualificacoes")]
)

PROBE_FILE = "Cnaes.zip"  # smallest file of the contract (~22 KB): cheap even if a server ignores the Range header

NOT_PUBLISHED = 99


class SourceError(RuntimeError):
    def __init__(self, event: str, message: str):
        super().__init__(message)
        self.event = event


def share_token(url: str | None = None, token: str | None = None) -> tuple[str, str]:
    """(token, where it came from). RFB_SHARE_URL (the browser link) > RFB_SHARE_TOKEN (legacy) > built-in link."""
    url = (url if url is not None else os.getenv("RFB_SHARE_URL", "")).strip().strip('"').strip("'")
    token = (token if token is not None else os.getenv("RFB_SHARE_TOKEN", "")).strip().strip('"').strip("'")
    if url:
        source = "RFB_SHARE_URL"
    elif token:
        return token, "RFB_SHARE_TOKEN"
    else:
        url, source = DEFAULT_SHARE_URL, "default link"
    m = re.search(r"/s/([A-Za-z0-9]+)", url)
    if not m:
        raise SourceError("source_config_invalid", f"RFB_SHARE_URL has no '/s/<token>': {url}")
    return m.group(1), source


def share_dir() -> str:
    d = os.getenv("RFB_SHARE_DIR", DEFAULT_SHARE_DIR).strip() or DEFAULT_SHARE_DIR
    return "/" + d.strip("/")


class Fetcher:
    """HTTP against a flaky source: retries with exponential backoff and a pause between requests."""

    RETRYABLE = {404, 408, 425, 429, 500, 502, 503, 504}

    def __init__(self, token: str, session=None, retries: int | None = None, pause_s: float | None = None,
                 backoff_max_s: float | None = None, sleep=time.sleep):
        self.auth = (token, "")
        self.session = session or requests.Session()
        self.retries = int(retries if retries is not None else os.getenv("RFB_HTTP_RETRIES", "6"))
        self.pause_s = float(pause_s if pause_s is not None else os.getenv("RFB_REQUEST_INTERVAL_S", "1.0"))
        self.backoff_max_s = float(backoff_max_s if backoff_max_s is not None else os.getenv("RFB_BACKOFF_MAX_S", "60"))
        self.sleep = sleep

    def _wait(self, attempt: int) -> None:
        self.sleep(min(2 ** attempt, self.backoff_max_s))

    def status(self, method: str, url: str, **kw) -> int:
        """Status code that survives flakiness: retries anything retryable, returns the last answer (0 = no answer)."""
        status = 0
        for attempt in range(self.retries):
            try:
                with self.session.request(method, url, auth=self.auth, timeout=60, allow_redirects=False, **kw) as r:
                    status = r.status_code
            except requests.RequestException:
                status = 0
            if status not in self.RETRYABLE and status != 0:
                return status
            if attempt < self.retries - 1:
                self._wait(attempt)
        return status

    def download(self, url: str, dest: str) -> tuple[bool, int, str]:
        """GET to `dest` (via .part). Returns (ok, last_status, reason)."""
        tmp, status, reason = dest + ".part", 0, ""
        for attempt in range(self.retries):
            try:
                with self.session.get(url, auth=self.auth, stream=True, timeout=(30, 300)) as r:
                    status = r.status_code
                    ctype = r.headers.get("Content-Type", "")
                    if status == 200 and "html" not in ctype:
                        expected = int(r.headers.get("Content-Length") or 0)
                        written = 0
                        with open(tmp, "wb") as f:
                            for chunk in r.iter_content(chunk_size=1024 * 1024):
                                f.write(chunk)
                                written += len(chunk)
                        if expected and written != expected:
                            reason = f"truncated: {written} of {expected} bytes"
                        elif not is_valid_zip(tmp):
                            reason = "not a readable ZIP"
                        else:
                            os.replace(tmp, dest)
                            return True, status, ""
                    else:
                        reason = f"HTTP {status} {ctype}".strip()
            except requests.RequestException as exc:
                status, reason = 0, f"{type(exc).__name__}: {exc}"[:200]
            if os.path.exists(tmp):
                os.remove(tmp)
            if status not in self.RETRYABLE and status not in (0, 200):
                break  # 401/403 and other definitive answers: retrying will not help
            if attempt < self.retries - 1:
                self._wait(attempt)
        return False, status, reason


def is_valid_zip(path: str) -> bool:
    """Cheap integrity check: a truncated ZIP has no central directory and fails to open."""
    try:
        with zipfile.ZipFile(path) as z:
            return len(z.namelist()) > 0
    except (zipfile.BadZipFile, OSError):
        return False


def previous_month(ano_mes: str) -> str:
    year, month = map(int, ano_mes.split("-"))
    return f"{year - 1}-12" if month == 1 else f"{year}-{month - 1:02d}"


def probe(fetcher: Fetcher, base: str, ano_mes: str) -> int:
    """Status of a 1-byte GET on the month's PROBE_FILE (retried while flaky)."""
    url = f"{WEBDAV}{quote(base)}/{ano_mes}/{PROBE_FILE}"
    return fetcher.status("GET", url, headers={"Range": "bytes=0-0"}, stream=True)


def check_source(fetcher: Fetcher, base: str, ano_mes: str) -> None:
    """Fail fast with the real cause. PROPFIND is useless here (200 + empty body for any path, any link), so the
    check reads one byte of a contract file: 206/200 present, 401/403 link rejected, 404 missing. A month that is
    missing while the previous one exists is "not published yet"; both missing means the directory is wrong."""
    month = probe(fetcher, base, ano_mes)
    if month in (200, 206):
        return
    if month == 0:
        raise SourceError("source_unreachable", f"no answer from {HOST} after {fetcher.retries} attempts")
    if month in (401, 403):
        raise SourceError("source_auth_failed", f"HTTP {month}: the share link was rejected. The Receita may have "
                                                "re-created it: paste the new link in RFB_SHARE_URL")
    if month == 404:
        prev = previous_month(ano_mes)
        if probe(fetcher, base, prev) in (200, 206):
            raise SourceError("source_month_not_published", f"{ano_mes} is not published under '{base}' yet "
                                                            f"({prev} is)")
        raise SourceError("source_dir_not_found", f"neither {ano_mes} nor {prev} found under '{base}': check "
                                                  "RFB_SHARE_DIR, or an old RFB_SHARE_TOKEN in .env pointing to a "
                                                  "previous share")
    raise SourceError("source_unreachable", f"HTTP {month} from {HOST} after {fetcher.retries} attempts")


def download_month(token: str, ano_mes: str, base_dir: str, fetcher: Fetcher | None = None,
                   token_source: str = "argument") -> int:
    step = current_step()
    fetcher = fetcher or Fetcher(token)
    base = share_dir()
    logger.info(f"source: share {token} ({token_source}), dir {base}/{ano_mes}")
    target_dir = os.path.join(base_dir, f"ref{ano_mes.replace('-', '')}")
    os.makedirs(target_dir, exist_ok=True)
    step.set(files_total=len(EXPECTED_FILES), ref_month=ano_mes, share_dir=base)

    if all(is_valid_zip(os.path.join(target_dir, n)) for n in EXPECTED_FILES):
        # the month is complete on disk: no request at all, so a re-run works even with the source down
        step.set(files_ok=len(EXPECTED_FILES), files_skipped=len(EXPECTED_FILES))
        logger.info(f"{ano_mes}: all {len(EXPECTED_FILES)} files already local and readable, source not contacted")
        return 0

    try:
        check_source(fetcher, base, ano_mes)
    except SourceError as exc:
        level = 30 if exc.event == "source_month_not_published" else 40
        log_event(logger, exc.event, str(exc), level=level, ref_month=ano_mes, share_dir=base)
        return NOT_PUBLISHED if exc.event == "source_month_not_published" else 1

    failed: dict[str, str] = {}
    ok = skipped = 0
    for name in EXPECTED_FILES:
        dest = os.path.join(target_dir, name)
        if os.path.exists(dest) and is_valid_zip(dest):
            skipped += 1
            continue
        if os.path.exists(dest):
            logger.warning(f"{name}: local file is not a readable ZIP (truncated by an earlier run?), downloading again")
            os.remove(dest)
        logger.info(f"downloading {name}")
        done, status, reason = fetcher.download(f"{WEBDAV}{quote(base)}/{ano_mes}/{name}", dest)
        if done:
            ok += 1
            step.add(bytes_downloaded=os.path.getsize(dest))
        else:
            failed[name] = f"{status}: {reason}"
            log_event(logger, "file_failed", f"{name}: HTTP {status} {reason}", level=40, file=name, http_status=status)
        fetcher.sleep(fetcher.pause_s)

    step.set(files_ok=ok + skipped, files_failed=len(failed), files_skipped=skipped)
    if failed:
        log_event(logger, "source_incomplete",
                  f"{len(failed)} of {len(EXPECTED_FILES)} files did not come for {ano_mes} (re-run resumes): {failed}",
                  level=40, ref_month=ano_mes, missing=sorted(failed))
        return 1  # bronze needs the full month
    logger.info(f"{ano_mes}: all {len(EXPECTED_FILES)} files present ({ok} downloaded, {skipped} already local)")
    return 0


if __name__ == "__main__":
    try:
        tok, src = share_token()
    except SourceError as exc:
        log_event(logger, exc.event, str(exc), level=40)
        sys.exit(1)
    sys.exit(download_month(tok, rfb_reference(), os.path.join(DATASOURCE_DIR, "rfb"), token_source=src))
