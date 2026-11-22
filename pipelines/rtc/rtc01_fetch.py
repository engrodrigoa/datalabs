"""Receita Federal tax calculator (GitHub mirror) -> rebuilt reference database in the lake.

Step 1 of the RTC reference pipeline (ADR 0019). It watches the official repository, and when the branch moves it
fetches only the Flyway scripts, rebuilds the calculator's SQLite database (flyway_replay.py) and lands it.

Source: github.com/ReceitaFederal/calculadora-rtc-regime-regular (MIT, Portaria Cotec nº 342/2026)
    The repository is a MIRROR of the Receita/Serpro internal repository: each release arrives as a few squashed
    commits (8 commits for releases up to 1.6.1, 2026-11-17) carrying the cumulative scripts, so one fetch of the
    branch head is a complete database. The internal changelog shows a release every 2-4 weeks (1.5.0 2026-09-08,
    1.5.4 2026-09-30, 1.6.0 2026-10-13); how often the mirror follows is not known yet: that is what the
    `rtc_reference_*` checks measure.

Source check: the request the step really makes
    `git ls-remote <repo> refs/heads/<ref>` answers the sha of the branch head. Same sha as the manifest's current
    -> exit 99 (SKIPPED, nothing new). A new sha is then fetched by that exact sha (`git fetch --depth 1 <sha>`,
    sparse: flyway/sql, pom.xml, CHANGELOG.md), so what is built is what was checked, even if the branch moves
    while the step runs. git, not the GitHub REST API: no token, no 60 requests/hour limit, and the same transport
    as a `git clone` (measured 2026-11-17: ls-remote and fetch work where api.github.com and codeload answer 403).

Source contract (structural, like LC 214)
    * every script replays (flyway_replay rules) and the database has no foreign key violation
    * every table the pipeline reads exists, with at least CONTRACT_MIN_ROWS rows
    A rejected release is not landed and the manifest is untouched (exit 1, event `source_contract_failed`).

Where it goes (same lake as LC 214: RTC_LAKE = s3 | file:///dir)
    landing/rtc/<sha12>/calculadora-rtc.db      the rebuilt database (read-only input of rtc02_load)
    landing/rtc/<sha12>/build_report.json       release, database version, row counts, overlaps, timings
    landing/rtc/manifest.json                   versions + "current": written LAST, it is the commit point

Exit codes: 0 new version landed | 99 unchanged | 1 failed (source, replay or contract).
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.commons.logger import get_logger, log_event  # noqa: E402
from pipelines.observability import current_step  # noqa: E402
from pipelines.rag.lc214.lake import Lake, LakeError, lake_from_env  # noqa: E402
from pipelines.rtc import flyway_replay as fr  # noqa: E402

logger = get_logger("rtc01_fetch")

REPO_URL = os.getenv("RTC_REPO_URL", "https://github.com/ReceitaFederal/calculadora-rtc-regime-regular.git")
REPO_REF = os.getenv("RTC_REPO_REF", "main")
GIT_TIMEOUT_S = int(os.getenv("RTC_GIT_TIMEOUT_S", "180"))
SPARSE_PATHS = ("flyway/sql", "pom.xml", "CHANGELOG.md")
DB_NAME = "calculadora-rtc.db"

EXIT_OK, EXIT_SKIPPED, EXIT_FAILED = 0, 99, 1


class SourceError(Exception):
    def __init__(self, event: str, message: str):
        super().__init__(message)
        self.event = event


def k_manifest() -> str:
    return "landing/rtc/manifest.json"


def k_db(sha: str) -> str:
    return f"landing/rtc/{sha[:12]}/{DB_NAME}"


def k_report(sha: str) -> str:
    return f"landing/rtc/{sha[:12]}/build_report.json"


def empty_manifest() -> dict:
    return {"current": None, "previous": None, "versions": []}


# --------------------------------------------------------------------------- git (injectable)
def _git(*args: str, cwd: str | None = None) -> str:
    try:
        r = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, timeout=GIT_TIMEOUT_S)
    except FileNotFoundError as exc:
        raise SourceError("git_missing", "git not found in the image (Dockerfile installs it)") from exc
    except subprocess.TimeoutExpired as exc:
        raise SourceError("source_unreachable", f"git {args[0]} timed out after {GIT_TIMEOUT_S}s") from exc
    if r.returncode != 0:
        raise SourceError("source_unreachable", f"git {args[0]} exit {r.returncode}: {r.stderr.strip()[:300]}")
    return r.stdout


def upstream_sha(repo: str = REPO_URL, ref: str = REPO_REF) -> str:
    out = _git("ls-remote", repo, f"refs/heads/{ref}").split()
    if not out:
        raise SourceError("ref_not_found", f"{repo}: branch {ref!r} not found (renamed? set RTC_REPO_REF)")
    return out[0]


def fetch_tree(sha: str, dest: str, repo: str = REPO_URL) -> Path:
    """Shallow, sparse fetch of exactly `sha` into dest. Returns the checkout root."""
    _git("init", "-q", dest)
    _git("remote", "add", "origin", repo, cwd=dest)
    _git("sparse-checkout", "set", "--no-cone", *SPARSE_PATHS, cwd=dest)
    _git("fetch", "-q", "--depth", "1", "origin", sha, cwd=dest)
    _git("checkout", "-q", "FETCH_HEAD", cwd=dest)
    return Path(dest)


# --------------------------------------------------------------------------- the step
def build(checkout: Path, db_path: Path) -> dict:
    """Replay + contract. Returns the build report; raises SourceError when the release is rejected."""
    sql_dir = checkout / "flyway" / "sql"
    if not sql_dir.is_dir():
        raise SourceError("source_layout_changed", "flyway/sql not found in the repository (layout changed?)")
    scripts = fr.read_scripts(sql_dir)
    t0 = time.monotonic()
    try:
        n = fr.replay(scripts, db_path)
    except fr.ReplayError as exc:
        raise SourceError("replay_failed", str(exc)) from exc
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        problems = fr.contract_violations(con)
        if problems:
            raise SourceError("source_contract_failed", "; ".join(problems))
        pom = checkout / "pom.xml"
        return {
            "scripts": n,
            "replay_s": round(time.monotonic() - t0, 2),
            "release": fr.release_version(pom.read_text(encoding="utf-8")) if pom.exists() else None,
            "database_version": fr.database_version(con),
            "row_counts": fr.table_counts(con),
            "overlapping_vigencias": [list(r) for r in fr.overlapping_vigencias(con)],
        }
    finally:
        con.close()


def run(lake: Lake, *, force: bool = False, sha_of=upstream_sha, fetch=fetch_tree,
        repo: str = REPO_URL, ref: str = REPO_REF) -> int:
    step = current_step()
    sha = sha_of(repo, ref)
    manifest = lake.get_json(k_manifest(), empty_manifest())
    current = manifest.get("current") or {}
    step.label(source_sha=sha[:12], contract_version=str(fr.CONTRACT_VERSION))
    if sha == current.get("sha") and current.get("contract_version") == fr.CONTRACT_VERSION and not force:
        log_event(logger, "source_unchanged", f"{ref} still at {sha[:12]} (contract v{fr.CONTRACT_VERSION}): "
                  "nothing to rebuild", sha=sha)
        return EXIT_SKIPPED
    if sha == current.get("sha"):
        log_event(logger, "contract_changed", f"same source {sha[:12]}, contract v{current.get('contract_version')}"
                  f" -> v{fr.CONTRACT_VERSION}: rebuilding", sha=sha)

    with tempfile.TemporaryDirectory(prefix="rtc01_") as tmp:
        checkout = fetch(sha, os.path.join(tmp, "src"), repo)
        db_path = Path(tmp) / DB_NAME
        report = build(checkout, db_path)
        report.update(sha=sha, ref=ref, repo=repo, built_at=datetime.now(timezone.utc).isoformat(),
                      contract_version=fr.CONTRACT_VERSION)
        if report["overlapping_vigencias"]:
            log_event(logger, "source_vigencia_overlap",
                      f"{len(report['overlapping_vigencias'])} cClassTrib with overlapping validity in the same "
                      "tax group (kept: the source is authoritative)", level=30,
                      overlaps=report["overlapping_vigencias"][:20])
        lake.put_file(k_db(sha), str(db_path))
        lake.put_json(k_report(sha), report)

    entry = {"sha": sha, "release": report["release"], "database_version": report["database_version"],
             "built_at": report["built_at"], "db": k_db(sha), "contract_version": fr.CONTRACT_VERSION}
    versions = [v for v in manifest.get("versions", []) if v.get("sha") != sha] + [entry]
    previous = manifest.get("current") if (manifest.get("current") or {}).get("sha") != sha else manifest.get("previous")
    lake.put_json(k_manifest(), {"current": entry, "previous": previous, "versions": versions})

    rows = sum(v or 0 for v in report["row_counts"].values())
    step.add(files_total=1, rows_out=rows)
    step.set(scripts=report["scripts"], replay_s=report["replay_s"])
    step.label(release=report["release"] or "?", database_version=report["database_version"] or "?")
    log_event(logger, "version_landed",
              f"release {report['release']} ({report['database_version']}, {sha[:12]}): "
              f"{report['scripts']} scripts in {report['replay_s']}s, {rows} reference rows",
              sha=sha, release=report["release"], database_version=report["database_version"])
    return EXIT_OK


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--force", action="store_true", help="rebuild and land even if the sha did not change")
    ap.add_argument("--lake", default=None, help="'s3' (default, RTC_LAKE) or file:///dir")
    args = ap.parse_args(argv)
    try:
        return run(lake_from_env(args.lake or os.getenv("RTC_LAKE", "s3")), force=args.force)
    except (SourceError, LakeError) as exc:
        log_event(logger, exc.event, str(exc), level=40)
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
