# ADR 0009 — RFB download driven by a file contract, not by the source's listing

**Status:** accepted (2026-10-12)

## Context

`rfb01` listed the month folder of the Receita Federal public share (Nextcloud WebDAV, `PROPFIND`) and
downloaded whatever came back. In October 2026, testing on a fresh machine, the source had changed without
notice:

- **The share was re-created:** a new public link, and its root moved up one level; the months now live under
  `/Dados/Cadastros/CNPJ/<YYYY-MM>`. The token kept in `.env` on the lab machine pointed to the old share.
- **Listing is gone:** `PROPFIND` on an existing folder answers `200` with an empty body (a missing folder still
  answers `404` with a WebDAV error), so there is nothing to parse. The pipeline reported "WebDAV did not return
  valid XML".
- **Direct downloads still work, but the server is flaky under bursts:** `HEAD` on the same file returned `200`,
  then `404`, then `200` within minutes, and probing the 37 files in a row returned a different random subset
  each time (12 found, then 2).
- The share link was treated as a secret (`RFB_SHARE_TOKEN`, empty in `.env.example`), so a fresh clone could
  not run the RFB pipeline at all, although the link is public.

## Decision

- **Contract instead of listing:** every reference month has the same 37 files (`Empresas0-9`,
  `Estabelecimentos0-9`, `Socios0-9`, `Simples` and 6 domain tables). The step downloads exactly that set and
  fails if any file does not come (bronze needs the whole month).
- **Flakiness is expected:** one HTTP session, retries with exponential backoff, a pause between files, and a
  `404` is retried too; it only counts as missing after every attempt.
- **A local file counts as done only if it is a readable ZIP of the announced size** (an interrupted download
  is never left as `.zip`; a truncated one from an older run is downloaded again).
- **Explicit causes, one event each:** `source_auth_failed` (link rejected → the share was re-created),
  `source_dir_not_found` (wrong `RFB_SHARE_DIR` or a stale token), `source_month_not_published` (exit `99`:
  the task is skipped, no new data), `source_unreachable`, `source_incomplete` (with the HTTP status per file).
- **The link is configuration, not a secret:** `RFB_SHARE_URL` takes the browser link as is; the current link is
  built in as the default, so a fresh clone works. `RFB_SHARE_TOKEN` is still read (legacy) but the URL wins.

## Consequences

- A fresh clone downloads the RFB month with no manual step.
- A month already complete on disk (37 readable ZIPs) does not contact the source at all: re-runs work while
  the source is down.
- A change of file names by the Receita now fails loudly (`source_incomplete` listing the names) instead of
  silently loading a partial month. The contract lives in one constant (`EXPECTED_FILES`).
- When the source is degraded the step is slower (bounded: retries × backoff per file) instead of failing on
  the first error.
- Not solved here: discovering a re-created share link automatically. There is no stable page known to publish
  it; the rejection is detected and named (`source_auth_failed`), and the fix is pasting the new link.
