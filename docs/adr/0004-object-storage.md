# ADR 0004 — Object storage: from MinIO to Silo

**Status:** accepted (2026-10-12) — supersedes the "MinIO frozen" decision

## Context

MinIO stopped publishing community Docker images in October 2025, and the lab kept running on a frozen
`minio/minio:latest`, accepted as a lab-only risk. The risk materialised: `minio/minio` was **deleted from
Docker Hub on 2026-09-11** (`quay.io/minio/minio` returns 401 since 2026-09-24, and the GitHub repository is
archived). Machines that had the image cached kept working; a fresh clone failed at `make up` with
`pull access denied for minio/minio, repository does not exist`. It was found a month later, by chance, when
the stack was brought up on a second machine.

The pipelines use object storage only through the S3 API (`boto3` in `pipelines/commons/s3_client.py`, Polars
`s3://` reads), and buckets are created by the code, so any S3-compatible server with the same credentials works.

## Decision

Use **Silo** (`pgsty/silo`), a community fork of MinIO, pinned by release tag **and** digest.

| Option | Why not (now) |
|---|---|
| Silo (chosen) | drop-in: same `server /data --console-address` CLI, same `MINIO_ROOT_*` env vars, same on-disk format (existing `minio_data` volumes keep working), ships `mc` so the healthcheck is unchanged |
| RustFS 1.0 | actively maintained, Apache-2.0, but incomplete S3 coverage and a one-way "preview" migration from MinIO data; different console path |
| Chainguard `minio` | free tier only publishes `latest` (breaks the pinned-image rule), runs as UID 65532 |
| Garage / SeaweedFS | different bootstrap (keys and buckets created through their own CLI/config) |

## Consequences

- Fresh clones work again; the change is one compose line plus `MINIO_UPDATE=off`.
- Silo is young and not affiliated with MinIO: its maintenance is a new dependency risk. RustFS stays the
  fallback; revisit if Silo stops releasing.
- Lesson: a frozen dependency is a time bomb, and "it works on my machine" hides it because of the local cache.
  Pinned images must be checked for availability continuously, not only when someone runs `make up`.
