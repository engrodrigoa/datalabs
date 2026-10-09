# ADR 0004 — Object storage: MinIO (frozen) and its replacement

**Status:** open

MinIO stopped publishing community Docker images in October 2025, so `minio/minio:latest` is effectively a
frozen image without security updates. It is acceptable for a local lab. Candidates for an actively
maintained S3-compatible store: Garage, SeaweedFS, RustFS, or a maintained MinIO rebuild. The pipelines only
use the S3 API through `boto3` (`pipelines/commons/s3_client.py`), so the swap is a compose change.
