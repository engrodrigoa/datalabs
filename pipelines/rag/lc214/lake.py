"""Where the LC 214 artifacts live: the data lake (S3 API, Silo in the stack), or a folder for tests and offline work.

Layout (medallion), sha12 = first 12 hex digits of the PDF sha256:
    landing/lc214/manifest.json                  versions + "current": the commit point of lc01
    landing/lc214/raw/lc214_<sha12>.pdf          the file as published, every version ever accepted
    landing/lc214/rejected/lc214_<sha12>.pdf     failed the source contract, kept for inspection
    bronze/lc214/text/lc214_<sha12>.txt          `pdftotext -layout` of that version (lossless, technical)
    silver/lc214/<sha12>/dispositivos.parquet    lc02, + parse_report.json
    silver/lc214/<sha12>/chunks.parquet          lc03, + artigos.parquet + chunks_report.json

Why reports and the manifest are written LAST
    S3 has no rename, but a PUT replaces a whole object atomically. Each step writes its data objects first and its
    report (or the manifest) at the end: a run killed half-way leaves data nobody points to, never a report pointing
    to data that is not there. Readers trust only what a report or the manifest says.

Processing still happens on local disk (pdftotext reads a file), in a temporary directory that is thrown away:
the lake is the only place where anything is kept.

LC214_LAKE = "s3" (default: MINIO_* from env_loader) | "file:///abs/dir" (tests, a laptop without the stack)
"""
from __future__ import annotations

import io
import json
import os

import polars as pl

BUCKET_LANDING = os.getenv("MINIO_BUCKET_LANDING", "landing")
BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")
PREFIX = "lc214"


class LakeError(Exception):
    """A failure with a stable event name (queried in Loki), like the steps' own errors."""

    def __init__(self, event: str, message: str):
        super().__init__(message)
        self.event = event


class LakeMissing(LakeError):
    def __init__(self, key: str):
        super().__init__("lake_object_missing", f"{key} not found in the lake")


# --------------------------------------------------------------------------- keys (one place, read by every step)
def k_manifest() -> str:
    return f"{BUCKET_LANDING}/{PREFIX}/manifest.json"


def k_raw(sha: str) -> str:
    return f"{BUCKET_LANDING}/{PREFIX}/raw/lc214_{sha[:12]}.pdf"


def k_rejected(sha: str) -> str:
    return f"{BUCKET_LANDING}/{PREFIX}/rejected/lc214_{sha[:12]}.pdf"


def k_text(sha: str) -> str:
    return f"{BUCKET_BRONZE}/{PREFIX}/text/lc214_{sha[:12]}.txt"


def k_silver(sha: str, name: str) -> str:
    """name: dispositivos.parquet | parse_report.json | chunks.parquet | artigos.parquet | chunks_report.json"""
    return f"{BUCKET_SILVER}/{PREFIX}/{sha[:12]}/{name}"


# --------------------------------------------------------------------------- stores
class Lake:
    """Keys are "<bucket>/<path>". Subclasses implement get/put/exists; the typed helpers are shared."""

    def get(self, key: str) -> bytes:
        raise NotImplementedError

    def put(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def keys(self, prefix: str) -> list[str]:
        """Every key under `prefix` (sorted): reading a folder of reports, e.g. answers/dev/."""
        raise NotImplementedError

    def url(self, key: str) -> str:
        raise NotImplementedError

    def get_json(self, key: str, default=None):
        try:
            return json.loads(self.get(key))
        except LakeMissing:
            if default is not None:
                return default
            raise

    def put_json(self, key: str, obj) -> None:
        self.put(key, json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"), "application/json")

    def get_parquet(self, key: str) -> pl.DataFrame:
        return pl.read_parquet(io.BytesIO(self.get(key)))

    def put_parquet(self, key: str, df: pl.DataFrame) -> None:
        buf = io.BytesIO()
        df.write_parquet(buf)
        self.put(key, buf.getvalue(), "application/vnd.apache.parquet")

    def put_file(self, key: str, path: str, content_type: str = "application/octet-stream") -> None:
        with open(path, "rb") as f:
            self.put(key, f.read(), content_type)


class LocalLake(Lake):
    """A folder with the same keys as the lake (bucket = first directory). Writes are write-then-rename."""

    def __init__(self, root: str):
        self.root = root

    def _path(self, key: str) -> str:
        return os.path.join(self.root, *key.split("/"))

    def get(self, key):
        try:
            with open(self._path(key), "rb") as f:
                return f.read()
        except FileNotFoundError:
            raise LakeMissing(key) from None

    def put(self, key, data, content_type="application/octet-stream"):
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path + ".tmp", "wb") as f:
            f.write(data)
        os.replace(path + ".tmp", path)

    def exists(self, key):
        return os.path.exists(self._path(key))

    def keys(self, prefix):
        base = self._path(prefix)
        if not os.path.isdir(base):
            return []
        out = []
        for dirpath, _, files in os.walk(base):
            for f in files:
                if not f.endswith(".tmp"):                          # write-then-rename temporaries
                    rel = os.path.relpath(os.path.join(dirpath, f), self.root)
                    out.append("/".join(rel.split(os.sep)))
        return sorted(out)

    def url(self, key):
        return "file://" + self._path(key)


class S3Lake(Lake):
    """S3 API (Silo/MinIO in the stack). Buckets are created by dag_setup_infrastructure, not here."""

    def __init__(self, client=None):
        if client is None:
            from pipelines.commons.s3_client import get_s3_client   # boto3 only where the lake is S3

            client = get_s3_client()
        self.s3 = client

    @staticmethod
    def _split(key: str) -> tuple[str, str]:
        bucket, _, path = key.partition("/")
        return bucket, path

    def _call(self, key: str, fn, **kw):
        from botocore.exceptions import ClientError

        bucket, path = self._split(key)
        try:
            return fn(Bucket=bucket, Key=path, **kw)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404", "NotFound"):
                raise LakeMissing(key) from None
            if code == "NoSuchBucket":
                raise LakeError("lake_bucket_missing", f"bucket {bucket!r} does not exist: run "
                                "dag_setup_infrastructure (it creates landing/bronze/silver/gold)") from None
            raise

    def get(self, key):
        return self._call(key, self.s3.get_object)["Body"].read()

    def put(self, key, data, content_type="application/octet-stream"):
        self._call(key, self.s3.put_object, Body=data, ContentType=content_type)

    def exists(self, key):
        try:
            self._call(key, self.s3.head_object)
            return True
        except LakeMissing:
            return False

    def keys(self, prefix):
        bucket, path = self._split(prefix.rstrip("/") + "/")
        out = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=path):
            out += [f"{bucket}/{o['Key']}" for o in page.get("Contents") or []]
        return sorted(out)

    def url(self, key):
        return f"s3://{key}"


def lake_from_env(spec: str | None = None) -> Lake:
    spec = spec or os.getenv("LC214_LAKE", "s3")
    if spec == "s3":
        return S3Lake()
    if spec.startswith("file://"):
        return LocalLake(spec.removeprefix("file://"))
    raise LakeError("lake_misconfigured", f"LC214_LAKE={spec!r}: use 's3' or 'file:///absolute/dir'")
