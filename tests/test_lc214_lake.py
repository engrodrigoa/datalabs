"""LC 214 lake: the same keys on S3 (Silo) and on a local folder; missing objects and buckets fail by name."""
import polars as pl
import pytest

from pipelines.rag.lc214 import lake as lk


def _roundtrip(lake):
    key = lk.k_silver("a" * 64, "dispositivos.parquet")
    assert not lake.exists(key)
    lake.put_parquet(key, pl.DataFrame({"id": [1, 2]}))
    assert lake.exists(key) and lake.get_parquet(key)["id"].to_list() == [1, 2]
    lake.put_json(lk.k_manifest(), {"current": "a" * 64})
    assert lake.get_json(lk.k_manifest()) == {"current": "a" * 64}
    with pytest.raises(lk.LakeMissing):
        lake.get(lk.k_text("b" * 64))
    assert lake.get_json(lk.k_text("b" * 64), default={"x": 1}) == {"x": 1}


def test_keys_follow_the_medallion_layout():
    sha = "0cf36f4433ce" + "0" * 52
    assert lk.k_raw(sha) == "landing/lc214/raw/lc214_0cf36f4433ce.pdf"
    assert lk.k_text(sha) == "bronze/lc214/text/lc214_0cf36f4433ce.txt"
    assert lk.k_silver(sha, "chunks.parquet") == "silver/lc214/0cf36f4433ce/chunks.parquet"


def test_local_lake(tmp_path):
    _roundtrip(lk.LocalLake(str(tmp_path)))


def test_lake_from_env(tmp_path):
    assert isinstance(lk.lake_from_env(f"file://{tmp_path}"), lk.LocalLake)
    with pytest.raises(lk.LakeError) as e:
        lk.lake_from_env("/sem/esquema")
    assert e.value.event == "lake_misconfigured"


def test_s3_lake():
    moto = pytest.importorskip("moto")
    import boto3

    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        for b in ("landing", "bronze", "silver"):
            s3.create_bucket(Bucket=b)
        _roundtrip(lk.S3Lake(client=s3))


def test_s3_lake_without_buckets_says_how_to_create_them():
    moto = pytest.importorskip("moto")
    import boto3

    with moto.mock_aws():
        lake = lk.S3Lake(client=boto3.client("s3", region_name="us-east-1"))
        with pytest.raises(lk.LakeError) as e:
            lake.put_json(lk.k_manifest(), {})
        assert e.value.event == "lake_bucket_missing" and "dag_setup_infrastructure" in str(e.value)
