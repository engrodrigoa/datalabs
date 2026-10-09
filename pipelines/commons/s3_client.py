import logging
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

#=============================================================================================================
#===================================
# env loader - connection setup
#===================================
from pipelines.commons.env_loader import (
    validate_env, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, REQUIRED_BUCKETS
)
from pipelines.commons.logger import get_logger, log_event
logger = get_logger("S3_CLIENT")
#=============================================================================================================

# endpoint resolution lives only in env_loader (no duplicated docker detection here)



def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=10, read_timeout=120),
    )

def ensure_bucket_exists(s3_client, bucket_name: str):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchBucket"):
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"bucket created [{bucket_name}]")
        else:
            raise


def ensure_buckets_exist(bucket_names: list = None):
    buckets = bucket_names or REQUIRED_BUCKETS
    s3 = get_s3_client()
    for bucket in buckets:
        ensure_bucket_exists(s3, bucket)
        logger.info(f"bucket ok [{bucket}]")
        
def test_s3_connection():
    """Fail fast before any work: ONE event on failure (with the real cause), silent on success."""
    validate_env({"MINIO_ENDPOINT": MINIO_ENDPOINT, "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
                  "MINIO_SECRET_KEY": MINIO_SECRET_KEY})
    try:
        s3 = get_s3_client()
        s3.list_buckets()
        for bucket in REQUIRED_BUCKETS:
            ensure_bucket_exists(s3, bucket)
    except EndpointConnectionError as exc:
        _s3_fail("cannot reach MinIO", exc)
    except ClientError as exc:
        _s3_fail("MinIO rejected the request (credentials/buckets)", exc)
    except Exception as exc:  # noqa: BLE001
        _s3_fail("unexpected S3 error", exc)
    log_event(logger, "s3_connected", f"connected to {MINIO_ENDPOINT}", level=logging.DEBUG,
              endpoint=MINIO_ENDPOINT, buckets=list(REQUIRED_BUCKETS))
    return True


def _s3_fail(what: str, exc: Exception) -> None:
    # the cause goes in the ERROR line itself: at DEBUG it would never reach the task log
    log_event(logger, "s3_connection_failed", f"{what} at {MINIO_ENDPOINT}: {str(exc).strip()}",
              level=logging.ERROR, endpoint=MINIO_ENDPOINT, error_type=type(exc).__name__)
    sys.exit(1)