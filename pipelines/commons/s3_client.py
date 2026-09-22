import sys, os
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

#=============================================================================================================
#===================================
# env loader - connection setup
#===================================
from pipelines.commons.env_loader import (
    validate_env, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, REQUIRED_BUCKETS
)
from pipelines.commons.logger import get_logger
logger = get_logger("S3_CLIENT")
#=============================================================================================================

is_docker = os.environ.get("AIRFLOW_UID") is not None or os.path.exists("/.dockerenv")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_INTERNAL") if is_docker else os.getenv("MINIO_ENDPOINT_EXTERNAL")
BUCKET_AUDIT = os.getenv("MINIO_BUCKET_AUDIT", "audit")



def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
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
    logger.info("#" * 60)
    s3_env_vars = {
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
        "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    }

    logger.info("s3 env vars validation")
    validate_env(s3_env_vars)
    logger.info("s3 env vars found successfully")

    try:
        logger.info(f"attempting to connect to endpoint - [{MINIO_ENDPOINT}]")
        s3 = get_s3_client()
        s3.list_buckets()
        logger.info("handshake success [MinIO]")

        for bucket in REQUIRED_BUCKETS:
            ensure_bucket_exists(s3, bucket)

        return True

    except EndpointConnectionError as conn_err:
        logger.error("FAILED CONNECTING ON MINIO (FAIL-FAST)")
        logger.debug(f"DETAILS {str(conn_err).strip()}")
        logger.critical("ABORTING RUN DUE TO S3 CONNECTION FAILURE")
        sys.exit(1)

    except ClientError as s3_err:
        logger.error("FAILED VALIDATING/CREATING BUCKETS (FAIL-FAST)")
        logger.debug(f"DETAILS {str(s3_err).strip()}")
        logger.critical("ABORTING RUN DUE TO S3 BUCKET FAILURE")
        sys.exit(1)

    except Exception as e:
        logger.error(f"UNEXPECTED ERROR {e}")
        logger.critical("ABORTING RUN DUE TO S3 CONNECTION FAILURE")
        sys.exit(1)

    finally:
        logger.info("[!] connection test finished")
        logger.info("#" * 60)