"""
Archive dbt/Elementary artifacts of a run to MinIO (cold storage / audit trail).

    python -m pipelines.observability.archive_dbt_artifacts --pipeline anp

dbt *results* are not parsed here anymore: with Cosmos every model runs in its own
dbt invocation, so target/run_results.json only holds the last one. The source of truth
for run/test history is Elementary (schema `elementary`: dbt_run_results,
elementary_test_results, dbt_invocations), which is what Grafana reads.
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
from datetime import datetime, timezone

from pipelines.commons.env_loader import BUCKET_AUDIT, DBT_PROJECT_DIR, MANIFEST_PATH
from pipelines.commons.logger import get_logger, log_event
from pipelines.commons.s3_client import get_s3_client
from pipelines.observability.tracker import current_step

logger = get_logger("observability.archive")
REPORTS_DIR = os.path.join(DBT_PROJECT_DIR, "logs", "elementary_logs")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", required=True)
    args = parser.parse_args(argv)

    s3 = get_s3_client()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    uploaded = 0

    reports = sorted(glob.glob(os.path.join(REPORTS_DIR, f"edr_report_{args.pipeline}_*.html")), key=os.path.getmtime)
    if reports:
        key = f"elementary/{args.pipeline}/edr_report_{stamp}.html"
        s3.upload_file(reports[-1], BUCKET_AUDIT, key)
        log_event(logger, "artifact_archived", f"s3://{BUCKET_AUDIT}/{key}", artifact="elementary_report", key=key)
        uploaded += 1
        for old in reports[:-5]:  # keep the 5 most recent reports locally
            os.remove(old)
    else:
        log_event(logger, "artifact_missing", f"no Elementary report in {REPORTS_DIR}", level=30)

    if os.path.exists(MANIFEST_PATH):
        key = f"dbt/{args.pipeline}/manifest_{stamp}.json"
        s3.upload_file(MANIFEST_PATH, BUCKET_AUDIT, key)
        log_event(logger, "artifact_archived", f"s3://{BUCKET_AUDIT}/{key}", artifact="dbt_manifest", key=key)
        uploaded += 1

    current_step().set(artifacts_uploaded=uploaded)
    return 0


if __name__ == "__main__":
    sys.exit(main())
