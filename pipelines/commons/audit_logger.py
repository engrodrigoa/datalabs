
import os
import re
import time
import logging
from datetime import datetime
from glob import glob
from zoneinfo import ZoneInfo

#=============================================================================================================
#===================================
# env loader - connection setup
#===================================
from pipelines.commons.s3_client import get_s3_client
from pipelines.commons.env_loader import BUCKET_AUDIT, validate_env, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
#=============================================================================================================


AUDIT_LOG_DIR = os.getenv("AUDIT_LOG_DIR", "/opt/airflow/audit_logs")
AIRFLOW_LOG_DIR = os.getenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs")
AUDIT_TZ = ZoneInfo(os.getenv("AUDIT_TZ", "America/Sao_Paulo"))
DIVIDER_LENGTH = 100

LOG_FLUSH_DELAY_SECONDS = float(os.getenv("AUDIT_LOG_FLUSH_DELAY", "2"))

logger = logging.getLogger("airflow.task")


def _divider(char: str = "=") -> str:
    return char * DIVIDER_LENGTH


def _format_local_ts(logical_date: datetime) -> str:
    return logical_date.astimezone(AUDIT_TZ).strftime("%Y%m%d%H%M%S")


def upload_audit_log_to_minio(local_path: str, dag_id: str, logical_date: datetime) -> str:

    validate_env({
        "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
        "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    })
    s3 = get_s3_client()
    execution_ts = _format_local_ts(logical_date)
    object_key = f"airflow/{dag_id}_{execution_ts}.log"
    s3.upload_file(local_path, BUCKET_AUDIT, object_key)
    return f"s3://{BUCKET_AUDIT}/{object_key}"


def _build_run_summary(context) -> list:
    dag_run = context.get("dag_run")
    if dag_run is None:
        return [" STATUS: UNKNOWN (dag_run not available in context)"]

    try:
        task_instances = dag_run.get_task_instances()
    except Exception:
        task_instances = []

    current_task_id = context["task"].task_id if context.get("task") else None

    total = 0
    by_state = {}
    for ti in task_instances:
        if ti.task_id == current_task_id:
            continue
        total += 1
        by_state[ti.state] = by_state.get(ti.state, 0) + 1

    success_count = by_state.get("success", 0)
    failed_count = by_state.get("failed", 0)
    skipped_count = by_state.get("skipped", 0)
    other_count = total - success_count - failed_count - skipped_count
    overall_status = "SUCCESS" if failed_count == 0 and total > 0 else "FAILED"

    start_date = dag_run.start_date
    end_date = dag_run.end_date or datetime.now(start_date.tzinfo if start_date else None)
    duration = "N/A"
    if start_date and end_date:
        total_seconds = int((end_date - start_date).total_seconds())
        minutes, seconds = divmod(total_seconds, 60)
        duration = f"{minutes}m{seconds}s"

    return [
        f" STATUS: {overall_status}",
        f" TASKS: {total} total, {success_count} success, {failed_count} failed, {skipped_count} skipped, {other_count} other",
        f" DURATION: {duration}",
    ]


def _build_header(dag_id: str, run_id: str, generated_at: datetime, summary_lines: list) -> list:
    lines = [
        _divider("#"),
        _divider("#"),
        _divider("="),
        " PIPELINE RUN SUMMARY & AUDIT TRAIL",
        _divider("="),
        f" DAG ID: {dag_id}",
        f" RUN ID: {run_id}",
        f" GENERATED AT: {generated_at.strftime('%Y-%m-%d %H:%M:%S')} (GMT-3)",
        _divider("-"),
    ]
    lines.extend(summary_lines)
    lines.append(_divider("="))
    return lines


def _collect_task_logs(dag_id: str, run_id: str, ts_nodash: str) -> list:
    dag_log_path = os.path.join(AIRFLOW_LOG_DIR, f"dag_id={dag_id}")
    all_logs = glob(os.path.join(dag_log_path, "**", "*.log"), recursive=True)
    matching_logs = [f for f in all_logs if run_id in f or (ts_nodash and ts_nodash in f)]
    return sorted(matching_logs, key=os.path.getmtime)


def consolidate_dag_audit_logs(context: dict = None, **kwargs):
    context = context if context is not None else kwargs
    dag_run = context.get("dag_run")
    if not dag_run:
        logger.warning("dag_run not found in context, skipping audit consolidation.")
        return

    dag_id = dag_run.dag_id
    run_id = context.get("run_id")
    logical_date = context.get("logical_date") or context.get("execution_date")
    execution_ts = _format_local_ts(logical_date)
    generated_at = datetime.now(ZoneInfo("UTC")).astimezone(AUDIT_TZ)

    summary_lines = _build_run_summary(context)
    header_lines = _build_header(dag_id, run_id, generated_at, summary_lines)

    for line in header_lines:
        logger.info(line)

    dag_audit_dir = os.path.join(AUDIT_LOG_DIR, dag_id)
    os.makedirs(dag_audit_dir, exist_ok=True)
    target_audit_path = os.path.join(dag_audit_dir, f"log_{dag_id}_{execution_ts}.log")

    ts_nodash = context.get("ts_nodash", "")
    run_logs = _collect_task_logs(dag_id, run_id, ts_nodash)

    with open(target_audit_path, "w", encoding="utf-8") as audit_file:
        audit_file.write("\n".join(header_lines) + "\n\n")

        if not run_logs:
            audit_file.write("[WARN] no task log was found\n")

        for log_path in run_logs:
            task_match = re.search(r"task_id=([^/]+)", log_path)
            task_id = task_match.group(1) if task_match else "unknown-task"
            audit_file.write("\n" + _divider("#") + "\n")
            audit_file.write(f"--- TASK EXECUTION: {task_id} ---\n")
            audit_file.write(_divider("#") + "\n\n")
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    audit_file.write(f.read())
            except Exception as e:
                audit_file.write(f"[ERROR] >>> Failed to read log for task {task_id}: {e}\n")

    logger.info(f"log deployed to local path >> {target_audit_path}")

    try:
        object_uri = upload_audit_log_to_minio(target_audit_path, dag_id, logical_date)
        upload_status_line = f"log deployed to s3 audit bucket >> {object_uri}"
        logger.info(upload_status_line)
    except Exception as e:
        upload_status_line = f"[ERROR] Failed to upload audit log to S3: {e}"
        logger.error(upload_status_line)

    with open(target_audit_path, "a", encoding="utf-8") as audit_file:
        audit_file.write("\n" + _divider("=") + "\n")
        audit_file.write(f" {upload_status_line}\n")
        audit_file.write(_divider("=") + "\n")

    closing_lines = [
        _divider("="),
        " PIPELINE RUN COMPLETE",
        _divider("="),
        _divider("#"),
        _divider("#")
    ]
    for line in closing_lines:
        logger.info(line)

    for handler in logger.handlers:
        try:
            handler.flush()
        except Exception:
            pass
    if LOG_FLUSH_DELAY_SECONDS > 0:
        time.sleep(LOG_FLUSH_DELAY_SECONDS)
