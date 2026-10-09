"""
Observability runner: executes any pipeline script and records it as a step run.

    python -m pipelines.observability.run <pipeline> <script.py> [--step NAME] [-- script args...]

What it adds, with zero changes to the script:
  * audit.pipeline_step_runs row (running -> success/skipped/failed, duration, exit code, error)
  * structured `step_started` / `step_finished` events (Loki)
  * alert on partial loads (Telegram / Slack / Discord / generic webhook, if configured);
    hard failures are alerted by the Airflow callbacks after the last retry
  * exit code is preserved, so Airflow semantics do not change (99 = skipped, !=0 = failed)

Scripts can enrich the run with volumes via `current_step().add(rows_out=...)`.
"""
from __future__ import annotations

import argparse
import os
import runpy
import signal
import sys
import traceback

from pipelines.observability.alerting import send_alert
from pipelines.observability.tracker import StepRun, _set_current


class _Terminated(BaseException):
    """Raised inside the step when Airflow (execution_timeout, manual stop) sends SIGTERM."""


def _on_sigterm(signum, _frame):
    raise _Terminated(signum)


def _exit_code(exc: SystemExit) -> int:
    code = exc.code
    if code is None:
        return 0
    if isinstance(code, int):
        return code
    print(code, file=sys.stderr)  # sys.exit("message") semantics
    return 1


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    script_args: list[str] = []
    if "--" in argv:
        idx = argv.index("--")
        argv, script_args = argv[:idx], argv[idx + 1:]

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pipeline", help="logical pipeline name: anp | rfb | rag | obs ...")
    parser.add_argument("script", help="path to the python script (or module with -m semantics if --module)")
    parser.add_argument("--step", help="step name (default: Airflow task_id or script file name)")
    parser.add_argument("--module", action="store_true", help="treat `script` as a python module path")
    args = parser.parse_args(argv)

    default_name = args.script.rsplit(".", 1)[-1] if args.module else os.path.splitext(os.path.basename(args.script))[0]
    step_name = args.step or os.getenv("AIRFLOW_CTX_TASK_ID") or default_name
    step = StepRun(pipeline=args.pipeline, step=step_name, script=args.script)
    _set_current(step)
    step.start()

    exit_code, error = 0, None
    sys.argv = [args.script, *script_args]
    # Without this, a SIGTERM kills the process before finish(): the audit row stays "running" forever
    signal.signal(signal.SIGTERM, _on_sigterm)
    try:
        if args.module:
            runpy.run_module(args.script, run_name="__main__", alter_sys=True)
        else:
            runpy.run_path(args.script, run_name="__main__")
    except SystemExit as exc:
        exit_code = _exit_code(exc)
        if exit_code not in (0, 99):
            error = f"script exited with code {exit_code}"
    except _Terminated:
        exit_code = 128 + signal.SIGTERM
        error = "terminated by SIGTERM (Airflow execution_timeout or manual stop)"
    except BaseException:  # noqa: BLE001 - we must record *anything* that kills the step
        exit_code = 1
        error = traceback.format_exc()
        traceback.print_exc()

    step.finish(exit_code=exit_code, error=error)

    # Hard failures are alerted once, by the Airflow on_failure_callback (after retries).
    # Here we only alert what Airflow cannot see: a step that "succeeded" with partial data.
    if step.status == "success" and step.counters.get("files_failed"):
        send_alert(
            title=f"Partial load: {args.pipeline}.{step_name}",
            message=f"{step.counters.get('files_failed')} file(s) failed, {step.counters.get('files_ok', 0)} ok",
            severity="warning",
            fields={"dag_id": step.dag_id, "run_id": step.dag_run_id},
        )
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
