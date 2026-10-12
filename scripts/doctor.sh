#!/usr/bin/env bash
# DataLabs doctor: checks a RUNNING stack for failures that do not raise any error by themselves.
#
#   ./scripts/doctor.sh        (make doctor)
#
# Found on a fresh machine: after `wsl --shutdown`, containers restarted by their restart policy can
# mount Windows-path bind mounts EMPTY. Grafana then shows an empty "DataLabs" folder and Promtail
# ships no logs, with every container "Up" and nothing in any log. Exit 1 when any check FAILs.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

FAILS=0
ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; [ -n "${2:-}" ] && printf '        -> %s\n' "$2"; FAILS=$((FAILS+1)); }
info() { printf '  \033[36mINFO\033[0m  %s\n' "$1"; }

running() { [ -n "$(docker compose ps -q --status running "$1" 2>/dev/null)" ]; }
RECREATE="docker compose up -d --force-recreate grafana promtail prometheus   (bind mounts came up empty)"

echo "Containers"
unhealthy=$(docker compose ps --format '{{.Service}} {{.Health}}' 2>/dev/null | awk '$2=="unhealthy"{print $1}')
if [ -n "$unhealthy" ]; then fail "unhealthy: $unhealthy" "docker compose logs <service>"; else ok "no unhealthy container"; fi
init_exit=$(docker inspect -f '{{.State.ExitCode}}' airflow_init 2>/dev/null || echo "?")
if [ "$init_exit" = "0" ]; then ok "airflow-init exited 0"; else fail "airflow-init exit code: $init_exit" "docker compose logs airflow-init"; fi

echo "Bind mounts (silent failures)"
if running grafana; then
  n=$(docker compose exec -T grafana sh -c 'ls /var/lib/grafana/dashboards/*.json 2>/dev/null | wc -l' | tr -d '[:space:]')
  if [ "${n:-0}" -gt 0 ]; then ok "Grafana sees $n dashboard file(s)"; else fail "Grafana dashboards folder is EMPTY inside the container" "$RECREATE"; fi
else fail "grafana is not running"; fi

if running promtail; then
  n=$(docker compose exec -T promtail sh -c 'find /opt/airflow/logs -name "*.log" 2>/dev/null | head -1 | wc -l' | tr -d '[:space:]')
  if [ "${n:-0}" -gt 0 ]; then ok "Promtail sees Airflow log files"
  else fail "Promtail sees NO Airflow log file (logs are not reaching Loki)" "$RECREATE  — or no DAG has run yet"; fi
else fail "promtail is not running"; fi

if running airflow-scheduler; then
  n=$(docker compose exec -T airflow-scheduler sh -c 'ls /opt/airflow/dags/*.py 2>/dev/null | wc -l' | tr -d '[:space:]')
  if [ "${n:-0}" -gt 0 ]; then ok "Airflow sees $n DAG file(s)"; else fail "Airflow dags folder is EMPTY" "docker compose up -d --force-recreate airflow-scheduler airflow-webserver"; fi

  echo "AI profile"
  if docker compose exec -T airflow-scheduler python -c "import importlib.util,sys; sys.exit(importlib.util.find_spec('sentence_transformers') is None)" 2>/dev/null; then
    if docker compose exec -T airflow-scheduler python -c "
import sys
from huggingface_hub import try_to_load_from_cache
from pipelines.commons.ai.embeddings import MODEL_NAME
sys.exit(not isinstance(try_to_load_from_cache(MODEL_NAME, 'config.json'), str))" 2>/dev/null; then
      ok "embedding model cached (hf_cache volume)"
    else fail "embedding model not downloaded yet" "make models  (or make models-import on a restricted network)"; fi
    if running ollama; then
      if docker compose exec -T ollama ollama list 2>/dev/null | grep -q .; then ok "Ollama has a model"
      else fail "Ollama has no model (the interactive assistant needs it; the RAG DAG does not)" "docker compose logs ollama-pull"; fi
    fi
  else info "base image (no INSTALL_AI): RAG DAG and checks are skipped by design"; fi
else fail "airflow-scheduler is not running"; fi

echo
if [ "$FAILS" -gt 0 ]; then echo "Doctor: $FAILS problem(s)."; exit 1; fi
echo "Doctor: OK."
