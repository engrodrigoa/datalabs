#!/usr/bin/env bash
# DataLabs preflight: checks the machine BEFORE `make up`, so a fresh clone fails early with the cause
# and the fix, instead of failing 10 minutes later inside a build or a DAG.
#
#   ./scripts/preflight.sh                 # everything (host, network from a container, images)
#   ./scripts/preflight.sh --images-only   # only "do all pinned images still exist?" (weekly CI job)
#   ./scripts/preflight.sh --ai            # also the AI downloads (PyTorch wheels, Hugging Face, Ollama)
#
# Every check found here was a real failure on a fresh machine (see README, "Fresh machine"):
# a deleted image, an ISP DNS that hijacks files.pythonhosted.org, corporate TLS inspection,
# a clone under /mnt/c in WSL, an unbounded WSL VM. Exit 1 when any check FAILs.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

MODE="all"; AI=0
for a in "$@"; do
  case "$a" in
    --images-only) MODE="images" ;;
    --ai) AI=1 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
  esac
done

FAILS=0; WARNS=0
ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$1"; [ -n "${2:-}" ] && printf '        -> %s\n' "$2"; WARNS=$((WARNS+1)); }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; [ -n "${2:-}" ] && printf '        -> %s\n' "$2"; FAILS=$((FAILS+1)); }

# docker-compose.yml references .env (env_file), so it must exist: `make env` creates it with random passwords
if [ ! -f .env ]; then
  echo "No .env yet: run 'make env' first (it generates random passwords), or 'cp .env.example .env' in CI."
  exit 1
fi
compose_cfg() { docker compose "$@"; }

# ------------------------------------------------------------------ images
check_images() {
  echo "Pinned images (do they still exist in their registries?)"
  local imgs missing=0
  imgs=$(compose_cfg --profile ai config --images 2>/dev/null | grep -v '^datalabs/' | sort -u)
  if [ -z "$imgs" ]; then fail "could not render docker-compose.yml" "run: docker compose config"; return; fi
  local out
  for img in $imgs; do
    if out=$(docker manifest inspect "$img" 2>&1); then ok "$img"
    elif grep -qiE "no such manifest|manifest unknown|not found|requested access to the resource is denied|unauthorized" <<< "$out"; then
      fail "$img no longer exists in its registry" "deleted/renamed upstream (it happened to minio/minio, see ADR 0004)"; missing=1
    else
      warn "$img: registry not reachable from here" "$(head -c 160 <<< "$out")"
    fi
  done
  return $missing
}

if [ "$MODE" = "images" ]; then
  check_images
  echo
  if [ "$FAILS" -gt 0 ]; then echo "$FAILS image(s) no longer exist upstream."; exit 1; fi
  if [ "$WARNS" -gt 0 ]; then echo "$WARNS image(s) could not be verified (registry unreachable)."; exit 1; fi
  echo "All pinned images available."
fi

# ------------------------------------------------------------------ host
echo "Host"
if ! command -v docker >/dev/null 2>&1; then fail "docker not found" "install Docker Desktop (Windows/macOS) or Docker Engine"; exit 1; fi
if ! docker info >/dev/null 2>&1; then fail "docker daemon not reachable" "start Docker Desktop"; exit 1; fi
ok "docker $(docker version --format '{{.Server.Version}}' 2>/dev/null)"
if docker compose version >/dev/null 2>&1; then ok "compose $(docker compose version --short)"
else fail "docker compose v2 not found" "update Docker; 'docker-compose' (v1) is not supported"; fi

mem_b=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)
mem_gb=$(awk -v b="$mem_b" 'BEGIN {printf "%.1f", b/1024/1024/1024}')
# measured: the base stack runs in a 6 GB WSL VM; the AI profile (torch + Ollama) needs 8 GB (Docker reports ~7.6)
need=5.5; [ "$AI" -eq 1 ] && need=7.5
if awk -v m="$mem_gb" -v n="$need" 'BEGIN {exit !(m >= n)}'; then ok "Docker memory: ${mem_gb} GB"
else warn "Docker memory: ${mem_gb} GB (below the ${need} GB this profile needs)" \
  "WSL2: raise 'memory=' in %USERPROFILE%\\.wslconfig (see docs/wslconfig.example), then 'wsl --shutdown'"; fi

free_gb=$(df -Pk . | awk 'NR==2 {print int($4/1024/1024)}')
if [ "${free_gb:-0}" -ge 15 ]; then ok "free disk: ${free_gb} GB"
else warn "free disk: ${free_gb} GB (< 15 GB)" "images + volumes need ~10 GB; the RFB load needs much more"; fi

if grep -qi microsoft /proc/version 2>/dev/null && [[ "$PWD" == /mnt/* ]]; then
  warn "repository is on the Windows drive ($PWD)" \
    "clone inside WSL (e.g. ~/datalabs): /mnt/c bind mounts are slow, break file permissions and can mount EMPTY after 'wsl --shutdown'"
else ok "repository path: $PWD"; fi

# Postgres applies DB_PASSWORD only when it CREATES pg_data. An .env made after the volume (new clone in another
# folder, deleted .env) leaves the database expecting the old password: airflow-init then fails with
# "password authentication failed for user". Compared by the .env birth time (edits keep it); skipped when the
# filesystem does not record it. `make doctor` does the definitive check on a running stack.
vol_created=$(docker volume inspect -f '{{.CreatedAt}}' datalabs_pg_data 2>/dev/null)
env_born=$(stat -c %W .env 2>/dev/null || echo 0)
if [ -z "$vol_created" ]; then ok "no pg_data volume yet: Postgres will take the passwords of this .env"
elif vol_s=$(date -d "$vol_created" +%s 2>/dev/null) && [ "${env_born:-0}" -gt $((vol_s + 60)) ]; then
  warn "this .env is newer than the pg_data volume ($vol_created)" \
    "Postgres keeps the passwords of the .env that created the volume: restore that .env, or (nothing to keep) docker compose down -v"
else ok "pg_data volume and .env (created $vol_created)"; fi

if [ -z "$(compose_cfg ps -q 2>/dev/null)" ]; then
  for port in 3000 5432 8080 9005 9091 9090 3100; do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      warn "port $port already in use" "another service (or an old stack) is listening; free it or change the mapping"
    fi
  done
  ok "ports checked (3000 5432 8080 9005 9091 9090 3100)"
fi

# ------------------------------------------------------------------ network, seen from a container (what builds and DAGs see)
echo "Network (from a container)"
CURL_IMG="curlimages/curl:8.10.1"
hosts="pypi.org files.pythonhosted.org raw.githubusercontent.com www.gov.br arquivos.receitafederal.gov.br"
[ "$AI" -eq 1 ] && hosts="$hosts download.pytorch.org huggingface.co cas-server.xethub.hf.co us.aws.cdn.hf.co registry.ollama.ai"
# one container, one line per host: exit code | resolved IP
results=$(docker run --rm --entrypoint sh "$CURL_IMG" -c '
  for h in '"$hosts"'; do
    ip=$(curl -s -o /dev/null --connect-timeout 10 -m 20 -w "%{remote_ip}" -I "https://$h/"); rc=$?
    echo "$h $rc ${ip:-none}"
  done' 2>/dev/null) || { fail "could not run $CURL_IMG" "is Docker Hub reachable?"; results=""; }

while read -r h rc ip; do
  [ -z "${h:-}" ] && continue
  if [[ "$ip" == 100.6[4-9].* || "$ip" == 100.[7-9][0-9].* || "$ip" == 100.1[01][0-9].* || "$ip" == 100.12[0-7].* ]]; then
    fail "$h resolves to $ip (carrier-grade NAT range): your DNS is redirecting it" \
      "set public DNS for Docker: Docker Desktop > Settings > Docker Engine > \"dns\": [\"1.1.1.1\", \"8.8.8.8\"]"
    continue
  fi
  case "$rc" in
    0)  ok "$h ($ip)" ;;
    60) fail "$h: certificate not trusted (TLS inspection by a corporate proxy/antivirus)" \
          "use another network; do NOT disable TLS verification. The AI downloads can be imported offline (make models-import)" ;;
    35) fail "$h: TLS handshake cut ($ip)" "DNS or SNI filtering on this network: try public DNS (see above) or another network" ;;
    6)  fail "$h: DNS resolution failed" "check the machine/Docker DNS" ;;
    28) fail "$h: timeout" "firewall or proxy blocking the host" ;;
    *)  warn "$h: curl exit $rc ($ip)" "see 'curl --help' exit codes; the build or DAG that needs it may fail" ;;
  esac
done <<< "$results"

# ------------------------------------------------------------------ images
check_images

echo
if [ "$FAILS" -gt 0 ]; then echo "Preflight: $FAILS FAIL, $WARNS WARN. Fix the FAILs before 'make up'."; exit 1; fi
echo "Preflight: OK ($WARNS warning(s))."
