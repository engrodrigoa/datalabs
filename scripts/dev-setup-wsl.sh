#!/usr/bin/env bash
# One-time, idempotent setup of a WSL (Ubuntu) dev environment for datalabs on a Windows machine.
# Run it INSIDE WSL. Re-running is safe: every step checks before it changes anything.
#
#   bash dev-setup-wsl.sh                                    # fresh machine
#   WIN_CLONE=/mnt/c/Users/me/datalabs bash dev-setup-wsl.sh # moving from a clone on the Windows disk
#
# WIN_CLONE carries over .env and datasource/. The .env matters: the Docker volumes (Postgres, Grafana,
# model cache) are named after the compose project (`name: datalabs`), not the folder, so the new clone
# reuses them, and Postgres only accepts the passwords it was created with.
set -euo pipefail

REPO_URL=${REPO_URL:-https://github.com/engrodrigoa/datalabs.git}
DEST=${DEST:-$HOME/datalabs}
WIN_CLONE=${WIN_CLONE:-}
GCM="/mnt/c/Program Files/Git/mingw64/bin/git-credential-manager.exe"

step() { printf '\n== %s\n' "$*"; }
ok()   { printf '  ok    %s\n' "$*"; }
warn() { printf '  WARN  %s\n' "$*"; }
fail() { printf '  FAIL  %s\n' "$*" >&2; exit 1; }

step "1/6 environment"
grep -qi microsoft /proc/version 2>/dev/null || fail "not WSL: open Ubuntu (wsl -d Ubuntu) and run it there, not in Git Bash or PowerShell"
# shellcheck disable=SC1091
ok "running in WSL ($(. /etc/os-release && echo "$PRETTY_NAME"))"
case "$DEST" in /mnt/*) fail "DEST=$DEST is on the Windows disk: keep the repo in the Linux filesystem (~/datalabs)";; esac

step "2/6 system packages"
pkgs=(make git python3-venv python3-pip poppler-utils openssl curl ca-certificates rsync)
missing=()
for p in "${pkgs[@]}"; do dpkg -s "$p" >/dev/null 2>&1 || missing+=("$p"); done
if ((${#missing[@]})); then
  echo "  installing: ${missing[*]}"
  sudo apt-get update -qq && sudo apt-get install -y -qq "${missing[@]}"
fi
ok "make $(make --version | head -1 | awk '{print $3}'), python $(python3 --version | awk '{print $2}'), pdftotext present"

step "3/6 git"
git config --global core.autocrlf input            # LF in the repo, whatever the editor writes
if git config --global credential.helper >/dev/null; then
  ok "credential helper already set"
elif [[ -x "$GCM" ]]; then
  git config --global credential.helper "$GCM"     # reuse the GitHub login of Git for Windows
  ok "credentials via Git Credential Manager (Windows)"
else
  warn "Git for Windows not found: authenticate with 'gh auth login' or an SSH key"
fi
git config --global user.name  >/dev/null || warn "git user.name not set:  git config --global user.name  'Your Name'"
git config --global user.email >/dev/null || warn "git user.email not set: git config --global user.email you@example.com"

step "4/6 docker from WSL"
command -v docker >/dev/null || fail "docker not found: Docker Desktop > Settings > Resources > WSL integration > enable this distro"
docker info >/dev/null 2>&1 || fail "docker not reachable: start Docker Desktop (and check the WSL integration toggle)"
ok "docker $(docker version --format '{{.Server.Version}}' 2>/dev/null) reachable"

step "5/6 repository in the Linux filesystem"
if [[ -d "$DEST/.git" ]]; then
  ok "repo already at $DEST"
else
  git clone -q "$REPO_URL" "$DEST" && ok "cloned into $DEST"
fi
if [[ -n "$WIN_CLONE" ]]; then
  [[ -d "$WIN_CLONE" ]] || fail "WIN_CLONE=$WIN_CLONE not found"
  if [[ -f "$WIN_CLONE/.env" && ! -f "$DEST/.env" ]]; then
    tr -d '\r' < "$WIN_CLONE/.env" > "$DEST/.env" && chmod 600 "$DEST/.env"   # CRLF from Windows editors
    ok ".env carried over (same passwords as the existing Postgres volume)"
  fi
  if [[ -d "$WIN_CLONE/datasource" ]]; then
    rsync -a --ignore-existing "$WIN_CLONE/datasource/" "$DEST/datasource/"
    ok "datasource/ carried over ($(du -sh "$DEST/datasource" | cut -f1))"
  fi
  if git -C "$WIN_CLONE" status --porcelain 2>/dev/null | grep -q .; then
    warn "the Windows clone has uncommitted changes: commit/push them before deleting it"
  fi
fi
if [[ ! -f "$DEST/.env" ]]; then
  if docker volume ls -q | grep -qx datalabs_pg_data; then
    warn "a datalabs_pg_data volume exists but there is no .env: copy the old one (WIN_CLONE=...) or Postgres will reject new passwords"
  else
    ok "no .env yet: 'make preflight' creates one with random passwords"
  fi
fi

step "6/6 python venv for tests and lint"
[[ -d "$DEST/.venv" ]] || python3 -m venv "$DEST/.venv"
"$DEST/.venv/bin/pip" install -q --upgrade pip
"$DEST/.venv/bin/pip" install -q -r "$DEST/requirements-dev.txt"
ok "venv at $DEST/.venv ($("$DEST/.venv/bin/pytest" --version 2>&1 | head -1))"

cat <<EOF

Done. Next:
  cd $DEST && source .venv/bin/activate
  make preflight        # checks Docker memory, DNS/TLS, pinned images (creates .env if missing)
  make up               # or make up-ai for the RAG profile
  make test
  code .                # VS Code with the WSL extension edits the Linux files directly
Patches from the Windows Downloads folder: git am /mnt/c/Users/<you>/Downloads/<file>.patch
EOF
