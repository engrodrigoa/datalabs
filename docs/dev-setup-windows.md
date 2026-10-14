# Dev machine on Windows: WSL setup

The stack runs on Docker Desktop, and the repo lives **inside WSL** (`~/datalabs`), not on the Windows disk.
On `C:\` the bind mounts are slow and can come back empty after `wsl --shutdown`, and shell scripts pick up
CRLF line endings ([ADR 0008](adr/0008-stateful-services-in-docker-volumes.md), README troubleshooting).
Inside WSL you also get `make`, `apt` and the same Linux tools the CI uses.

## 1. Windows side (once)

PowerShell **as administrator**:

```powershell
wsl --install -d Ubuntu-24.04      # reboot if asked, then create the Linux user and password
```

Size the WSL VM so the stack does not take all the RAM: copy [`wslconfig.example`](wslconfig.example) to
`%USERPROFILE%\.wslconfig`, then `wsl --shutdown`.

Docker Desktop: *Settings → General → Use the WSL 2 based engine*, and
*Settings → Resources → WSL integration → enable Ubuntu-24.04*. Apply & restart.

## 2. Inside WSL (once, re-runnable)

Open Ubuntu (`wsl -d Ubuntu-24.04`) and run [`scripts/dev-setup-wsl.sh`](../scripts/dev-setup-wsl.sh).
If you already have a clone on the Windows disk, point `WIN_CLONE` to it so `.env` and `datasource/` come along:

```bash
WIN_CLONE=/mnt/c/Users/<you>/<path>/datalabs bash /mnt/c/Users/<you>/Downloads/dev-setup-wsl.sh
```

| Step | What it does |
|---|---|
| 1 | refuses to run outside WSL, or with the repo under `/mnt/` |
| 2 | `make`, `git`, `python3-venv`, `poppler-utils`, `rsync`... only the missing ones |
| 3 | `core.autocrlf=input`; reuses the GitHub login of Git for Windows (Git Credential Manager) |
| 4 | checks that Docker Desktop answers from WSL (the integration toggle above) |
| 5 | clones into `~/datalabs`; with `WIN_CLONE`, copies `.env` (CRLF removed, mode 600) and `datasource/`, and warns about uncommitted changes in the old clone |
| 6 | `.venv` with `requirements-dev.txt` (pytest, ruff) |

**Why the old `.env` matters.** The compose project is `name: datalabs`, so the Docker volumes (`datalabs_pg_data`,
Grafana, model cache) keep their names whatever folder the repo is in: the new clone reuses your data. Postgres
only accepts the passwords it was created with, so a new `.env` would fail with
`password authentication failed for user "datalab"`.

## 3. Daily use

```bash
cd ~/datalabs && source .venv/bin/activate
make up            # make up-ai for the RAG profile
make test
code .             # VS Code + WSL extension: edits the Linux files directly
git am /mnt/c/Users/<you>/Downloads/<file>.patch
```

The old clone on `C:\` can be deleted once `git -C <old clone> status` is clean and every branch is pushed.
