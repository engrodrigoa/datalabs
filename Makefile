# DataLabs — common tasks.  `make help`
SHELL := /bin/bash
COMPOSE := docker compose
AF := $(COMPOSE) exec -T airflow-scheduler

.PHONY: help preflight doctor creds models models-export models-import elementary env up up-ai up-gpu down clean ps logs demo demo-offline health dbt-deps dbt-parse db-bootstrap dashboards sample-data test lint urls nfe-quickstart nfe-demo nfe-stream nfe-backfill

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

env: ## create .env with random passwords (keeps an existing one, adds missing Airflow keys)
	@test -f .env && echo ".env already exists" || ( \
	  cp .env.example .env && \
	  for k in DB_PASSWORD GRAFANA_DB_PASSWORD AIRFLOW_ADMIN_PASSWORD MINIO_ROOT_PASSWORD GRAFANA_ADMIN_PASSWORD; do \
	    v=$$(openssl rand -hex 12); sed -i.bak "s|^$$k=.*|$$k=$$v|" .env; done && \
	  sed -i.bak "s|^MINIO_SECRET_KEY=.*|MINIO_SECRET_KEY=$$(grep ^MINIO_ROOT_PASSWORD= .env | cut -d= -f2)|" .env && \
	  sed -i.bak "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$$(id -u)|" .env && rm -f .env.bak && \
	  echo ".env created (passwords randomised)" && $(MAKE) --no-print-directory creds )
	@# Airflow keys: generated when missing or a placeholder, so an .env from before they existed gets them too.
	@# Never regenerated once set: a new FERNET_KEY cannot decrypt what the old one encrypted.
	@for k in AIRFLOW__WEBSERVER__SECRET_KEY AIRFLOW__CORE__FERNET_KEY; do \
	  cur=$$(grep -E "^$$k=" .env | cut -d= -f2-); \
	  case "$$cur" in ""|change-me*) \
	    if [ "$$k" = AIRFLOW__CORE__FERNET_KEY ]; then v=$$(openssl rand -base64 32 | tr '+/' '-_'); \
	    else v=$$(openssl rand -hex 32); fi; \
	    if grep -qE "^$$k=" .env; then sed -i.bak "s|^$$k=.*|$$k=$$v|" .env && rm -f .env.bak; \
	    else [ -n "$$(tail -c1 .env)" ] && echo >> .env; echo "$$k=$$v" >> .env; fi; \
	    echo "$$k generated in .env (recreate Airflow to apply: docker compose up -d)";; \
	  esac; done

creds: ## logins for THIS machine (from .env; passwords are random per machine)
	@test -f .env || { echo "no .env yet: make env"; exit 1; }
	@v() { grep -E "^$$1=" .env | cut -d= -f2-; }; \
	 echo ""; \
	 echo "  Airflow  http://localhost:8080  $$(v AIRFLOW_ADMIN_USER) / $$(v AIRFLOW_ADMIN_PASSWORD)"; \
	 echo "  Grafana  http://localhost:3000  $$(v GRAFANA_ADMIN_USER) / $$(v GRAFANA_ADMIN_PASSWORD)"; \
	 echo "  S3 (Silo) console  http://localhost:9091  $$(v MINIO_ROOT_USER) / $$(v MINIO_ROOT_PASSWORD)"; \
	 echo "  Your browser may autofill passwords saved for localhost on ANOTHER machine: ignore them."; \
	 echo ""

preflight: env ## check the machine before `make up` (Docker, memory, DNS/TLS from a container, pinned images)
	./scripts/preflight.sh

doctor: ## check a running stack for silent failures (empty bind mounts, missing models)
	./scripts/doctor.sh

up: env ## start core + observability stack
	$(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory urls

up-ai: env ## + Ollama (CPU) and the AI image (sentence-transformers)
	INSTALL_AI=true $(COMPOSE) --profile ai up -d --build

up-gpu: env ## + Ollama on an NVIDIA GPU
	INSTALL_AI=true $(COMPOSE) -f docker-compose.yml -f docker-compose.gpu.yml --profile ai up -d --build

down: ## stop everything (data is kept)
	$(COMPOSE) --profile ai --profile tools down

clean: ## stop and DELETE all local data (Docker volumes: Postgres, MinIO, models, Grafana...)
	$(COMPOSE) --profile ai --profile tools down -v
	rm -rf datalake airflow/logs/dag_id=* airflow/logs/scheduler   # datalake/: pre-ADR-0008 bind mounts

ps: ## container status / health
	$(COMPOSE) ps

logs: ## follow Airflow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

demo: ## run the ANP pipeline end to end (real data from gov.br/anp)
	$(AF) airflow dags unpause dag_setup_infrastructure
	$(AF) airflow dags unpause dag_dbt_anp
	$(AF) airflow dags unpause dag_observability_health
	$(AF) airflow dags unpause dag_rag_anp
	@# unpausing already schedules the latest interval: trigger only if no run shows up (no duplicate run)
	@for i in 1 2 3 4 5 6; do \
	  $(AF) airflow dags list-runs -d dag_dbt_anp -o plain 2>/dev/null | grep -q dag_dbt_anp && break; sleep 5; done; \
	 if $(AF) airflow dags list-runs -d dag_dbt_anp -o plain 2>/dev/null | grep -q dag_dbt_anp; then \
	   echo "dag_dbt_anp already has a run (scheduled on unpause): not triggering a duplicate"; \
	 else $(AF) airflow dags trigger dag_dbt_anp; fi
	@echo "follow it at http://localhost:8080  ·  results at http://localhost:3000"

DBT := cd /opt/airflow/pipelines/dbt_projects && /opt/airflow/dbt_venv/bin/dbt
demo-offline: sample-data ## no internet: synthetic ANP files -> landing -> dbt -> health checks
	$(AF) python -m pipelines.observability.run anp /opt/airflow/pipelines/anp/anp_03_anp_landing_semanal.py --step landing_semanal
	$(AF) python -m pipelines.observability.run anp /opt/airflow/pipelines/anp/anp_04_anp_landing_mensal.py --step landing_mensal
	$(AF) bash -c "$(DBT) run --select elementary --profiles-dir . && $(DBT) build --select tag:anp --profiles-dir ."
	$(AF) python -m pipelines.observability.run obs pipelines.observability.checks --module --step health_checks_anp -- --pipeline anp --fail-on never

health: ## run all data health checks now
	$(AF) python -m pipelines.observability.run obs pipelines.observability.checks --module --step health_checks_manual -- --pipeline all --fail-on never

dbt-deps: ## install/refresh dbt packages (only when packages.yml changes; DAGs never run `dbt deps`)
	docker compose exec -T airflow-scheduler /opt/airflow/dbt_venv/bin/dbt deps --project-dir /opt/airflow/pipelines/dbt_projects --profiles-dir /opt/airflow/pipelines/dbt_projects

dbt-parse: ## regenerate the dbt manifest Cosmos reads (run after adding/renaming dbt models or tags)
	docker compose exec -T airflow-scheduler /opt/airflow/dbt_venv/bin/dbt parse --project-dir /opt/airflow/pipelines/dbt_projects --profiles-dir /opt/airflow/pipelines/dbt_projects --target-path /opt/airflow/dbt_vol/manifest

db-bootstrap: ## (existing volume) create the airflow DB, grafana_ro role and apply DDL
	$(COMPOSE) exec -T postgres bash /docker-entrypoint-initdb.d/00_bootstrap.sh

dashboards: ## regenerate Grafana dashboards from grafana/dashboards-src/build.py
	python grafana/dashboards-src/build.py

elementary: ## create/refresh the Elementary tables (airflow-init does it once on an empty database)
	$(AF) bash -c "$(DBT) run --select elementary --profiles-dir ."

models: ## download the embedding model into the hf_cache volume now (fails early instead of inside the RAG DAG)
	$(AF) python -c "from pipelines.commons.ai.embeddings import MODEL_NAME, load_model; load_model(); print('cached:', MODEL_NAME)"

models-export: ## pack the cached models (hf_cache volume) into ./hf_cache.tgz, to carry to a restricted network
	docker run --rm -v datalabs_hf_cache:/c:ro -v "$(CURDIR)":/out alpine:3.20 tar czf /out/hf_cache.tgz -C /c .
	@ls -lh hf_cache.tgz

models-import: ## restore ./hf_cache.tgz into the hf_cache volume (then set HF_HUB_OFFLINE=1 in .env)
	@test -f hf_cache.tgz || { echo "hf_cache.tgz not found (make models-export on a machine with access)"; exit 1; }
	docker volume create datalabs_hf_cache >/dev/null
	docker run --rm -v datalabs_hf_cache:/c -v "$(CURDIR)":/in:ro alpine:3.20 sh -c "tar xzf /in/hf_cache.tgz -C /c && chown -R $$(id -u):0 /c"

sample-data: ## offline synthetic ANP files (no internet needed)
	$(AF) python /opt/airflow/scripts/generate_sample_anp.py /mnt/datasource/anp

nfe-quickstart: up ## NF-e do zero: tabelas Elementary, 2000 notas, despausa e dispara o dag_nfe
	$(AF) bash -c "$(DBT) run --select elementary --profiles-dir ."
	$(AF) python /opt/airflow/scripts/nfe/gen_nfe.py --inbox /mnt/datasource/nfe/inbox --n 2000 \
	  --data-ini 2026-01-01 --data-fim 2026-09-30 --defect-rate 0.02 --dup-rate 0.01 --corrupt-rate 0.005
	@echo "aguardando o Airflow registrar o dag_nfe..."
	@until $(AF) airflow dags unpause dag_nfe >/dev/null 2>&1; do sleep 5; done
	$(AF) airflow dags trigger dag_nfe
	@echo ""
	@echo "  acompanhe:  http://localhost:8080/dags/dag_nfe/grid"
	@echo "  resultado:  http://localhost:3000/d/datalabs-nfe   (DataLabs · NF-e)"

nfe-demo: ## NF-e: simula a chegada de 500 notas na inbox (o dag_nfe processa em até 10 min)
	$(AF) python /opt/airflow/scripts/nfe/gen_nfe.py --inbox /mnt/datasource/nfe/inbox --n 500 \
	  --defect-rate 0.02 --dup-rate 0.01 --corrupt-rate 0.005

nfe-stream: ## NF-e: chegada contínua (12 lotes de 200 notas, um a cada 5 min)
	$(AF) python /opt/airflow/scripts/nfe/gen_nfe.py --inbox /mnt/datasource/nfe/inbox --n 200 \
	  --lotes 12 --intervalo 300 --defect-rate 0.02 --dup-rate 0.01

nfe-backfill: ## NF-e: histórico jan-set/2026 (5000 notas) para popular o DW
	$(AF) python /opt/airflow/scripts/nfe/gen_nfe.py --inbox /mnt/datasource/nfe/inbox --n 5000 \
	  --data-ini 2026-01-01 --data-fim 2026-09-30 --defect-rate 0.02 --dup-rate 0.01 --corrupt-rate 0.005

test: ## unit tests
	pytest

lint: ## ruff
	ruff check .

urls:
	@echo ""
	@echo "  Airflow     http://localhost:8080"
	@echo "  Grafana     http://localhost:3000   (DataLabs · Pipeline Health · NF-e: /d/datalabs-nfe)"
	@echo "  S3 (Silo)   http://localhost:9091"
	@echo "  Prometheus  http://localhost:9090"
	@echo "  logins: make creds"
