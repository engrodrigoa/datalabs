# DataLabs — common tasks.  `make help`
SHELL := /bin/bash
COMPOSE := docker compose
AF := $(COMPOSE) exec -T airflow-scheduler

.PHONY: help env up up-ai up-gpu down clean ps logs demo demo-offline health db-bootstrap dashboards sample-data test lint urls

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

env: ## create .env with random passwords (keeps an existing one)
	@test -f .env && echo ".env already exists" || ( \
	  cp .env.example .env && \
	  for k in DB_PASSWORD GRAFANA_DB_PASSWORD AIRFLOW_ADMIN_PASSWORD MINIO_ROOT_PASSWORD GRAFANA_ADMIN_PASSWORD; do \
	    v=$$(openssl rand -hex 12); sed -i.bak "s|^$$k=.*|$$k=$$v|" .env; done && \
	  sed -i.bak "s|^MINIO_SECRET_KEY=.*|MINIO_SECRET_KEY=$$(grep ^MINIO_ROOT_PASSWORD= .env | cut -d= -f2)|" .env && \
	  sed -i.bak "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$$(id -u)|" .env && rm -f .env.bak && \
	  echo ".env created (passwords randomised)" )

up: env ## start core + observability stack
	$(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory urls

up-ai: env ## + Ollama (CPU) and the AI image (sentence-transformers)
	INSTALL_AI=true $(COMPOSE) --profile ai up -d --build

up-gpu: env ## + Ollama on an NVIDIA GPU
	INSTALL_AI=true $(COMPOSE) -f docker-compose.yml -f docker-compose.gpu.yml --profile ai up -d --build

down: ## stop everything (data is kept)
	$(COMPOSE) --profile ai --profile tools down

clean: ## stop and DELETE all local data (volumes + ./datalake)
	$(COMPOSE) --profile ai --profile tools down -v
	rm -rf datalake airflow/logs/dag_id=* airflow/logs/scheduler

ps: ## container status / health
	$(COMPOSE) ps

logs: ## follow Airflow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

demo: ## run the ANP pipeline end to end (real data from gov.br/anp)
	$(AF) airflow dags unpause dag_setup_infrastructure
	$(AF) airflow dags unpause dag_dbt_anp
	$(AF) airflow dags unpause dag_observability_health
	$(AF) airflow dags unpause dag_rag_anp
	$(AF) airflow dags trigger dag_dbt_anp
	@echo "follow it at http://localhost:8080  ·  results at http://localhost:3000"

DBT := cd /opt/airflow/pipelines/dbt_projects && /opt/airflow/dbt_venv/bin/dbt
demo-offline: sample-data ## no internet: synthetic ANP files -> landing -> dbt -> health checks
	$(AF) python -m pipelines.observability.run anp /opt/airflow/pipelines/anp/anp_03_anp_landing_semanal.py --step landing_semanal
	$(AF) python -m pipelines.observability.run anp /opt/airflow/pipelines/anp/anp_04_anp_landing_mensal.py --step landing_mensal
	$(AF) bash -c "$(DBT) run --select elementary --profiles-dir . && $(DBT) build --select tag:anp --profiles-dir ."
	$(AF) python -m pipelines.observability.run obs pipelines.observability.checks --module --step health_checks_anp -- --pipeline anp --fail-on never

health: ## run all data health checks now
	$(AF) python -m pipelines.observability.run obs pipelines.observability.checks --module --step health_checks_manual -- --pipeline all --fail-on never

db-bootstrap: ## (existing volume) create the airflow DB, grafana_ro role and apply DDL
	$(COMPOSE) exec -T postgres bash /docker-entrypoint-initdb.d/00_bootstrap.sh

dashboards: ## regenerate Grafana dashboards from grafana/dashboards-src/build.py
	python grafana/dashboards-src/build.py

sample-data: ## offline synthetic ANP files (no internet needed)
	$(AF) python /opt/airflow/scripts/generate_sample_anp.py /mnt/datasource/anp

test: ## unit tests
	pytest

lint: ## ruff
	ruff check .

urls:
	@echo ""
	@echo "  Airflow     http://localhost:8080"
	@echo "  Grafana     http://localhost:3000   (DataLabs · Pipeline Health)"
	@echo "  MinIO       http://localhost:9091"
	@echo "  Prometheus  http://localhost:9090"
	@echo "  credentials: .env"
