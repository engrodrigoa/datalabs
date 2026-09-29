# DataLabs — Data Engineering & Observability Lab

A local data engineering environment focused on **pipeline orchestration, data lake storage, centralized logging, and operational observability**.

The project was built as a hands-on laboratory to design, run, monitor, and troubleshoot data pipelines using open-source technologies commonly found in modern data platforms.

## What this project demonstrates

DataLabs provides a reproducible environment where data pipelines can be executed and their operational behavior can be observed through centralized logs and dashboards.

The stack currently includes:

* **Apache Airflow** — workflow orchestration
* **PostgreSQL** — relational database and Airflow metadata storage
* **MinIO** — S3-compatible object storage / data lake layer
* **Grafana** — observability dashboards
* **Loki** — centralized log aggregation
* **Promtail** — log collection and shipping
* **Docker Compose** — local infrastructure orchestration
* **Python / SQL** — pipeline and data processing logic

The main focus is not only moving data from one system to another, but also making pipeline execution **traceable, observable, and easier to troubleshoot**.

## Architecture

```text
                         ┌─────────────────┐
                         │    Airflow      │
                         │  Orchestration  │
                         └────────┬────────┘
                                  │
                         ┌────────▼────────┐
                         │    Pipelines    │
                         │   Python / SQL  │
                         └───────┬───┬─────┘
                                 │   │
                    ┌────────────┘   └─────────────┐
                    │                              │
             ┌──────▼──────┐                ┌──────▼──────┐
             │  PostgreSQL │                │    MinIO    │
             │   Database  │                │ S3 Storage  │
             └─────────────┘                └─────────────┘

                         Operational Logs
                                │
                         ┌──────▼──────┐
                         │  Promtail   │
                         │ Log Shipper  │
                         └──────┬──────┘
                                │
                         ┌──────▼──────┐
                         │    Loki     │
                         │ Log Storage  │
                         └──────┬──────┘
                                │
                         ┌──────▼──────┐
                         │   Grafana   │
                         │ Observability│
                         └─────────────┘
```

## Repository structure

```text
datalabs/
├── airflow/
│   ├── dags/
│   ├── plugins/
│   ├── logs/
│   └── audit_logs/
│
├── pipelines/
│   └── ...
│
├── grafana/
│   └── provisioning/
│
├── logs/
├── datasource/
├── datalake/
│
├── Dockerfile
├── docker-compose.yml
├── promtail-config.yaml
├── requirements.txt
└── .env.example
```

## Getting started

### Prerequisites

Install:

* Docker
* Docker Compose
* Git

No local Python, PostgreSQL, Airflow, MinIO, Loki, or Grafana installation is required. The main components run as Docker services.

### 1. Clone the repository

```bash
git clone https://github.com/engrodrigoa/datalabs.git
cd datalabs
```

### 2. Create the environment file

Copy the example environment file:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

The `.env` file contains the credentials and connection settings used by the local stack.

At minimum, configure:

```env
DB_USER=your_user
DB_PASSWORD=your_password
DB_DB=your_database
DB_PORT=5432

AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=your_password
AIRFLOW_ADMIN_EMAIL=admin@example.org

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=your_password

MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=your_password
MINIO_AUDIT_BUCKET=audit

GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=your_password
```

The repository provides `.env.example` as a template. **Do not commit your `.env` file or real credentials.**

### 3. Start the stack

```bash
docker compose up -d
```

The first startup may take longer because the Airflow image is built locally and the required services are initialized.

Check the running containers:

```bash
docker compose ps
```

### 4. Access the services

| Service       | URL                   | Purpose                   |
| ------------- | --------------------- | ------------------------- |
| Airflow       | http://localhost:8080 | Pipeline orchestration    |
| Grafana       | http://localhost:3000 | Observability dashboards  |
| Adminer       | http://localhost:8081 | PostgreSQL administration |
| MinIO API     | http://localhost:9005 | Object storage            |
| MinIO Console | http://localhost:9091 | Object storage management |
| Loki          | http://localhost:3100 | Log aggregation API       |

The Airflow and Grafana credentials are defined in `.env`.

## Observability workflow

The observability layer is designed around the following flow:

```text
Airflow
   │
   │ execution logs
   ▼
Promtail
   │
   │ log shipping
   ▼
Loki
   │
   │ queries
   ▼
Grafana
```

Airflow logs are collected by **Promtail** and sent to **Loki**, where they can be queried and explored through **Grafana**.

This makes it possible to investigate pipeline executions without relying exclusively on individual container logs.

The project therefore treats logs as an operational data source that can be centralized, searched, and visualized.

## Data engineering workflow

The environment also provides the infrastructure required to experiment with end-to-end data pipelines:

```text
Source
  │
  ▼
Airflow DAG
  │
  ├──► Python / SQL processing
  │
  ├──► PostgreSQL
  │
  └──► MinIO / S3
          │
          ▼
      Data Lake
```

The goal is to use the same environment for both **data pipeline development** and **pipeline operational monitoring**.

## Why this project exists

This repository is part of my Data Engineering portfolio and is used to experiment with production-oriented concepts in a controlled local environment.

The main areas of interest are:

* Data pipeline orchestration
* ETL / ELT workflows
* Data lake architecture
* Object storage
* Workflow monitoring
* Centralized logging
* Pipeline troubleshooting
* Reproducible infrastructure
* Containerized development environments

The project is intentionally practical: the infrastructure is designed to be started locally, modified, broken, monitored, and rebuilt.

## Current scope

The project is continuously evolving. Current development focuses on:

* Building and testing Airflow pipelines
* Storing data and artifacts in MinIO
* Persisting structured data in PostgreSQL
* Centralizing pipeline logs with Loki
* Creating operational dashboards in Grafana
* Improving pipeline traceability and failure analysis

## Author

**Rodrigo Arruda Carneiro**

Data / BI professional transitioning toward Data Engineering, with a focus on data pipelines, orchestration, distributed processing, and data platform engineering.

* GitHub: [https://github.com/engrodrigoa]
* LinkedIn: [https://www.linkedin.com/in/rodrigoarruda89]

---

> **Portfolio project:** built for experimentation, learning, and demonstrating practical Data Engineering skills.

---
