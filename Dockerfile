# syntax=docker/dockerfile:1.6
FROM apache/airflow:2.9.0-python3.12

ARG AIRFLOW_VERSION=2.9.0
ARG PYTHON_VERSION=3.12
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential git curl \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow

# Flaky networks (VPN, antivirus HTTPS scanning, Wi-Fi): retry harder instead of failing the build.
# TLS verification stays on.
ENV PIP_RETRIES=10 \
    PIP_DEFAULT_TIMEOUT=60

# Pipeline dependencies, resolved against Airflow's official constraints so they can
# never upgrade/downgrade a package Airflow depends on (e.g. SQLAlchemy).
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /opt/airflow/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt + Elementary in an isolated venv (their pins conflict with Airflow's)
# g+rwX: containers run as an arbitrary UID with GID 0 (AIRFLOW_UID, Airflow image convention), and
# `edr report` installs its internal dbt package INSIDE site-packages at runtime -> needs group write
COPY requirements-dbt.txt /opt/airflow/requirements-dbt.txt
RUN python -m venv /opt/airflow/dbt_venv \
  && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /opt/airflow/requirements-dbt.txt \
  && chmod -R g+rwX /opt/airflow/dbt_venv

# Optional AI stack (RAG): CPU wheels only, no CUDA download.
# INSTALL_AI=true adds sentence-transformers (CPU-only torch, ~1 GB). Default image stays lean.
# The ARG is declared HERE, not at the top: a changed build arg invalidates the cache of every RUN
# after its declaration, so it is declared last: toggling it only rebuilds this layer.
# Every package already in the image is frozen as a constraint, so the AI install can ADD packages
# but never upgrade one the base depends on (e.g. numpy 2.x breaking pyarrow/pandas built for 1.x).
# sympy/mpmath/networkx are pure-Python torch deps left free (torch needs a newer sympy).
# The import at the end makes a broken AI image fail the build instead of failing at runtime.
ARG INSTALL_AI=false
ARG TORCH_VERSION=2.8.0
COPY requirements-ai.txt /opt/airflow/requirements-ai.txt
RUN if [ "$INSTALL_AI" = "true" ]; then \
      pip list --format=freeze | grep -viE '^(sympy|mpmath|networkx)==' > /tmp/base-constraints.txt && \
      pip install --no-cache-dir -c /tmp/base-constraints.txt "torch==${TORCH_VERSION}" \
        --index-url https://download.pytorch.org/whl/cpu --extra-index-url https://pypi.org/simple && \
      pip install --no-cache-dir -c /tmp/base-constraints.txt -r /opt/airflow/requirements-ai.txt && \
      python -c "import numpy, pyarrow, pandas, torch, sentence_transformers; \
assert not torch.cuda.is_available(); print('AI stack OK: numpy', numpy.__version__, '| torch', torch.__version__)" && \
      rm /tmp/base-constraints.txt; \
    fi
