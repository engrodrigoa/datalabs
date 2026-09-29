FROM apache/airflow:2.9.0

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         git \
  && chmod 755 /usr/bin/git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow


COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt \
    connectorx \
    pyarrow \
    astronomer-cosmos \
    boto3


RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core dbt-postgres 'elementary-data[postgres]' && \
    chmod -R 775 /opt/airflow/dbt_venv



    