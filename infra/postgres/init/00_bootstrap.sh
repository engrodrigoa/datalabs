#!/usr/bin/env bash
# Runs once, on an EMPTY data directory (docker-entrypoint-initdb.d).
# For an existing volume run:  make db-bootstrap
set -e  # no -u: when the file loses +x (Windows checkout) the entrypoint sources it

if [ -z "${GRAFANA_DB_PASSWORD:-}" ]; then echo "GRAFANA_DB_PASSWORD must be set" >&2; exit 1; fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
  -- Airflow metadata lives in its own database, isolated from the DW
  SELECT 'CREATE DATABASE airflow OWNER "$POSTGRES_USER"'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

  -- Read-only role used by Grafana (least privilege)
  DO \$\$
  BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_ro') THEN
      CREATE ROLE grafana_ro LOGIN PASSWORD '${GRAFANA_DB_PASSWORD}';
    ELSE
      ALTER ROLE grafana_ro PASSWORD '${GRAFANA_DB_PASSWORD}';
    END IF;
  END
  \$\$;
EOSQL

# Schemas/tables of the DW (same files the setup DAG re-applies idempotently)
for f in "${INIT_SQL_DIR:-/docker-entrypoint-initdb.d/sql}"/*.sql; do
  echo "applying $f"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$f"
done

# Grants: DW (observability + serving layers) and Airflow metadata
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  GRANT CONNECT ON DATABASE "$POSTGRES_DB" TO grafana_ro;
  DO \$\$
  DECLARE s text;
  BEGIN
    FOREACH s IN ARRAY ARRAY['audit','obs','elementary','gold','silver','stg','bronze','landing_rfb','landing_nfe','dq_nfe','ai','ctrl'] LOOP
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO grafana_ro', s);
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO grafana_ro', s);
      EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT ON TABLES TO grafana_ro', current_user, s);
    END LOOP;
  END
  \$\$;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname airflow <<-EOSQL
  GRANT CONNECT ON DATABASE airflow TO grafana_ro;
  GRANT USAGE ON SCHEMA public TO grafana_ro;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
  ALTER DEFAULT PRIVILEGES FOR ROLE "$POSTGRES_USER" IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;
EOSQL
