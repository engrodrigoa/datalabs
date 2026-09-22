# RFB Lakehouse - Quick Start (5 min)

## 1. Install Dependencies

```bash
cd rfb_lakehouse_optimized
pip install -r requirements.txt
```

## 2. Configure Environment

```bash
cp .env.example .env

# Edit .env with your actual values:
# - S3/MinIO: MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
# - Postgres: POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD
```

## 3. Create Postgres Serving Schema

```bash
# Copy serving_layer_ddl.sql from parent project
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d rfb_dw < serving_layer_ddl.sql
```

## 4. Run Full Pipeline

```bash
# First time: process all 3 stages
python rfb_lakehouse_orchestrator.py --ref-month 202608

# Subsequent runs: skip Silver (much faster)
python rfb_lakehouse_orchestrator.py --ref-month 202609 --skip-silver-to-delta
```

**Expected duration:**
- First run: 25-30 min
- Subsequent runs: 15-20 min

## 5. Verify Results

```bash
# Check Postgres serving layer
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d rfb_dw -c \
  "SELECT COUNT(*) FROM serving_rfb.dim_estabelecimento;"

# Check Gold Delta via DuckDB
python -c "
import duckdb
conn = duckdb.connect(':memory:')
conn.execute('LOAD delta;')
result = conn.execute(\"SELECT COUNT(*) FROM read_parquet('s3://gold/rfb/dim/dim_rfb__estabelecimento/*')\").fetchall()
print(f'Gold records: {result[0][0]}')
"

# Monitor performance
python rfb_performance_monitor.py --all
```

## 6. Query Results

### DuckDB (Direct from Gold Delta)
```python
import duckdb

conn = duckdb.connect(':memory:')
conn.execute("LOAD delta;")

# Multi-month aggregation (fast on Delta)
result = conn.execute("""
  SELECT 
    referencia_mes,
    uf,
    COUNT(*) as qtd,
    SUM(qtd_ativa) as total_ativas
  FROM read_parquet('s3://gold/rfb/fct/fct_rfb__estabelecimento_mensal/*')
  WHERE referencia_mes >= 202601
  GROUP BY 1, 2
""").fetch_all()
```

### Postgres (Serving Layer)
```sql
-- Connect to Postgres
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d rfb_dw

-- Query dimension
SELECT * FROM serving_rfb.dim_estabelecimento 
WHERE uf = 'SP' AND is_current = true 
LIMIT 10;

-- Query facts
SELECT * FROM serving_rfb.fct_estabelecimento_mensal 
WHERE referencia_mes = 202608 
LIMIT 10;

-- Aggregated view
SELECT * FROM serving_rfb.v_estabelecimentos_por_uf 
WHERE referencia_mes = 202608
ORDER BY total_estabelecimentos DESC;
```

### BI Dashboard (Metabase/Superset)
Connect to Postgres `serving_rfb` schema:
- Dimensions: `dim_estabelecimento` (SCD2)
- Facts: `fct_estabelecimento_mensal` (monthly)
- Views: `v_estabelecimentos_por_uf`, `v_top_cnaes`, etc.

## Troubleshooting

### "No such bucket" error
```bash
# Check S3 connectivity
python -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
  aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
print(s3.list_buckets()['Buckets'])
"
```

### dbt "profiles.yml not found"
```bash
# Ensure DBT_PROFILES_DIR points to correct location
export DBT_PROFILES_DIR=~/.dbt
cd rfb_lakehouse_project
dbt parse
```

### Postgres connection refused
```bash
# Test connectivity
psql -h postgres -U postgres -c "SELECT version();"

# Check if pg_vector installed
psql -h postgres -U postgres -d rfb_dw -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

### Out of memory during S3 download
```bash
# Reduce parallel workers
export PARALLEL_WORKERS=2

# Or edit script: MAX_WORKERS = 2
```

## File Structure

```
rfb_lakehouse_optimized/
├── README.md                                  # Full documentation
├── QUICKSTART.md                              # This file
├── requirements.txt                           # Python dependencies
├── .env.example                               # Environment template
│
├── rfb_delta_utils.py                         # Shared Delta utilities
├── rfb03_silver_to_delta_optimized.py        # Stage 1: Silver → Delta
├── rfb_gold_build_optimized.py               # Stage 2: dbt Gold layer
├── rfb_sync_gold_to_postgres_optimized.py   # Stage 3: Gold → Postgres
├── rfb_lakehouse_orchestrator.py             # Full pipeline orchestrator
├── rfb_performance_monitor.py                 # Monitoring & diagnostics
│
└── ../rfb_lakehouse_project/                  # dbt project (link)
    ├── dbt_project.yml
    ├── profiles.yml
    ├── models/
    │   ├── staging/
    │   ├── intermediate/
    │   └── dimensional/
    ├── snapshots/
    ├── macros/
    └── tests/
```

## Next Steps

1. **First load:** Let orchestrator run for first month (25-30 min)
2. **Verify data:** Check Postgres `serving_rfb` schema has data
3. **Setup BI:** Connect Metabase/Superset to Postgres
4. **Schedule runs:** Add `rfb_lakehouse_orchestrator.py` to Airflow DAG
5. **Monitor:** Run `rfb_performance_monitor.py --all` weekly

## Airflow Integration (Optional)

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG('rfb_lakehouse_monthly', default_view='graph',
          start_date=datetime(2026, 8, 1))

task_pipeline = BashOperator(
    task_id='lakehouse_pipeline',
    bash_command="""
        cd /path/to/rfb_lakehouse_optimized && \
        python rfb_lakehouse_orchestrator.py \
          --ref-month {{ execution_date.strftime('%Y%m') }} \
          --skip-silver-to-delta
    """,
    dag=dag
)
```

## Performance Targets

After optimization:
- **Monthly load:** ~20 min (vs 5+ hours before)
- **Storage:** ~50 GB/month (vs 240+ GB for 12 months Postgres)
- **Query:** <2 sec for 1-month, <15 sec for 12-month

## Support

- Documentation: `README.md`
- Migration guide: `../rfb_lakehouse_project/MIGRATION_GUIDE.md`
- Performance: `python rfb_performance_monitor.py --all`

Good luck! 🚀
