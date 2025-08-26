# Product Pricing ETL

## Overview
This example ETL ingests product data from Fake Store API, converts prices from USD to GBP using exchangerate.host, performs basic feature engineering, stores cleaned data in SQLite, and exposes a small API.

## Quick start (local)

1. Create a virtualenv and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r etl/requirements.txt
```

2. Run pipeline locally (fetch -> transform -> load):

```bash
cd etl
python fetch_raw.py
python transform.py
python load_db.py
```

Or run the runner:

```bash
python run_pipeline.py
```

3. Start API (after pipeline ran at least once):

```bash
uvicorn api.app:app --reload --port 8000
```

4. Try the endpoints:
- `GET http://localhost:8000/top5`
- `GET http://localhost:8000/search?min_price=10&max_price=100`

## Airflow

- Drop `dags/product_etl_dag.py` into your Airflow DAGs folder.
- Ensure Airflow Python env can import the repository (add repo to PYTHONPATH or configure pythonpath in Airflow).
- Trigger the DAG (manual trigger) to run the tasks.

## Assumptions

- Fake Store API returns `price` as USD.
- ExchangeRate.host free endpoint is used for USD->GBP conversion.
- For simplicity we use a single `products` table in SQLite (denormalized). In production, we'd normalize product, category, and price history tables.

## Improvements with more time

- Add robust logging and error handling, retries and circuit-breaker around external API calls.
- Add tests (unit + integration) and CI pipeline.
- Add schema migration tool (Alembic) for SQL evolution.
- Move storage to cloud (S3 for raw/clean artifacts, PostgreSQL/RDS for DB).
- Add data quality checks (Great Expectations) and observability (Prometheus/Grafana).
- Add authentication and pagination to API.

## Git

- Commit code with `git init`, add `.gitignore` for `data/`, `.venv`, etc.
