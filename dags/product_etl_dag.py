"""
Airflow DAG: product_etl_dag.py
- DAG defines tasks: fetch_data -> transform_data -> load_to_db
- Uses PythonOperator for simplicity
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from etl.fetch_raw import fetch_products, save_raw
from etl.transform import transform
from etl.load_db import load_to_sqlite

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='product_pricing_etl',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # set to '@daily' in production
    catchup=False,
    tags=['example', 'etl'],
) as dag:

    def task_fetch():
        data = fetch_products()
        save_raw(data)

    def task_transform():
        transform()

    def task_load():
        load_to_sqlite()

    t1 = PythonOperator(task_id='fetch_data', python_callable=task_fetch)
    t2 = PythonOperator(task_id='transform_data', python_callable=task_transform)
    t3 = PythonOperator(task_id='load_to_db', python_callable=task_load)

    t1 >> t2 >> t3
