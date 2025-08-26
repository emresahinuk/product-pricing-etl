"""
run_pipeline.py

- Simple runner to execute fetch -> transform -> load in sequence (useful for testing without Airflow)
"""
from fetch_raw import fetch_products, save_raw
from transform import transform
from load_db import load_to_sqlite

def main():
    print('Fetching raw...')
    data = fetch_products()
    save_raw(data)

    print('Transforming...')
    transform()

    print('Loading to DB...')
    load_to_sqlite()

    print('Pipeline completed')

if __name__ == '__main__':
    main()
