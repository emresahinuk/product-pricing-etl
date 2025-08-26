"""
fetch_raw.py

- Fetch product data from Fake Store API
- Save raw JSON and Parquet to `data/raw/` with timestamped filename
"""
import os
import json
from datetime import datetime
import requests
import pandas as pd

RAW_DIR = os.getenv('RAW_DIR', 'data/raw')
API_URL = 'https://fakestoreapi.com/products'

os.makedirs(RAW_DIR, exist_ok=True)

def fetch_products():
    resp = requests.get(API_URL, timeout=10)
    resp.raise_for_status()
    return resp.json()

def save_raw(data):
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    json_path = os.path.join(RAW_DIR, f'products_raw_{ts}.json')
    parquet_path = os.path.join(RAW_DIR, f'products_raw_{ts}.parquet')

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    df = pd.json_normalize(data)
    df.to_parquet(parquet_path, index=False)

    return json_path, parquet_path

if __name__ == '__main__':
    data = fetch_products()
    jpath, ppath = save_raw(data)
    print('Saved:', jpath, ppath)
