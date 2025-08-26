"""
transform.py

- Load the latest raw Parquet
- Fetch exchange rate (USD -> GBP) from exchangerate.host
- Convert price to GBP and add flags/categories
- Save cleaned data to `data/clean/` as Parquet and CSV
"""
import os
import glob
from datetime import datetime
import requests
import pandas as pd

RAW_DIR = os.getenv('RAW_DIR', 'data/raw')
CLEAN_DIR = os.getenv('CLEAN_DIR', 'data/clean')
EXCHANGE_API = 'https://api.exchangerate.host/latest?base=USD&symbols=GBP'

os.makedirs(CLEAN_DIR, exist_ok=True)

def latest_parquet_path():
    files = sorted(glob.glob(os.path.join(RAW_DIR, '*.parquet')))
    if not files:
        raise FileNotFoundError('No raw parquet files found in ' + RAW_DIR)
    return files[-1]

def fetch_usd_to_gbp():
    resp = requests.get(EXCHANGE_API, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    rate = data['rates']['GBP']
    return float(rate)

def categorize_category(category_text: str) -> str:
    # Very simple categorization / feature engineering
    lo = category_text.lower()
    if 'jewel' in lo or 'jewelery' in lo:
        return 'accessory'
    if 'electronics' in lo:
        return 'electronics'
    if 'men' in lo or 'women' in lo or 'clothing' in lo:
        return 'apparel'
    return 'other'

def transform():
    p = latest_parquet_path()
    df = pd.read_parquet(p)

    rate = fetch_usd_to_gbp()

    # Normalize column names
    df = df.rename(columns={
        'id': 'product_id',
        'title': 'title',
        'price': 'price_usd',
        'description': 'description',
        'category': 'category',
        'image': 'image'
    })

    df['price_gbp'] = (df['price_usd'].astype(float) * rate).round(2)
    df['is_premium'] = df['price_gbp'] > 50.0  # flagging threshold (GBP)
    df['category_group'] = df['category'].fillna('unknown').apply(categorize_category)
    df['fetched_at'] = datetime.utcnow()

    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    parquet_path = os.path.join(CLEAN_DIR, f'products_clean_{ts}.parquet')
    csv_path = os.path.join(CLEAN_DIR, f'products_clean_{ts}.csv')

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)

    return parquet_path, csv_path

if __name__ == '__main__':
    p, c = transform()
    print('Saved cleaned files:', p, c)
