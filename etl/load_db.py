"""
load_db.py

- Load latest cleaned parquet and upsert into SQLite
- Schema: products (normalized where useful)
"""
import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text

CLEAN_DIR = os.getenv('CLEAN_DIR', 'data/clean')
DB_PATH = os.getenv('DB_PATH', 'data/product_data.db')

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def latest_clean_parquet():
    files = sorted(glob.glob(os.path.join(CLEAN_DIR, '*.parquet')))
    if not files:
        raise FileNotFoundError('No cleaned parquet files found in ' + CLEAN_DIR)
    return files[-1]

def load_to_sqlite():
    engine = create_engine(f'sqlite:///{DB_PATH}', echo=False)
    p = latest_clean_parquet()
    df = pd.read_parquet(p)

    # Keep schema fairly normalized: we will store a single products table for simplicity
    df = df[['product_id', 'title', 'description', 'category', 'category_group', 'price_usd', 'price_gbp', 'is_premium', 'image', 'fetched_at']]

    # Upsert logic: for SQLite we replace rows with same product_id by using a temporary table
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE IF NOT EXISTS products (product_id INTEGER PRIMARY KEY, title TEXT, description TEXT, category TEXT, category_group TEXT, price_usd REAL, price_gbp REAL, is_premium INTEGER, image TEXT, fetched_at TIMESTAMP)'))

        # write df to a temp table
        df.to_sql('products_tmp', conn, if_exists='replace', index=False)

        # upsert from tmp into products
        conn.execute(text('''
            INSERT INTO products (product_id, title, description, category, category_group, price_usd, price_gbp, is_premium, image, fetched_at)
            SELECT product_id, title, description, category, category_group, price_usd, price_gbp, CAST(is_premium AS INTEGER), image, fetched_at
            FROM products_tmp
            ON CONFLICT(product_id) DO UPDATE SET
                title=excluded.title,
                description=excluded.description,
                category=excluded.category,
                category_group=excluded.category_group,
                price_usd=excluded.price_usd,
                price_gbp=excluded.price_gbp,
                is_premium=excluded.is_premium,
                image=excluded.image,
                fetched_at=excluded.fetched_at;
        '''))

        conn.execute(text('DROP TABLE IF EXISTS products_tmp'))

    print('Loaded data into SQLite at', DB_PATH)

if __name__ == '__main__':
    load_to_sqlite()
