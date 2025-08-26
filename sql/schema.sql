-- schema.sql

CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    category TEXT,
    category_group TEXT,
    price_usd REAL,
    price_gbp REAL,
    is_premium INTEGER,
    image TEXT,
    fetched_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_products_price_gbp ON products(price_gbp);
CREATE INDEX IF NOT EXISTS idx_products_category_group ON products(category_group);
