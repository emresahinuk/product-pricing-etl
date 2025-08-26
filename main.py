import os
import json
import requests
from datetime import datetime
import pandas as pd
import sqlite3

# --------------------
# Directories
# --------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")
DB_PATH = os.path.join(DATA_DIR, "product_data.db")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)

# --------------------
# Fetch product data (Fake Store API)
# --------------------
print("Fetching product data...")
products_url = "https://fakestoreapi.com/products"
resp = requests.get(products_url)
products = resp.json()

ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

# Save raw JSON and CSV
raw_json_path = os.path.join(RAW_DIR, f"products_raw_{ts}.json")
raw_csv_path = os.path.join(RAW_DIR, f"products_raw_{ts}.csv")

with open(raw_json_path, "w", encoding="utf-8") as f:
    json.dump(products, f, indent=2)

pd.json_normalize(products).to_csv(raw_csv_path, index=False)

print(f"Raw data saved to {RAW_DIR}")

# --------------------
# Fetch exchange rate USD -> GBP (safer)
# --------------------
print("Fetching exchange rate USD -> GBP...")
rate_resp = requests.get("https://api.exchangerate.host/latest?base=USD&symbols=GBP")
rate_json = rate_resp.json()

if "rates" in rate_json and "GBP" in rate_json["rates"]:
    exchange_rate = rate_json["rates"]["GBP"]
else:
    print("⚠ Could not fetch exchange rate, using default 0.85")
    exchange_rate = 0.85  # fallback rate

print(f"Exchange rate: 1 USD = {exchange_rate:.2f} GBP")


# --------------------
# Transform data
# --------------------
df = pd.read_csv(raw_csv_path)
df = df.rename(columns={"id":"product_id","price":"price_usd"})
df["price_gbp"] = (df["price_usd"] * exchange_rate).round(2)
df["is_premium"] = (df["price_gbp"] > 50).astype(int)

def categorize(cat):
    lo = str(cat).lower()
    if "jewel" in lo:
        return "accessory"
    if "electronics" in lo:
        return "electronics"
    if "men" in lo or "women" in lo or "clothing" in lo:
        return "apparel"
    return "other"

df["category_group"] = df["category"].apply(categorize)
df["fetched_at"] = datetime.utcnow().isoformat()

# Save cleaned CSV
clean_csv_path = os.path.join(CLEAN_DIR, f"products_clean_{ts}.csv")
df.to_csv(clean_csv_path, index=False)
print(f"Cleaned data saved to {CLEAN_DIR}")

# --------------------
# Load into SQLite
# --------------------
conn = sqlite3.connect(DB_PATH)
df_to_db = df[["product_id","title","description","category","category_group",
               "price_usd","price_gbp","is_premium","image","fetched_at"]]
df_to_db.to_sql("products", conn, if_exists="replace", index=False)

cur = conn.cursor()
cur.execute("CREATE INDEX IF NOT EXISTS idx_price_gbp ON products(price_gbp);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_category_group ON products(category_group);")
conn.commit()
conn.close()
print(f"Data loaded into SQLite DB at {DB_PATH}")

# --------------------
# Quick preview
# --------------------
print("Sample data:")
print(df.head())
