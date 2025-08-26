from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
import os

# Build DB path relative to this file
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "product_data.db")
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)

app = FastAPI(title="Product Pricing API")

def rows_to_dict_list(result):
    """Convert SQLAlchemy Result to list of plain dicts safely."""
    return [dict(r._mapping) for r in result]

@app.get("/top5")
def top5():
    with engine.connect() as conn:
        res = conn.execute(text(
            "SELECT *, "
            "COALESCE(price_usd,0) as price_usd, "
            "COALESCE(price_gbp,0) as price_gbp, "
            "COALESCE(is_premium,0) as is_premium "
            "FROM products ORDER BY price_gbp DESC LIMIT 5"
        ))
        return rows_to_dict_list(res)

@app.get("/search")
def search(min_price: float = Query(0.0, ge=0.0), max_price: float = Query(1e9, ge=0.0)):
    with engine.connect() as conn:
        res = conn.execute(text(
            "SELECT *, "
            "COALESCE(price_usd,0) as price_usd, "
            "COALESCE(price_gbp,0) as price_gbp, "
            "COALESCE(is_premium,0) as is_premium "
            "FROM products WHERE price_gbp BETWEEN :minp AND :maxp ORDER BY price_gbp DESC"
        ), {"minp": min_price, "maxp": max_price})
        return rows_to_dict_list(res)
