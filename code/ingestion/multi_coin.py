import requests
from pymongo import MongoClient
from datetime import datetime
import os
import sys

# ============================================================
# CONFIGURATION
# ============================================================

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:password@mongo:27017/?authSource=admin"
)

DB_NAME = "crypto_db"
COLLECTION_NAME = "market_snapshots"

MARKETS_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd"
    "&order=market_cap_desc"
    "&per_page=20"
    "&page=1"
    "&sparkline=false"
    "&price_change_percentage=1h,24h,7d,30d"
)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def fetch_market_data():
    try:
        response = requests.get(MARKETS_URL, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        print(f"❌ API Error: {exc}", file=sys.stderr)
        return None

def ingest_snapshot():
    data = fetch_market_data()
    if not data:
        return

    snapshot_time = datetime.utcnow()
    docs = []

    for coin in data:
        docs.append({
            "ingested_at": snapshot_time,
            "data_type": "market_snapshot",
            "source": "coingecko",

            "rank": coin.get("market_cap_rank"),
            "coin_id": coin.get("id"),
            "symbol": coin.get("symbol"),
            "name": coin.get("name"),

            "price_usd": coin.get("current_price"),
            "pct_change_1h": coin.get("price_change_percentage_1h_in_currency"),
            "pct_change_24h": coin.get("price_change_percentage_24h"),
            "pct_change_7d": coin.get("price_change_percentage_7d_in_currency"),
            "pct_change_30d": coin.get("price_change_percentage_30d_in_currency"),

            "volume_24h": coin.get("total_volume"),
            "market_cap": coin.get("market_cap"),
            "circulating_supply": coin.get("circulating_supply"),
            "total_supply": coin.get("total_supply")
        })

    collection.insert_many(docs)
    print(f"✅ Inserted {len(docs)} market snapshot records")

if __name__ == "__main__":
    ingest_snapshot()
