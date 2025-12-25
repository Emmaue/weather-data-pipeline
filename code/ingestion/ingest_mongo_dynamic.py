import requests
from pymongo import MongoClient
from datetime import datetime
import os
import sys

# ---------------- CONFIG ----------------
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:password@mongo:27017/?authSource=admin"
)

DB_NAME = "crypto_db"
COLLECTION_NAME = "bitcoin_prices"

COINGECKO_CURRENT_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=bitcoin&vs_currencies=usd"
)

COINGECKO_HISTORICAL_URL = (
    "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    "?vs_currency=usd&days=30"
)
# ----------------------------------------

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]


def fetch_json(url: str):
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        print(f"❌ API error ({url}): {exc}", file=sys.stderr)
        return None


def ingest_current_price():
    data = fetch_json(COINGECKO_CURRENT_URL)
    if not data:
        return

    doc = {
        "data_type": "realtime",
        "asset": "bitcoin",
        "price_usd": data["bitcoin"]["usd"],
        "ingested_at": datetime.utcnow(),
        "source": "coingecko"
    }

    collection.insert_one(doc)
    print(f"✅ [Realtime] BTC price ingested: ${doc['price_usd']}")


def ingest_historical_data():
    data = fetch_json(COINGECKO_HISTORICAL_URL)
    if not data:
        return

    count = 0
    for ts, price in data["prices"]:
        collection.update_one(
            {
                "timestamp": ts,
                "data_type": "historical"
            },
            {
                "$set": {
                    "timestamp": ts,
                    "price_usd": price,
                    "asset": "bitcoin",
                    "data_type": "historical",
                    "last_updated": datetime.utcnow(),
                    "source": "coingecko"
                }
            },
            upsert=True
        )
        count += 1

    print(f"✅ [Historical] Synced {count} records (30 days)")


def main():
    print(f"🚀 Mongo ingestion started @ {datetime.utcnow()}")

    ingest_current_price()
    ingest_historical_data()

    print("\n📊 Sample documents:")
    for doc in collection.find().limit(3):
        print(doc)


if __name__ == "__main__":
    main()
