import os
import json
import boto3
from pymongo import MongoClient
from datetime import datetime

# --- CONFIGURATION ---
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:password@mongo:27017/?authSource=admin"
)

DB_NAME = "crypto_db"
COLLECTION_NAME = "bitcoin_prices"

# FIX: Changed to match your .env variable (AWS_BUCKET_NAME)
S3_BUCKET = os.getenv("AWS_BUCKET_NAME") 
S3_FOLDER = "raw/crypto_data"

def connect_mongo():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

def upload_to_s3(file_path, s3_key):
    """Uploads a file to S3 using env vars for auth"""
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION', 'us-east-1')
    )
    try:
        s3.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"✅ Uploaded to S3: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"❌ S3 Upload Failed: {e}")

def extract_and_load():
    try:
        db = connect_mongo()
        collection = db[COLLECTION_NAME]
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return

    # 1. Define filename (Daily Batch)
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    local_filename = f"/tmp/bitcoin_data_{today_str}.json"
    s3_key = f"{S3_FOLDER}/{today_str}/bitcoin_data.json"
    
    print(f"🚀 Starting extraction from {DB_NAME}.{COLLECTION_NAME}...")

    # 2. Query Data
    cursor = collection.find({})
    
    # 3. Write to local NDJSON file
    record_count = 0
    try:
        with open(local_filename, 'w') as f:
            for doc in cursor:
                f.write(json.dumps(doc, default=str) + '\n')
                record_count += 1
                
        print(f"📦 Extracted {record_count} records to {local_filename}")

        # 4. Upload to S3
        if record_count > 0:
            upload_to_s3(local_filename, s3_key)
            os.remove(local_filename)
        else:
            print("⚠️ No records found to upload.")

    except Exception as e:
        print(f"❌ Error during processing: {e}")

if __name__ == "__main__":
    extract_and_load()