import sys
import os
import json
from datetime import datetime
from typing import Dict, List, Tuple
from dotenv import load_dotenv

# --- IMPORT SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

try:
    from ingestion.s3_utils import list_s3_files, read_from_s3, upload_to_s3, move_s3_object
except ImportError:
    from s3_utils import list_s3_files, read_from_s3, upload_to_s3, move_s3_object

load_dotenv()

# S3 Configuration
RAW_FOLDER = 'raw/market_snapshots'
VALIDATED_FOLDER = 'validated/market_snapshots'
REJECTED_FOLDER = 'rejected/market_snapshots'
ARCHIVE_FOLDER = 'archive/market_snapshots'

class MarketSnapshotValidator:
    """Validates Top 20 Crypto Market Data"""
    
    def validate(self, data: Dict) -> Tuple[bool, List[str]]:
        errors = []
        
        # 1. Check Identifiers (MATCHING YOUR INGESTION SCRIPT)
        if not data.get('coin_id'):
            errors.append("Missing Coin ID")
        if not data.get('symbol'):
            errors.append("Missing Symbol")
            
        # 2. Check Numeric Integrity
        # UPDATED: 'price_usd' matches your ingest_multicoin.py
        price = data.get('price_usd')
        if price is None or not isinstance(price, (int, float)) or price < 0:
            errors.append(f"Invalid Price: {price}")
            
        market_cap = data.get('market_cap')
        if market_cap is None or not isinstance(market_cap, (int, float)) or market_cap < 0:
            errors.append(f"Invalid Market Cap: {market_cap}")

        # 3. Rank Check
        rank = data.get('rank')
        if not rank or rank < 1:
            errors.append(f"Invalid Rank: {rank}")

        return len(errors) == 0, errors

def process_batches():
    print(f"🚀 Starting Multicoin Validation on {RAW_FOLDER}...")
    
    validator = MarketSnapshotValidator()
    files = list_s3_files(RAW_FOLDER)
    
    if not files:
        print("⚠️ No files found to validate.")
        return

    for key in files:
        filename = key.split('/')[-1]
        print(f"\n📄 Processing: {filename}")
        
        try:
            content = read_from_s3(key)
            records = []
            
            if isinstance(content, str):
                for line in content.strip().split('\n'):
                    if line: records.append(json.loads(line))
            elif isinstance(content, list):
                records = content
            
            valid_batch = []
            rejected_batch = []
            
            for record in records:
                is_valid, errors = validator.validate(record)
                
                if '_meta' not in record: record['_meta'] = {}
                
                if is_valid:
                    record['_meta']['validated_at'] = datetime.now().isoformat()
                    valid_batch.append(record)
                else:
                    record['_meta']['validation_errors'] = errors
                    rejected_batch.append(record)
            
            # Save Validated
            if valid_batch:
                new_name = filename.replace('.json', '_valid.json')
                upload_to_s3(valid_batch, VALIDATED_FOLDER, new_name)
                print(f"   ✅ Validated: {len(valid_batch)} records")
                
            # Save Rejected
            if rejected_batch:
                new_name = filename.replace('.json', '_rejected.json')
                upload_to_s3(rejected_batch, REJECTED_FOLDER, new_name)
                print(f"   ❌ Rejected: {len(rejected_batch)} records")
                
            # Archive Original
            move_s3_object(key, ARCHIVE_FOLDER)
            
        except Exception as e:
            print(f"💥 Error processing {filename}: {e}")

if __name__ == "__main__":
    process_batches()