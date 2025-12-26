import sys
import os
import json
from datetime import datetime
from typing import Dict, List, Tuple
from dotenv import load_dotenv

# --- PATH SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

try:
    from ingestion.s3_utils import list_s3_files, read_from_s3, upload_to_s3, move_s3_object
except ImportError:
    from s3_utils import list_s3_files, read_from_s3, upload_to_s3, move_s3_object

load_dotenv()

# S3 Configuration
RAW_FOLDER = 'raw/crypto_data'
VALIDATED_FOLDER = 'validated/crypto_data'
REJECTED_FOLDER = 'rejected/crypto_data'
ARCHIVE_FOLDER = 'archive/crypto_data'

class BitcoinValidator:
    """Validates Specific Bitcoin Data"""
    
    def validate(self, data: Dict) -> Tuple[bool, List[str]]:
        errors = []
        
        # 1. Asset Check
        asset = data.get('asset')
        if not asset or asset.lower() != 'bitcoin':
            errors.append(f"Invalid Asset: {asset}")

        # 2. Price Check
        price = data.get('price_usd')
        if price is None or not isinstance(price, (int, float)) or price <= 0:
            errors.append(f"Invalid Bitcoin Price: {price}")
            
        # 3. Source Check
        if not data.get('source'):
            errors.append("Missing Data Source")

        # 4. Timestamp Check
        # Supports both 'timestamp' (historical) and 'ingested_at' (realtime)
        if not data.get('timestamp') and not data.get('ingested_at'):
            errors.append("Missing Timestamp")

        return len(errors) == 0, errors

def process_batches():
    print(f"🚀 Starting Bitcoin Validation on {RAW_FOLDER}...")
    
    validator = BitcoinValidator()
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
            
            # Handle NDJSON vs List
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
            
            if valid_batch:
                new_name = filename.replace('.json', '_valid.json')
                upload_to_s3(valid_batch, VALIDATED_FOLDER, new_name)
                print(f"   ✅ Validated: {len(valid_batch)} records")
                
            if rejected_batch:
                new_name = filename.replace('.json', '_rejected.json')
                upload_to_s3(rejected_batch, REJECTED_FOLDER, new_name)
                print(f"   ❌ Rejected: {len(rejected_batch)} records")
                
            move_s3_object(key, ARCHIVE_FOLDER)
            
        except Exception as e:
            print(f"💥 Error processing {filename}: {e}")

if __name__ == "__main__":
    process_batches()