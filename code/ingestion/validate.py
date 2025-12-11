import sys
import os
import json
from datetime import datetime
from typing import Dict, List, Tuple
from dotenv import load_dotenv

# Import S3 helpers
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from s3_utils import list_s3_files, read_from_s3, upload_to_s3, move_s3_object
except ImportError:
    from code.ingestion.s3_utils import list_s3_files, read_from_s3, upload_to_s3, move_s3_object

load_dotenv()

# S3 Folders
RAW_FOLDER = 'data/raw'
VALIDATED_FOLDER = 'data/validated'
REJECTED_FOLDER = 'data/rejected'
ARCHIVE_FOLDER = 'data/archive'

class WeatherDataValidator:
    """Validator logic (Same as before)"""
    def validate(self, data: Dict) -> Tuple[bool, List[str]]:
        errors = []
        if not data.get('name'):
            errors.append("Missing City Name")
        if not data.get('main', {}).get('temp'):
            errors.append("Missing Temperature")
        
        temp = data.get('main', {}).get('temp')
        if temp and (temp < -90 or temp > 60):
            errors.append(f"Temperature {temp} is unrealistic")
            
        return len(errors) == 0, errors

def process_s3_batches():
    print("="*60)
    print("🔍 STARTING CLOUD VALIDATION (S3)")
    print("="*60)
    
    validator = WeatherDataValidator()
    
    # 1. List files in S3 'data/raw'
    files = list_s3_files(RAW_FOLDER)
    
    if not files:
        print("⚠️ No files found in S3 'data/raw'.")
        return

    for key in files:
        filename = key.split('/')[-1]
        print(f"\n📄 Processing S3 File: {filename}")
        
        try:
            # 2. Read from S3
            content = read_from_s3(key)
            if not content:
                continue

            records = content.get('records', [])
            valid_batch = []
            rejected_batch = []
            
            # 3. Validate
            for record in records:
                is_valid, errors = validator.validate(record)
                
                # Ensure metadata exists
                if '_meta' not in record:
                    record['_meta'] = {}

                if is_valid:
                    record['_meta']['validated_at'] = datetime.now().isoformat()
                    valid_batch.append(record)
                else:
                    record['_meta']['validation_errors'] = errors
                    rejected_batch.append(record)
            
            # 4. Save Results back to S3
            if valid_batch:
                new_name = filename.replace('.json', '_valid.json')
                upload_to_s3(valid_batch, VALIDATED_FOLDER, new_name)
                
            if rejected_batch:
                new_name = filename.replace('.json', '_rejected.json')
                upload_to_s3(rejected_batch, REJECTED_FOLDER, new_name)
            
            # 5. Archive the original raw file (Move to data/archive)
            move_s3_object(key, ARCHIVE_FOLDER)
            
        except Exception as e:
            print(f"💥 Error processing {filename}: {e}")

    print("\n" + "="*60)
    print("✅ Cloud Validation Complete.")

if __name__ == "__main__":
    process_s3_batches()