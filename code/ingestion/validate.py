import json
import os
import shutil
from datetime import datetime
from typing import Dict, List, Tuple

# Configuration
RAW_DIR = 'data/raw'
VALIDATED_DIR = 'data/validated'
REJECTED_DIR = 'data/rejected'

# Ensure directories exist
os.makedirs(VALIDATED_DIR, exist_ok=True)
os.makedirs(REJECTED_DIR, exist_ok=True)

class WeatherDataValidator:
    """Validator that works on individual record level"""
    
    def validate(self, data: Dict) -> Tuple[bool, List[str]]:
        errors = []
        
        # 1. Critical Check: Missing Fields
        if not data.get('name'):
            errors.append("Missing City Name")
        if not data.get('main', {}).get('temp'):
            errors.append("Missing Temperature")
            
        # 2. Critical Check: Temperature Physics
        temp = data.get('main', {}).get('temp')
        if temp and (temp < -90 or temp > 60):
            errors.append(f"Temperature {temp} is unrealistic")
            
        return len(errors) == 0, errors

def save_batch(data: List[Dict], original_filename: str, folder: str, suffix: str):
    """Save a filtered list of records as a new batch file"""
    if not data:
        return
        
    new_filename = original_filename.replace('.json', f'_{suffix}.json')
    filepath = os.path.join(folder, new_filename)
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"   -> Saved {len(data)} records to {folder}/{new_filename}")

def process_batches():
    print("="*50)
    print("🔍 STARTING BATCH VALIDATION")
    print("="*50)
    
    validator = WeatherDataValidator()
    
    # Get all JSON files in the raw folder
    files = [f for f in os.listdir(RAW_DIR) if f.endswith('.json')]
    
    if not files:
        print("⚠️ No files found in data/raw to validate.")
        return

    for filename in files:
        src_path = os.path.join(RAW_DIR, filename)
        print(f"\n📄 Processing Batch: {filename}")
        
        try:
            with open(src_path, 'r') as f:
                content = json.load(f)
            
            # Extract the list of records from the batch structure
            records = content.get('records', [])
            
            valid_batch = []
            rejected_batch = []
            
            # Validate each city inside the batch
            for record in records:
                is_valid, errors = validator.validate(record)
                
                # --- FIX START: Ensure _meta exists ---
                if '_meta' not in record:
                    record['_meta'] = {} 
                # --- FIX END ---

                if is_valid:
                    # Add metadata about when it was validated
                    record['_meta']['validated_at'] = datetime.now().isoformat()
                    valid_batch.append(record)
                else:
                    record['_meta']['validation_errors'] = errors
                    rejected_batch.append(record)
            
            # Save the results
            save_batch(valid_batch, filename, VALIDATED_DIR, 'valid')
            save_batch(rejected_batch, filename, REJECTED_DIR, 'rejected')
            
        except Exception as e:
            print(f"💥 Error processing {filename}: {e}")

    print("\n" + "="*50)
    print("✅ Validation Complete.")

if __name__ == "__main__":
    process_batches()