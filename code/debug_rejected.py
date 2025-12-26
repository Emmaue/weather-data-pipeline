import sys
import os
import json

# Fix imports to find s3_utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/ingestion")
try:
    from s3_utils import read_from_s3, list_s3_files
except ImportError:
    # Manual fallback if pathing is tricky
    sys.path.append("/opt/airflow/code/ingestion")
    from s3_utils import read_from_s3, list_s3_files

# Config
REJECTED_FOLDER = 'rejected/market_snapshots'

def inspect_errors():
    print(f"🔍 Inspecting {REJECTED_FOLDER}...")
    
    files = list_s3_files(REJECTED_FOLDER)
    if not files:
        print("No rejected files found.")
        return

    # Look at the most recent file
    latest_file = files[-1]
    print(f"📄 Reading: {latest_file}")
    
    data = read_from_s3(latest_file)
    
    if not data:
        print("File is empty.")
        return
        
    # Print the first 3 errors
    print("\n❌ Sample Errors:")
    count = 0
    for record in data:
        if count >= 3: break
        
        meta = record.get('_meta', {})
        errors = meta.get('validation_errors', [])
        
        print(f"Record {count+1}: {record.get('name', 'Unknown')}")
        print(f"   Errors: {errors}")
        print("-" * 30)
        count += 1

if __name__ == "__main__":
    inspect_errors()