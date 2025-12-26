import sys
import os
import json
import snowflake.connector
from dotenv import load_dotenv

# --- IMPORT SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

try:
    from ingestion.s3_utils import list_s3_files, read_from_s3, move_s3_object
except ImportError:
    from s3_utils import list_s3_files, read_from_s3, move_s3_object

load_dotenv()

# Configuration
# NOTE: We use the generic DB/Schema variables. 
# In a real run, you might set SNOWFLAKE_SCHEMA='crypto_dev' in your .env or Airflow vars.
SNOWFLAKE_DB = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA') 
ARCHIVE_FOLDER = 'archive/loaded_to_snowflake'

def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )

def load_variant_batch(conn, records, table_name):
    """
    Generic Loader: Takes a list of dicts and inserts them 
    into a table with a single 'json_data' VARIANT column.
    """
    if not records: return 0
    
    cursor = conn.cursor()
    
    # --- THE FIX ---
    # We select the bind variable (%s) as a column named 'column1' (Snowflake default)
    # and wrap it in PARSE_JSON().
    sql = f"""
    INSERT INTO {table_name} (json_data) 
    SELECT PARSE_JSON(column1) 
    FROM VALUES (%s)
    """
    
    # 2. Prepare Data
    data_to_insert = []
    for r in records:
        # Serialize Dict -> JSON String
        json_str = json.dumps(r, default=str)
        data_to_insert.append((json_str,))

    # 3. Execute
    try:
        cursor.executemany(sql, data_to_insert)
        return len(data_to_insert)
    except Exception as e:
        print(f"❌ Error inserting into {table_name}: {e}")
        return 0

def process_folder(folder_path, table_name):
    """Generic processor using the Variant Loader"""
    print(f"\n📂 Checking {folder_path} -> {table_name}...")
    files = list_s3_files(folder_path)
    
    if not files:
        print("   ⚠️ No files found.")
        return

    conn = get_snowflake_conn()
    
    for key in files:
        filename = key.split('/')[-1]
        print(f"   📄 Processing: {filename}")
        
        try:
            records = read_from_s3(key)
            
            # Normalize structure if needed
            if isinstance(records, dict) and 'records' in records:
                records = records['records']
            
            if records:
                # CALL THE VARIANT LOADER
                count = load_variant_batch(conn, records, table_name)
                print(f"      ✅ Loaded {count} raw records to {table_name}")
                
                # Archive
                move_s3_object(key, ARCHIVE_FOLDER)
            else:
                print("      ⚠️ Empty file, skipping.")
                
        except Exception as e:
            print(f"      💥 Failed to process {filename}: {e}")
            
    conn.close()

if __name__ == "__main__":
    print("="*60)
    print("❄️  STARTING VARIANT LOAD (ELT)")
    print("="*60)

    # 1. Load Bitcoin Data
    process_folder(
        "validated/crypto_data", 
        "stg_bitcoin"
    )

    # 2. Load Market Snapshots
    process_folder(
        "validated/market_snapshots", 
        "stg_market_snapshots"
    )
    
    print("\n✅ Loading Cycle Complete.")