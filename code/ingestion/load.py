import json
import os
import shutil
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configuration
VALIDATED_DIR = 'data/validated'
ARCHIVE_DIR = 'data/archive'
SNOWFLAKE_DB = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

# Ensure archive exists
os.makedirs(ARCHIVE_DIR, exist_ok=True)

def get_snowflake_conn():
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )
    return conn

def ensure_table_exists(conn):
    """
    Check if table exists, if not, create it.
    This makes the script 'Idempotent' - it works on fresh systems too.
    """
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER AUTOINCREMENT,
        city STRING,
        country STRING,
        temperature FLOAT,
        feels_like FLOAT,
        temp_min FLOAT,
        temp_max FLOAT,
        pressure INTEGER,
        humidity INTEGER,
        weather_main STRING,
        weather_description STRING,
        wind_speed FLOAT,
        wind_deg INTEGER,
        clouds INTEGER,
        timestamp TIMESTAMP_NTZ,
        ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (id)
    );
    """
    try:
        cursor.execute(create_table_query)
    except Exception as e:
        print(f"⚠️ Error checking/creating table: {e}")
    cursor.close()

def load_batch(conn, filepath):
    with open(filepath, 'r') as f:
        records = json.load(f)
        
    if not records:
        return 0

    cursor = conn.cursor()
    success_count = 0
    
    print(f"   ⏳ Merging {len(records)} records...")
    
    for record in records:
        # 1. Extract Fields (Matching your original Schema)
        city = record.get('name')
        dt = record.get('dt')
        timestamp = datetime.fromtimestamp(dt)
        
        # Main Block
        main = record.get('main', {})
        temp = main.get('temp')
        feels_like = main.get('feels_like')
        temp_min = main.get('temp_min')
        temp_max = main.get('temp_max')
        pressure = main.get('pressure')
        humidity = main.get('humidity')
        
        # Weather Block
        weather = record.get('weather', [{}])[0]
        weather_main = weather.get('main', '')
        description = weather.get('description', '')
        
        # Wind & Clouds
        wind = record.get('wind', {})
        wind_speed = wind.get('speed')
        wind_deg = wind.get('deg')
        clouds = record.get('clouds', {}).get('all')
        
        # Sys Block
        country = record.get('sys', {}).get('country', '')
        
        # 2. MERGE Query (Now includes ALL columns)
        merge_query = """
        MERGE INTO weather_data AS target
        USING (SELECT %s AS city, %s AS obs_time) AS source
        ON target.city = source.city AND target.timestamp = source.obs_time
        WHEN MATCHED THEN
            UPDATE SET 
                temperature = %s,
                humidity = %s,
                weather_description = %s,
                ingestion_timestamp = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (
                city, country, temperature, feels_like, temp_min, temp_max,
                pressure, humidity, weather_main, weather_description,
                wind_speed, wind_deg, clouds, timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            # The params list is long because we have to pass values for:
            # 1. Source Check (City, Time)
            # 2. Update Set (Temp, Hum, Desc)
            # 3. Insert Values (All 14 fields)
            cursor.execute(merge_query, (
                # Source Check
                city, timestamp,
                # Update
                temp, humidity, description,
                # Insert
                city, country, temp, feels_like, temp_min, temp_max,
                pressure, humidity, weather_main, description,
                wind_speed, wind_deg, clouds, timestamp
            ))
            success_count += 1
        except Exception as e:
            print(f"   ❌ Error merging {city}: {e}")

    cursor.close()
    return success_count

def run_loading():
    print("="*50)
    print("🚚 STARTING LOAD TO SNOWFLAKE")
    print("="*50)
    
    files = [f for f in os.listdir(VALIDATED_DIR) if f.endswith('.json')]
    
    if not files:
        print("⚠️ No validated files found to load.")
        return

    conn = get_snowflake_conn()
    
    # Step 0: Ensure the table exists before we try to load
    ensure_table_exists(conn)
    
    for filename in files:
        src_path = os.path.join(VALIDATED_DIR, filename)
        print(f"\n📄 Loading: {filename}")
        
        try:
            # 1. Load Data
            count = load_batch(conn, src_path)
            print(f"   ✅ Merged {count} rows.")
            
            # 2. Move to Archive
            dest_path = os.path.join(ARCHIVE_DIR, filename)
            shutil.move(src_path, dest_path)
            print(f"   📦 Archived to {dest_path}")
            
        except Exception as e:
            print(f"   💥 Critical Error loading {filename}: {e}")
            
    conn.close()
    print("\n" + "="*50)
    print("✅ Loading Complete.")

if __name__ == "__main__":
    run_loading() 