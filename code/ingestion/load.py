import sys
import os
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv

# Import S3 helpers
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from s3_utils import list_s3_files, read_from_s3, move_s3_object
except ImportError:
    from code.ingestion.s3_utils import list_s3_files, read_from_s3, move_s3_object

load_dotenv()

# Configuration
VALIDATED_FOLDER = 'data/validated'
ARCHIVE_FOLDER = 'data/archive'
SNOWFLAKE_DB = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )

def ensure_table_exists(conn):
    """Create table if it doesn't exist"""
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
        print(f"⚠️ Error checking table: {e}")
    cursor.close()

def load_batch(conn, records):
    """Load a list of dictionary records into Snowflake"""
    if not records:
        return 0

    cursor = conn.cursor()
    success_count = 0
    
    print(f"   ⏳ Merging {len(records)} records...")
    
    for record in records:
        # Extract Fields
        city = record.get('name')
        dt = record.get('dt')
        timestamp = datetime.fromtimestamp(dt)
        
        main = record.get('main', {})
        weather = record.get('weather', [{}])[0]
        wind = record.get('wind', {})
        sys_data = record.get('sys', {})
        
        # MERGE Query
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
            cursor.execute(merge_query, (
                # Source Match
                city, timestamp,
                # Update
                main.get('temp'), main.get('humidity'), weather.get('description'),
                # Insert
                city, sys_data.get('country'), main.get('temp'), main.get('feels_like'),
                main.get('temp_min'), main.get('temp_max'), main.get('pressure'),
                main.get('humidity'), weather.get('main'), weather.get('description'),
                wind.get('speed'), wind.get('deg'), record.get('clouds', {}).get('all'),
                timestamp
            ))
            success_count += 1
        except Exception as e:
            print(f"   ❌ Error merging {city}: {e}")

    cursor.close()
    return success_count

def run_cloud_loading():
    print("="*60)
    print("🚚 STARTING CLOUD LOAD (S3 -> SNOWFLAKE)")
    print("="*60)
    
    # 1. Check S3 for files
    files = list_s3_files(VALIDATED_FOLDER)
    
    if not files:
        print("⚠️ No validated files found in S3.")
        return

    conn = get_snowflake_conn()
    ensure_table_exists(conn)
    
    for key in files:
        filename = key.split('/')[-1]
        print(f"\n📄 Processing S3 File: {filename}")
        
        try:
            # 2. Read Data directly from S3
            records = read_from_s3(key)
            
            # 3. Load to Snowflake
            count = load_batch(conn, records)
            print(f"   ✅ Merged {count} rows.")
            
            # 4. Move S3 file to Archive
            move_s3_object(key, ARCHIVE_FOLDER)
            
        except Exception as e:
            print(f"   💥 Critical Error loading {filename}: {e}")
            
    conn.close()
    print("\n" + "="*60)
    print("✅ Loading Complete.")

if __name__ == "__main__":
    run_cloud_loading()