import os
import snowflake.connector
from dotenv import load_dotenv

# Load env vars (this works for local dev, GitHub Actions injects them automatically)
load_dotenv()

def test_snowflake_connection():
    print("❄️ Attempting to connect to Snowflake...")
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()[0]
        print(f"✅ Success! Connected to Snowflake Version: {version}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        # Exit with error code 1 so CI knows it failed
        exit(1)

if __name__ == "__main__":
    test_snowflake_connection()