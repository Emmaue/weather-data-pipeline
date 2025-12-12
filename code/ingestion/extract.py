import os
import sys
import asyncio
import aiohttp
import json
from datetime import datetime
from dotenv import load_dotenv

# --- IMPORT FIX ---
# This tells Python: "Look for files in the current directory too"
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from s3_utils import upload_to_s3
except ImportError:
    # Fallback for different run contexts
    from s3_utils import upload_to_s3

load_dotenv()

# Configuration
API_KEY = os.getenv('WEATHER_API_KEY')
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
BATCH_SIZE = 50

# Extended City List
CITIES = [
    "Lagos", "Abuja", "London", "New York", "Tokyo", "Paris", "Berlin", "Mumbai",
    "Cairo", "Nairobi", "Johannesburg", "Accra", "Dubai", "Riyadh", "Istanbul",
    "Sydney", "Melbourne", "Toronto", "Vancouver", "Mexico City", "Sao Paulo",
    "Buenos Aires", "Lima", "Santiago", "Bogota", "Singapore", "Bangkok", "Seoul",
    "Beijing", "Shanghai", "Hong Kong", "Jakarta", "Manila", "Hanoi", "Kuala Lumpur",
    "Madrid", "Rome", "Amsterdam", "Brussels", "Vienna", "Lisbon", "Athens",
    "Dublin", "Stockholm", "Oslo", "Helsinki", "Copenhagen", "Warsaw", "Prague"
]

async def fetch_weather_async(session, city):
    """Fetch data asynchronously"""
    url = f"{BASE_URL}?q={city}&appid={API_KEY}&units=metric"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"⚠️ API Error {city}: {response.status}")
                return None
    except Exception as e:
        print(f"❌ Failed to fetch {city}: {e}")
        return None

def process_batches(data):
    """Split into batches and upload to S3"""
    total_records = len(data)
    
    for i in range(0, total_records, BATCH_SIZE):
        batch = data[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"weather_batch_{batch_num:03d}_{timestamp}.json"
        
        payload = {
            "_meta": {
                "batch_id": batch_num,
                "record_count": len(batch),
                "fetched_at": datetime.now().isoformat()
            },
            "records": batch
        }
        
        # Upload to S3 (This calls your s3_utils.py)
        upload_to_s3(payload, "data/raw", filename)

async def run_extraction_async():
    print("="*60)
    print(f"🚀 STARTING ASYNC CLOUD EXTRACTION: {len(CITIES)} Cities")
    print("="*60)
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for city in CITIES:
            tasks.append(fetch_weather_async(session, city))
        
        # This is where the speed happens (Parallel Requests)
        results = await asyncio.gather(*tasks)
    
    valid_data = [d for d in results if d]
    print(f"\n✅ Fetched {len(valid_data)}/{len(CITIES)} records successfully.")
    
    # Upload to S3
    process_batches(valid_data)

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(run_extraction_async())

    #nothing here just testing my CI to see if it works