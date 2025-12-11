import os
import json
import requests
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv('WEATHER_API_KEY')
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
STAGING_DIR = 'data/raw'
BATCH_SIZE = 50  # Save 1 file for every 50 cities

# Ensure staging exists
os.makedirs(STAGING_DIR, exist_ok=True)

# A larger list of ~150 Global Cities for testing batching
CITIES = [
    # Africa
    "Lagos", "Abuja", "Cairo", "Nairobi", "Johannesburg", "Casablanca", "Accra", "Addis Ababa", "Kigali", "Dakar",
    "Tunis", "Algiers", "Luanda", "Dar es Salaam", "Khartoum", "Abidjan", "Alexandria", "Kampala", "Bamako", "Lusaka",
    # Europe
    "London", "Paris", "Berlin", "Madrid", "Rome", "Amsterdam", "Brussels", "Vienna", "Lisbon", "Athens",
    "Dublin", "Stockholm", "Oslo", "Helsinki", "Copenhagen", "Warsaw", "Prague", "Budapest", "Zurich", "Geneva",
    # Americas
    "New York", "Los Angeles", "Chicago", "Houston", "Toronto", "Vancouver", "Mexico City", "Sao Paulo", "Buenos Aires", "Lima",
    "Bogota", "Santiago", "Rio de Janeiro", "Caracas", "San Francisco", "Miami", "Boston", "Seattle", "Atlanta", "Denver",
    # Asia
    "Tokyo", "Beijing", "Mumbai", "Delhi", "Shanghai", "Singapore", "Bangkok", "Seoul", "Jakarta", "Manila",
    "Dubai", "Riyadh", "Istanbul", "Tehran", "Baghdad", "Hanoi", "Kuala Lumpur", "Hong Kong", "Taipei", "Osaka",
    # Oceania & Others
    "Sydney", "Melbourne", "Auckland", "Brisbane", "Perth", "Fiji", "Honolulu"
]

def fetch_weather(city):
    """Fetch raw JSON data"""
    try:
        url = f"{BASE_URL}?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"⚠️ API Error for {city}: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Failed to fetch {city}: {e}")
        return None

def save_batch(batch_data, batch_number):
    """
    Save A LIST of cities to a single JSON file.
    Filename: weather_batch_001_20251211.json
    """
    if not batch_data:
        return
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"weather_batch_{batch_number:03d}_{timestamp}.json"
    filepath = os.path.join(STAGING_DIR, filename)
    
    # Wrap the list in a dictionary with metadata
    output_payload = {
        "_meta": {
            "batch_id": batch_number,
            "record_count": len(batch_data),
            "fetched_at": datetime.now().isoformat()
        },
        "records": batch_data  # This is the list of 50 cities
    }

    with open(filepath, 'w') as f:
        json.dump(output_payload, f, indent=4)
    
    print(f"\n💾 BATCH SAVED: {filepath} (Contains {len(batch_data)} records)")

def run_extraction():
    print("="*60)
    print(f"🚀 STARTING EXTRACTION: {len(CITIES)} Cities | Batch Size: {BATCH_SIZE}")
    print("="*60)
    
    batch_buffer = []  # Temporary list to hold data in memory
    batch_count = 1
    total_fetched = 0
    
    for i, city in enumerate(CITIES, 1):
        print(f"📍 [{i}/{len(CITIES)}] Fetching {city}...", end="\r")
        
        data = fetch_weather(city)
        if data:
            batch_buffer.append(data)
            total_fetched += 1
        
        # CHECK: Is buffer full?
        if len(batch_buffer) >= BATCH_SIZE:
            save_batch(batch_buffer, batch_count)
            batch_buffer = []  # Clear memory
            batch_count += 1
            
        # Polite sleep to avoid hitting API limits
        time.sleep(0.1)

    # CHECK: Save any "leftovers" (e.g., if total is 75, save the last 25)
    if batch_buffer:
        save_batch(batch_buffer, batch_count)

    print("\n" + "="*60)
    print(f"✅ COMPLETED. Fetched {total_fetched} records.")

if __name__ == "__main__":
    run_extraction()