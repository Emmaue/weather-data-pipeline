# Production Data Platform: Multi-Pipeline Analytics System

- Type: End-to-End Data Engineering Platform
- Architecture: ELT · Polyglot Storage · Containerized
- Infrastructure: AWS EC2 · Docker · Apache Airflow · Snowflake
- Data Sources: Structured (S3) · Semi-Structured (MongoDB)

---

## Project Overview

### The Problem
This project is a production-grade data platform designed to support multiple ingestion paradigms within a single, unified analytics architecture.

Instead of building isolated, single-purpose pipelines, I engineered a shared analytics backbone that enables different data sources and formats to coexist under one orchestration, transformation, and serving layer. This approach reflects how modern data organizations design platforms that scale across teams and use cases.

I needed a system to ingest high-frequency data, validate it for quality, and store it in a warehouse where it is immediately ready for analysis.

The platform supports:

- Structured batch ingestion from object storage (S3), with explicit schema definitions, validation rules, and data quality guarantees.
- Semi-structured ingestion from a NoSQL database (MongoDB), leveraging schema-on-read to accommodate evolving data structures without breaking downstream analytics.

Both pipelines are intentionally designed to:

- Run on the same AWS EC2 instance to simulate resource-constrained production environments.
- Share a Dockerized infrastructure, ensuring consistent runtime environments across ingestion, orchestration, and transformation layers.
- Be centrally orchestrated using Apache Airflow, with independent DAGs, task dependencies, retries, and monitoring.
- Use dbt to transform raw data through layered models (dev → test → prod), enforcing modularity, version control, and testable transformations.
- Serve curated analytical outputs through Streamlit dashboards, enabling business-facing exploration of metrics and insights.

---

## Architecture Overview

**Flow:** `API` → `Python (EC2)` → `AWS S3 (Raw/Validated)` → `Snowflake (Staging)` → `dbt (Transformation)` → `Streamlit`

### Tech Stack
* **Orchestration:** Apache Airflow (Dockerized)
* **Language:** Python
* **Storage:** AWS S3 (Data Lake), MongoDB & Snowflake (Data Warehouse)
* **Validation:** Great Expectation
* **Transformation:** dbt Core (Data Build Tool)
* **Visualization:** Streamlit
* **Infrastructure:** Docker & Docker Compose
* **CI/CD:** Automated build & deployment pipeline

#### Infrastructure & Runtime Environment

The platform runs on a single AWS EC2 instance and uses Dockerized services to simulate a production-like, resource-constrained environment.

- EC2: hosts Airflow, dbt, and Streamlit
- Docker: isolates services and dependencies
- Docker Compose: manages multi-service orchestration
- Snowflake: external analytical warehouse
- S3: raw and validated data lake storage

*Dockerfile to build the Docker Compose*
```
# Start with the official Airflow image
FROM apache/airflow:2.7.1

# Switch to root to install system tools
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy your requirements.txt into the container
COPY requirements.txt /requirements.txt

# Install your Python libraries (Snowflake, etc.)
RUN pip install --no-cache-dir -r /requirements.txt
```

---

## How It Works

This platform supports two ingestion paradigms under a single orchestration and infrastructure layer:

- Pipeline A — Structured Batch ELT: Country & Weather data
- Pipeline B — Semi-Structured / Market Data: Crypto (Bitcoin & Multi-Coin)

Both pipelines follow the same control flow (ingest → validate → load → transform) while differing only in source, storage medium, and validation logic.

### 1. Data Extraction (Parallel Ingestion)
Custom Python operators are used to ingest data in parallel, allowing each dataset to scale independently.

Pipeline A — Weather & Country Data

- Pulls structured API responses (country metadata, weather metrics).
- Designed for deterministic schemas and batch processing.

Pipeline B — Crypto Market Data
- Hits the CoinGecko API using two independent services:
     - Bitcoin Service: Asset-specific historical and real-time pricing.
     - Market Service: Snapshot of the Top-20 cryptocurrencies (market cap, volume, 24h change).

Design Choice
All semi-structured data is extracted as NDJSON (Newline-Delimited JSON) to support:

- Streaming-friendly ingestion
- Efficient storage
- Schema evolution without pipeline breakage

### 2. The Quality Gate (Validation)
Before any data reaches the warehouse, it must pass through a hard validation boundary.

Common Validation Pattern

- Raw Zone: All extracted data lands here first.
- Dataset-Specific Validators:
       - Weather/Country → schema completeness, null checks, business rules
       - Crypto → non-negative prices, valid timestamps, numeric consistency

Routing Logic:

- ✅ Validated Data → validated/
-❌ Rejected Data → rejected/ (quarantined for inspection)
- 📦 Original Payloads → archive/ (audit & replay support)

Why this matters
Validation is not optional and not deferred to analytics. Bad data never contaminates downstream systems

### 3. Loading (ELT Pattern)
Instead of enforcing rigid schemas at ingestion time, the platform uses a schema-on-read ELT strategy.

Pipeline A: Structured Loads

- Validated data is loaded into Snowflake using typed columns.
- Optimized for relational querying and dimensional modeling.

Pipeline B: Semi-Structured Loads

- Valid JSON payloads are loaded directly into Snowflake VARIANT columns.
- A single column (json_data) captures the full API response.

Key Benefit
If upstream APIs introduce schema drift, the pipeline does not fail.
New fields are safely captured and parsed later during transformation.

### 4. Transformation (dbt)
dbt provides the logical unification layer across both pipelines.

- Core Responsibilities
- JSON parsing and flattening
- Metric normalization
- Time-series preparation
- Dimensional modeling
- Environment isolation

---

## Key Features

🔹 **Robust Error Handling**
* Data is never lost. Bad data is quarantined, not discarded.
* Slack notifications trigger immediately on pipeline failure.

🔹 **Idempotent Design**
* The pipeline is replayable. Running the DAG multiple times for the same day does not result in duplicate records in the final analytics tables.

🔹 Environment Management

- dbt dynamically routes outputs to:
   - DEV schemas for iteration
   - PROD schemas after tests pass

- Promotion is gated by dbt test.

🔹 **Infrastructure as Code**
* The entire environment (Airflow Scheduler, Webserver, Postgres Backend) is defined in `docker-compose.yaml` for instant reproducibility.

<img width="1024" height="559" alt="image" src="https://github.com/user-attachments/assets/d9c7538d-3193-4396-8b9b-407beb429e83" />
*full platform architecture*
---

## Pipeline A: Structured ELT (Weather & Country Data)

Purpose

Demonstrate a classical enterprise-grade ELT pipeline with explicit data quality gates, controlled data promotion, and warehouse-safe loading patterns.
This pipeline mirrors how regulated or metrics-critical datasets are handled in production.

🔄 ### Data Flow

1️⃣ Ingestion (API → Object Storage)

- Weather and country reference data are ingested from external REST APIs
- Data is extracted in batch mode using scheduled Airflow tasks
- API responses are normalized into structured JSON / tabular formats
- Raw payloads are written unchanged to Amazon S3 for traceability and replayability

Why this matters:
Preserving raw data ensures auditability, debugging, and backfills without re-calling external APIs.

*The weather ingestion code*
```
import os
import sys
import asyncio
import aiohttp
import json
from datetime import datetime
from dotenv import load_dotenv

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
        
        # Upload to S3
        upload_to_s3(payload, "data/raw", filename)

async def run_extraction_async():
    print("="*60)
    print(f"🚀 STARTING ASYNC CLOUD EXTRACTION: {len(CITIES)} Cities")
    print("="*60)
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for city in CITIES:
            tasks.append(fetch_weather_async(session, city))
        
        results = await asyncio.gather(*tasks)
    
    valid_data = [d for d in results if d]
    print(f"\n✅ Fetched {len(valid_data)}/{len(CITIES)} records successfully.")
    
    # Upload to S3
    process_batches(valid_data)

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(run_extraction_async())
```

<img width="1075" height="409" alt="image" src="https://github.com/user-attachments/assets/3fe265ee-5e3f-42f1-9b7f-dad3adb95ea7" />
*Data loaded in s3* 

2️⃣ S3 Data Quality Routing

S3 is organized into explicit quality-based folders, acting as a lightweight data lake governance layer:
s3://weather-data/
 ├── raw/         # Unvalidated API responses
 ├── validated/   # Schema-compliant & clean data
 ├── bad/         # Records failing validation
 └── archive/     # Historical snapshots

- Data initially lands in raw/
- Validation jobs inspect each batch
- Records are programmatically routed based on validation outcome

Why this matters:
This prevents polluted data from ever reaching the warehouse while still retaining failed records for analysis.

<img width="1061" height="383" alt="image" src="https://github.com/user-attachments/assets/d58de191-f6e3-4a67-ac3f-a5c8f81c0b79" />

*S3 folder structure*

3️⃣ Validation & Quality Enforcement

Each dataset passes through a validation layer that applies:

- Schema validation
  - Required columns present
  - Column order and naming consistency
- Null & type enforcement
    - Mandatory fields cannot be null
    - Data types (e.g., temperature, latitude, population) are enforced
- Business rule validation
   - Temperature ranges within realistic bounds
   - Country codes follow ISO standards
   - Duplicate records handled explicitly

Records that fail any rule are diverted to bad/, while successful records move to validated/.

I did a hybrid validation strategy using custom Python decorators for runtime checks and Great Expectations for data profiling.

*Validation code*
```
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
```

*Using Great Expectations local Server to check validation*
<img width="1202" height="642" alt="image" src="https://github.com/user-attachments/assets/5ca09cc6-124e-4001-994d-f57283e89468" />
<img width="1164" height="525" alt="image" src="https://github.com/user-attachments/assets/aae17f17-eee5-4172-99cf-7d846cefbaea" />

To confirm that validation is working, I manually altered the schema.

*Result of failed validation*
<img width="1207" height="464" alt="image" src="https://github.com/user-attachments/assets/2a0d78b5-34d4-4c0c-87b9-9996df319650" />
<img width="1168" height="624" alt="image" src="https://github.com/user-attachments/assets/2b1fc3f1-5ca9-4611-94ec-3131e9dbc813" />

4️⃣ Warehouse Load (Snowflake)

- Only validated datasets are eligible for warehouse loading
- Data is loaded into Snowflake using COPY INTO or staged ELT logic
- Raw and validated layers remain decoupled from analytics models
- Load jobs are idempotent to support safe re-runs

Why this matters:
Analytics teams can trust that Snowflake contains clean, business-ready data only.

*snowflake image showing the loaded data that have been validated*
<img width="1010" height="473" alt="image" src="https://github.com/user-attachments/assets/b2205de8-2ccb-4965-86b3-2a51d5d944c2" />

5️⃣ Transformation & Modeling (dbt)

- dbt models transform validated data into:
  - Staging (stg_)
```
  select
    ID as weather_id,
    CITY,
    COUNTRY as country_name, -- We will use this to join
    TEMPERATURE,
    HUMIDITY
from {{ source('snowflake_data', 'WEATHER_DATA') }}
```
  - Analytics-ready marts
```
{{ config(materialized='table') }}

with country_info as (
    select * from {{ ref('stg_country') }}
),

weather_info as (
    select * from {{ ref('stg_weather') }}
)

select
    -- 1. Create a Surrogate Key (Unique Hash)
    md5(cast(w.weather_id as varchar) || '-' || cast(c.country_id as varchar)) as unique_key,

    -- 2. Select columns from Weather
    w.weather_id,
    w.city,
    w.temperature as temperature_celsius, -- <--- CHANGED THIS LINE (Added alias)
    w.humidity,

    -- 3. Select columns from Country
    c.country_name,
    c.capital,
    c.population,
    c.region

from weather_info w
-- Join on the Country Name
left join country_info c
    on w.country_name = c.country_name
```

- dbt tests reinforce constraints at the warehouse level
- Models are promoted across dev → test → prod

*image showing dev and prod transformation layer done using dbt*
<img width="379" height="124" alt="image" src="https://github.com/user-attachments/assets/730ccf69-6134-43cc-963a-3fb3cad41377" />


7️⃣ Orchestration Layer: Apache Airflow

Apache Airflow serves as the central orchestration engine for the entire data platform, coordinating ingestion, validation, loading, and transformation workflows across multiple pipelines.

Rather than acting as a simple scheduler, Airflow is used as a control plane that enforces execution order, quality gates, and environment promotion across the analytics lifecycle.

Task flow
extract_to_s3 → validate_data → load_to_snowflake → dbt_dev_run → dbt_dev_test → dbt_prod_run

Each task executes sequentially, enforcing strict upstream dependencies.

Execution status

All tasks completed successfully, indicating:

- Successful API ingestion and raw data landing
- Validation gates passed before warehouse loading
- Controlled promotion of transformations from development to production

Operator usage

- PythonOperator used for ingestion and validation tasks
- BashOperator used for dbt execution steps

Pipeline behavior

- Tasks are idempotent and retry-safe
- Failures at any stage prevent downstream execution
- Execution metadata and run history are tracked per DAG run

*Dag run snippet*
```
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback
}

with DAG(
    'weather_production_pipeline',
    default_args=default_args,
    description='Production ETL: API -> S3 -> Snowflake -> dbt (Dev → Test → Prod)',
    schedule_interval='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['production', 'weather'],
) as dag:

    # ===== INGESTION PIPELINE =====
    t1 = PythonOperator(
        task_id='extract_to_s3',
        python_callable=extract_wrapper
    )

    t2 = PythonOperator(
        task_id='validate_data',
        python_callable=validate_wrapper
    )

    t3 = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_wrapper
    )

# ===== PIPELINE FLOW =====
    t1 >> t2 >> t3 >> dbt_dev_run >> dbt_dev_test >> dbt_prod_run
```


*image of the dag after successfully running*

<img width="1280" height="566" alt="image" src="https://github.com/user-attachments/assets/9c1e6cf5-7943-494b-8b09-85c97270c82b" />

Intentionally modified the dag logic to test failed notification on Slack and it's working.

*image showing the slack notification*
<img width="988" height="634" alt="image" src="https://github.com/user-attachments/assets/89716a62-6736-42dd-bf60-a3f11e9c12c1" />



## Pipeline B: Semi-Structured / NRDBMS (Crypto Market Data)

Purpose
Demonstrate polyglot data ingestion and schema-on-read analytics using a NoSQL data store, enabling flexible ingestion of evolving, semi-structured datasets without breaking downstream workflows.

🔄 ### Data Flow
Ingestion

Raw responses are initially stored in MongoDB to demonstrate NoSQL handling, before being serialized to S3 for warehouse loading.

- Cryptocurrency market data ingested from external APIs:
  - Bitcoin time-series data
  - Multi-coin market snapshots (CoinGecko)
- Data is ingested in batch mode
- Raw responses are loaded into MongoDB, running as a containerized service within Docker

*snippet of code to ingest multicoin data*
```
import requests
from pymongo import MongoClient
from datetime import datetime
import os
import sys

# ============================================================
# CONFIGURATION
# ============================================================

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:password@mongo:27017/?authSource=admin"
)

DB_NAME = "crypto_db"
COLLECTION_NAME = "market_snapshots"

MARKETS_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd"
    "&order=market_cap_desc"
    "&per_page=20"
    "&page=1"
    "&sparkline=false"
    "&price_change_percentage=1h,24h,7d,30d"
)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def fetch_market_data():
    try:
        response = requests.get(MARKETS_URL, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        print(f"❌ API Error: {exc}", file=sys.stderr)
        return None

def ingest_snapshot():
    data = fetch_market_data()
    if not data:
        return

    snapshot_time = datetime.utcnow()
    docs = []

    for coin in data:
        docs.append({
            "ingested_at": snapshot_time,
            "data_type": "market_snapshot",
            "source": "coingecko",

            "rank": coin.get("market_cap_rank"),
            "coin_id": coin.get("id"),
            "symbol": coin.get("symbol"),
            "name": coin.get("name"),

            "price_usd": coin.get("current_price"),
            "pct_change_1h": coin.get("price_change_percentage_1h_in_currency"),
            "pct_change_24h": coin.get("price_change_percentage_24h"),
            "pct_change_7d": coin.get("price_change_percentage_7d_in_currency"),
            "pct_change_30d": coin.get("price_change_percentage_30d_in_currency"),

            "volume_24h": coin.get("total_volume"),
            "market_cap": coin.get("market_cap"),
            "circulating_supply": coin.get("circulating_supply"),
            "total_supply": coin.get("total_supply")
        })

    collection.insert_many(docs)
    print(f"✅ Inserted {len(docs)} market snapshot records")

if __name__ == "__main__":
    ingest_snapshot()

```

*image showing ingested data in MongoDB*
<img width="1014" height="447" alt="image" src="https://github.com/user-attachments/assets/eb6fc175-774d-4b9e-b3f8-34f426330e96" />

### Dataset Separation

- Bitcoin and multi-coin datasets are ingested independently
- Each dataset:
   - Uses its own collection
   - Applies dataset-specific validation logic
- This allows independent schema evolution without cross-dataset coupling

*image showing ingestion seperation in S3*
<img width="1062" height="435" alt="image" src="https://github.com/user-attachments/assets/4e0105c5-90f9-4d9e-b720-2c06744df841" />



### Validation & Serialization

- Incoming documents are validated for:
  - Required fields
  - Timestamp integrity
  - Metric completeness
- Valid records are serialized and stored as JSON files
- Invalid or incomplete records are handled separately to avoid downstream contamination

*validation code*
```
import sys
import os
import json
from datetime import datetime
from typing import Dict, List, Tuple
from dotenv import load_dotenv

# --- IMPORT SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

try:
    from ingestion.s3_utils import list_s3_files, read_from_s3, upload_to_s3, move_s3_object
except ImportError:
    from s3_utils import list_s3_files, read_from_s3, upload_to_s3, move_s3_object

load_dotenv()

# S3 Configuration
RAW_FOLDER = 'raw/market_snapshots'
VALIDATED_FOLDER = 'validated/market_snapshots'
REJECTED_FOLDER = 'rejected/market_snapshots'
ARCHIVE_FOLDER = 'archive/market_snapshots'

class MarketSnapshotValidator:
    """Validates Top 20 Crypto Market Data"""
    
    def validate(self, data: Dict) -> Tuple[bool, List[str]]:
        errors = []
        
        # 1. Check Identifiers (MATCHING YOUR INGESTION SCRIPT)
        if not data.get('coin_id'):
            errors.append("Missing Coin ID")
        if not data.get('symbol'):
            errors.append("Missing Symbol")
            
        # 2. Check Numeric Integrity
        price = data.get('price_usd')
        if price is None or not isinstance(price, (int, float)) or price < 0:
            errors.append(f"Invalid Price: {price}")
            
        market_cap = data.get('market_cap')
        if market_cap is None or not isinstance(market_cap, (int, float)) or market_cap < 0:
            errors.append(f"Invalid Market Cap: {market_cap}")

        # 3. Rank Check
        rank = data.get('rank')
        if not rank or rank < 1:
            errors.append(f"Invalid Rank: {rank}")

        return len(errors) == 0, errors

def process_batches():
    print(f"🚀 Starting Multicoin Validation on {RAW_FOLDER}...")
    
    validator = MarketSnapshotValidator()
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
            
            # Save Validated
            if valid_batch:
                new_name = filename.replace('.json', '_valid.json')
                upload_to_s3(valid_batch, VALIDATED_FOLDER, new_name)
                print(f"   ✅ Validated: {len(valid_batch)} records")
                
            # Save Rejected
            if rejected_batch:
                new_name = filename.replace('.json', '_rejected.json')
                upload_to_s3(rejected_batch, REJECTED_FOLDER, new_name)
                print(f"   ❌ Rejected: {len(rejected_batch)} records")
                
            # Archive Original
            move_s3_object(key, ARCHIVE_FOLDER)
            
        except Exception as e:
            print(f"💥 Error processing {filename}: {e}")

if __name__ == "__main__":
    process_batches()
```

### Warehouse Integration

- Validated JSON files are ingested into Snowflake
- Semi-structured columns are stored using Snowflake’s native JSON support
- dbt transformations convert:
    - JSON → structured relational models
    - JSON → CSV outputs where required for analytics tools

*image showing the ingested data in Snowflake*
<img width="1013" height="565" alt="image" src="https://github.com/user-attachments/assets/72410d95-2f56-4647-8c27-d888a3e50c27" />



## dbt Transformations (Pipeline B)

dbt models are used to normalize and prepare crypto data for analytics:

- JSON parsing and flattening
- Metric normalization across multiple coins
- Time-series structuring for trend analysis
- Shared dimensional modeling patterns with Pipeline A

This ensures consistent analytics behavior across structured and semi-structured pipelines.

*dbt script showing transformation to convert JSON files to CSV and be analytical ready*
```
WITH source AS (
    SELECT * FROM {{ source('crypto_raw', 'stg_market_snapshots') }}
),

flattened AS (
    SELECT
        json_data:ingested_at::TIMESTAMP_NTZ as snapshot_at,
        json_data:rank::INTEGER as rank,
        json_data:coin_id::STRING as coin_id,
        json_data:symbol::STRING as symbol,
        json_data:name::STRING as name,
        json_data:price_usd::FLOAT as price_usd,
        json_data:market_cap::FLOAT as market_cap,
        json_data:volume_24h::FLOAT as volume_24h,
        json_data:pct_change_24h::FLOAT as pct_change_24h,
        ingested_at
    FROM source
)

SELECT * FROM flattened
-- IDEMPOTENCY FIX:
-- Group by Coin + Snapshot Time, and keep the latest ingestion
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY coin_id, snapshot_at 
    ORDER BY ingested_at DESC
) = 1
```

*image showing transformed data as csv*
<img width="719" height="504" alt="image" src="https://github.com/user-attachments/assets/246ca864-d940-44d2-b6ab-608abe801b32" />


## Orchestration Strategy — Apache Airflow

Airflow orchestrates both pipelines using modular, reusable DAGs under a shared orchestration framework.

Key Design Decisions

- Validation treated as a first-class task
- Explicit dependency enforcement between pipeline stages
- dbt tests used as promotion gates
- Shared operators reused across structured and semi-structured pipelines

Execution Order

- Ingest
- Validate
- Load
- Transform (dev)
- Test
- Transform (prod)

<img width="985" height="538" alt="image" src="https://github.com/user-attachments/assets/a6d81f8d-be49-41b9-8bcf-a788c630ed6e" />

Image graph shows Independent extraction and validation run in parallel to isolate schema and quality differences, then converge at a single load and transformation path to ensure consistent analytics output.

*image showing successful dag run*
<img width="1280" height="524" alt="image" src="https://github.com/user-attachments/assets/73510420-a883-4007-9114-2b4bad9e56af" />



## Analytics & Visualization — Streamlit

Streamlit dashboards consume final Snowflake models to deliver analytics outputs.

Separate views are provided for:

- Weather trends
- Country-level insights
- Crypto market movements

This completes the data-to-decision loop, from ingestion to business-ready insights.

*following images are different dashboards for the two pipeline*

<img width="883" height="424" alt="image" src="https://github.com/user-attachments/assets/86f05200-455e-49c2-88c3-4ba694443652" />
<img width="939" height="612" alt="image" src="https://github.com/user-attachments/assets/70563bce-f2ba-453b-b941-e781f83499a9" />


*crypto dashboard*
<img width="1080" height="513" alt="image" src="https://github.com/user-attachments/assets/32819677-15e5-43aa-8c12-4f40f96c73b1" />
<img width="1038" height="596" alt="image" src="https://github.com/user-attachments/assets/aae94a83-b523-46ff-9b85-1130d07da15b" />
<img width="988" height="521" alt="image" src="https://github.com/user-attachments/assets/58caaf10-ffa7-4a61-8672-1c9a79c47452" />



## 🔄 CI/CD & Deployment

The project utilizes a continuous integration and deployment pipeline to ensure that code changes in the repository are immediately reflected in the production Airflow environment on AWS.

### Architecture
**GitHub Actions (Self-Hosted Runner)** → **AWS EC2** → **Docker Compose**

Instead of using a remote SSH connection, I configured a **GitHub Self-Hosted Runner** directly on the AWS EC2 instance. This "worker" listens for jobs from the GitHub repository and executes deployment commands locally on the server.

### The Pipeline Workflow (`.github/workflows/deploy.yml`)

Every time code is pushed to the `main` branch, the following automated steps occur:

1.  **Trigger:** The pipeline detects a commit to `main`.
2.  **Checkout:** The runner on the EC2 instance pulls the latest code from GitHub.
3.  **Environment Sync:** It verifies the `.env` file exists (secrets are managed manually on the server for security).
4.  **Container Refresh:**
    * It builds any new Python dependencies.
    * It restarts the Airflow Scheduler and Webserver using `docker-compose up -d --build`.
    * It prunes unused Docker images to save disk space.

### Setup Steps (How it was built)

1.  **Infrastructure Provisioning:**
    * Launched an **AWS EC2 (t3.medium)** instance running Ubuntu.
    * Installed Docker and Docker Compose v2.

2.  **Runner Configuration:**
    * Registered the EC2 instance as a runner in the GitHub Repository settings (*Settings > Actions > Runners*).
    * Installed the runner agent on the EC2 instance as a background service (`./svc.sh install`).
    * This established a secure, one-way polling connection (no inbound ports needed).

3.  **Deployment Script:**
    The CI/CD configuration file used:
    ```yaml
    name: Production Deployment

    on:
      push:
        branches: [ "main" ]

    jobs:
      deploy:
        runs-on: self-hosted  # Targets the AWS EC2 instance
        steps:
          - name: Checkout Code
            uses: actions/checkout@v3

          - name: Refresh Airflow Containers
            run: |
              echo " Deploying updates to AWS EC2..."
              # Navigate to project directory
              cd /home/ubuntu/weather-data-pipeline
              
              # Pull latest changes (Redundant with checkout, but ensures folder consistency)
              git pull origin main
              
              # Rebuild and Restart Containers
              docker compose up -d --build --remove-orphans
              
              # Cleanup space
              docker image prune -f
              echo " Deployment Complete."
    ```

*image showing successfull continous integration*
<img width="1280" height="507" alt="image" src="https://github.com/user-attachments/assets/3f34f86e-2ce2-4fd4-a264-d9eed7960e61" />

*image showing successfull continous deployment*
<img width="1280" height="617" alt="image" src="https://github.com/user-attachments/assets/97e21ef9-1590-4544-ab40-64385ce528f9" />

## 🏁 Conclusion

This project represents a complete implementation of a **Modern Data Stack**, moving beyond simple extraction scripts to a robust, scalable, and automated engineering platform.

By transitioning from manual workflows to an orchestrated **Airflow** environment, I was able to solve critical data challenges:
* **Data Quality:** The implementation of a "Quality Gate" on S3 ensures that only valid, schema-compliant data reaches the warehouse, keeping analytics clean.
* **Resilience:** The pipeline is fully **idempotent**. Thanks to **dbt** transformations and window functions, the system can self-correct and handle re-runs without creating duplicate records—a frequent pitfall in production systems.
* **Agility:** Using **Snowflake's Variant** data type allows the pipeline to adapt to upstream API changes ("Schema Drift") without breaking, decoupling extraction from transformation.
* **Automation:** The **CI/CD** integration with GitHub Actions means that improvements are deployed to production in seconds, reducing the "works on my machine" friction.

Ultimately, this pipeline transforms raw, high-frequency cryptocurrency noise into structured, actionable insights, demonstrating the power of automated Data Engineering to drive real-time decision-making.




















