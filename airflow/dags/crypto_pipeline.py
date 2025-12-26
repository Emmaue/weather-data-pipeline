from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os
import requests
from dotenv import load_dotenv

# --- PATH SETUP ---
# We need to make sure Airflow can find your new 'mongo_ingestion' folder
PROJECT_ROOT = '/opt/airflow'
CODE_DIR = '/opt/airflow/code'
DBT_DIR = '/opt/airflow/dbt' 

sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, CODE_DIR)

# Load Environment Variables
env_path = os.path.join(PROJECT_ROOT, '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)

# --- IMPORTS FROM YOUR NEW SCRIPTS ---
# Note: We alias them to avoid name collisions since both use 'extract_and_load'
try:
    from mongo_ingestion.mongo_to_s3 import extract_and_load as extract_btc_func
    from mongo_ingestion.multicoin_to_s3 import extract_and_load as extract_multi_func
    from mongo_ingestion.validate_bitcoin import process_batches as validate_btc_func
    from mongo_ingestion.validate_multicoin import process_batches as validate_multi_func
    # For loading, we import the helper to run the logic manually in the wrapper
    from mongo_ingestion.load_crypto_variant import process_folder, load_variant_batch
except ImportError as e:
    print(f"❌ Import Error: {e}")

# --- SLACK ALERT FUNCTION (Reused) ---
def on_failure_callback(context):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url: return
    task_instance = context.get('task_instance')
    slack_msg = {
        "text": f":rotating_light: *Crypto Pipeline Failed!* :rotating_light:\n*Task:* {task_instance.task_id}\n*Logs:* <{task_instance.log_url}|View Logs>"
    }
    try:
        requests.post(webhook_url, json=slack_msg)
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")

# --- PYTHON WRAPPERS ---
def extract_btc_wrapper():
    print("🚀 Starting Bitcoin Extraction...")
    extract_btc_func()

def extract_multi_wrapper():
    print("🚀 Starting Multicoin Snapshot Extraction...")
    extract_multi_func()

def validate_btc_wrapper():
    print("🔍 Starting Bitcoin Validation...")
    validate_btc_func()

def validate_multi_wrapper():
    print("🔍 Starting Multicoin Validation...")
    validate_multi_func()

def load_variant_wrapper():
    print("❄️ Starting Variant Load to Snowflake...")
    
    # This ensures the worker has the latest code and avoids top-level scope issues
    from mongo_ingestion.load_crypto import process_folder
    
    # 1. Load Bitcoin
    process_folder("validated/crypto_data", "stg_bitcoin")
    
    # 2. Load Market Snapshots
    process_folder("validated/market_snapshots", "stg_market_snapshots")

# --- DAG DEFINITION ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback
}

with DAG(
    'crypto_production_pipeline',
    default_args=default_args,
    description='Crypto ELT: Mongo -> S3 -> Snowflake Variant -> dbt (Dev/Prod)',
    schedule_interval='@daily',  # Market snapshots are usually daily
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['production', 'crypto'],
) as dag:

    # 1. Extraction Tasks (Parallel)
    t_extract_btc = PythonOperator(
        task_id='extract_bitcoin',
        python_callable=extract_btc_wrapper
    )

    t_extract_multi = PythonOperator(
        task_id='extract_multicoin',
        python_callable=extract_multi_wrapper
    )

    # 2. Validation Tasks (Parallel)
    t_validate_btc = PythonOperator(
        task_id='validate_bitcoin',
        python_callable=validate_btc_wrapper
    )

    t_validate_multi = PythonOperator(
        task_id='validate_multicoin',
        python_callable=validate_multi_wrapper
    )

    # 3. Load Task (Sequential - loads both)
    t_load_snowflake = PythonOperator(
        task_id='load_to_snowflake_variant',
        python_callable=load_variant_wrapper
    )

    # 4. dbt Transformation Pipeline
    # We define the env vars needed for dbt
    dbt_env = {
        'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
        'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
        'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
        'SNOWFLAKE_ROLE': os.getenv('SNOWFLAKE_ROLE'), 
        'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
        'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'), # Default fallback
        'PATH': os.getenv('PATH')
    }

    # dbt DEV: Only run 'crypto' models
    dbt_dev_run = BashOperator(
        task_id='dbt_dev_run',
        bash_command=f'cd {DBT_DIR} && dbt run --select models/crypto --profiles-dir . --target dev 2>&1',
        env=dbt_env,
        append_env=True 
    )



    # dbt PROD: Promote 'crypto' models to Prod
    dbt_prod_run = BashOperator(
        task_id='dbt_prod_run',
        bash_command=f'cd {DBT_DIR} && dbt run --select models/crypto --profiles-dir . --target prod 2>&1',
        env=dbt_env,
        append_env=True
    )

    # ===== ORCHESTRATION FLOW =====
    
    # Extract BTC -> Validate BTC -> Load
    t_extract_btc >> t_validate_btc >> t_load_snowflake

    # Extract Multi -> Validate Multi -> Load
    t_extract_multi >> t_validate_multi >> t_load_snowflake

    # Load -> dbt Dev ->  dbt Prod
    t_load_snowflake >> dbt_dev_run >> dbt_prod_run