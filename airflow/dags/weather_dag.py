from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import asyncio

# --- PATH SETUP ---
PROJECT_ROOT = '/home/ubuntu/weather-data-pipeline'
CODE_DIR = '/home/ubuntu/weather-data-pipeline/code'

# Add paths so Python can find your scripts
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, CODE_DIR)

# --- IMPORTS ---
from ingestion.extract import run_extraction_async
from ingestion.validate import process_s3_batches
from ingestion.load import run_cloud_loading

# --- WRAPPERS ---
def extract_wrapper():
    os.chdir(PROJECT_ROOT) # Manually change directory inside function
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_extraction_async())

def validate_wrapper():
    os.chdir(PROJECT_ROOT)
    process_s3_batches()

def load_wrapper():
    os.chdir(PROJECT_ROOT)
    run_cloud_loading()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_production_pipeline',
    default_args=default_args,
    description='Production ETL: API -> S3 -> Snowflake',
    schedule_interval='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['production', 'weather'],
) as dag:

    # Notice: NO 'cwd' argument here!
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

    t1 >> t2 >> t3