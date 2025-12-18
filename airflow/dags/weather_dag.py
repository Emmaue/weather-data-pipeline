from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import asyncio
import requests # New import for sending alerts
from dotenv import load_dotenv

# --- PATH SETUP ---
PROJECT_ROOT = '/opt/airflow'
CODE_DIR = '/opt/airflow/code'
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, CODE_DIR)

env_path = os.path.join(PROJECT_ROOT, '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)

# Load env vars (to get the SLACK URL)
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

# --- IMPORTS ---
from ingestion.extract import run_extraction_async
from ingestion.validate import process_s3_batches
from ingestion.load import run_cloud_loading

# --- SLACK ALERT FUNCTION ---
def on_failure_callback(context):
    """
    This runs AUTOMATICALLY if a task fails.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    
    if not webhook_url:
        print("No Slack URL found in .env, skipping alert.")
        return

    # Get info about the failure
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url

    # The Message to send
    slack_msg = {
        "text": f":rotating_light: *Pipeline Failed!* :rotating_light:\n\n*DAG:* {dag_id}\n*Task:* {task_id}\n*Time:* {execution_date}\n*Logs:* <{log_url}|Click here to view logs>"
    }

    try:
        requests.post(webhook_url, json=slack_msg)
        print("Slack alert sent successfully!")
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")

# --- WRAPPERS ---
def extract_wrapper():
    os.chdir(PROJECT_ROOT)
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
    # THIS IS THE MAGIC CONNECTION:
    'on_failure_callback': on_failure_callback
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