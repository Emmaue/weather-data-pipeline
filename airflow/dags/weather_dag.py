from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os
import asyncio
import requests
from dotenv import load_dotenv

# --- PATH SETUP ---
PROJECT_ROOT = '/opt/airflow'
CODE_DIR = '/opt/airflow/code'
DBT_DIR = '/opt/airflow/dbt' 

sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, CODE_DIR)

env_path = os.path.join(PROJECT_ROOT, '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)

# --- IMPORTS ---
from ingestion.extract import run_extraction_async
from ingestion.validate import process_s3_batches
from ingestion.load import run_cloud_loading

# --- SLACK ALERT FUNCTION ---
def on_failure_callback(context):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return
    task_instance = context.get('task_instance')
    slack_msg = {
        "text": f":rotating_light: *Pipeline Failed!* :rotating_light:\n*Task:* {task_instance.task_id}\n*Logs:* <{task_instance.log_url}|View Logs>"
    }
    try:
        requests.post(webhook_url, json=slack_msg)
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")

# --- WRAPPERS ---
def extract_wrapper():
    os.chdir(PROJECT_ROOT)
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

    # ===== DBT TRANSFORMATION PIPELINE =====
    
    dbt_env = {
        'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
        'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
        'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
        'SNOWFLAKE_ROLE': os.getenv('SNOWFLAKE_ROLE'), 
        'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
        'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'),
        'PATH': os.getenv('PATH') # Keep system path
    }

    # Step 1: Run transformations in DEV schema
    # REMOVED: 'dbt deps' to prevent hourly permission/lock issues
    dbt_dev_run = BashOperator(
        task_id='dbt_dev_run',
        bash_command=f'cd {DBT_DIR} && dbt run --profiles-dir . --target dev 2>&1',
        env=dbt_env,
        append_env=True 
    )

    # Step 2: Test the DEV data (THE QUALITY GATE)
    dbt_dev_test = BashOperator(
        task_id='dbt_dev_test',
        bash_command=f'cd {DBT_DIR} && dbt test --profiles-dir . --target dev 2>&1',
        env=dbt_env,
        append_env=True
    )

    # Step 3: If tests pass, promote to PROD schema
    # REMOVED: 'dbt deps' here as well
    dbt_prod_run = BashOperator(
        task_id='dbt_prod_run',
        bash_command=f'cd {DBT_DIR} && dbt run --profiles-dir . --target prod 2>&1',
        env=dbt_env,
        append_env=True
    )

    # ===== PIPELINE FLOW =====
    t1 >> t2 >> t3 >> dbt_dev_run >> dbt_dev_test >> dbt_prod_run