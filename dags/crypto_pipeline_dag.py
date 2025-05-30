from datetime import datetime, timedelta
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Configurations
PROJECT_ID = 'blockpulse-insights-project'
REGION = 'us-central1'
BUCKET_NAME = 'blockpulse-data-bucket'
RAW_DATA_PATH = f'gs://{BUCKET_NAME}/raw/crypto_snapshot_{{{{ ds_nodash }}}}.json'

default_args = {
    'owner': 'you',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': True,
    'email_on_retry': False
}

with models.DAG(
    dag_id='crypto_etl_split_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Modular CoinGecko ETL: extract to GCS, process into BigQuery',
    tags=['crypto', 'ETL', 'Dataflow']
) as dag:

    start = EmptyOperator(task_id='start')

    # Task 1: Download JSON from API and save to GCS
    fetch_json_from_api = PythonOperator(
        task_id='fetch_json_from_api',
        python_callable='etl.fetch_api_to_gcs.download_and_store_json',
        op_args=[RAW_DATA_PATH],
    )

    # Task 2: Load market snapshot data from JSON to BigQuery fact table
    process_snapshot_fact = PythonOperator(
        task_id='process_snapshot_fact',
        python_callable='etl.process_snapshot_data.process_and_load_snapshot',
        op_args=[RAW_DATA_PATH],
    )

    # Task 3: Load asset metadata from JSON to BigQuery asset dimension table
    process_asset_dim = PythonOperator(
        task_id='process_asset_dim',
        python_callable='etl.process_asset_data.process_and_load_assets',
        op_args=[RAW_DATA_PATH],
    )

    end = EmptyOperator(task_id='end')

    start >> fetch_json_from_api >> [process_snapshot_fact, process_asset_dim] >> end
