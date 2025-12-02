from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl_binance_to_gcs import run_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

# Secret GCS
# gcs_secret = Secret(
#     deploy_type='env',
#     deploy_target='GOOGLE_APPLICATION_CREDENTIALS',
#     secret='gcs-key',
#     key='key.json'
# )


with DAG(
    'crypto_cold_path_gcs_1am',
    default_args=default_args,
    description='ETL (REST -> GCS) chạy lúc 1h sáng',
    schedule='0 1 * * *',
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['crypto', 'etl'],
) as dag:
    
    run_etl_task = PythonOperator(
        task_id='run_daily_gcs_etl',
        python_callable=run_etl,
        # Truyền ngày chạy DAG vào hàm ETL của bạn
        op_kwargs={'date_to_process': "{{ ds }}"}, 
    )