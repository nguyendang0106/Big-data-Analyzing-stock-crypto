from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
load_dotenv()
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2, 
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    'crypto_cold_path_etl_1am',
    default_args=default_args,
    description='ETL (REST -> MinIO) chạy lúc 1h sáng',
    schedule_interval='0 1 * * *',  # 1h sáng
    start_date=datetime(2025, 10, 28), # Bắt đầu từ hôm nay
    catchup=False, # Chỉ chạy cho lần tiếp theo
    tags=['crypto', 'binance', 'etl', 'cold-path'],
) as dag:

    run_etl_task = DockerOperator(
        task_id='run_daily_minio_etl',
        
        image='minio-etl-job:latest', # Image đã build
        network_mode='host', # Cho phép container thấy MinIO (host.docker.internal)
        auto_remove=True,
        
        # Tiêm credentials của MinIO
        environment={
            'MINIO_USER': os.getenv("MINIO_USER"),
            'MINIO_PASSWORD': os.getenv("MINIO_PASSWORD")
        },
        
        # Chạy image với tham số là ngày hôm qua
        # {{ ds }} là macro của Airflow cho ngày "hôm qua" (YYYY-MM-DD)
        command="{{ ds }}"
    )