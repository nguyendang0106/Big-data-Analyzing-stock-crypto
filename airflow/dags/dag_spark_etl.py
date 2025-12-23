from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from datetime import datetime, timedelta

spark_secret = Secret('volume', '/opt/spark/secrets', 'gcp-key-secret', 'key.json')

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_gcs_to_bigquery_130am',
    default_args=default_args,
    schedule_interval='30 1 * * *', 
    catchup=False
) as dag:



    # run_spark = KubernetesPodOperator(
    #     task_id="spark_process_to_bq",
    #     name="spark-etl-pod",
    #     namespace="airflow",
    #     image="hoanganhbui2110/spark-bq-etl:v1",
    #     secrets=[spark_secret],
    #     env_vars={
    #         "GOOGLE_APPLICATION_CREDENTIALS": "/opt/spark/secrets/key.json",
    #         "HADOOP_USER_NAME": "root"
    #     },
    #     cmds=["/opt/spark/bin/spark-submit"],
    #     arguments=[
    #         "--master", "local[*]",
    #         "--conf", "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp -Duser.name=spark",
    #         "--conf", "spark.executor.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp -Duser.name=spark",
    #         "--jars", "/opt/spark/jars/gcs-connector-hadoop3-latest.jar,/opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar",
    #         "/opt/spark/code/write_to_big_query.py", "{{ ds }}"
    #     ],
    #     is_delete_operator_pod=False,
    #     get_logs=True
    # )

    run_spark = KubernetesPodOperator(
        task_id="spark_process_to_bq",
        name="spark-etl-pod",
        namespace="airflow",
        in_cluster=True,          
        config_file=None,
        service_account_name="airflow-scheduler",
        image="hoanganhbui2110/spark-bq-etl:v8", 
        image_pull_policy="Never", # Ép dùng bản Local vừa load
        secrets=[spark_secret],
        env_vars={
            "KUBERNETES_SERVICE_HOST": "10.96.0.1",
            "KUBERNETES_SERVICE_PORT": "443",
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/spark/secrets/key.json",
            "HADOOP_USER_NAME": "spark" # Đồng bộ với Dockerfile
        },
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "local[*]",
            # Cấu hình Java để dập tắt hoàn toàn lỗi null name
            "--conf", "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp -Duser.name=spark",
            "--conf", "spark.executor.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp -Duser.name=spark",
            "--jars", "/opt/spark/jars/gcs-connector-hadoop3-latest.jar,/opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar",
            "/opt/spark/code/write_to_big_query.py", 
            "{{ ds }}"
        ],
        is_delete_operator_pod=False,
        get_logs=True
    )

    run_spark