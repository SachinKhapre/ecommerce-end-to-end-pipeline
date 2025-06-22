from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2025,6,22),
    'retries' : 1
}

dag = DAG(
    'fakestore-pipeline',
    default_args=default_args,
    description='Fakestore E-commerce pipeline',
    schedule_interval='@daily',
    catchup=False
)

#Fakestore API -> PostgreSQL
api_ingest_task = BashOperator(
    task_id = 'ingest_api_data',
    bash_command = 'python3 /opt/airflow/pg-ingestion/api_data.py',
    dag=dag
)

#PostgreSQL -> S3
upload_to_s3_task = BashOperator(
    task_id= 'upload_postgres_to_s3',
    bash_command = 'python3 /opt/airflow/pg-loader/push-to-s3.py',
    dag=dag
)

#Run spark
spark_run_task = BashOperator(
    task_id='run_spark_job',
    bash_command="""
    docker run --rm -v $PWD:/app \
        -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
        -e AWS_SECRET_ACCESS_KEY = $AWS_SECRET_ACCESS_KEY \
        fakestore-spark ./docker/spark-submit.sh
    """,
    dag=dag
)

#S3 Parquet -> PostgreSQL
parquet_to_postgres = BashOperator(
    task_id='load_s3_to_postgres',
    bash_command='python3 /opt/airflow/pg-loader/s3-to-postgres.py',
    dag=dag
)

#Run DBT
dbt_run_task = BashOperator(
    task_id='run_dbt_models',
    bash_command='python3 /opt/airflow/dbt fakestore && dbt run',
    dag=dag
)

#Task Dependencies
api_ingest_task >> upload_to_s3_task >> spark_run_task >> parquet_to_postgres >> dbt_run_task