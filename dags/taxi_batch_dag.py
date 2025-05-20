from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import sys
import subprocess

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'yellow_taxi_batch_pipeline',
    default_args=default_args,
    description='ETL pipeline for NYC Yellow Taxi data',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Task 1: Download Yellow Taxi data
download_taxi_data = BashOperator(
    task_id='download_taxi_data',
    bash_command='python /opt/airflow/dags/scripts/download_taxi_data.py',
    dag=dag,
)

# Task 2: Transform Yellow Taxi data using Spark
transform_taxi_data = BashOperator(
    task_id='transform_taxi_data',
    bash_command="""
    spark-submit \
        --master spark://spark-master:7077 \
        --jars /opt/airflow/dags/jars/postgresql-42.5.0.jar \
        --packages org.apache.hadoop:hadoop-aws:3.3.1 \
        /opt/airflow/dags/scripts/taxi_transform.py
    """,
    dag=dag,
)

# Define task dependencies
download_taxi_data >> transform_taxi_data