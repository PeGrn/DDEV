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
    'weather_streaming_pipeline',
    default_args=default_args,
    description='Streaming pipeline for NYC Weather data',
    schedule_interval=timedelta(hours=1),  # Run every hour to simulate streaming
    catchup=False,
)

# Task 1: Fetch Weather data (simulating streaming)
fetch_weather_data = BashOperator(
    task_id='fetch_weather_data',
    bash_command='python /opt/airflow/dags/scripts/fetch_weather_data.py',
    dag=dag,
)

# Task 2: Process Weather data using Spark (simulating streaming)
process_weather_data = BashOperator(
    task_id='process_weather_data',
    bash_command="""
    spark-submit \
        --master spark://spark-master:7077 \
        --packages org.postgresql:postgresql:42.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
        /opt/airflow/dags/scripts/weather_transform.py
    """,
    dag=dag,
)

# Define task dependencies
fetch_weather_data >> process_weather_data