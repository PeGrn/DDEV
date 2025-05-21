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
    'weather_batch_pipeline',
    default_args=default_args,
    description='Batch pipeline for processing all NYC Weather data at once',
    schedule_interval=None,  # Set to None for manual triggers only
    catchup=False,
)

# Task 1: Process all Weather data using Spark (batch mode)
process_weather_data = BashOperator(
    task_id='process_weather_data_batch',
    bash_command="""
    spark-submit \
        --master local[*] \
        --packages org.postgresql:postgresql:42.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
        /opt/airflow/dags/scripts/weather_batch_transform.py
    """,
    dag=dag,
)

# Task 2: Run dbt models after data processing
run_dbt_models = BashOperator(
    task_id='run_dbt_models',
    bash_command="""
    docker exec dbt dbt run || echo "dbt run failed but continuing"
    """,
    dag=dag,
)

# Define task dependencies
process_weather_data >> run_dbt_models