from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),  # Set the start date appropriately
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'data_pipeline_dag',
    default_args=default_args,
    description='A simple data pipeline DAG',
    schedule_interval=timedelta(days=1),  # Adjust the schedule as needed
)

# Task 1: Mount the S3 bucket
mount_s3_task = BashOperator(
    task_id='mount_s3_bucket',
    bash_command='/home/ubuntu/mount_s3_bucket.sh',
    dag=dag,
)

# Task 2: Process data
process_data_task = BashOperator(
    task_id='process_data',
    bash_command='/home/ubuntu/s3-drive/Scripts/process_data.sh',
    dag=dag,
)

# Task 3: Insert data into RDS
insert_data_task = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='your_postgres_connection_id',  # Replace with your actual connection ID
    sql='/home/ubuntu/s3-drive/Scripts/insert_query.sql',
    dag=dag,
)

# Define task dependencies
mount_s3_task >> process_data_task >> insert_data_task
