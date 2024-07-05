import os
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3

# AWS region
AWS_REGION = 'eu-west-1'

# Initialize Boto3 client for SNS with IAM role credentials
sns_client = boto3.client('sns', region_name=AWS_REGION)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to send SNS notification
def send_sns_notification(message, subject):
    response = sns_client.publish(
        TopicArn='arn:aws:sns:eu-west-1:445492270995:2401-mbd-predict-Sithabiseni-Mtshali-SNS',
        Message=message,
        Subject=subject
    )
    return response

# Function to mount S3 bucket using subprocess
def mount_s3_bucket():
    ssh_command = "ssh -i /home/ubuntu/2401FTDE-SITMTS-ec2-de-mbd-predict-key.pem ubuntu@52.17.182.138 'bash /home/ubuntu/mount_s3_bucket.sh'"
    result = os.system(ssh_command)
    if result != 0:
        raise RuntimeError(f"Failed to mount S3 bucket: {result}")
    return "S3 Bucket Mounted"

# Function to process data using subprocess
def process_data():
    ssh_command = "ssh -i /home/ubuntu/2401FTDE-SITMTS-ec2-de-mbd-predict-key.pem ubuntu@52.17.182.138 'bash /home/ubuntu/process_data.sh'"
    result = os.system(ssh_command)
    if result != 0:
        raise RuntimeError(f"Failed to process data: {result}")
    return "Data Processed"

# Function to upload data to PostgreSQL using psycopg2
def upload_to_postgres(**kwargs):
    # Assuming 'historical_stock_data.csv' is in the '/home/ubuntu' directory
    csv_file_path = '/home/ubuntu/historical_stock_data.csv'
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname='deft2401-sitmts-mbd-predict-rds',
        user='postgres',
        password='Buhlebenkosi06',
        host='deft2401-sitmts-mbd-predict-rds.cyg5kxo7cs9q.eu-west-1.rds.amazonaws.com',  # Replace with your RDS endpoint
        port='5432'  # Replace with your RDS port
    )
    cursor = conn.cursor()
    
    # Read SQL insertion query from file
    with open('/home/ubuntu/insert_query.sql', 'r') as f:
        insert_query = f.read()
    
    # Execute SQL insertion query
    with open(csv_file_path, 'r') as csv_file:
        cursor.copy_expert(insert_query, csv_file)
    
    # Commit transaction
    conn.commit()
    
    # Close cursor and connection
    cursor.close()
    conn.close()
    
    return "CSV Uploaded to PostgreSQL"

# Function to handle failure and send SNS notification
def failure_sns(context):
    message = f"Data pipeline failed: {context.get('exception')}"
    subject = "Data Pipeline Failure"
    send_sns_notification(message, subject)
    return "Failure SNS Sent"

# Function to handle success and send SNS notification
def success_sns(context):
    message = "Data pipeline succeeded."
    subject = "Data Pipeline Success"
    send_sns_notification(message, subject)
    return "Success SNS Sent"

# Define the DAG
dag = DAG(
    'data_pipeline_to_postgres_with_sns',
    default_args=default_args,
    description='A DAG to mount S3 bucket, process data, upload to PostgreSQL, and send SNS notifications',
    schedule_interval=None,  # Define your schedule interval here or set to None for manual triggering
    start_date=datetime(2024, 7, 6),  # Define your preferred start date
    catchup=False,  # Set to False to prevent backfilling
)

# Task to mount S3 bucket
mount_s3_task = PythonOperator(
    task_id='mount_s3_bucket',
    python_callable=mount_s3_bucket,
    dag=dag,
)

# Task to process data
process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

# Task to upload data to PostgreSQL
upload_task = PythonOperator(
    task_id='upload_to_postgres',
    python_callable=upload_to_postgres,
    provide_context=True,  # Provide context to use {{ execution_date }} and other variables
    dag=dag,
)

# Task to send success SNS notification
success_sns_task = PythonOperator(
    task_id='success_sns',
    python_callable=success_sns,
    provide_context=True,
    dag=dag,
)

# Task to send failure SNS notification
failure_sns_task = PythonOperator(
    task_id='failure_sns',
    python_callable=failure_sns,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
mount_s3_task >> process_data_task >> upload_task
upload_task >> success_sns_task
upload_task >> failure_sns_task
