import os
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta

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

# Function to mount S3 bucket
def mount_s3_bucket():
    ssh_command = """
    ssh -i /home/ubuntu/2401FTDE-SITMTS-ec2-de-mbd-predict-key.pem ubuntu@52.17.182.138 'bash /home/ubuntu/mount_s3_bucket.sh'
    """
    os.system(ssh_command)
    return "S3 Bucket Mounted"

# Function to process data
def process_data():
    ssh_command = """
    ssh -i /home/ubuntu/2401FTDE-SITMTS-ec2-de-mbd-predict-key.pem ubuntu@52.17.182.138 'bash /home/ubuntu/process_data.sh'
    """
    os.system(ssh_command)
    return "Data Processed"

# Function to upload data to PostgreSQL (placeholder)
def upload_to_postgres():
    # Implement your logic here to upload data to PostgreSQL
    # You can use any PostgreSQL client or ORM (e.g., psycopg2, SQLAlchemy)
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
    provide_context=True,
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

