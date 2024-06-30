from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.sns import AwsSnsHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import boto3
import paramiko
import os

# DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'aws_data_pipeline',
    default_args=default_args,
    description='A simple data pipeline with AWS services',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# SSH and S3 details
EC2_KEY_PATH = '/home/ubuntu/2401FTDE-SITMTS-ec2-de-mbd-predict-key.pem'
EC2_HOST = 'ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com'
EC2_USERNAME = 'ubuntu'
MOUNT_SCRIPT = '/home/ubuntu/mount_s3.sh'
PROCESSING_SCRIPT = '/mnt/s3/Scripts/run_processing.sh'
S3_BUCKET = 'your-s3-bucket-name'
SNS_TOPIC_ARN = 'arn:aws:sns:region:account-id:your-sns-topic'

# SSH to EC2 instance and run a command
def ssh_and_run_script(ssh_host, ssh_user, ssh_key_path, script_path):
    k = paramiko.RSAKey.from_private_key_file(ssh_key_path)
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(ssh_host, username=ssh_user, pkey=k)
    stdin, stdout, stderr = c.exec_command(f'bash {script_path}')
    print(stdout.read())
    print(stderr.read())
    c.close()

# Task to mount S3 bucket on EC2 instance
def mount_s3():
    ssh_and_run_script(EC2_HOST, EC2_USERNAME, EC2_KEY_PATH, MOUNT_SCRIPT)

# Task to run data processing script on EC2 instance
def run_processing_script():
    ssh_and_run_script(EC2_HOST, EC2_USERNAME, EC2_KEY_PATH, PROCESSING_SCRIPT)

# Task to move processed data from S3 to RDS
def move_data_to_rds():
    s3 = boto3.client('s3')
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Download the processed CSV file from S3
    s3.download_file(S3_BUCKET, 'Output/processed_data.csv', '/tmp/processed_data.csv')

    # Read the CSV file and insert data into RDS
    with open('/tmp/processed_data.csv', 'r') as f:
        cursor.copy_expert("COPY your_table_name FROM STDIN WITH CSV HEADER", f)
    conn.commit()
    cursor.close()
    conn.close()
    os.remove('/tmp/processed_data.csv')

# Task to send notification via SNS
def send_notification(message):
    sns_hook = AwsSnsHook(aws_conn_id='your_aws_conn_id', target_arn=SNS_TOPIC_ARN)
    sns_hook.publish_to_target(message)

# Define the tasks
mount_s3_task = PythonOperator(
    task_id='mount_s3',
    python_callable=mount_s3,
    dag=dag,
)

run_processing_task = PythonOperator(
    task_id='run_processing_script',
    python_callable=run_processing_script,
    dag=dag,
)

move_data_task = PythonOperator(
    task_id='move_data_to_rds',
    python_callable=move_data_to_rds,
    dag=dag,
)

notify_success_task = PythonOperator(
    task_id='notify_success',
    python_callable=send_notification,
    op_kwargs={'message': 'Pipeline succeeded!'},
    dag=dag,
)

notify_failure_task = PythonOperator(
    task_id='notify_failure',
    python_callable=send_notification,
    op_kwargs={'message': 'Pipeline failed!'},
    trigger_rule='one_failed',
    dag=dag,
)

# Set task dependencies
mount_s3_task >> run_processing_task >> move_data_task
move_data_task >> notify_success_task
mount_s3_task >> notify_failure_task
run_processing_task >> notify_failure_task
move_data_task >> notify_failure_task
