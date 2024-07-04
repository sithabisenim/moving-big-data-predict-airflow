from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
import boto3
import paramiko

# Constants for paths and credentials
EC2_KEY_PATH = '/home/ubuntu/s3-drive/ec2-de-mbd-predict-key-secmoh.pem'
EC2_HOST = '99.81.68.55'
EC2_USERNAME = 'ubuntu'
S3_BUCKET = '2401ft-mbd-predict-sechaba-mohlabeng-s3-source'
SNS_TOPIC_ARN = 'arn:aws:sns:eu-west-1:445492270995:2401ftde-mbd-predict-sechaba-mohlabeng-SNS'
PROCESSING_SCRIPT_PATH = '/home/ubuntu/s3-drive/Scripts/process_and_mount_data.sh'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'complete_data_pipeline',
    default_args=default_args,
    description='A complete data pipeline that processes data and moves it to RDS',
    schedule_interval=None,
    catchup=False,
)

def ssh_and_run_commands():
    """SSH to the instance and run mounting and processing scripts."""
    key = paramiko.RSAKey.from_private_key_file(EC2_KEY_PATH)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(EC2_HOST, username=EC2_USERNAME, pkey=key)

    # Mount S3 bucket
    mount_command = f'sudo s3fs {S3_BUCKET} ~/s3-drive -o iam_role=MBD_Predict_Mounting_Role -o url=https://s3.eu-west-1.amazonaws.com -o endpoint=eu-west-1 -o allow_other -o dbglevel=info -o curldbg'
    stdin, stdout, stderr = client.exec_command(mount_command)
    stdout.channel.recv_exit_status()  # Wait for command to complete

    # Process data
    stdin, stdout, stderr = client.exec_command(f'bash {PROCESSING_SCRIPT_PATH}')
    stdout.channel.recv_exit_status()  # Wait for command to complete

    client.close()

def move_data_to_rds():
    """Move processed data from S3 to RDS."""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    query = "COPY historical_stocks_data FROM '/home/ubuntu/s3-drive/Output/historical_stock_data.csv' CSV HEADER;"
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

def send_success_notification():
    """Send a success notification via SNS."""
    sns = boto3.client('sns')
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message='Pipeline executed successfully',
        Subject='Pipeline Success Notification'
    )

def send_failure_notification():
    """Send a failure notification via SNS."""
    sns = boto3.client('sns')
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message='Pipeline execution failed',
        Subject='Pipeline Failure Notification'
    )

# Task Definitions
execute_scripts_task = PythonOperator(
    task_id='execute_scripts',
    python_callable=ssh_and_run_commands,
    dag=dag,
)

move_data_task = PythonOperator(
    task_id='move_data_to_rds',
    python_callable=move_data_to_rds,
    dag=dag,
)

notify_success_task = PythonOperator(
    task_id='notify_success',
    python_callable=send_success_notification,
    trigger_rule=TriggerRule.ALL_SUCCESS,  # Only triggers if all upstream tasks succeed
    dag=dag,
)

notify_failure_task = PythonOperator(
    task_id='notify_failure',
    python_callable=send_failure_notification,
    trigger_rule=TriggerRule.ONE_FAILED,  # Triggers if any upstream task fails
    dag=dag,
)

# Task Dependencies
execute_scripts_task >> move_data_task
move_data_task >> notify_success_task
execute_scripts_task >> notify_failure_task
move_data_task >> notify_failure_task
