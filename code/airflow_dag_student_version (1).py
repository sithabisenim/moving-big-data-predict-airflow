import airflow
import json
import os
import csv
import boto3
from airflow import DAG
from datetime import timedelta, datetime
from pathlib import Path

import psycopg2
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook

HOME_DIR = "/opt/airflow"

#insert your mount folder

MOUNT_DIR = f"{HOME_DIR}/s3-drive"
db_endpoint = " ft2401stesim-mbd-predict-rds.cyg5kxo7cs9q.eu-west-1.rds.amazonaws.com "
sns_topic_arn = " arn:aws:sns:eu-west-1:445492270995:2401ft-mbd-predict-steven-simelane-SNS"
user_name = "Steven_Simelane"

# ==============================================================

# The default arguments for your Airflow, these have no reason to change for the purposes of this predict.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


# The function that uploads data to the RDS database, it is called upon later.

def upload_to_postgres(**kwargs):

	# Write a function that will upload data to your Postgres Database
	conn = psycopg2.connect(
		host=db_endpoint,
		database="postgres",
		user="postgres",
		password="postgres",
		port="5432"
	)

	# Open the SQL Query to from S3 bucket
	with open(f"{MOUNT_DIR}/Scripts/insert_data.sql", "r") as q:
		query = q.read()

	cur = conn.cursor()

	# Open proccessed raw data file - historical_stock_data.csv from S3 bucket
	with open(f"{MOUNT_DIR}/Output/historical_stock_data.csv", "r") as stocks_data:
		reader = csv.reader(stocks_data)

		for numb, row in enumerate(reader):
			print(numb)
			cur.execute(query, row)
			if numb > 10000: # limits the number of insertions to 10000, any more and it puts considerable strain on your CPU and RAM.
				break

	conn.commit()
	cur.close()
	conn.close()

	return "CSV Uploaded to postgres database"

def failure_sns(context):#TODO!
	# aws_hook = AwsHook("aws_default") #TODO!
	# Write a function that will send a failure SNS notificaiton
	client = boto3.client("sns", region_name="eu-west-1")

	response = client.publish(
		TopicArn = sns_topic_arn,
		Message = f"Airflow Pipeline Failure on ARN {sns_topic_arn} for the user {user_name}",
		Subject = f"{user_name} Airflow Pipeline Failure"
	)

	print(f"Task failure on run {context['run_id']}")

	return "Failure SNS Sent"

def success_sns(context):
	# aws_hook = AwsHook("aws_default")
	# Write a function that will send a success SNS Notification
	client = boto3.client("sns", region_name = "eu-west-1")

	response = client.publish(
		TopicArn = sns_topic_arn,
		Message = f"Airflow Pipeline Success on ARN {sns_topic_arn} for user {user_name}",
		Subject = f"{user_name} Airflow Pipeline Success"
	)

	print(f"Pipeline success on run_id {context['run_id']}")

	return "Success SNS sent"

# The dag configuration ===========================================================================
# Ensure your DAG calls on the success and failure functions above as it succeeds or fails.
dag = DAG(
	dag_id = "edsa-mbd-airflow-pipeline",
	default_args=default_args,
	start_date = airflow.utils.dates.days_ago(0),
	on_failure_callback = failure_sns,
	on_success_callback = success_sns,
	schedule_interval = timedelta(days=1)
)

# Write your DAG tasks below ============================================================
# Test to print s3 directory and no output if fails meaning s3-mount not correct
path = BashOperator(
	task_id = "print-mount-dir",
	bash_command = f"ls {MOUNT_DIR}",
	dag = dag
)

# Task to run the python processing script.
process_script = BashOperator(
	task_id = "run-the-python-processing-script",
	bash_command = f"python3 {MOUNT_DIR}/Scripts/data_processing.py",
	dag=dag
)


postgres_upload = PythonOperator(
	task_id="insert_data_to_postgres",
	python_callable=upload_to_postgres,
	dag=dag
)


# Define your Task flow below ===========================================================
path >> process_script >> postgres_upload
