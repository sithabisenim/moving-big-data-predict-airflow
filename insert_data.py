import boto3
import psycopg2
import pandas as pd
from io import StringIO

# AWS S3 configuration
S3_BUCKET_NAME = '2401ft-mbd-predict-sithabiseni-mtshali-s3-source'
S3_FILE_KEY = 'Output/historical_stock_data.csv'

# PostgreSQL RDS configuration
DB_HOST = 'deft2401-sitmts-mbd-predict-rds.cyg5kxo7cs9q.eu-west-1.rds.amazonaws.com'
DB_PORT = '5432'
DB_NAME = 'deft2401-sitmts-mbd-predict-rds'
DB_USER = 'postgres'
DB_PASS = 'Buhlebenkosi06'

def download_csv_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = csv_obj['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(csv_content))

def insert_data_to_rds(df):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO historical_stocks_data (
        stock_date,
        open_value,
        high_value,
        low_value,
        close_value,
        volume_traded,
        daily_percent_change,
        value_change,
        company_name
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row['stock_date'],
            row['open_value'],
            row['high_value'],
            row['low_value'],
            row['close_value'],
            row['volume_traded'],
            row['daily_percent_change'],
            row['value_change'],
            row['company_name']
        ))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    # Download the CSV file from S3
    df = download_csv_from_s3(S3_BUCKET_NAME, S3_FILE_KEY)
    
    # Insert the data into RDS
    insert_data_to_rds(df)

    print("Data inserted successfully into RDS.")
