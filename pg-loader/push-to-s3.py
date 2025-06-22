import boto3
import pandas as pd
import logging
import psycopg2
from io import StringIO

logging.basicConfig(level=logging.INFO)

#AWS S3 config
S3_BUCKET = 'pipeline-bucket-fakestore'
S3_PREFIX = 'ecommerce/raw/'

#postgres config
DB_CONFIG = {
    'dbname' : 'fakestore-ecommerce',
    'user' : 'postgres',
    'password' : 'password',
    'host' : 'localhost',
    'port' : '5432'
}

#tables to export
tables = ['users', 'orders', 'products', 'ordered_items']

def upload_to_s3(df, table_name, s3_client):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_name = f'{S3_PREFIX}{table_name}.csv'

    s3_client.put_object(
        Bucket = S3_BUCKET,
        Key = s3_name,
        Body = csv_buffer.getvalue()
    )
    logging.info(f"Uploaded {table_name} to s3://{S3_BUCKET}/{s3_name}")

def export_and_upload():
    conn = psycopg2.connect(**DB_CONFIG)
    s3 = boto3.client('s3')

    for table in tables:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        upload_to_s3(df, table, s3)

    conn.close()
    logging.info("Tables uploaded to s3")

if __name__=='__main__':
    export_and_upload()