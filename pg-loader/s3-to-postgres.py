import pandas as pd
import boto3
import logging
import os
import io
import psycopg2
from sqlalchemy import create_engine
from urllib.parse import quote_plus

logging.basicConfig(level=logging.INFO)

#AWS configuration
S3_BUKCET ="pipeline-bucket-fakestore"
PROCESSED_PREFIX ="ecommerce/processed/"
REGION ="ap-south-1"

#PostgreSQL config
DB_CONFIG={
    'user': 'postgres',
    'password': 'password',
    'host':'localhost',
    'port':'5432',
    'database':'fakestore-ecommerce'
}

pg_pass = quote_plus(DB_CONFIG['password'])

#PostgreSQL table mapping
TABLES = {
    "customer_revenue" : f"{PROCESSED_PREFIX}customer_revenue/",
    "product_revenue" : f"{PROCESSED_PREFIX}product_revenue/"
}

def s3_parquet_df(s3_client, prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=S3_BUKCET, Prefix=prefix)

    dfs=[]
    for page in pages:
        for obj in page.get('Contents', []):
            key= obj['Key']
            if key.endswith('.parquet'):
                logging.info(f"Reading {key}")
                obj_data = s3_client.get_object(Bucket=S3_BUKCET, Key=key)['Body'].read()
                buffer = io.BytesIO(obj_data)
                df = pd.read_parquet(buffer)
                dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else None

def to_postgres(df, table_name, engine):
    if df is not None and not df.empty:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Loaded {len(df)} rows in '{table_name}'")
    else:
        logging.info(f"No data found for {table_name}")

def main():
    s3 = boto3.client('s3', region_name=REGION)
    pg_url = f"postgresql://{DB_CONFIG['user']}:{pg_pass}@" \
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(pg_url)

    for table_name, s3_prefix in TABLES.items():
        df = s3_parquet_df(s3, s3_prefix)
        to_postgres(df, f'stg_{table_name}', engine)

if __name__=='__main__':
    main()