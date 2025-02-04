import pymysql
import pandas as pd
from google.cloud import bigquery
import json
import logging
from datetime import datetime, timedelta
import os
import glob
import argparse
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import io  # Added missing import

# Setup Logging
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = f"/backup/logs/MYSQL_to_BQ_{CURRENT_DATE}.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Directories
DUMPS_DIR = "/backup/dumps"
os.makedirs(DUMPS_DIR, exist_ok=True)

# Load MySQL credentials
CREDENTIALS_PATH = "/backup/configs/db_credentials.conf"
mysql_config = {}
with open(CREDENTIALS_PATH, "r") as f:
    creds = {k.strip(): v.strip() for k, v in (line.split("=") for line in f if "=" in line)}

mysql_config["user"] = creds.get("DB_USR", "")
mysql_config["password"] = creds.get("DB_PWD", "")
mysql_config.update({'host': 'localhost', 'database': 'ti_db_inventory', 'port': 3306})

# Load Table Schema Config
SCHEMA_PATH = "/backup/configs/MYSQL_to_BigQuery_tables.json"
with open(SCHEMA_PATH, "r") as f:
    schema_config = json.load(f)

# Setup Google BigQuery
KEY_FILE = "/root/jsonfiles/ti-dba-prod-01.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_FILE
bq_client = bigquery.Client()
project_id = "ti-dba-prod-01"
dataset_id = "ti_db_inventory"

# Function to Correct Timestamp

def fix_timestamps(df, table_name):
    """Fixes invalid timestamps in the dataframe."""
    datetime_columns = {
        'backup_log': ['backup_date', 'last_update'],
        'daily_log': ['BackupDate', 'LastUpdate']
    }
    
    if table_name in datetime_columns:
        for col in datetime_columns[table_name]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')  # Convert to datetime
                df[col] = df[col].apply(lambda x: x if pd.notnull(x) and x.year >= 1900 else None)  # Filter out invalid dates
    return df

# Load Data to BigQuery

def load_to_bigquery(df, table_name, is_daily=False):
    """Load data into BigQuery"""
    try:
        table_ref = f"{project_id}.{dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig()
        job_config.schema = get_schema_from_config(table_name)

        # Ensure BackupDate is properly formatted as TIMESTAMP
        df = fix_timestamps(df, table_name)  # Apply timestamp correction

        if table_name == 'daily_log':
            job_config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="BackupDate")

        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND if is_daily else bigquery.WriteDisposition.WRITE_TRUNCATE

        # Convert DataFrame to JSON for BigQuery upload
        json_data = df.to_json(orient="records")
        data = json.loads(json_data)
        
        # Load data into BigQuery
        job = bq_client.load_table_from_json(data, table_ref, job_config=job_config)
        job.result()  # Wait for completion

        table = bq_client.get_table(table_ref)
        logging.info(f"Loaded {len(df)} rows into {table_name} (Total: {table.num_rows} rows)")
    
    except Exception as e:
        logging.error(f"Error loading data into BigQuery table {table_name}: {e}")
        raise
