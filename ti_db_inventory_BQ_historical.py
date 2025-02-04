import pymysql
import pandas as pd
from google.cloud import bigquery
import configparser
import logging
from datetime import datetime, timedelta
import os
import glob
import json
import argparse

# Set up logging
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = f"/backup/logs/MYSQL_to_BQ_{CURRENT_DATE}.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the dumps directory
DUMPS_DIR = "/backup/dumps"

# Load MySQL credentials from config file
CREDENTIALS_PATH = "/backup/configs/db_credentials.conf"
config = configparser.ConfigParser()
config.read(CREDENTIALS_PATH)
DB_USR = config['credentials']['DB_USR']
DB_PWD = config['credentials']['DB_PWD']

# MySQL database configuration
mysql_config = {
    'host': 'localhost',
    'user': DB_USR,
    'password': DB_PWD,
    'database': 'ti_db_inventory',
    'port': 3306
}

# Google BigQuery configuration
KEY_FILE = "/root/jsonfiles/ti-dba-prod-01.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_FILE
bq_client = bigquery.Client()
project_id = "ti-dba-prod-01"
dataset_id = "ti_db_inventory"

def cleanup_old_files():
    """Delete JSON files older than 7 days from the dumps directory"""
    try:
        current_time = datetime.now()
        json_files = glob.glob(os.path.join(DUMPS_DIR, "*.json"))
        
        for file_path in json_files:
            file_modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if (current_time - file_modified_time) > timedelta(days=7):
                os.remove(file_path)
                logging.info(f"Deleted old file: {file_path}")
    except Exception as e:
        logging.error(f"Error during cleanup of old files: {e}")

def extract_from_mysql(table_name, is_daily=False):
    try:
        connection = pymysql.connect(**mysql_config)
        if is_daily:
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            query = f"SELECT * FROM {table_name} WHERE DATE(backup_date) = '{yesterday}'"
        else:
            query = f"SELECT * FROM {table_name}"
        
        df = pd.read_sql(query, connection)
        connection.close()
        
        os.makedirs(DUMPS_DIR, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"{table_name}_{timestamp}.json"
        json_path = os.path.join(DUMPS_DIR, json_filename)
        
        for column in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[column] = df[column].astype(str)
        
        df.to_json(json_path, orient='records', lines=True)
        
        logging.info(f"Successfully extracted data from MySQL table: {table_name}")
        logging.info(f"Saved extracted data to: {json_path}")
        
        return df
    except Exception as e:
        logging.error(f"Error extracting data from MySQL table {table_name}: {e}")
        raise

def transform_data(df, table_name):
    if table_name == 'servers_temp':
        bool_columns = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 
                        'encrypted', 'ssl', 'backup', 'load', 'size', 'active']
        for col in bool_columns:
            if col in df.columns:
                df[col] = df[col].astype(bool)
    
    datetime_columns = {
        'backup_log': ['backup_date', 'last_update'],
        'daily_log': ['backup_date', 'last_update'],
    }
    
    if table_name in datetime_columns:
        for col in datetime_columns[table_name]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
    
    if table_name == 'daily_log':
        df = df.rename(columns={
            'backup_date': 'BackupDate',
            'server': 'Server',
            'database': 'Database',
            'size': 'Size',
            'state': 'State',
            'last_update': 'LastUpdate'
        })
        df = df.drop(columns=['fileName'], errors='ignore')
    
    logging.info(f"Transformed data for table: {table_name}")
    return df

def load_to_bigquery(df, table_name, is_daily=False):
    try:
        table_ref = f"{project_id}.{dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig()
        
        if table_name == 'daily_log':
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="BackupDate"
            )
        
        if is_daily:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        
        job_config.autodetect = True
        
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        logging.info(f"Successfully loaded {len(df)} rows into BigQuery table: {table_name}")
    except Exception as e:
        logging.error(f"Error loading data into BigQuery table {table_name}: {e}")
        raise

def run_etl(is_daily=False):
    try:
        connection = pymysql.connect(**mysql_config)
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        connection.close()
        logging.info(f"Found tables in MySQL: {tables}")

        for table_name in tables:
            logging.info(f"Processing table: {table_name}")
            df = extract_from_mysql(table_name, is_daily)
            df = transform_data(df, table_name)
            load_to_bigquery(df, table_name, is_daily)
            
        cleanup_old_files()
        
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL process for MySQL to BigQuery")
    parser.add_argument("--daily", action="store_true", help="Run daily update instead of full load")
    args = parser.parse_args()

    if args.daily:
        logging.info("Starting daily ETL process")
        run_etl(is_daily=True)
        logging.info("Daily ETL process completed successfully")
    else:
        logging.info("Starting full historical ETL process")
        run_etl(is_daily=False)
        logging.info("Full historical ETL process completed successfully")
