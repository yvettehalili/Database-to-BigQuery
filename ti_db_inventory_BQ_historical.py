import pymysql
import pandas as pd
from google.cloud import bigquery
import json
import logging
from datetime import datetime, timedelta
import os
import glob
import argparse
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from configparser import ConfigParser

# Set up logging
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = f"/backup/logs/MYSQL_to_BQ_{CURRENT_DATE}.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the dumps directory
DUMPS_DIR = "/backup/dumps"
os.makedirs(DUMPS_DIR, exist_ok=True)

# Load MySQL credentials from config file
CREDENTIALS_PATH = "/backup/configs/db_credentials.conf"
config = ConfigParser()
config.read(CREDENTIALS_PATH)

mysql_config = {
    "user": config.get("credentials", "DB_USR"),
    "password": config.get("credentials", "DB_PWD"),
    "host": "localhost",
    "database": "ti_db_inventory",
    "port": 3306
}

# Log database user (without exposing password)
logging.info(f"Using MySQL user: {mysql_config['user']}")

# Load table schemas from JSON file
SCHEMA_PATH = "/backup/configs/MYSQL_to_BigQuery_tables.json"
with open(SCHEMA_PATH, "r") as f:
    schema_config = json.load(f)

# Google BigQuery configuration
KEY_FILE = "/root/jsonfiles/ti-dba-prod-01.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_FILE
bq_client = bigquery.Client()
project_id = "ti-dba-prod-01"
dataset_id = "ti_db_inventory"

def create_engine_url():
    """Create SQLAlchemy engine URL safely"""
    try:
        password = quote_plus(mysql_config['password'])  # Encode special characters in password
        engine_url = f"mysql+pymysql://{mysql_config['user']}:{password}@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
        return create_engine(engine_url)
    except Exception as e:
        logging.error(f"Error creating MySQL engine URL: {e}")
        raise

def get_mysql_tables():
    """Get list of MySQL tables"""
    engine = None
    try:
        engine = create_engine_url()
        logging.info("Successfully created MySQL engine.")

        with engine.connect() as connection:
            logging.info("Connected to MySQL database.")

            result = connection.execute("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
            tables = [row[0] for row in result]

            logging.info(f"Tables found in MySQL: {tables}")
            return tables
    except Exception as e:
        logging.error(f"Error fetching MySQL tables: {e}")
        raise
    finally:
        if engine:
            engine.dispose()
            logging.info("Closed MySQL connection.")

def extract_from_mysql(table_name, is_daily=False):
    """Extract data from MySQL table"""
    engine = None
    try:
        engine = create_engine_url()
        
        if is_daily:
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            query = f"SELECT * FROM {table_name} WHERE DATE(backup_date) = '{yesterday}'"
        else:
            query = f"SELECT * FROM {table_name}"
        
        df = pd.read_sql(query, engine)
        
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
    finally:
        if engine:
            engine.dispose()

def transform_data(df, table_name):
    """Transform the data according to table requirements"""
    try:
        if table_name == 'servers_temp':
            bool_columns = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 
                          'encrypted', 'ssl', 'backup', 'load', 'size', 'active']
            for col in bool_columns:
                if col in df.columns:
                    df[col] = df[col].astype(bool)
        
        datetime_columns = {
            'backup_log': ['backup_date', 'last_update'],
            'daily_log': ['BackupDate', 'LastUpdate'],
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
    except Exception as e:
        logging.error(f"Error transforming data for table {table_name}: {e}")
        raise

def get_schema_from_config(table_name):
    """Get BigQuery schema from JSON file"""
    if table_name not in schema_config:
        raise ValueError(f"No schema defined for table: {table_name}")
    
    schema = [
        bigquery.SchemaField(field["name"], field["type"])
        for field in schema_config[table_name]
    ]
    
    return schema

def load_to_bigquery(df, table_name, is_daily=False):
    """Load data into BigQuery"""
    try:
        table_ref = f"{project_id}.{dataset_id}.{table_name}"
        
        job_config = bigquery.LoadJobConfig()
        job_config.schema = get_schema_from_config(table_name)

        if table_name == 'daily_log':
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="BackupDate"
            )
        
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND if is_daily else bigquery.WriteDisposition.WRITE_TRUNCATE
        
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        
        table = bq_client.get_table(table_ref)
        logging.info(f"Successfully loaded {len(df)} rows into BigQuery table: {table_name}")
        logging.info(f"Total rows in table after load: {table.num_rows}")
        
    except Exception as e:
        logging.error(f"Error loading data into BigQuery table {table_name}: {e}")
        raise

def run_etl(is_daily=False):
    """Main ETL process"""
    try:
        tables = get_mysql_tables()
        logging.info(f"Found tables in MySQL: {tables}")

        for table_name in tables:
            logging.info(f"Processing table: {table_name}")
            df = extract_from_mysql(table_name, is_daily)
            if not df.empty:
                df = transform_data(df, table_name)
                load_to_bigquery(df, table_name, is_daily)
            else:
                logging.warning(f"No data extracted for table: {table_name}")
        
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL process for MySQL to BigQuery")
    parser.add_argument("--daily", action="store_true", help="Run daily update instead of full load")
    args = parser.parse_args()

    run_etl(is_daily=args.daily)
