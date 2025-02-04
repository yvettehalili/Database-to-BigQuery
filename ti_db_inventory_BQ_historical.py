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
import configparser

# Set up logging
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = f"/backup/logs/MYSQL_to_BQ_{CURRENT_DATE}.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the dumps directory
DUMPS_DIR = "/backup/dumps"
os.makedirs(DUMPS_DIR, exist_ok=True)

# Load MySQL credentials from config file
CREDENTIALS_PATH = "/backup/configs/db_credentials.conf"
mysql_config = {}

try:
    config = configparser.ConfigParser()
    config.read(CREDENTIALS_PATH)

    mysql_config["user"] = config.get("credentials", "DB_USR")
    mysql_config["password"] = config.get("credentials", "DB_PWD")
except Exception as e:
    logging.error(f"Error loading MySQL credentials: {e}")
    exit(1)

# Load table schemas from JSON file
SCHEMA_PATH = "/backup/configs/MYSQL_to_BigQuery_tables.json"
with open(SCHEMA_PATH, "r") as f:
    schema_config = json.load(f)

# MySQL database configuration
mysql_config.update({
    'host': 'localhost',
    'database': 'ti_db_inventory',
    'port': 3306
})

# Google BigQuery configuration
KEY_FILE = "/root/jsonfiles/ti-dba-prod-01.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_FILE
bq_client = bigquery.Client()
project_id = "ti-dba-prod-01"
dataset_id = "ti_db_inventory"

def create_engine_url():
    """Create SQLAlchemy engine URL safely"""
    password = quote_plus(mysql_config['password'])
    return create_engine(
        f"mysql+pymysql://{mysql_config['user']}:{password}@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
    )

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
        
        cleanup_old_files()
        
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL process for MySQL to BigQuery")
    parser.add_argument("--daily", action="store_true", help="Run daily update instead of full load")
    args = parser.parse_args()

    run_etl(is_daily=args.daily)

