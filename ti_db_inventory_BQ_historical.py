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

# Create MySQL Engine
def create_engine_url():
    """Create SQLAlchemy engine URL safely"""
    password = quote_plus(mysql_config['password'])
    return create_engine(
        f"mysql+pymysql://{mysql_config['user']}:{password}@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
    )

# Cleanup old JSON files
def cleanup_old_files():
    """Delete JSON files older than 7 days from the dumps directory"""
    try:
        now = datetime.now()
        for file_path in glob.glob(os.path.join(DUMPS_DIR, "*.json")):
            if (now - datetime.fromtimestamp(os.path.getmtime(file_path))) > timedelta(days=7):
                os.remove(file_path)
                logging.info(f"Deleted old file: {file_path}")
    except Exception as e:
        logging.error(f"Error cleaning up old files: {e}")

# Extract MySQL Data
def extract_from_mysql(table_name, is_daily=False):
    """Extract data from MySQL"""
    engine = None
    try:
        engine = create_engine_url()
        query = f"SELECT * FROM {table_name}" if not is_daily else f"SELECT * FROM {table_name} WHERE DATE(backup_date) = '{(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')}'"
        
        df = pd.read_sql(query, engine)

        # Convert datetime columns to string for JSON serialization
        for column in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[column] = df[column].astype(str)

        json_filename = f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        json_path = os.path.join(DUMPS_DIR, json_filename)
        df.to_json(json_path, orient='records')

        logging.info(f"Extracted {len(df)} rows from {table_name} and saved to {json_path}")
        return df
    except Exception as e:
        logging.error(f"Error extracting data from MySQL table {table_name}: {e}")
        raise
    finally:
        if engine:
            engine.dispose()

# Transform Data
def transform_data(df, table_name):
    """Transform data to match BigQuery schema"""
    try:
        # Convert specific columns to boolean if applicable
        if table_name == 'servers_temp':
            bool_columns = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'encrypted', 'ssl', 'backup', 'load', 'size', 'active']
            for col in bool_columns:
                if col in df.columns:
                    df[col] = df[col].astype(bool)

        # Convert date columns to proper format
        datetime_columns = {
            'backup_log': ['backup_date', 'last_update'],
            'daily_log': ['BackupDate', 'LastUpdate'],
        }

        if table_name in datetime_columns:
            for col in datetime_columns[table_name]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])

        # Rename and remove unnecessary columns for BigQuery compatibility
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

        logging.info(f"Transformed data for {table_name}")
        return df
    except Exception as e:
        logging.error(f"Error transforming data for {table_name}: {e}")
        raise

# Get BigQuery Schema from Config
def get_schema_from_config(table_name):
    """Retrieve BigQuery schema from JSON configuration"""
    if table_name not in schema_config:
        raise ValueError(f"No schema defined for {table_name}")
    
    return [bigquery.SchemaField(field["name"], field["type"]) for field in schema_config[table_name]]

# Load Data to BigQuery
def load_to_bigquery(df, table_name, is_daily=False):
    """Load data into BigQuery"""
    try:
        table_ref = f"{project_id}.{dataset_id}.{table_name}"
        
        job_config = bigquery.LoadJobConfig()
        job_config.schema = get_schema_from_config(table_name)

        # Ensure BackupDate is properly formatted as TIMESTAMP
        if 'BackupDate' in df.columns:
            df['BackupDate'] = pd.to_datetime(df['BackupDate'])

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

# Get MySQL Tables
def get_mysql_tables():
    """Retrieve list of tables from MySQL"""
    engine = None
    try:
        engine = create_engine_url()
        with engine.connect() as conn:
            result = conn.execute(text("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'"))
            return [row[0] for row in result]
    finally:
        if engine:
            engine.dispose()

# Run ETL
def run_etl(is_daily=False):
    """Main ETL process"""
    try:
        tables = get_mysql_tables()
        logging.info(f"Found MySQL tables: {tables}")

        for table_name in tables:
            logging.info(f"Processing {table_name}")
            df = extract_from_mysql(table_name, is_daily)
            if not df.empty:
                df = transform_data(df, table_name)
                load_to_bigquery(df, table_name, is_daily)
        
        cleanup_old_files()
    except Exception as e:
        logging.error(f"ETL process failed: {e}")

# Script Execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily", action="store_true", help="Run daily ETL")
    args = parser.parse_args()
    run_etl(is_daily=args.daily)

