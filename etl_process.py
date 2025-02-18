This script orchestrates the ETL process.

import logging
import argparse
from config import LOG_FILE
from mysql_operations import extract_from_mysql, get_mysql_tables
from transformations import transform_data
from bigquery_operations import load_to_bigquery

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily", action="store_true", help="Run daily ETL process")
    args = parser.parse_args()

    try:
        run_etl(is_daily=args.daily)
        logging.info("ETL process completed successfully")
    except Exception as e:
        logging.error(f"Script execution failed: {e}")
        raise
