import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import logging
from datetime import datetime, timedelta
from config import mysql_config

def create_engine_url():
    """Create SQLAlchemy engine URL safely"""
    password = quote_plus(mysql_config['password'])
    return create_engine(
        f"mysql+pymysql://{mysql_config['user']}:{password}@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
    )

def extract_from_mysql(table_name, is_daily=False):
    """Extract data from MySQL table"""
    engine = None
    try:
        engine = create_engine_url()

        if is_daily and table_name in ['backup_log', 'daily_log']:
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            query = f"SELECT * FROM {table_name} WHERE DATE(backup_date) = '{yesterday}'"
        else:
            query = f"SELECT * FROM {table_name}"

        df = pd.read_sql(query, engine)

        # Convert datetime columns to string format
        for col in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        logging.info(f"Successfully extracted {len(df)} rows from MySQL table: {table_name}")

        return df
    except Exception as e:
        logging.error(f"Error extracting data from MySQL table {table_name}: {e}")
        raise
    finally:
        if engine:
            engine.dispose()

def get_mysql_tables():
    """Get list of MySQL tables"""
    allowed_tables = ['backup_log', 'daily_log', 'servers_temp']
    engine = None
    try:
        engine = create_engine_url()
        with engine.connect() as connection:
            result = connection.execute(text("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'"))
            tables = [row[0] for row in result if row[0] in allowed_tables]
            return tables
    finally:
        if engine:
            engine.dispose()
